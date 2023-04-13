// UTT
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "sigProcessor.hpp"
#include "common.hpp"

#include "bftengine/ReplicaConfig.hpp"
#include "bftengine/messages/MessageBase.hpp"
#include "bftengine/messages/ClientRequestMsg.hpp"
#include "bftengine/Replica.hpp"
#include <algorithm>

namespace utt {
namespace messages {
class PartialSigMsg : public bftEngine::impl::MessageBase {
 public:
  PartialSigMsg(uint16_t srcReplicaId, const std::vector<uint8_t>& partialSig, uint64_t sig_id)
      : bftEngine::impl::MessageBase(
            srcReplicaId, bftEngine::impl::MsgCode::Reserved, 0, sizeof(Header) + (uint32_t)partialSig.size()) {
    b()->partialSigLength = (uint16_t)partialSig.size();
    b()->sig_id = sig_id;
    b()->genReplicaId = srcReplicaId;
    char* position = body() + sizeof(Header);
    memcpy(position, partialSig.data(), partialSig.size());
  }

  BFTENGINE_GEN_CONSTRUCT_FROM_BASE_MESSAGE(PartialSigMsg)

  std::vector<uint8_t> getPartialSig() const {
    char* position = body() + sizeof(Header);
    std::vector<uint8_t> ret(b()->partialSigLength);
    memcpy(ret.data(), position, b()->partialSigLength);
    return ret;
  }
  uint16_t idOfGeneratedReplica() const { return b()->genReplicaId; }

  uint16_t partialSignatureLen() const { return b()->partialSigLength; }

  uint64_t getsig_id() const { return b()->sig_id; }

  void validate(const ReplicasInfo&) const override {}

  static uint32_t sizeOfHeader() { return sizeof(Header); }

 private:
#pragma pack(push, 1)

  struct Header : public bftEngine::impl::MessageBase::Header {
    uint16_t genReplicaId;
    uint64_t sig_id;
    uint16_t partialSigLength;
  };
#pragma pack(pop)
  static_assert(sizeof(Header) == (6 + 2 + 8 + 2), "Header is 18B");

  Header* b() const { return (Header*)msgBody_; }
};
}  // namespace messages
const SigProcessor::GenerateAppClientRequestCb SigProcessor::default_client_app_request_generator =
    [](uint64_t sig_id, const std::vector<uint8_t>& sig) {
      std::vector<uint8_t> ret(sizeof(uint64_t) + sig.size(), 0);
      std::memcpy(ret.data(), &sig_id, sizeof(uint64_t));
      std::memcpy(ret.data() + sizeof(uint64_t), sig.data(), sig.size());
      return ret;
    };
SigProcessor::SigProcessor(uint16_t repId,
                           uint16_t n,
                           uint16_t threshold,
                           uint64_t timer_handler_timeout,
                           std::shared_ptr<bftEngine::impl::MsgsCommunicator> msgs_communicator,
                           std::shared_ptr<bftEngine::impl::MsgHandlersRegistrator> msg_handlers,
                           concordUtil::Timers& timers)
    : msgs_communicator_{msgs_communicator},
      timers_{timers},
      repId_{repId},
      n_{n},
      threshold_{threshold},
      timer_handler_timeout_{timer_handler_timeout} {
  msg_handlers->registerMsgHandler(
      bftEngine::impl::MsgCode::Reserved, [&](std::unique_ptr<bftEngine::impl::MessageBase> message) {
        std::unique_ptr<messages::PartialSigMsg> msg(reinterpret_cast<messages::PartialSigMsg*>(message.release()));
        onReceivingNewPartialSig(msg->getsig_id(), msg->idOfGeneratedReplica(), msg->getPartialSig());
      });
  timeout_handler_ = timers_.add(std::chrono::milliseconds(timer_handler_timeout_),
                                 concordUtil::Timers::Timer::RECURRING,
                                 [&](concordUtil::Timers::Handle h) {
                                   (void)h;
                                   uint64_t now =
                                       (uint64_t)(std::chrono::steady_clock::now().time_since_epoch().count());
                                   // In order not to rotate over all the jobs, we will rotate the ordered timeouts_job_
                                   // map only up till "now"
                                   std::unique_lock lck(jobs_lock_);
                                   for (const auto& [timestamp, data] : timeouts_map_) {
                                     if (timestamp > now) break;
                                     for (const auto& [sid, sig] : data) onJobTimeout(sid, sig);
                                   }
                                 });
}

// Called from the signing thread
void SigProcessor::processSignature(uint64_t sig_id,
                                    const std::vector<uint8_t>& sig,
                                    const validationCb& vcb,
                                    uint64_t job_timeout_ms,
                                    const GenerateAppClientRequestCb& client_app_generator_cb_) {
  uint64_t job_timeout = std::chrono::steady_clock::now().time_since_epoch().count() + job_timeout_ms;
  SigJobEntry* entry{nullptr};
  {
    std::unique_lock lck(jobs_lock_);
    timeouts_map_[job_timeout][sig_id] = sig;
    entry = &(jobs_[sig_id]);
  }
  // Acquire the entry lock to prevent rc between this thread (signing thread) and the message processor thread
  {
    SigJobEntry::guard guard{*entry};
    // remove all already collected invalid partial signatures
    for (auto iter = entry->partial_sigs.begin(); iter != entry->partial_sigs.end();) {
      if (!vcb(iter->first, iter->second)) {
        iter = entry->partial_sigs.erase(iter);
      } else {
        ++iter;
      }
    }
    entry->job_id = sig_id;
    entry->job_timeout_ms = job_timeout;
    entry->vcb = vcb;
    entry->client_app_data_generator_cb_ = client_app_generator_cb_;
#ifdef DEBUG
    if (entry->partial_sigs.find((uint32_t)repId_) != entry->partial_sigs.end()) {
      throw std::runtime_error{"replica has already added this signature"};
    }
#endif
    // if we already have enough valid partial signatures, lets publish the complete signature and return
    if (entry->partial_sigs.size() == threshold_) {
      publishCompleteSignature(*entry);
      return;
    }
  }
  onReceivingNewPartialSig(sig_id, repId_, sig);
  // Now, send the new partial signature to the signature coordinator
  auto job_coordinator = get_job_coordinator_cb(sig_id);
  if (job_coordinator == repId_) return;
  messages::PartialSigMsg msg(repId_, sig, sig_id);
  msgs_communicator_->sendAsyncMessage((uint64_t)job_coordinator, msg.body(), msg.size());
}

SigProcessor::~SigProcessor() { timers_.cancel(timeout_handler_); }

// Called either, the message processor thread or the sining thread
void SigProcessor::onReceivingNewPartialSig(uint64_t sig_id,
                                            uint16_t sig_source,
                                            const std::vector<uint8_t>& partial_sig) {
  SigJobEntry* entry{nullptr};
  {
    std::unique_lock lck(jobs_lock_);
    entry = &jobs_[sig_id];
  }
  {
    SigJobEntry::guard guard{*entry};
    if (entry->partial_sigs.find(sig_source) != entry->partial_sigs.end()) return;
    if (entry->partial_sigs.size() == threshold_) return;
    if (sig_source != repId_ && entry->vcb != nullptr) {
      if (!entry->vcb(sig_source, partial_sig)) {
        if (sig_source == repId_) LOG_WARN(getLogger(), "got invalid signature " << KVLOG(sig_source, sig_id));
        return;
      }
    }
    entry->partial_sigs[sig_source] = partial_sig;
    // We don't care doing this part under the entry lock, because if we did reach the threshold, we won't use this
    // entry anymore
    if (entry->client_app_data_generator_cb_ != nullptr && entry->partial_sigs.size() == threshold_) {
      publishCompleteSignature(*entry);
    }
  }
}

void SigProcessor::publishCompleteSignature(const SigJobEntry& job_entry) {
  auto sig_id = job_entry.job_id;
  auto fsig = libutt::api::Utils::aggregateSigShares(n_, job_entry.partial_sigs);
  CompleteSignatureMsg msg(n_, job_entry.partial_sigs, fsig);
  auto requestSeqNum =
      std::chrono::duration_cast<std::chrono::microseconds>(getMonotonicTime().time_since_epoch()).count();
  std::vector<uint8_t> appClientReq = job_entry.client_app_data_generator_cb_(sig_id, msg.serialize());
  auto crm = std::make_unique<bftEngine::impl::ClientRequestMsg>(repId_,
                                                                 bftEngine::MsgFlag::INTERNAL_FLAG,
                                                                 requestSeqNum,
                                                                 (uint32_t)appClientReq.size(),
                                                                 (const char*)appClientReq.data(),
                                                                 60000,
                                                                 "new-utt-sig-" + std::to_string(sig_id));
  msgs_communicator_->getIncomingMsgsStorage()->pushExternalMsg(std::move(crm));
}
// Called by the validating thread
void SigProcessor::onReceivingNewValidFullSig(uint64_t sig_id) {
  // Erase the relevant data from the in-memory data structures
  {
    std::unique_lock lck(jobs_lock_);
    auto& entry = jobs_[sig_id];
    uint64_t job_timeout{0};
    uint64_t sig_id{0};
    {
      SigJobEntry::guard guard{entry};
      job_timeout = entry.job_timeout_ms;
      sig_id = entry.job_id;
    }
    timeouts_map_[job_timeout].erase(sig_id);
    if (timeouts_map_[job_timeout].size() == 0) timeouts_map_.erase(job_timeout);
    jobs_.erase(sig_id);
  }
}

// Called by the timer handler (which is the message processor thread)
void SigProcessor::onJobTimeout(uint64_t job_id, const std::vector<uint8_t>& sig) {
  SigJobEntry* entry{nullptr};
  {
    // std::unique_lock lck(jobs_lock_);
    if (jobs_.find(job_id) != jobs_.end()) {
      entry = &jobs_[job_id];
      if (entry->client_app_data_generator_cb_ != nullptr && entry->partial_sigs.size() == threshold_) {
        publishCompleteSignature(*entry);
        return;
      }
    }
  }
  for (uint16_t rid = 0; rid < n_; rid++) {
    if (rid == repId_) continue;
    messages::PartialSigMsg msg(repId_, sig, job_id);
    msgs_communicator_->sendAsyncMessage((uint64_t)rid, msg.body(), msg.size());
  }
}

SigProcessor::CompleteSignatureMsg::CompleteSignatureMsg(uint32_t n,
                                                         const std::map<uint32_t, std::vector<uint8_t>>& psigs,
                                                         const std::vector<uint8_t>& fsig)
    : sigs{psigs}, full_sig{fsig}, num_replicas{n} {}
bool SigProcessor::CompleteSignatureMsg::validate() const {
  auto complete_sig = libutt::api::Utils::aggregateSigShares(num_replicas, sigs);
  return full_sig == complete_sig;
}
SigProcessor::CompleteSignatureMsg::CompleteSignatureMsg(const std::vector<uint8_t>& buffer) { deserialize(buffer); }
const std::vector<uint8_t>& SigProcessor::CompleteSignatureMsg::getFullSig() const { return full_sig; }
const std::map<uint32_t, std::vector<uint8_t>>& SigProcessor::CompleteSignatureMsg::getPartialSigs() const {
  return sigs;
}
std::vector<uint8_t> SigProcessor::CompleteSignatureMsg::serialize() const {
  uint64_t loc{0};
  uint64_t fsig_size = full_sig.size();
  uint64_t psigs_size = sigs.size();
  size_t msg_size = sizeof(num_replicas) + sizeof(fsig_size) + full_sig.size();
  msg_size += sizeof(psigs_size);
  for (const auto& [k, v] : sigs) msg_size += (sizeof(k) + sizeof(uint64_t) + v.size());
  std::vector<uint8_t> ret(msg_size);
  std::memcpy(ret.data(), &num_replicas, sizeof(num_replicas));
  loc += sizeof(num_replicas);
  std::memcpy(ret.data() + loc, &fsig_size, sizeof(fsig_size));
  loc += sizeof(fsig_size);
  std::memcpy(ret.data() + loc, full_sig.data(), full_sig.size());
  loc += full_sig.size();
  std::memcpy(ret.data() + loc, &psigs_size, sizeof(psigs_size));
  loc += sizeof(psigs_size);
  for (const auto& [k, v] : sigs) {
    std::memcpy(ret.data() + loc, &k, sizeof(k));
    loc += sizeof(k);
    uint64_t ssize = v.size();
    std::memcpy(ret.data() + loc, &ssize, sizeof(ssize));
    loc += sizeof(ssize);
    std::memcpy(ret.data() + loc, v.data(), v.size());
    loc += v.size();
  }
  return ret;
}

SigProcessor::CompleteSignatureMsg& SigProcessor::CompleteSignatureMsg::deserialize(
    const std::vector<uint8_t>& buffer) {
  size_t loc{0};
  std::memcpy(&num_replicas, buffer.data(), sizeof(num_replicas));
  loc += sizeof(num_replicas);
  uint64_t fsig_size{0};
  std::memcpy(&fsig_size, buffer.data() + loc, sizeof(fsig_size));
  loc += sizeof(fsig_size);
  full_sig.resize(fsig_size);
  std::memcpy(full_sig.data(), buffer.data() + loc, fsig_size);
  loc += fsig_size;
  uint64_t psigs_nums{0};
  std::memcpy(&psigs_nums, buffer.data() + loc, sizeof(psigs_nums));
  loc += sizeof(psigs_nums);
  for (size_t i = 0; i < psigs_nums; i++) {
    uint32_t rep_id{0};
    std::memcpy(&rep_id, buffer.data() + loc, sizeof(rep_id));
    loc += sizeof(rep_id);
    uint64_t s_size{0};
    std::memcpy(&s_size, buffer.data() + loc, sizeof(s_size));
    loc += sizeof(s_size);
    sigs[rep_id] = std::vector<uint8_t>(s_size);
    std::memcpy(sigs[rep_id].data(), buffer.data() + loc, s_size);
    loc += s_size;
  }
  return *this;
}
}  // namespace utt