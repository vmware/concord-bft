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
  msg_handlers->registerMsgHandler(bftEngine::impl::MsgCode::Reserved, [&](bftEngine::impl::MessageBase* message) {
    messages::PartialSigMsg* msg = (messages::PartialSigMsg*)message;
    onReceivingNewPartialSig(msg->getsig_id(), msg->idOfGeneratedReplica(), msg->getPartialSig());
    delete msg;
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
                                    uint64_t job_timeout_ms) {
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
#ifdef DEBUG
    if (entry->partial_sigs.find((uint32_t)repId_) != entry->partial_sigs.end()) {
      throw std::runtime_error{"replica has already added this signature"};
    }
#endif
  }
  onReceivingNewPartialSig(sig_id, repId_, sig);
  // Now, send the new partial signature to the signature coordinator
  auto job_coordinator = get_job_coordinator_cb(sig_id);
  if (job_coordinator == repId_) return;
  messages::PartialSigMsg msg(repId_, sig, sig_id);
  msgs_communicator_->sendAsyncMessage((uint64_t)job_coordinator, msg.body(), msg.size());
}

SigProcessor::~SigProcessor() { timers_.cancel(timeout_handler_); }

// Can be called by either the signing thread or the message processor thread.
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
    if (entry->partial_sigs.size() == threshold_) {
      auto sig = libutt::api::Utils::aggregateSigShares(n_, entry->partial_sigs);
      auto requestSeqNum =
          std::chrono::duration_cast<std::chrono::microseconds>(getMonotonicTime().time_since_epoch()).count();
      std::vector<uint8_t> appClientReq = generate_app_client_request_cb_(sig_id, sig);
      auto crm = new bftEngine::impl::ClientRequestMsg(repId_,
                                                       0x0,
                                                       requestSeqNum,
                                                       (uint32_t)appClientReq.size(),
                                                       (const char*)appClientReq.data(),
                                                       60000,
                                                       "new-utt-sig-" + std::to_string(sig_id));
      msgs_communicator_->getIncomingMsgsStorage()->pushExternalMsg(std::unique_ptr<MessageBase>(crm));
    }
  }
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
  for (uint16_t rid = 0; rid < n_; rid++) {
    if (rid == repId_) continue;
    messages::PartialSigMsg* msg = new messages::PartialSigMsg(repId_, sig, job_id);
    msgs_communicator_->sendAsyncMessage((uint64_t)rid, msg->body(), msg->size());
  }
}
}  // namespace utt