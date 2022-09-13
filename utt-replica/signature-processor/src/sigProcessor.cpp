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
                                   const auto& entries = jobs_.getEntries();
                                   for (const auto& [id, entry] : entries) {
                                     (void)id;
                                     if (entry.start_time == 0) continue;
                                     auto now = std::chrono::steady_clock::now().time_since_epoch().count();
                                     if (now - entry.start_time >= entry.job_timeout_ms) {
                                       onJobTimeout(entry);
                                     }
                                   }
                                 });
}

void SigProcessor::processSignature(uint64_t sig_id,
                                    const std::vector<uint8_t>& sig,
                                    const validationCb& vcb,
                                    uint64_t job_timeout_ms) {
  jobs_.registerEntry(sig_id, vcb, job_timeout_ms);
  const auto& jobEntry = jobs_.get(sig_id);
  auto& validator = jobs_.getJobValidator(sig_id);
  std::vector<uint16_t> invalids;
  for (auto& [id, psig] : jobEntry.partial_sigs) {
    if (!validator(id, psig)) invalids.push_back((uint16_t)id);
  }
  for (auto id : invalids) jobs_.removePartialSig(sig_id, (uint32_t)id);

  jobs_.addPartialSig(sig_id, (uint32_t)repId_, sig);
  auto job_coordinator = get_job_coordinator_cb(sig_id);
  if (job_coordinator == repId_) return;
  messages::PartialSigMsg msg(repId_, sig, sig_id);
  msgs_communicator_->sendAsyncMessage((uint64_t)job_coordinator, msg.body(), msg.size());
}

SigProcessor::~SigProcessor() { timers_.cancel(timeout_handler_); }
void SigProcessor::onReceivingNewPartialSig(uint64_t sig_id,
                                            uint16_t sig_source,
                                            const std::vector<uint8_t>& partial_sig) {
  if (jobs_.hasPartialSig(sig_id, sig_source)) return;
  if (jobs_.getNumOfPartialSigs(sig_id) == threshold_) return;
  if (jobs_.isRegistered(sig_id)) {
    auto& validator = jobs_.getJobValidator(sig_id);
    if (!validator(sig_source, partial_sig)) return;
  }
  jobs_.addPartialSig(sig_id, (uint32_t)sig_source, partial_sig);
  const SigJobEntry& entry = jobs_.get(sig_id);
  if (entry.partial_sigs.size() == threshold_) {
    auto sig = libutt::api::Utils::aggregateSigShares(n_, entry.partial_sigs);
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
void SigProcessor::onReceivingNewValidFullSig(uint64_t sig_id) { jobs_.erase(sig_id); }
void SigProcessor::onJobTimeout(const SigJobEntry& entry) {
  if (entry.partial_sigs.find(repId_) == entry.partial_sigs.end()) return;
  for (uint16_t rid = 0; rid < n_; rid++) {
    if (rid == repId_) continue;
    messages::PartialSigMsg* msg = new messages::PartialSigMsg(repId_, entry.partial_sigs.at(repId_), entry.job_id);
    msgs_communicator_->sendAsyncMessage((uint64_t)rid, msg->body(), msg->size());
  }
}
}  // namespace utt