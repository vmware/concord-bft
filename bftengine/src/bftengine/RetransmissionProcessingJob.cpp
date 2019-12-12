// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub--component's license, as noted in the LICENSE
// file.

#include "RetransmissionProcessingJob.hpp"
#include "assertUtils.hpp"

using namespace std;
using namespace std::chrono;

namespace bftEngine::impl {

void RetransmissionProcessingJob::release() {
  delete setOfEvents_;
  delete this;
}

void RetransmissionProcessingJob::execute() {
  if (clearPending_) logic_->clearPendingRetransmissions();

  logic_->setLastStable(lastStable_);

  if (setOfEvents_ != nullptr) {
    for (size_t i = 0; i < setOfEvents_->size(); i++) {
      RetransmissionEvent& e = setOfEvents_->at(i);

      if (e.eventType == RetransmissionEventType::SENT)
        logic_->processSend(e.time, e.replicaId, e.msgSeqNum, e.msgType, false);
      else if (e.eventType == RetransmissionEventType::ACK)
        logic_->processAck(e.time, e.replicaId, e.msgSeqNum, e.msgType);
      else if (e.eventType == RetransmissionEventType::SENT_AND_IGNORE_PREV)
        logic_->processSend(e.time, e.replicaId, e.msgSeqNum, e.msgType, true);
      else
        Assert(false);
    }
  }

  auto* suggestedRetransmissions = new std::forward_list<RetSuggestion>();
  const Time currTime = getMonotonicTime();
  logic_->getSuggestedRetransmissions(currTime, *suggestedRetransmissions);

  std::unique_ptr<InternalMessage> iMsg(new RetransmissionProcResultInternalMsg(
      replica_, lastStable_, lastView_, suggestedRetransmissions, retransmissionManager_));

  // send to main thread
  msgsCommunicator_->pushInternalMsg(std::move(iMsg));
}

}  // namespace bftEngine::impl
