// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include <stdint.h>

#include "InternalReplicaApi.hpp"
#include "MsgsCommunicator.hpp"
#include "RetransmissionParams.hpp"
#include "messages/RetransmissionProcResultInternalMsg.hpp"

namespace util {
class SimpleThreadPool;
}

namespace bftEngine::impl {

class RetransmissionsManager {
  friend RetransmissionProcResultInternalMsg;

 public:
  RetransmissionsManager();  // retransmissions logic is disabled

  RetransmissionsManager(InternalReplicaApi* replica,
                         util::SimpleThreadPool* threadPool,
                         std::shared_ptr<MsgsCommunicator>& msgsCommunicator,
                         uint16_t maxOutNumOfSeqNumbers,
                         SeqNum lastStableSeqNum);

  ~RetransmissionsManager();

  void onSend(uint16_t replicaId, SeqNum msgSeqNum, uint16_t msgType, bool ignorePreviousAcks = false);

  void onAck(uint16_t replicaId, SeqNum msgSeqNum, uint16_t msgType);

  bool tryToStartProcessing();

  void setLastStable(SeqNum newLastStableSeqNum);

  void setLastView(ViewNum newView);

 private:
  void onProcessingComplete();

 protected:
  void add(const RetransmissionEvent& e);

  InternalReplicaApi* const replica_;
  util::SimpleThreadPool* const threadPool_;  // TODO(GG): not needed (use InternalReplicaApi*)
  std::shared_ptr<MsgsCommunicator> msgsCommunicator_;
  const uint16_t maxOutSeqNumbers_;
  void* const internalLogicInfo_ = nullptr;

  SeqNum lastStable_ = 0;
  SeqNum lastView_ = 0;
  bool backgroundProcessing_ = false;
  std::vector<RetransmissionEvent>* setOfEvents_ = nullptr;
  bool needToClearPendingRetransmissions_ = false;
};

}  // namespace bftEngine::impl
