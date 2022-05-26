// Concord
//
// Copyright (c) 2018-2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include <unordered_set>

#include "ReplicaBase.hpp"
#include "IStateTransfer.hpp"

namespace bftEngine::impl {

/**
 *
 */
class ReplicaForStateTransfer : public IReplicaForStateTransfer, public ReplicaBase {
 public:
  ReplicaForStateTransfer(const ReplicaConfig&,
                          std::shared_ptr<IRequestsHandler>,
                          IStateTransfer*,
                          std::shared_ptr<MsgsCommunicator>,
                          std::shared_ptr<MsgHandlersRegistrator>,
                          bool firstTime,  // TODO [TK] get rid of this
                          concordUtil::Timers& timers);

  IStateTransfer* getStateTransfer() const { return stateTransfer.get(); }

  // IReplicaForStateTransfer
  void freeStateTransferMsg(char* m) override;
  void sendStateTransferMessage(char* m, uint32_t size, uint16_t replicaId) override;
  void onTransferringComplete(uint64_t checkpointNumberOfNewState) override;
  void changeStateTransferTimerPeriod(uint32_t timerPeriodMilli) override;
  Timers::Handle addOneShotTimer(uint32_t timeoutMilli) override;
  bool isCollectingState() const { return stateTransfer->isCollectingState(); }

  void start() override;
  void stop() override;

 protected:
  virtual void onTransferringCompleteImp(uint64_t checkpointNumberOfNewState) = 0;

  template <typename T>
  void peekConsensusMessage(MessageBase* msg) {
    if (msgs_to_peek_.find(msg->type()) != msgs_to_peek_.end()) {
      stateTransfer->handleIncomingConsensusMessage(ConsensusMsg(msg->type(), msg->senderId()));
    }
  }

  template <typename T>
  void messageHandler(MessageBase* msg) {
    T* trueTypeObj = new T(msg);
    delete msg;
    if (validateMessage(trueTypeObj))
      onMessage<T>(static_cast<T*>(trueTypeObj));
    else
      delete trueTypeObj;
  }

  template <class T>
  void onMessage(T*);

 protected:
  std::unique_ptr<bftEngine::IStateTransfer> stateTransfer;
  Timers::Handle stateTranTimer_;
  concordMetrics::CounterHandle metric_received_state_transfers_;
  concordMetrics::GaugeHandle metric_state_transfer_timer_;
  bool firstTime_;
  std::shared_ptr<concord::client::reconfiguration::ClientReconfigurationEngine> cre_;
  std::unordered_set<uint16_t> msgs_to_peek_{MsgCode::PrePrepare};
};

}  // namespace bftEngine::impl
