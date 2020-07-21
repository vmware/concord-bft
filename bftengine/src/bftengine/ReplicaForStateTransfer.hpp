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

#include "ReplicaBase.hpp"
#include "IStateTransfer.hpp"

namespace bftEngine::impl {

/**
 *
 */
class ReplicaForStateTransfer : public IReplicaForStateTransfer, public ReplicaBase {
 public:
  ReplicaForStateTransfer(const ReplicaConfig&,
                          IStateTransfer*,
                          std::shared_ptr<MsgsCommunicator>,
                          std::shared_ptr<MsgHandlersRegistrator>,
                          bool firstTime,  // TODO [TK] get rid of this
                          concordUtil::Timers& timers);

  IStateTransfer* getStateTransfer() const { return stateTransfer.get(); }

  // IReplicaForStateTransfer
  void freeStateTransferMsg(char* m) override;
  void sendStateTransferMessage(char* m, uint32_t size, uint16_t replicaId) override;
  void onTransferringComplete(int64_t checkpointNumberOfNewState) override;
  void changeStateTransferTimerPeriod(uint32_t timerPeriodMilli) override;

  bool isCollectingState() const { return stateTransfer->isCollectingState(); }

  void start() override;
  void stop() override;

 protected:
  virtual void onTransferringCompleteImp(int64_t checkpointNumberOfNewState) = 0;

  template <typename T>
  void messageHandler(MessageBase* msg) {
    if (validateMessage(msg))
      onMessage<T>(static_cast<T*>(msg));
    else
      delete msg;
  }

  template <class T>
  void onMessage(T*);

 protected:
  std::unique_ptr<bftEngine::IStateTransfer> stateTransfer;
  Timers::Handle stateTranTimer_;
  CounterHandle metric_received_state_transfers_;
  GaugeHandle metric_state_transfer_timer_;
  bool firstTime_;
};

}  // namespace bftEngine::impl
