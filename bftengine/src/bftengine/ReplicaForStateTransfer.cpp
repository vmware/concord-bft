// Concord
//
// Copyright (c) 2018, 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "ReplicaForStateTransfer.hpp"
#include "Timers.hpp"
#include "assertUtils.hpp"
#include "Logger.hpp"
#include "NullStateTransfer.hpp"
#include "MsgHandlersRegistrator.hpp"
#include "MsgsCommunicator.hpp"
#include "ReplicasInfo.hpp"
#include "messages/StateTransferMsg.hpp"
#include "ReservedPages.hpp"

namespace bftEngine::impl {
using namespace std::chrono_literals;

ReplicaForStateTransfer::ReplicaForStateTransfer(const ReplicaConfig &config,
                                                 IStateTransfer *stateTransfer,
                                                 std::shared_ptr<MsgsCommunicator> msgComm,
                                                 std::shared_ptr<MsgHandlersRegistrator> msgHandlerReg,
                                                 bool firstTime,
                                                 concordUtil::Timers &timers)
    : ReplicaBase(config, msgComm, msgHandlerReg, timers),
      stateTransfer{(stateTransfer != nullptr ? stateTransfer : new NullStateTransfer())},
      metric_received_state_transfers_{metrics_.RegisterCounter("receivedStateTransferMsgs")},
      metric_state_transfer_timer_{metrics_.RegisterGauge("replicaForStateTransferTimer", 0)},
      firstTime_(firstTime) {
  msgHandlers_->registerMsgHandler(
      MsgCode::StateTransfer,
      std::bind(&ReplicaForStateTransfer::messageHandler<StateTransferMsg>, this, std::placeholders::_1));
  if (config_.debugStatisticsEnabled) DebugStatistics::initDebugStatisticsData();
}

void ReplicaForStateTransfer::start() {
  if (firstTime_ || !config_.debugPersistentStorageEnabled)
    stateTransfer->init(kWorkWindowSize / checkpointWindowSize + 1,
                        ReservedPages::totalNumberOfPages(),
                        ReplicaConfig::instance().getsizeOfReservedPage());
  const std::chrono::milliseconds defaultTimeout = 5s;
  stateTranTimer_ =
      timers_.add(defaultTimeout, Timers::Timer::RECURRING, [this](Timers::Handle h) { stateTransfer->onTimer(); });
  metric_state_transfer_timer_.Get().Set(defaultTimeout.count());
  stateTransfer->startRunning(this);
  ReplicaBase::start();  // msg communicator should be last in the starting chain
}

void ReplicaForStateTransfer::stop() {
  // stop in reverse order
  ReplicaBase::stop();
  stateTransfer->stopRunning();
  timers_.cancel(stateTranTimer_);
}

template <>
void ReplicaForStateTransfer::onMessage(StateTransferMsg *m) {
  metric_received_state_transfers_.Get().Inc();
  size_t h = sizeof(MessageBase::Header);
  stateTransfer->handleStateTransferMessage(m->body() + h, m->size() - h, m->senderId());
  m->releaseOwnership();
  delete m;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// IStateTransfer
//
void ReplicaForStateTransfer::freeStateTransferMsg(char *m) {
  // This method may be called by external threads
  char *p = (m - sizeof(MessageBase::Header));
  std::free(p);
}

void ReplicaForStateTransfer::sendStateTransferMessage(char *m, uint32_t size, uint16_t replicaId) {
  // TODO(GG): if this method is invoked by an external thread, then send an "internal message" to the commands
  // processing thread

  MessageBase *p = new MessageBase(config_.replicaId, MsgCode::StateTransfer, size + sizeof(MessageBase::Header));
  char *x = p->body() + sizeof(MessageBase::Header);
  memcpy(x, m, size);
  send(p, replicaId);
  delete p;
}

void ReplicaForStateTransfer::onTransferringComplete(int64_t checkpointNumberOfNewState) {
  // TODO(GG): if this method is invoked by an external thread, then send an "internal message" to the commands
  // processing thread
  onTransferringCompleteImp(checkpointNumberOfNewState * checkpointWindowSize);
}

void ReplicaForStateTransfer::changeStateTransferTimerPeriod(uint32_t timerPeriodMilli) {
  // TODO(GG): if this method is invoked by an external thread, then send an "internal message" to the commands
  // processing thread
  timers_.reset(stateTranTimer_, std::chrono::milliseconds(timerPeriodMilli));
  metric_state_transfer_timer_.Get().Set(timerPeriodMilli);
}

}  // namespace bftEngine::impl
