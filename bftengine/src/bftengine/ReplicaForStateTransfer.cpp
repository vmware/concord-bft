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
#include "ReservedPagesClient.hpp"
#include "ClientsManager.hpp"
#include "KeyStore.h"
#include "bcstatetransfer/AsyncStateTransferCRE.hpp"
#include "client/reconfiguration/poll_based_state_client.hpp"

namespace bftEngine::impl {
using namespace std::chrono_literals;

ReplicaForStateTransfer::ReplicaForStateTransfer(const ReplicaConfig &config,
                                                 std::shared_ptr<IRequestsHandler> requestsHandler,
                                                 IStateTransfer *stateTransfer,
                                                 std::shared_ptr<MsgsCommunicator> msgComm,
                                                 std::shared_ptr<MsgHandlersRegistrator> msgHandlerReg,
                                                 bool firstTime,
                                                 concordUtil::Timers &timers)
    : ReplicaBase(config, requestsHandler, msgComm, msgHandlerReg, timers),
      stateTransfer{(stateTransfer != nullptr ? stateTransfer : new NullStateTransfer())},
      metric_received_state_transfers_{metrics_.RegisterCounter("receivedStateTransferMsgs")},
      metric_state_transfer_timer_{metrics_.RegisterGauge("replicaForStateTransferTimer", 0)},
      firstTime_(firstTime) {
  bftEngine::ControlStateManager::instance().setRemoveMetadataFunc([&](bool include_st) {
    if (include_st) this->stateTransfer->setEraseMetadataFlag();
  });
  if (config_.debugStatisticsEnabled) DebugStatistics::initDebugStatisticsData();

  // Reserved Pages and State Transfer initialization
  ClientsManager::setNumResPages(
      (config.numReplicas + config.numRoReplicas + config.numOfClientProxies + config.numOfExternalClients +
       config.numReplicas + config.numOfClientServices) *
      ClientsManager::reservedPagesPerClient(config.getsizeOfReservedPage(), config.maxReplyMessageSize));
  ClusterKeyStore::setNumResPages(config.numReplicas);

  if (firstTime_ || !config_.debugPersistentStorageEnabled)
    stateTransfer->init(kWorkWindowSize / checkpointWindowSize + 1,
                        ReservedPagesClientBase::totalNumberOfPages(),
                        ReplicaConfig::instance().getsizeOfReservedPage());
  const std::chrono::milliseconds defaultTimeout = 5s;
  stateTranTimer_ = timers_.add(
      defaultTimeout, Timers::Timer::RECURRING, [stateTransfer](Timers::Handle h) { stateTransfer->onTimer(); });
  metric_state_transfer_timer_.Get().Set(defaultTimeout.count());
}

void ReplicaForStateTransfer::start() {
  cre_ = bftEngine::bcst::asyncCRE::CreFactory::create(msgsCommunicator_, msgHandlers_);
  stateTransfer->setReconfigurationEngine(cre_);
  stateTransfer->addOnTransferringCompleteCallback(
      [this](std::uint64_t) {
        // TODO - The next lines up to comment 'YYY' do not belong here (CRE) - consider refactor or move outside
        if (!config_.isReadOnly) {
          // At this point, we, if are not going to have another blocks in state transfer. So, we can safely stop CRE.
          // if there is a reconfiguration state change that prevents us from starting another state transfer (i.e.
          // scaling) then CRE probably won't work as well.
          // 1. First, make sure we handled the most recent available updates.
          concord::client::reconfiguration::PollBasedStateClient *pbc =
              (concord::client::reconfiguration::PollBasedStateClient *)(cre_->getStateClient());
          bool succ = false;
          while (!succ) {
            auto latestHandledUpdate = cre_->getLatestKnownUpdateBlock();
            auto latestReconfUpdates = pbc->getStateUpdate(succ);
            if (!succ) {
              LOG_ERROR(GL, "unable to get the latest reconfiguration updates");
            }
            for (const auto &update : latestReconfUpdates) {
              if (update.blockid > latestHandledUpdate) {
                succ = false;
                break;
              }  // else if (!isGettingBlocks)
              LOG_INFO(GL, "halting cre");
              // 2. Now we can safely halt cre. We know for sure that there are no update in the state transffered
              // blocks that haven't been handled yet
              cre_->halt();
            }
          }  // while (!succ) {
        }
      },
      IStateTransfer::StateTransferCallBacksPriorities::HIGH);
  stateTransfer->startRunning(this);
  ReplicaBase::start();  // msg communicator should be last in the starting chain
}

void ReplicaForStateTransfer::stop() {
  // stop in reverse order
  cre_->stop();
  ReplicaBase::stop();
  stateTransfer->stopRunning();
  timers_.cancel(stateTranTimer_);
}

template <>
void ReplicaForStateTransfer::onMessage(StateTransferMsg *m) {
  metric_received_state_transfers_++;
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

void ReplicaForStateTransfer::onTransferringComplete(uint64_t checkpointNumberOfNewState) {
  // TODO(GG): if this method is invoked by an external thread, then send an "internal message" to the commands
  // processing thread
  onTransferringCompleteImp(checkpointNumberOfNewState);
}

void ReplicaForStateTransfer::changeStateTransferTimerPeriod(uint32_t timerPeriodMilli) {
  // TODO(GG): if this method is invoked by an external thread, then send an "internal message" to the commands
  // processing thread
  LOG_INFO(GL, "Changing stateTranTimer_ timeout to " << KVLOG(timerPeriodMilli));
  timers_.reset(stateTranTimer_, std::chrono::milliseconds(timerPeriodMilli));
  metric_state_transfer_timer_.Get().Set(timerPeriodMilli);
}

Timers::Handle ReplicaForStateTransfer::addOneShotTimer(uint32_t timeoutMilli) {
  return timers_.add(std::chrono::milliseconds(timeoutMilli),
                     concordUtil::Timers::Timer::ONESHOT,
                     [this](concordUtil::Timers::Handle h) { stateTransfer->onTimer(); });
}

}  // namespace bftEngine::impl
