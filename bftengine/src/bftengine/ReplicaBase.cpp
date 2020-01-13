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

#include <chrono>
#include "ReplicaBase.hpp"
#include "assertUtils.hpp"
#include "Logger.hpp"
#include "TimersSingleton.hpp"
#include "MsgHandlersRegistrator.hpp"
#include "MsgsCommunicator.hpp"
#include "ReplicasInfo.hpp"


namespace bftEngine::impl {
using namespace std::chrono_literals;

ReplicaBase::ReplicaBase( const ReplicaConfig& config,
                          std::shared_ptr<MsgsCommunicator> msgComm,
                          std::shared_ptr<MsgHandlersRegistrator> msgHandlerReg):
            config_(config),
            numOfReplicas{(uint16_t)(3 * config_.fVal + 2 * config_.cVal + 1)}, //TODO [TK] include ro-replicas
            msgsCommunicator_(msgComm),
            msgHandlers_(msgHandlerReg),
            metrics_{concordMetrics::Component("replica", std::make_shared<concordMetrics::Aggregator>())}{

  if (config_.debugStatisticsEnabled)
    DebugStatistics::initDebugStatisticsData();
}

void ReplicaBase::start() {
  if (config_.debugStatisticsEnabled)
    debugStatTimer_ = TimersSingleton::getInstance().add(std::chrono::seconds(DEBUG_STAT_PERIOD_SECONDS),
                                                         Timers::Timer::RECURRING,
                                                         [this](Timers::Handle h){DebugStatistics::onCycleCheck();});

  metricsTimer_ = TimersSingleton::getInstance().add(100ms,
                                                     Timers::Timer::RECURRING,
                                                     [this](Timers::Handle h){metrics_.UpdateAggregator();});
  msgsCommunicator_->startCommunication(config_.replicaId);
  msgsCommunicator_->startMsgsProcessing(config_.replicaId);
}

void ReplicaBase::stop() {
  TimersSingleton::getInstance().cancel(metricsTimer_);
  if (config_.debugStatisticsEnabled){
    TimersSingleton::getInstance().cancel(debugStatTimer_);
    DebugStatistics::freeDebugStatisticsData();
  }
  msgsCommunicator_->stopMsgsProcessing();
  msgsCommunicator_->stopCommunication();

}

bool ReplicaBase::isRunning() const {
  return msgsCommunicator_->isMsgsProcessingRunning();
}

void ReplicaBase::sendRaw(MessageBase* m, NodeIdType dest) {
  if (config_.debugStatisticsEnabled)
    DebugStatistics::onSendExMessage(m->type());

  if (msgsCommunicator_->sendAsyncMessage(dest, m->body(), m->size()))
    LOG_ERROR(GL, "sendAsyncMessage failed for message type: " <<  m->type());
}

}
