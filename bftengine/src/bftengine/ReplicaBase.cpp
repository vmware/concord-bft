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

ReplicaBase::ReplicaBase(const ReplicaConfig& config,
                         std::shared_ptr<MsgsCommunicator> msgComm,
                         std::shared_ptr<MsgHandlersRegistrator> msgHandlerReg)
    : config_(config),
      msgsCommunicator_(msgComm),
      msgHandlers_(msgHandlerReg),
      dump_interval_in_sec_(config_.metricsDumpIntervalSeconds),
      metrics_{concordMetrics::Component("replica", std::make_shared<concordMetrics::Aggregator>())} {
  if (config_.debugStatisticsEnabled) DebugStatistics::initDebugStatisticsData();
}

void ReplicaBase::start() {
  if (config_.debugStatisticsEnabled)
    debugStatTimer_ = TimersSingleton::getInstance().add(std::chrono::seconds(DEBUG_STAT_PERIOD_SECONDS),
                                                         Timers::Timer::RECURRING,
                                                         [](Timers::Handle h) { DebugStatistics::onCycleCheck(); });

  metricsTimer_ = TimersSingleton::getInstance().add(100ms, Timers::Timer::RECURRING, [this](Timers::Handle h) {
    metrics_.UpdateAggregator();
    auto timeSinceEpoch = std::chrono::steady_clock::now().time_since_epoch();
    uint64_t currTime = std::chrono::duration_cast<std::chrono::milliseconds>(timeSinceEpoch).count();
    if (currTime - last_dump_time_ >= dump_interval_in_sec_ * 1000) {
      last_dump_time_ = currTime;
      LOG_INFO(GL, "-- ReplicaBase metrics dump--\n" + metrics_.ToJson());
    }
  });
  msgsCommunicator_->startCommunication(config_.replicaId);
  msgsCommunicator_->startMsgsProcessing(config_.replicaId);
}

void ReplicaBase::stop() {
  TimersSingleton::getInstance().cancel(metricsTimer_);
  if (config_.debugStatisticsEnabled) {
    TimersSingleton::getInstance().cancel(debugStatTimer_);
    DebugStatistics::freeDebugStatisticsData();
  }
  msgsCommunicator_->stopMsgsProcessing();
  msgsCommunicator_->stopCommunication();
}

bool ReplicaBase::isRunning() const { return msgsCommunicator_->isMsgsProcessingRunning(); }

void ReplicaBase::sendRaw(MessageBase* m, NodeIdType dest) {
  if (config_.debugStatisticsEnabled) DebugStatistics::onSendExMessage(m->type());

  LOG_TRACE(GL, "sending msg type: " << m->type() << " dest: " << dest);
  if (msgsCommunicator_->sendAsyncMessage(dest, m->body(), m->size()))
    LOG_ERROR(GL, "sendAsyncMessage failed for message type: " << m->type());
}

}  // namespace bftEngine::impl
