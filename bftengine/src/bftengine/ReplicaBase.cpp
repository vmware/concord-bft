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
#include "MsgHandlersRegistrator.hpp"
#include "MsgsCommunicator.hpp"
#include "ReplicasInfo.hpp"

namespace bftEngine::impl {
using namespace std::chrono_literals;

ReplicaBase::ReplicaBase(const ReplicaConfig& config,
                         std::shared_ptr<IRequestsHandler> requestsHandler,
                         std::shared_ptr<MsgsCommunicator> msgComm,
                         std::shared_ptr<MsgHandlersRegistrator> msgHandlerReg,
                         concordUtil::Timers& timers)
    : config_(config),
      msgsCommunicator_(msgComm),
      msgHandlers_(msgHandlerReg),
      bftRequestsHandler_{requestsHandler},
      last_metrics_dump_time_(0),
      metrics_dump_interval_in_sec_(config_.metricsDumpIntervalSeconds),
      metrics_{concordMetrics::Component("replica", std::make_shared<concordMetrics::Aggregator>())},
      timers_{timers} {
  LOG_INFO(GL, "");
  if (config_.debugStatisticsEnabled) DebugStatistics::initDebugStatisticsData();
}

void ReplicaBase::start() {
  if (config_.debugStatisticsEnabled)
    debugStatTimer_ = timers_.add(std::chrono::seconds(DEBUG_STAT_PERIOD_SECONDS),
                                  Timers::Timer::RECURRING,
                                  [](Timers::Handle h) { DebugStatistics::onCycleCheck(); });

  metricsTimer_ = timers_.add(100ms, Timers::Timer::RECURRING, [this](Timers::Handle h) {
    metrics_.UpdateAggregator();
    auto currTime =
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch());
    if (currTime - last_metrics_dump_time_ >= metrics_dump_interval_in_sec_) {
      last_metrics_dump_time_ = currTime;
      LOG_INFO(GL, "-- ReplicaBase metrics dump--" + metrics_.ToJson());
    }
  });
  msgsCommunicator_->startCommunication(config_.replicaId);
}

void ReplicaBase::stop() {
  timers_.cancel(metricsTimer_);
  if (config_.debugStatisticsEnabled) {
    timers_.cancel(debugStatTimer_);
    DebugStatistics::freeDebugStatisticsData();
  }
  msgsCommunicator_->stopMsgsProcessing();
  msgsCommunicator_->stopCommunication();
}

bool ReplicaBase::isRunning() const { return msgsCommunicator_->isMsgsProcessingRunning(); }

void ReplicaBase::sendToAllOtherReplicas(MessageBase* m, bool includeRo) {
  MsgCode::Type type = static_cast<MsgCode::Type>(m->type());
  LOG_DEBUG(CNSUS, "Sending msg type: " << type << " to all replicas.");

  const auto& ids = repsInfo->idsOfPeerReplicas();
  std::set<bft::communication::NodeNum> replicas;
  std::transform(ids.begin(),
                 ids.end(),
                 std::inserter(replicas, replicas.begin()),
                 [](ReplicaId id) -> bft::communication::NodeNum { return id; });
  if (includeRo) {
    replicas.insert(repsInfo->idsOfPeerROReplicas().begin(), repsInfo->idsOfPeerROReplicas().end());
  }
  msgsCommunicator_->send(replicas, m->body(), m->size());
}

void ReplicaBase::sendRaw(MessageBase* m, NodeIdType dest) {
  if (config_.debugStatisticsEnabled) DebugStatistics::onSendExMessage(m->type());
  MsgCode::Type type = static_cast<MsgCode::Type>(m->type());

  LOG_DEBUG(CNSUS, "sending msg type: " << type << ", dest: " << dest);
  if (msgsCommunicator_->sendAsyncMessage(dest, m->body(), m->size())) {
    LOG_ERROR(CNSUS, "sendAsyncMessage failed: " << KVLOG(type, dest));
  }
}

}  // namespace bftEngine::impl
