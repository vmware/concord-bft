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

#include <memory>

#include "PrimitiveTypes.hpp"
#include "ReplicaConfig.hpp"
#include "SeqNumInfo.hpp"
#include "DebugStatistics.hpp"
#include "Metrics.hpp"
#include "Timers.hpp"

namespace bftEngine::impl {

class MsgHandlersRegistrator;
class MsgsCommunicator;
class CheckpointMsg;
class ReplicasInfo;

using concordMetrics::GaugeHandle;
using concordMetrics::StatusHandle;
using concordMetrics::CounterHandle;
using concordUtil::Timers;
using bftEngine::ReplicaConfig;

/**
 *
 */
class ReplicaBase {
  friend class MessageBase;

 public:
  ReplicaBase(const ReplicaConfig&, std::shared_ptr<MsgsCommunicator>, std::shared_ptr<MsgHandlersRegistrator>);

  virtual ~ReplicaBase() {}

  virtual bool isReadOnly() const = 0;

  std::shared_ptr<MsgsCommunicator> getMsgsCommunicator() const { return msgsCommunicator_; }
  std::shared_ptr<MsgHandlersRegistrator> getMsgHandlersRegistrator() const { return msgHandlers_; }

  void SetAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) {
    if (aggregator) {
      aggregator_ = aggregator;
      metrics_.SetAggregator(aggregator);
    }
  }

  std::shared_ptr<concordMetrics::Aggregator> getAggregator() const { return aggregator_; }

  virtual void start();
  virtual void stop();
  SeqNum getLastExecutedSequenceNum() const { return lastExecutedSeqNum; }
  virtual bool isRunning() const;

 protected:
  // Message handling
  virtual void onReportAboutInvalidMessage(MessageBase* msg, const char* reason) = 0;

  virtual void send(MessageBase* m, NodeIdType dest) { sendRaw(m, dest); }

  void sendToAllOtherReplicas(MessageBase* m, bool includeRo = false) {
    for (ReplicaId dest : repsInfo->idsOfPeerReplicas()) sendRaw(m, dest);
    if (includeRo)
      for (ReplicaId dest : repsInfo->idsOfPeerROReplicas()) sendRaw(m, dest);
  }

  void sendRaw(MessageBase* m, NodeIdType dest);

  bool validateMessage(MessageBase* msg) {
    try {
      if (config_.debugStatisticsEnabled) DebugStatistics::onReceivedExMessage(msg->type());

      msg->validate(*repsInfo);
      return true;
    } catch (std::exception& e) {
      onReportAboutInvalidMessage(msg, e.what());
      return false;
    }
  }

 protected:
  static const uint16_t ALL_OTHER_REPLICAS = UINT16_MAX;

  ReplicaConfig config_;
  ReplicasInfo* repsInfo = nullptr;
  std::shared_ptr<MsgsCommunicator> msgsCommunicator_;
  std::shared_ptr<MsgHandlersRegistrator> msgHandlers_;

  // TODO [TK] move to ReplicaImpl
  // last SeqNum executed  by this replica (or its affect was transferred to this replica)
  SeqNum lastExecutedSeqNum = 0;

  //////////////////////////////////////////////////
  // METRICS
  uint64_t last_dump_time_ = 0;
  uint64_t dump_interval_in_sec_ = 600;
  concordMetrics::Component metrics_;
  std::shared_ptr<concordMetrics::Aggregator> aggregator_;

  ///////////////////////////////////////////////////
  // Timers
  Timers::Handle debugStatTimer_;
  Timers::Handle metricsTimer_;
};

}  // namespace bftEngine::impl
