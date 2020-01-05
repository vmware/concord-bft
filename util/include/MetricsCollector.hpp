// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#pragma once

#include <string>
#include <unordered_map>
#include <functional>

namespace concordMetrics {
typedef enum : uint32_t {
  REPLICA_VIEW,
  REPLICA_LAST_STABLE_SEQ_NUM,
  REPLICA_LAST_EXECUTED_SEQ_NUM,
  REPLICA_LAST_AGREED_VIEW,
  REPLICA_FIRST_COMMIT_PATH,
  REPLICA_SLOW_PATH_COUNT,
  REPLICA_RECEIVED_INTERNAL_MSGS,
  REPLICA_RECEIVED_CLIENT_REQUEST_MSGS,
  REPLICA_RECEIVED_PREPREPARE_MSGS,
  REPLICA_RECEIVED_START_SLOW_COMMIT_MSGS,
  REPLICA_RECEIVED_PARTIAL_COMMIT_PROOF_MSGS,
  REPLICA_RECEIVED_FULL_COMMIT_PROOF_MSGS,
  REPLICA_RECEIVED_PREPARER_PARTIAL_MSGS,
  REPLICA_RECEIVED_COMMIT_PARTIAL_MSGS,
  REPLICA_RECEIVED_PREPARE_FULL_MSGS,
  REPLICA_RECEIVED_COMMIT_FULL_MSGS,
  REPLICA_RECEIVED_CHECKPOINT_MSGS,
  REPLICA_RECEIVED_STATUS_MSGS,
  REPLICA_RECEIVED_VIEW_CHANGE_MSGS,
  REPLICA_RECEIVED_NEW_VIEW_MSGS,
  REPLICA_RECEIVED_REQ_MISSING_DATA_MSGS,
  REPLICA_RECEIVED_SIMPLE_ACK_MAGS,
  REPLICA_RECEIVED_STATE_TRANSFER_MSGS
} ReplicaMetricsCode;
typedef enum : uint32_t {
  BCST_FETCHING_STATE,
  BCST_PEDANTIC_CHECKS_ENABLED,
  BCST_PREFERRED_REPLICAS,
  BCST_CURRENT_SOURCE_REPLICA,
  BCST_CHECKPOINT_BEING_FETCHED,
  BCST_LAST_STORED_CHECKPOINT,
  BCST_NUMBER_OF_RESERVED_PAGES,
  BCST_SIZE_OF_RESERVED_PAGES,
  BCST_LAST_MSG_SEQ_NUM,
  BCST_NEXT_REQUIRED_BLOCK,
  BCST_NUM_PENDING_ITEM_DATA_MSGS,
  BCST_TOTAL_SIZE_OF_PENDING_ITEM_DATA_MSGS,
  BCST_LAST_BLOCK,
  BCST_LAST_REACHABLE_BLOCK,
  BCST_SENT_ASK_FOR_CHECKPOINT_SUMMARIES_MSG,
  BCST_SENT_CHECKPOINT_SUMMARY_MSG,
  BCST_SENT_FETCH_BLOCKS_MSG,
  BCST_SENT_FETCH_RES_PAGES_MSG,
  BCST_SENT_REJECT_FETCH_MSG,
  BCST_SENT_ITEM_DATA_MSG,
  BCST_RECEIVED_ASK_FOR_CHECKPOINT_SUMMARIES_MSG,
  BCST_RECEIVED_CHECKPOINT_SUMMARY_MSG,
  BCST_RECEIVED_FETCH_BLOCKS_MSG,
  BCST_RECEIVED_FETCH_RES_PAGES_MSG,
  BCST_RECEIVED_REJECT_FETCHING_MSG,
  BCST_RECEIVED_ITEM_DATA_MSG,
  BCST_RECEIVED_ILLEGAL_MSG,
  BCST_INVALID_ASK_FOR_CHECKPOINT_SUMMARIES_MSG,
  BCST_IRRELEVANT_ASK_FOR_CHECKPOINT_SUMMARIES_MSG,
  BCST_INVALID_CHECKPOINT_SUMMARY_MSG,
  BCST_IRRELEVANT_CHECKPOINT_SUMMARY_MSG,
  BCST_INVALID_FETCH_BLOCKS_MSG,
  BCST_IRRELEVANT_FETCH_BLOCKS_MSG,
  BCST_INVALID_FETCH_RES_PAGES_MSG,
  BCST_IRRELEVANT_FETCH_RES_PAGES_MSG,
  BCST_INVALID_REJECT_FETCHING_MSG,
  BCST_IRRELEVANT_REJECT_FETCHING_MSG,
  BCST_INVALID_ITEM_DATA_MSG,
  BCST_IRRELEVANT_ITEM_DATA_MSG,
  BCST_CREATE_CHECKPOINT,
  BCST_MARK_CHECKPOINT_AS_STABLE,
  BCST_LOAD_RESERVED_PAGE,
  BCST_LOAD_RESERVED_PAGE_FROM_PENDING,
  BCST_LOAD_RESERVED_PAGE_FROM_CHECKPOINT,
  BCST_SAVE_RESERVED_PAGE,
  BCST_ZERO_RESERVED_PAGE,
  BCST_START_COLLECTION_STATE,
  BCST_ON_TIMER,
  BCST_ON_TRANSFERRING_COMPLETE,
} StateTransferMetricCode;

class CounterHandler {
 public:
  virtual ~CounterHandler(){};
  virtual void inc() = 0;
};

class GaugeHandler {
 public:
  virtual ~GaugeHandler(){};
  virtual void set(uint64_t val) = 0;
};

class StatusHandler {
 public:
  virtual ~StatusHandler(){};
  virtual void set(const std::string& val) = 0;
};

class ComponentCollector {
  std::unordered_map<uint32_t, std::vector<std::shared_ptr<CounterHandler>>> counters;
  std::unordered_map<uint32_t, std::vector<std::shared_ptr<GaugeHandler>>> gauges;
  std::unordered_map<uint32_t, std::vector<std::shared_ptr<StatusHandler>>> statuses;

 public:
  void add(uint32_t t, std::shared_ptr<CounterHandler> h) { counters[t].emplace_back(std::move(h)); }

  void add(uint32_t t, std::shared_ptr<GaugeHandler> h) { gauges[t].emplace_back(h); }

  void add(uint32_t t, std::shared_ptr<StatusHandler> h) { statuses[t].emplace_back(std::move(h)); }

  void takeMetric(uint32_t t) {
    for (auto& c : counters[t]) {
      c->inc();
    }
  }

  void takeMetric(uint32_t t, uint64_t val) {
    for (auto& g : gauges[t]) {
      g->set(val);
    }
  }

  void takeMetric(uint32_t t, const std::string& val) {
    for (auto& s : statuses[t]) {
      s->set(val);
    }
  }
};

class MetricsCollector {
  ComponentCollector replica;
  ComponentCollector stateTransfer;

 public:
  ComponentCollector& getReplicaComp() { return replica; }

  ComponentCollector& getStateTransferComp() { return stateTransfer; };

  static MetricsCollector& getInstance(uint32_t id) {
    static std::unordered_map<uint32_t, MetricsCollector> instances;
    return instances[id];
  }
};
}  // namespace concordMetrics