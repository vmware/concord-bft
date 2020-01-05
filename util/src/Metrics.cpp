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

#include "Metrics.hpp"
#include <stdexcept>
#include <sstream>

using namespace std;
namespace concordMetrics {

const char* const kGaugeName = "gauge";
const char* const kStatusName = "status";
const char* const kCounterName = "counter";

template <typename T>
T& FindValue(const char* const val_type, const string& val_name, const vector<string>& names, vector<T>& values) {
  for (size_t i = 0; i < names.size(); i++) {
    if (names[i] == val_name) {
      return values[i];
    }
  }
  ostringstream oss;
  oss << "Invalid " << val_type << " name: " << val_name;
  throw invalid_argument(oss.str());
}

Component::Handle<Gauge> Component::RegisterGauge(const string& name, const uint64_t val) {
  names_.gauge_names_.emplace_back(name);
  values_.gauges_.emplace_back(Gauge(val));
  return Component::Handle<Gauge>(values_.gauges_, values_.gauges_.size() - 1);
}

Component::Handle<Status> Component::RegisterStatus(const string& name, const string& val) {
  names_.status_names_.emplace_back(name);
  values_.statuses_.emplace_back(Status(val));
  return Component::Handle<Status>(values_.statuses_, values_.statuses_.size() - 1);
}

Component::Handle<Counter> Component::RegisterCounter(const string& name, const uint64_t val) {
  names_.counter_names_.emplace_back(name);
  values_.counters_.emplace_back(Counter(val));
  return Component::Handle<Counter>(values_.counters_, values_.counters_.size() - 1);
}

void Aggregator::RegisterComponent(Component& component) {
  std::lock_guard<std::mutex> lock(lock_);
  components_.insert(make_pair(component.Name(), component));
}

// Throws if the component doesn't exist.
// This is only called from the component itself so it will never actually
// throw.
void Aggregator::UpdateValues(const string& name, Values&& values) {
  std::lock_guard<std::mutex> lock(lock_);
  components_.at(name).SetValues(std::move(values));
}

Gauge Aggregator::GetGauge(const string& component_name, const string& val_name) {
  std::lock_guard<std::mutex> lock(lock_);
  auto& component = components_.at(component_name);
  return FindValue(kGaugeName, val_name, component.names_.gauge_names_, component.values_.gauges_);
}

Status Aggregator::GetStatus(const string& component_name, const string& val_name) {
  std::lock_guard<std::mutex> lock(lock_);
  auto& component = components_.at(component_name);
  return FindValue(kStatusName, val_name, component.names_.status_names_, component.values_.statuses_);
}

Counter Aggregator::GetCounter(const string& component_name, const string& val_name) {
  std::lock_guard<std::mutex> lock(lock_);
  auto& component = components_.at(component_name);
  return FindValue(kCounterName, val_name, component.names_.counter_names_, component.values_.counters_);
}

// Generate a JSON string of all aggregated components. To save space we don't
// add any newline characters.
std::string Aggregator::ToJson() {
  ostringstream oss;
  std::lock_guard<std::mutex> lock(lock_);

  // Add the object opening
  oss << "{\"Components\":[";

  // Add all the components
  for (auto it = components_.begin(); it != components_.end(); ++it) {
    // Add a comma between every component
    if (it != components_.begin()) {
      oss << ",";
    }
    oss << it->second.ToJson();
  }

  // Add the object end
  oss << "]}";

  return oss.str();
}

// Generate a JSON string of the component. To save space we don't add any
// newline characters.
std::string Component::ToJson() {
  ostringstream oss;

  // Add the object opening and component name
  oss << "{\"Name\":\"" << name_ << "\",";

  // Add any gauges
  oss << "\"Gauges\":{";

  for (size_t i = 0; i < names_.gauge_names_.size(); i++) {
    if (i != 0) {
      oss << ",";
    }
    oss << "\"" << names_.gauge_names_[i] << "\":" << values_.gauges_[i].Get() << "";
  }

  // End gauges
  oss << "},";

  // Add any status
  oss << "\"Statuses\":{";

  for (size_t i = 0; i < names_.status_names_.size(); i++) {
    if (i != 0) {
      oss << ",";
    }
    oss << "\"" << names_.status_names_[i] << "\":"
        << "\"" << values_.statuses_[i].Get() << "\"";
  }

  // End status
  oss << "},";

  // Add any counters
  oss << "\"Counters\":{";

  for (size_t i = 0; i < names_.counter_names_.size(); i++) {
    if (i != 0) {
      oss << ",";
    }
    oss << "\"" << names_.counter_names_[i] << "\":" << values_.counters_[i].Get() << "";
  }

  // End counters
  oss << "}";

  // End component
  oss << "}";

  return oss.str();
}

void ConcordBftMetricsComp::createReplicaComponent(Component& component, ComponentCollector& collector) {
  collector.add(ReplicaMetricsCode::REPLICA_VIEW, std::make_shared<Gauge_>(component.RegisterGauge("view", 0)));

  collector.add(ReplicaMetricsCode::REPLICA_LAST_STABLE_SEQ_NUM,
                std::make_shared<Gauge_>(component.RegisterGauge("lastStableSeqNum", 0)));
  collector.add(ReplicaMetricsCode::REPLICA_LAST_EXECUTED_SEQ_NUM,
                std::make_shared<Gauge_>(component.RegisterGauge("lastExecutedSeqNum", 0)));
  collector.add(ReplicaMetricsCode::REPLICA_LAST_AGREED_VIEW,
                std::make_shared<Gauge_>(component.RegisterGauge("lastAgreedView", 0)));

  collector.add(ReplicaMetricsCode::REPLICA_FIRST_COMMIT_PATH,
                std::make_shared<Status_>(component.RegisterStatus("firstCommitPath", "")));

  collector.add(ReplicaMetricsCode::REPLICA_SLOW_PATH_COUNT,
                std::make_shared<Counter_>(component.RegisterCounter("slowPathCount")));
  collector.add(ReplicaMetricsCode::REPLICA_RECEIVED_INTERNAL_MSGS,
                std::make_shared<Counter_>(component.RegisterCounter("receivedInternalMsgs")));
  collector.add(ReplicaMetricsCode::REPLICA_RECEIVED_CLIENT_REQUEST_MSGS,
                std::make_shared<Counter_>(component.RegisterCounter("receivedClientRequestMsgs")));
  collector.add(ReplicaMetricsCode::REPLICA_RECEIVED_PREPREPARE_MSGS,
                std::make_shared<Counter_>(component.RegisterCounter("receivedPrePrepareMsgs")));
  collector.add(ReplicaMetricsCode::REPLICA_RECEIVED_START_SLOW_COMMIT_MSGS,
                std::make_shared<Counter_>(component.RegisterCounter("receivedStartSlowCommitMsgs")));
  collector.add(ReplicaMetricsCode::REPLICA_RECEIVED_PARTIAL_COMMIT_PROOF_MSGS,
                std::make_shared<Counter_>(component.RegisterCounter("receivedPartialCommitProofMsgs")));
  collector.add(ReplicaMetricsCode::REPLICA_RECEIVED_FULL_COMMIT_PROOF_MSGS,
                std::make_shared<Counter_>(component.RegisterCounter("receivedFullCommitProofMsgs")));
  collector.add(ReplicaMetricsCode::REPLICA_RECEIVED_PREPARER_PARTIAL_MSGS,
                std::make_shared<Counter_>(component.RegisterCounter("receivedPreparePartialMsgs")));
  collector.add(ReplicaMetricsCode::REPLICA_RECEIVED_COMMIT_PARTIAL_MSGS,
                std::make_shared<Counter_>(component.RegisterCounter("receivedCommitPartialMsgs")));
  collector.add(ReplicaMetricsCode::REPLICA_RECEIVED_PREPARE_FULL_MSGS,
                std::make_shared<Counter_>(component.RegisterCounter("receivedPrepareFullMsgs")));
  collector.add(ReplicaMetricsCode::REPLICA_RECEIVED_COMMIT_FULL_MSGS,
                std::make_shared<Counter_>(component.RegisterCounter("receivedCommitFullMsgs")));
  collector.add(ReplicaMetricsCode::REPLICA_RECEIVED_CHECKPOINT_MSGS,
                std::make_shared<Counter_>(component.RegisterCounter("receivedCheckpointMsgs")));
  collector.add(ReplicaMetricsCode::REPLICA_RECEIVED_STATUS_MSGS,
                std::make_shared<Counter_>(component.RegisterCounter("receivedReplicaStatusMsgs")));
  collector.add(ReplicaMetricsCode::REPLICA_RECEIVED_VIEW_CHANGE_MSGS,
                std::make_shared<Counter_>(component.RegisterCounter("receivedViewChangeMsgs")));
  collector.add(ReplicaMetricsCode::REPLICA_RECEIVED_NEW_VIEW_MSGS,
                std::make_shared<Counter_>(component.RegisterCounter("receivedNewViewMsgs")));
  collector.add(ReplicaMetricsCode::REPLICA_RECEIVED_REQ_MISSING_DATA_MSGS,
                std::make_shared<Counter_>(component.RegisterCounter("receivedReqMissingDataMsgs")));
  collector.add(ReplicaMetricsCode::REPLICA_RECEIVED_SIMPLE_ACK_MAGS,
                std::make_shared<Counter_>(component.RegisterCounter("receivedSimpleAckMsgs")));
  collector.add(ReplicaMetricsCode::REPLICA_RECEIVED_STATE_TRANSFER_MSGS,
                std::make_shared<Counter_>(component.RegisterCounter("receivedStateTransferMsgs")));
  component.Register();
  component.UpdateAggregator();
}

void ConcordBftMetricsComp::createStateTransferComponent(Component& component, ComponentCollector& collector) {
  collector.add(StateTransferMetricCode::BCST_FETCHING_STATE,
                std::make_shared<Status_>(component.RegisterStatus("fetching_state", "")));
  collector.add(StateTransferMetricCode::BCST_PEDANTIC_CHECKS_ENABLED,
                std::make_shared<Status_>(component.RegisterStatus("pedantic_checks_enabled", "")));
  collector.add(StateTransferMetricCode::BCST_PREFERRED_REPLICAS,
                std::make_shared<Status_>(component.RegisterStatus("preferred_replicas", "")));

  collector.add(StateTransferMetricCode::BCST_CURRENT_SOURCE_REPLICA,
                std::make_shared<Gauge_>(component.RegisterGauge("current_source_replica", 0)));
  collector.add(StateTransferMetricCode::BCST_CHECKPOINT_BEING_FETCHED,
                std::make_shared<Gauge_>(component.RegisterGauge("checkpoint_being_fetched", 0)));
  collector.add(StateTransferMetricCode::BCST_LAST_STORED_CHECKPOINT,
                std::make_shared<Gauge_>(component.RegisterGauge("last_stored_checkpoint", 0)));
  collector.add(StateTransferMetricCode::BCST_NUMBER_OF_RESERVED_PAGES,
                std::make_shared<Gauge_>(component.RegisterGauge("number_of_reserved_pages", 0)));
  collector.add(StateTransferMetricCode::BCST_SIZE_OF_RESERVED_PAGES,
                std::make_shared<Gauge_>(component.RegisterGauge("size_of_reserved_page", 0)));
  collector.add(StateTransferMetricCode::BCST_LAST_MSG_SEQ_NUM,
                std::make_shared<Gauge_>(component.RegisterGauge("last_msg_seq_num", 0)));
  collector.add(StateTransferMetricCode::BCST_NEXT_REQUIRED_BLOCK,
                std::make_shared<Gauge_>(component.RegisterGauge("next_required_block_", 0)));
  collector.add(StateTransferMetricCode::BCST_NUM_PENDING_ITEM_DATA_MSGS,
                std::make_shared<Gauge_>(component.RegisterGauge("num_pending_item_data_msgs_", 0)));
  collector.add(StateTransferMetricCode::BCST_TOTAL_SIZE_OF_PENDING_ITEM_DATA_MSGS,
                std::make_shared<Gauge_>(component.RegisterGauge("total_size_of_pending_item_data_msgs", 0)));
  collector.add(StateTransferMetricCode::BCST_LAST_BLOCK,
                std::make_shared<Gauge_>(component.RegisterGauge("last_block_", 0)));
  collector.add(StateTransferMetricCode::BCST_LAST_REACHABLE_BLOCK,
                std::make_shared<Gauge_>(component.RegisterGauge("last_reachable_block", 0)));

  collector.add(StateTransferMetricCode::BCST_SENT_ASK_FOR_CHECKPOINT_SUMMARIES_MSG,
                std::make_shared<Counter_>(component.RegisterCounter("sent_ask_for_checkpoint_summaries_msg")));
  collector.add(StateTransferMetricCode::BCST_SENT_CHECKPOINT_SUMMARY_MSG,
                std::make_shared<Counter_>(component.RegisterCounter("sent_checkpoint_summary_msg")));
  collector.add(StateTransferMetricCode::BCST_SENT_FETCH_BLOCKS_MSG,
                std::make_shared<Counter_>(component.RegisterCounter("sent_fetch_blocks_msg")));
  collector.add(StateTransferMetricCode::BCST_SENT_FETCH_RES_PAGES_MSG,
                std::make_shared<Counter_>(component.RegisterCounter("sent_fetch_res_pages_msg")));
  collector.add(StateTransferMetricCode::BCST_SENT_REJECT_FETCH_MSG,
                std::make_shared<Counter_>(component.RegisterCounter("sent_reject_fetch_msg")));
  collector.add(StateTransferMetricCode::BCST_SENT_ITEM_DATA_MSG,
                std::make_shared<Counter_>(component.RegisterCounter("sent_item_data_msg")));

  collector.add(StateTransferMetricCode::BCST_RECEIVED_ASK_FOR_CHECKPOINT_SUMMARIES_MSG,
                std::make_shared<Counter_>(component.RegisterCounter("received_ask_for_checkpoint_summaries_msg")));
  collector.add(StateTransferMetricCode::BCST_RECEIVED_CHECKPOINT_SUMMARY_MSG,
                std::make_shared<Counter_>(component.RegisterCounter("received_checkpoint_summary_msg")));
  collector.add(StateTransferMetricCode::BCST_RECEIVED_FETCH_BLOCKS_MSG,
                std::make_shared<Counter_>(component.RegisterCounter("received_fetch_blocks_msg")));
  collector.add(StateTransferMetricCode::BCST_RECEIVED_FETCH_RES_PAGES_MSG,
                std::make_shared<Counter_>(component.RegisterCounter("received_fetch_res_pages_msg")));
  collector.add(StateTransferMetricCode::BCST_RECEIVED_REJECT_FETCHING_MSG,
                std::make_shared<Counter_>(component.RegisterCounter("received_reject_fetching_msg")));
  collector.add(StateTransferMetricCode::BCST_RECEIVED_ITEM_DATA_MSG,
                std::make_shared<Counter_>(component.RegisterCounter("received_item_data_msg")));
  collector.add(StateTransferMetricCode::BCST_RECEIVED_ILLEGAL_MSG,
                std::make_shared<Counter_>(component.RegisterCounter("received_illegal_msg_")));

  collector.add(StateTransferMetricCode::BCST_INVALID_ASK_FOR_CHECKPOINT_SUMMARIES_MSG,
                std::make_shared<Counter_>(component.RegisterCounter("invalid_ask_for_checkpoint_summaries_msg")));
  collector.add(StateTransferMetricCode::BCST_IRRELEVANT_ASK_FOR_CHECKPOINT_SUMMARIES_MSG,
                std::make_shared<Counter_>(component.RegisterCounter("irrelevant_ask_for_checkpoint_summaries_msg")));
  collector.add(StateTransferMetricCode::BCST_INVALID_CHECKPOINT_SUMMARY_MSG,
                std::make_shared<Counter_>(component.RegisterCounter("invalid_checkpoint_summary_msg")));
  collector.add(StateTransferMetricCode::BCST_IRRELEVANT_CHECKPOINT_SUMMARY_MSG,
                std::make_shared<Counter_>(component.RegisterCounter("irrelevant_checkpoint_summary_msg")));
  collector.add(StateTransferMetricCode::BCST_INVALID_FETCH_BLOCKS_MSG,
                std::make_shared<Counter_>(component.RegisterCounter("invalid_fetch_blocks_msg")));
  collector.add(StateTransferMetricCode::BCST_IRRELEVANT_FETCH_BLOCKS_MSG,
                std::make_shared<Counter_>(component.RegisterCounter("irrelevant_fetch_blocks_msg")));
  collector.add(StateTransferMetricCode::BCST_INVALID_FETCH_RES_PAGES_MSG,
                std::make_shared<Counter_>(component.RegisterCounter("invalid_fetch_res_pages_msg")));
  collector.add(StateTransferMetricCode::BCST_IRRELEVANT_FETCH_RES_PAGES_MSG,
                std::make_shared<Counter_>(component.RegisterCounter("irrelevant_fetch_res_pages_msg")));
  collector.add(StateTransferMetricCode::BCST_INVALID_REJECT_FETCHING_MSG,
                std::make_shared<Counter_>(component.RegisterCounter("invalid_reject_fetching_msg")));
  collector.add(StateTransferMetricCode::BCST_IRRELEVANT_REJECT_FETCHING_MSG,
                std::make_shared<Counter_>(component.RegisterCounter("irrelevant_reject_fetching_msg")));
  collector.add(StateTransferMetricCode::BCST_INVALID_ITEM_DATA_MSG,
                std::make_shared<Counter_>(component.RegisterCounter("invalid_item_data_msg")));
  collector.add(StateTransferMetricCode::BCST_IRRELEVANT_ITEM_DATA_MSG,
                std::make_shared<Counter_>(component.RegisterCounter("irrelevant_item_data_msg")));

  collector.add(StateTransferMetricCode::BCST_CREATE_CHECKPOINT,
                std::make_shared<Counter_>(component.RegisterCounter("create_checkpoint")));
  collector.add(StateTransferMetricCode::BCST_MARK_CHECKPOINT_AS_STABLE,
                std::make_shared<Counter_>(component.RegisterCounter("mark_checkpoint_as_stable")));
  collector.add(StateTransferMetricCode::BCST_LOAD_RESERVED_PAGE,
                std::make_shared<Counter_>(component.RegisterCounter("load_reserved_page")));
  collector.add(StateTransferMetricCode::BCST_LOAD_RESERVED_PAGE_FROM_PENDING,
                std::make_shared<Counter_>(component.RegisterCounter("load_reserved_page_from_pending")));
  collector.add(StateTransferMetricCode::BCST_LOAD_RESERVED_PAGE_FROM_CHECKPOINT,
                std::make_shared<Counter_>(component.RegisterCounter("load_reserved_page_from_checkpoint")));
  collector.add(StateTransferMetricCode::BCST_SAVE_RESERVED_PAGE,
                std::make_shared<Counter_>(component.RegisterCounter("save_reserved_page")));
  collector.add(StateTransferMetricCode::BCST_ZERO_RESERVED_PAGE,
                std::make_shared<Counter_>(component.RegisterCounter("zero_reserved_page")));
  collector.add(StateTransferMetricCode::BCST_START_COLLECTION_STATE,
                std::make_shared<Counter_>(component.RegisterCounter("start_collecting_state")));
  collector.add(StateTransferMetricCode::BCST_ON_TIMER,
                std::make_shared<Counter_>(component.RegisterCounter("on_timer")));
  collector.add(StateTransferMetricCode::BCST_ON_TRANSFERRING_COMPLETE,
                std::make_shared<Counter_>(component.RegisterCounter("on_transferring_complete")));
  component.Register();
  component.UpdateAggregator();
}
}  // namespace concordMetrics
