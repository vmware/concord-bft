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

Gauge& Aggregator::GetGauge(const string& component_name, const string& val_name) {
  std::lock_guard<std::mutex> lock(lock_);
  auto& component = components_.at(component_name);
  return FindValue(kGaugeName, val_name, component.names_.gauge_names_, component.values_.gauges_);
}

Status& Aggregator::GetStatus(const string& component_name, const string& val_name) {
  std::lock_guard<std::mutex> lock(lock_);
  auto& component = components_.at(component_name);
  return FindValue(kStatusName, val_name, component.names_.status_names_, component.values_.statuses_);
}

Counter& Aggregator::GetCounter(const string& component_name, const string& val_name) {
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

std::vector<std::string> ConcordbftMetricsCollector::resolveMetricPath(const std::string& metricPath) {
  std::stringstream path(metricPath);
  std::string segment;
  std::vector<std::string> seglist;
  while (std::getline(path, segment, '/')) {
    seglist.push_back(segment);
  }
  return seglist;
}

ConcordbftMetricsCollector::ConcordbftMetricsCollector() {}

void ConcordbftMetricsCollector::increment(const std::string& counterPath) {
  auto paths = resolveMetricPath(counterPath);
  int repID = std::stoi(paths[pathOrder::REP]);
  addAggregatorForNewRegisteredReplica(repID);
  aggregators_.at(repID)->GetCounter(paths[pathOrder::COMP], paths[pathOrder::NAME]).Inc();
}

void ConcordbftMetricsCollector::set(const std::string& gaugePath, uint64_t val) {
  auto paths = resolveMetricPath(gaugePath);
  int repID = std::stoi(paths[pathOrder::REP]);
  addAggregatorForNewRegisteredReplica(repID);
  aggregators_.at(repID)->GetGauge(paths[pathOrder::COMP], paths[pathOrder::NAME]).Set(val);
}

void ConcordbftMetricsCollector::update(const std::string& statusPath, const std::string& val) {
  auto paths = resolveMetricPath(statusPath);
  int repID = std::stoi(paths[pathOrder::REP]);
  addAggregatorForNewRegisteredReplica(repID);
  aggregators_.at(repID)->GetStatus(paths[pathOrder::COMP], paths[pathOrder::NAME]).Set(val);
}

void ConcordbftMetricsCollector::addAggregatorForNewRegisteredReplica(int repID) {
  std::lock_guard<std::mutex> lock(lock_);
  if (aggregators_.find(repID) != aggregators_.end()) return;
  aggregators_[repID] = std::make_shared<Aggregator>();
  Component replicaMetrics_("replica", aggregators_[repID]);
  replicaMetrics_.RegisterGauge("view", 0);
  replicaMetrics_.RegisterGauge("lastStableSeqNum", 0);
  replicaMetrics_.RegisterGauge("lastExecutedSeqNum", 0);
  replicaMetrics_.RegisterGauge("lastAgreedView", 0);
  replicaMetrics_.RegisterStatus("firstCommitPath", "");
  replicaMetrics_.RegisterCounter("slowPathCount");
  replicaMetrics_.RegisterCounter("receivedInternalMsgs");
  replicaMetrics_.RegisterCounter("receivedClientRequestMsgs");
  replicaMetrics_.RegisterCounter("receivedPrePrepareMsgs");
  replicaMetrics_.RegisterCounter("receivedStartSlowCommitMsgs");
  replicaMetrics_.RegisterCounter("receivedPartialCommitProofMsgs");
  replicaMetrics_.RegisterCounter("receivedFullCommitProofMsgs");
  replicaMetrics_.RegisterCounter("receivedPreparePartialMsgs");
  replicaMetrics_.RegisterCounter("receivedCommitPartialMsgs");
  replicaMetrics_.RegisterCounter("receivedPrepareFullMsgs");
  replicaMetrics_.RegisterCounter("receivedCommitFullMsgs");
  replicaMetrics_.RegisterCounter("receivedCheckpointMsgs");
  replicaMetrics_.RegisterCounter("receivedReplicaStatusMsgs");
  replicaMetrics_.RegisterCounter("receivedViewChangeMsgs");
  replicaMetrics_.RegisterCounter("receivedNewViewMsgs");
  replicaMetrics_.RegisterCounter("receivedReqMissingDataMsgs");
  replicaMetrics_.RegisterCounter("receivedSimpleAckMsgs");
  replicaMetrics_.RegisterCounter("receivedStateTransferMsgs");
  replicaMetrics_.Register();

  Component stateTransferMetrics_("bc_state_transfer", aggregators_[repID]);
  stateTransferMetrics_.RegisterStatus("fetching_state", "");
  stateTransferMetrics_.RegisterStatus("pedantic_checks_enabled", "");
  stateTransferMetrics_.RegisterStatus("preferred_replicas", "");
  stateTransferMetrics_.RegisterGauge("current_source_replica", 0);
  stateTransferMetrics_.RegisterGauge("checkpoint_being_fetched", 0);
  stateTransferMetrics_.RegisterGauge("last_stored_checkpoint", 0);
  stateTransferMetrics_.RegisterGauge("number_of_reserved_pages", 0);
  stateTransferMetrics_.RegisterGauge("size_of_reserved_page", 0);
  stateTransferMetrics_.RegisterGauge("last_msg_seq_num", 0);
  stateTransferMetrics_.RegisterGauge("next_required_block_", 0);
  stateTransferMetrics_.RegisterGauge("num_pending_item_data_msgs_", 0);
  stateTransferMetrics_.RegisterGauge("total_size_of_pending_item_data_msgs", 0);
  stateTransferMetrics_.RegisterGauge("last_block_", 0);
  stateTransferMetrics_.RegisterGauge("last_reachable_block", 0);

  stateTransferMetrics_.RegisterCounter("sent_ask_for_checkpoint_summaries_msg");
  stateTransferMetrics_.RegisterCounter("sent_checkpoint_summary_msg");
  stateTransferMetrics_.RegisterCounter("sent_fetch_blocks_msg");
  stateTransferMetrics_.RegisterCounter("sent_fetch_res_pages_msg");
  stateTransferMetrics_.RegisterCounter("sent_reject_fetch_msg");
  stateTransferMetrics_.RegisterCounter("sent_item_data_msg");

  stateTransferMetrics_.RegisterCounter("received_ask_for_checkpoint_summaries_msg");
  stateTransferMetrics_.RegisterCounter("received_checkpoint_summary_msg");
  stateTransferMetrics_.RegisterCounter("received_fetch_blocks_msg");
  stateTransferMetrics_.RegisterCounter("received_fetch_res_pages_msg");
  stateTransferMetrics_.RegisterCounter("received_reject_fetching_msg");
  stateTransferMetrics_.RegisterCounter("received_item_data_msg");
  stateTransferMetrics_.RegisterCounter("received_illegal_msg_");

  stateTransferMetrics_.RegisterCounter("invalid_ask_for_checkpoint_summaries_msg");
  stateTransferMetrics_.RegisterCounter("irrelevant_ask_for_checkpoint_summaries_msg");
  stateTransferMetrics_.RegisterCounter("invalid_checkpoint_summary_msg");
  stateTransferMetrics_.RegisterCounter("irrelevant_checkpoint_summary_msg");
  stateTransferMetrics_.RegisterCounter("invalid_fetch_blocks_msg");
  stateTransferMetrics_.RegisterCounter("irrelevant_fetch_blocks_msg");
  stateTransferMetrics_.RegisterCounter("invalid_fetch_res_pages_msg");
  stateTransferMetrics_.RegisterCounter("irrelevant_fetch_res_pages_msg");
  stateTransferMetrics_.RegisterCounter("invalid_reject_fetching_msg");
  stateTransferMetrics_.RegisterCounter("irrelevant_reject_fetching_msg");
  stateTransferMetrics_.RegisterCounter("invalid_item_data_msg");
  stateTransferMetrics_.RegisterCounter("irrelevant_item_data_msg");

  stateTransferMetrics_.RegisterCounter("create_checkpoint");
  stateTransferMetrics_.RegisterCounter("mark_checkpoint_as_stable");
  stateTransferMetrics_.RegisterCounter("load_reserved_page");
  stateTransferMetrics_.RegisterCounter("load_reserved_page_from_pending");
  stateTransferMetrics_.RegisterCounter("load_reserved_page_from_checkpoint");
  stateTransferMetrics_.RegisterCounter("save_reserved_page");
  stateTransferMetrics_.RegisterCounter("zero_reserved_page");
  stateTransferMetrics_.RegisterCounter("start_collecting_state");
  stateTransferMetrics_.RegisterCounter("on_timer");
  stateTransferMetrics_.RegisterCounter("on_transferring_complete");
  stateTransferMetrics_.Register();
}

std::shared_ptr<Aggregator> ConcordbftMetricsCollector::getAggregator(int repID) {
  std::lock_guard<std::mutex> lock(lock_);
  if (aggregators_.find(repID) == aggregators_.end()) return std::make_shared<Aggregator>();
  return aggregators_[repID];
}
std::unique_ptr<IMetricsCollector> ConcordbftMetricsCollector::_instance =
    std::make_unique<ConcordbftMetricsCollector>();

}  // namespace concordMetrics
