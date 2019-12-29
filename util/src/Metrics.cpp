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

ConcordbftMetricsCollector::ConcordbftMetricsCollector(uint32_t id, std::shared_ptr<Aggregator> aggregator) :
    aggregator_{aggregator},
    replicaMetrics_{"replica", aggregator_},
    stateTransferMetrics_{"bc_state_transfer", aggregator_}
    {
        gauges.insert(std::pair<MetricType, gaugeHandler>(MetricType::REPLICA_VIEW, replicaMetrics_.RegisterGauge("view", 0)));
        gauges.insert(std::pair<MetricType, gaugeHandler>(MetricType::REPLICA_LAST_STABLE_SEQ_NUM, replicaMetrics_.RegisterGauge("lastStableSeqNum", 0)));
        gauges.insert(std::pair<MetricType, gaugeHandler>(MetricType::REPLICA_LAST_EXECUTED_SEQ_NUM, replicaMetrics_.RegisterGauge("lastExecutedSeqNum", 0)));
        gauges.insert(std::pair<MetricType, gaugeHandler>(MetricType::REPLICA_LAST_AGREED_VIEW, replicaMetrics_.RegisterGauge("lastAgreedView", 0)));

        statuses.insert(std::pair<MetricType, statusHandler>(MetricType::REPLICA_FIRST_COMMIT_PATH, replicaMetrics_.RegisterStatus("firstCommitPath", "")));

        counters.insert(std::pair<MetricType, counterHandler>(MetricType::REPLICA_SLOW_PATH_COUNT,replicaMetrics_.RegisterCounter("slowPathCount")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::REPLICA_RECEIVED_INTERNAL_MSGS,replicaMetrics_.RegisterCounter("receivedInternalMsgs")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::REPLICA_RECEIVED_CLIENT_REQUEST_MSGS,replicaMetrics_.RegisterCounter("receivedClientRequestMsgs")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::REPLICA_RECEIVED_PREPREPARE_MSGS,replicaMetrics_.RegisterCounter("receivedPrePrepareMsgs")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::REPLICA_RECEIVED_START_SLOW_COMMIT_MSGS,replicaMetrics_.RegisterCounter("receivedStartSlowCommitMsgs")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::REPLICA_RECEIVED_PARTIAL_COMMIT_PROOF_MSGS,replicaMetrics_.RegisterCounter("receivedPartialCommitProofMsgs")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::REPLICA_RECEIVED_FULL_COMMIT_PROOF_MSGS,replicaMetrics_.RegisterCounter("receivedFullCommitProofMsgs")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::REPLICA_RECEIVED_PREPARER_PARTIAL_MSGS,replicaMetrics_.RegisterCounter("receivedPreparePartialMsgs")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::REPLICA_RECEIVED_COMMIT_PARTIAL_MSGS,replicaMetrics_.RegisterCounter("receivedCommitPartialMsgs")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::REPLICA_RECEIVED_PREPARE_FULL_MSGS,replicaMetrics_.RegisterCounter("receivedPrepareFullMsgs")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::REPLICA_RECEIVED_COMMIT_FULL_MSGS,replicaMetrics_.RegisterCounter("receivedCommitFullMsgs")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::REPLICA_RECEIVED_CHECKPOINT_MSGS,replicaMetrics_.RegisterCounter("receivedCheckpointMsgs")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::REPLICA_RECEIVED_STATUS_MSGS,replicaMetrics_.RegisterCounter("receivedReplicaStatusMsgs")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::REPLICA_RECEIVED_VIEW_CHANGE_MSGS,replicaMetrics_.RegisterCounter("receivedViewChangeMsgs")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::REPLICA_RECEIVED_NEW_VIEW_MSGS,replicaMetrics_.RegisterCounter("receivedNewViewMsgs")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::REPLICA_RECEIVED_REQ_MISSING_DATA_MSGS,replicaMetrics_.RegisterCounter("receivedReqMissingDataMsgs")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::REPLICA_RECEIVED_SIMPLE_ACK_MAGS,replicaMetrics_.RegisterCounter("receivedSimpleAckMsgs")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::REPLICA_RECEIVED_STATE_TRANSFER_MSGS,replicaMetrics_.RegisterCounter("receivedStateTransferMsgs")));
        replicaMetrics_.Register();

        statuses.insert(std::pair<MetricType, statusHandler>(MetricType::BCST_FETCHING_STATE, stateTransferMetrics_.RegisterStatus("fetching_state", "")));
        statuses.insert(std::pair<MetricType, statusHandler>(MetricType::BCST_PEDANTIC_CHECKS_ENABLED, stateTransferMetrics_.RegisterStatus("pedantic_checks_enabled", "")));
        statuses.insert(std::pair<MetricType, statusHandler>(MetricType::BCST_PREFERRED_REPLICAS, stateTransferMetrics_.RegisterStatus("preferred_replicas", "")));

        gauges.insert(std::pair<MetricType, gaugeHandler>(MetricType::BCST_CURRENT_SOURCE_REPLICA, stateTransferMetrics_.RegisterGauge("current_source_replica", 0)));
        gauges.insert(std::pair<MetricType, gaugeHandler>(MetricType::BCST_CHECKPOINT_BEING_FETCHED, stateTransferMetrics_.RegisterGauge("checkpoint_being_fetched", 0)));
        gauges.insert(std::pair<MetricType, gaugeHandler>(MetricType::BCST_LAST_STORED_CHECKPOINT, stateTransferMetrics_.RegisterGauge("last_stored_checkpoint", 0)));
        gauges.insert(std::pair<MetricType, gaugeHandler>(MetricType::BCST_NUMBER_OF_RESERVED_PAGES, stateTransferMetrics_.RegisterGauge("number_of_reserved_pages", 0)));
        gauges.insert(std::pair<MetricType, gaugeHandler>(MetricType::BCST_SIZE_OF_RESERVED_PAGES, stateTransferMetrics_.RegisterGauge("size_of_reserved_page", 0)));
        gauges.insert(std::pair<MetricType, gaugeHandler>(MetricType::BCST_LAST_MSG_SEQ_NUM, stateTransferMetrics_.RegisterGauge("last_msg_seq_num", 0)));
        gauges.insert(std::pair<MetricType, gaugeHandler>(MetricType::BCST_NEXT_REQUIRED_BLOCK, stateTransferMetrics_.RegisterGauge("next_required_block_", 0)));
        gauges.insert(std::pair<MetricType, gaugeHandler>(MetricType::BCST_NUM_PENDING_ITEM_DATA_MSGS, stateTransferMetrics_.RegisterGauge("num_pending_item_data_msgs_", 0)));
        gauges.insert(std::pair<MetricType, gaugeHandler>(MetricType::BCST_TOTAL_SIZE_OF_PENDING_ITEM_DATA_MSGS, stateTransferMetrics_.RegisterGauge("total_size_of_pending_item_data_msgs", 0)));
        gauges.insert(std::pair<MetricType, gaugeHandler>(MetricType::BCST_LAST_BLOCK, stateTransferMetrics_.RegisterGauge("last_block_", 0)));
        gauges.insert(std::pair<MetricType, gaugeHandler>(MetricType::BCST_LAST_REACHABLE_BLOCK, stateTransferMetrics_.RegisterGauge("last_reachable_block", 0)));

        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_SENT_ASK_FOR_CHECKPOINT_SUMMARIES_MSG, stateTransferMetrics_.RegisterCounter("sent_ask_for_checkpoint_summaries_msg")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_SENT_CHECKPOINT_SUMMARY_MSG, stateTransferMetrics_.RegisterCounter("sent_checkpoint_summary_msg")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_SENT_FETCH_BLOCKS_MSG, stateTransferMetrics_.RegisterCounter("sent_fetch_blocks_msg")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_SENT_FETCH_RES_PAGES_MSG, stateTransferMetrics_.RegisterCounter("sent_fetch_res_pages_msg")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_SENT_REJECT_FETCH_MSG, stateTransferMetrics_.RegisterCounter("sent_reject_fetch_msg")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_SENT_ITEM_DATA_MSG, stateTransferMetrics_.RegisterCounter("sent_item_data_msg")));

        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_RECEIVED_ASK_FOR_CHECKPOINT_SUMMARIES_MSG, stateTransferMetrics_.RegisterCounter("received_ask_for_checkpoint_summaries_msg")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_RECEIVED_CHECKPOINT_SUMMARY_MSG, stateTransferMetrics_.RegisterCounter("received_checkpoint_summary_msg")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_RECEIVED_FETCH_BLOCKS_MSG, stateTransferMetrics_.RegisterCounter("received_fetch_blocks_msg")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_RECEIVED_FETCH_RES_PAGES_MSG, stateTransferMetrics_.RegisterCounter("received_fetch_res_pages_msg")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_RECEIVED_REJECT_FETCHING_MSG, stateTransferMetrics_.RegisterCounter("received_reject_fetching_msg")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_RECEIVED_ITEM_DATA_MSG, stateTransferMetrics_.RegisterCounter("received_item_data_msg")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_RECEIVED_ILLEGAL_MSG, stateTransferMetrics_.RegisterCounter("received_illegal_msg_")));

        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_INVALID_ASK_FOR_CHECKPOINT_SUMMARIES_MSG, stateTransferMetrics_.RegisterCounter("invalid_ask_for_checkpoint_summaries_msg")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_IRRELEVANT_ASK_FOR_CHECKPOINT_SUMMARIES_MSG, stateTransferMetrics_.RegisterCounter("irrelevant_ask_for_checkpoint_summaries_msg")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_INVALID_CHECKPOINT_SUMMARY_MSG, stateTransferMetrics_.RegisterCounter("invalid_checkpoint_summary_msg")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_IRRELEVANT_CHECKPOINT_SUMMARY_MSG, stateTransferMetrics_.RegisterCounter("irrelevant_checkpoint_summary_msg")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_INVALID_FETCH_BLOCKS_MSG, stateTransferMetrics_.RegisterCounter("invalid_fetch_blocks_msg")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_IRRELEVANT_FETCH_BLOCKS_MSG, stateTransferMetrics_.RegisterCounter("irrelevant_fetch_blocks_msg")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_INVALID_FETCH_RES_PAGES_MSG, stateTransferMetrics_.RegisterCounter("invalid_fetch_res_pages_msg")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_IRRELEVANT_FETCH_RES_PAGES_MSG, stateTransferMetrics_.RegisterCounter("irrelevant_fetch_res_pages_msg")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_INVALID_REJECT_FETCHING_MSG, stateTransferMetrics_.RegisterCounter("invalid_reject_fetching_msg")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_IRRELEVANT_REJECT_FETCHING_MSG, stateTransferMetrics_.RegisterCounter("irrelevant_reject_fetching_msg")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_INVALID_ITEM_DATA_MSG, stateTransferMetrics_.RegisterCounter("invalid_item_data_msg")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_IRRELEVANT_ITEM_DATA_MSG, stateTransferMetrics_.RegisterCounter("irrelevant_item_data_msg")));

        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_CREATE_CHECKPOINT, stateTransferMetrics_.RegisterCounter("create_checkpoint")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_MARK_CHECKPOINT_AS_STABLE, stateTransferMetrics_.RegisterCounter("mark_checkpoint_as_stable")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_LOAD_RESERVED_PAGE, stateTransferMetrics_.RegisterCounter("load_reserved_page")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_LOAD_RESERVED_PAGE_FROM_PENDING, stateTransferMetrics_.RegisterCounter("load_reserved_page_from_pending")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_LOAD_RESERVED_PAGE_FROM_CHECKPOINT, stateTransferMetrics_.RegisterCounter("load_reserved_page_from_checkpoint")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_SAVE_RESERVED_PAGE, stateTransferMetrics_.RegisterCounter("save_reserved_page")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_ZERO_RESERVED_PAGE, stateTransferMetrics_.RegisterCounter("zero_reserved_page")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_START_COLLECTION_STATE, stateTransferMetrics_.RegisterCounter("start_collecting_state")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_ON_TIMER, stateTransferMetrics_.RegisterCounter("on_timer")));
        counters.insert(std::pair<MetricType, counterHandler>(MetricType::BCST_ON_TRANSFERRING_COMPLETE, stateTransferMetrics_.RegisterCounter("on_transferring_complete")));
        stateTransferMetrics_.Register();

        for (int t = MetricType::BEGIN + 1 ; t != MetricType::LAST ; t++) {
            registerDefaultHandler(id, (MetricType) t);
        }

        updator = std::thread([this]() {
            updateComponent();
        });
    }

    void ConcordbftMetricsCollector::registerDefaultHandler(uint32_t id, MetricType t) {
        switch (t) {
            case REPLICA_VIEW:
            case REPLICA_LAST_STABLE_SEQ_NUM:
            case REPLICA_LAST_EXECUTED_SEQ_NUM:
            case REPLICA_LAST_AGREED_VIEW:
            case BCST_CURRENT_SOURCE_REPLICA:
            case BCST_CHECKPOINT_BEING_FETCHED:
            case BCST_LAST_STORED_CHECKPOINT:
            case BCST_NUMBER_OF_RESERVED_PAGES:
            case BCST_SIZE_OF_RESERVED_PAGES:
            case BCST_LAST_MSG_SEQ_NUM:
            case BCST_NEXT_REQUIRED_BLOCK:
            case BCST_NUM_PENDING_ITEM_DATA_MSGS:
            case BCST_TOTAL_SIZE_OF_PENDING_ITEM_DATA_MSGS:
            case BCST_LAST_BLOCK:
            case BCST_LAST_REACHABLE_BLOCK:
                concordMetrics::MetricsCollector::instance(id).registerCallBack(t, [this, t](uint64_t val) {
                    gauges.at(t).Get().Set(val);
                });
                break;
            case REPLICA_FIRST_COMMIT_PATH:
            case BCST_FETCHING_STATE:
            case BCST_PEDANTIC_CHECKS_ENABLED:
            case BCST_PREFERRED_REPLICAS:
                concordMetrics::MetricsCollector::instance(id).registerCallBack(t, [this, t](const std::string &val) {
                    statuses.at(t).Get().Set(val);
                });
                break;
            default:
                concordMetrics::MetricsCollector::instance(id).registerCallBack(t, [this, t]() {
                    counters.at(t).Get().Inc();
                });
        }
    }

    void ConcordbftMetricsCollector::updateComponent() {
        while (active) {
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
            replicaMetrics_.UpdateAggregator();
            stateTransferMetrics_.UpdateAggregator();
        }

    }

    ConcordbftMetricsCollector::~ConcordbftMetricsCollector() {
        active = false;
        updator.join();
    }

}  // namespace concordMetrics
