// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <string>

namespace concord {
namespace thin_replica {

struct ThinReplicaServerMetrics {
  ThinReplicaServerMetrics(std::string stream_type, std::string client_id)
      : metrics_component_{"ThinReplicaServer", std::make_shared<concordMetrics::Aggregator>()},
        subscriber_list_size{metrics_component_.RegisterGauge("subscriber_list_size", 0)},
        queue_size{metrics_component_.RegisterGauge(
            "queue_size", 0, {{"stream_type", stream_type}, {"client_id", client_id}})},
        last_sent_block_id{metrics_component_.RegisterGauge(
            "last_sent_block_id", 0, {{"stream_type", stream_type}, {"client_id", client_id}})},
        last_sent_event_group_id{metrics_component_.RegisterGauge(
            "last_sent_event_group_id", 0, {{"stream_type", stream_type}, {"client_id", client_id}})},
        num_skipped_event_groups{metrics_component_.RegisterCounter(
            "num_skipped_event_groups", 0, {{"stream_type", stream_type}, {"client_id", client_id}})} {
    metrics_component_.Register();
  }

  void setAggregator(const std::shared_ptr<concordMetrics::Aggregator>& aggregator) {
    metrics_component_.SetAggregator(aggregator);
  }

  void updateAggregator() { metrics_component_.UpdateAggregator(); }

 private:
  concordMetrics::Component metrics_component_;

 public:
  // number of current subscriptions
  concordMetrics::GaugeHandle subscriber_list_size;
  // live update queue size
  concordMetrics::GaugeHandle queue_size;
  // last sent block id
  concordMetrics::GaugeHandle last_sent_block_id;
  // last sent event group id
  concordMetrics::GaugeHandle last_sent_event_group_id;
  // number of event groups skipped after filtering
  concordMetrics::CounterHandle num_skipped_event_groups;
};
}  // namespace thin_replica
}  // namespace concord
