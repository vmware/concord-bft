// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "Metrics.hpp"
#include "base_types.h"

namespace bft::client {

struct Metrics {
  Metrics(ClientId id)
      : component_{"clientMetrics_" + std::to_string(id.val), std::make_shared<concordMetrics::Aggregator>()},
        retransmissions{component_.RegisterCounter("retransmissions")},
        transactionSigning{component_.RegisterCounter("transactionSigning")},
        retransmissionTimer{component_.RegisterGauge("retransmissionTimer", 0)},
        repliesCleared{component_.RegisterCounter("repliesCleared", 0)} {
    component_.Register();
  }

  void setAggregator(const std::shared_ptr<concordMetrics::Aggregator>& aggregator) {
    component_.SetAggregator(aggregator);
  }

  void updateAggregator() { component_.UpdateAggregator(); }

 private:
  concordMetrics::Component component_;

 public:
  concordMetrics::CounterHandle retransmissions;
  concordMetrics::CounterHandle transactionSigning;
  concordMetrics::GaugeHandle retransmissionTimer;
  concordMetrics::CounterHandle repliesCleared;
};

}  // namespace bft::client
