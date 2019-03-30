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
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
//
#include "gtest/gtest.h"
#include "Metrics.hpp"

using namespace std;

namespace concordMetrics {

TEST(MetricsTest, UseValues) {
  auto aggregator = std::make_shared<Aggregator>();
  Component c("replica", aggregator);
  auto h_gauge = c.RegisterGauge("connected_peers", 3);
  auto h_status = c.RegisterStatus("state", "primary");
  auto h_counter = c.RegisterCounter("messages_sent", 0);

  ASSERT_EQ(3, h_gauge.Get().Get());
  ASSERT_EQ("primary", h_status.Get().Get());
  ASSERT_EQ(0, h_counter.Get().Get());

  h_gauge.Get().Set(5);
  ASSERT_EQ(5, h_gauge.Get().Get());
  h_status.Get().Set("backup");
  ASSERT_EQ("backup", h_status.Get().Get());
  ASSERT_EQ(1, h_counter.Get().Inc());
}

TEST(MetricsTest, Aggregator) {
  auto aggregator = std::make_shared<Aggregator>();
  Component c("replica", aggregator);
  auto h_gauge = c.RegisterGauge("connected_peers", 3);
  auto h_status = c.RegisterStatus("state", "primary");
  auto h_counter = c.RegisterCounter("messages_sent", 0);

  c.Register();

  ASSERT_EQ(3, aggregator->GetGauge(c.Name(), "connected_peers").Get());
  ASSERT_THROW(aggregator->GetGauge(c.Name(), "non-existent-gauge"),
               invalid_argument);
  ASSERT_EQ("primary", aggregator->GetStatus(c.Name(), "state").Get());
  ASSERT_THROW(aggregator->GetStatus(c.Name(), "no-such-status"),
               invalid_argument);
  ASSERT_EQ(0, aggregator->GetCounter(c.Name(), "messages_sent").Get());
  ASSERT_THROW(aggregator->GetCounter(c.Name(), "no-such-counter"),
               invalid_argument);

  h_gauge.Get().Set(5);
  h_status.Get().Set("backup");
  h_counter.Get().Inc();
  // We haven't updated the aggregator yet, so it still has the old values
  ASSERT_EQ(3, aggregator->GetGauge(c.Name(), "connected_peers").Get());
  ASSERT_EQ("primary", aggregator->GetStatus(c.Name(), "state").Get());
  ASSERT_EQ(0, aggregator->GetCounter(c.Name(), "messages_sent").Get());

  c.UpdateAggregator();
  ASSERT_EQ(5, aggregator->GetGauge(c.Name(), "connected_peers").Get());
  ASSERT_EQ("backup", aggregator->GetStatus(c.Name(), "state").Get());
  ASSERT_EQ(1, aggregator->GetCounter(c.Name(), "messages_sent").Get());
}

}  // namespace concordMetrics
