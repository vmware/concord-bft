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

#include <cstdlib>
#include <utility>
#include "gtest/gtest.h"
#include "Metrics.hpp"

using namespace std;

namespace concordMetrics {
class Counter_ : public CounterHandler {
  uint64_t val;

 public:
  Counter_() : val{0} {}
  void inc() override { val++; }
  uint64_t get() { return val; }
};

class Gauge_ : public GaugeHandler {
  uint64_t val;

 public:
  Gauge_(uint64_t v) : val{v} {}
  void set(uint64_t val_) override { val = val_; }
  uint64_t get() { return val; }
};

class Status_ : public StatusHandler {
  std::string val;

 public:
  Status_(std::string v) : val{std::move(v)} {}
  void set(const std::string& val_) override { val = val_; }
  const std::string& get() { return val; }
};

TEST(MetricsCollectorTest, SimpleHandlers) {
  MetricsCollector mc;
  auto c = std::make_shared<Counter_>();
  auto g = std::make_shared<Gauge_>(10);
  auto s = std::make_shared<Status_>("Hello World");

  mc.getReplicaComp().add(0, c);
  mc.getReplicaComp().add(1, g);
  mc.getReplicaComp().add(2, s);

  ASSERT_EQ(0, c->get());
  ASSERT_EQ(10, g->get());
  ASSERT_EQ("Hello World", s->get());

  mc.getReplicaComp().takeMetric(0);
  mc.getReplicaComp().takeMetric(1, 12);
  mc.getReplicaComp().takeMetric(2, "HW");

  ASSERT_EQ(1, c->get());
  ASSERT_EQ(12, g->get());
  ASSERT_EQ("HW", s->get());
}

TEST(MetricsCollectorTest, MultipleForOneMetric) {
  MetricsCollector mc;
  auto c1 = std::make_shared<Counter_>();
  auto c2 = std::make_shared<Counter_>();
  auto c3 = std::make_shared<Counter_>();

  mc.getReplicaComp().add(0, c1);
  mc.getReplicaComp().add(0, c2);
  mc.getReplicaComp().add(0, c3);

  ASSERT_EQ(0, c1->get());
  ASSERT_EQ(0, c2->get());
  ASSERT_EQ(0, c3->get());

  mc.getReplicaComp().takeMetric(0);

  ASSERT_EQ(1, c1->get());
  ASSERT_EQ(1, c2->get());
  ASSERT_EQ(1, c3->get());
}
TEST(MetricsCollectorTest, TestSingleton) {
  auto c = std::make_shared<Counter_>();
  auto g = std::make_shared<Gauge_>(10);
  auto s = std::make_shared<Status_>("Hello World");
  ComponentCollector& rep = MetricsCollector::getInstance(0).getReplicaComp();

  rep.add(0, c);
  rep.add(1, g);
  rep.add(2, s);

  ASSERT_EQ(0, c->get());
  ASSERT_EQ(10, g->get());
  ASSERT_EQ("Hello World", s->get());

  rep.takeMetric(0);
  rep.takeMetric(1, 12);
  rep.takeMetric(2, "HW");

  ASSERT_EQ(1, c->get());
  ASSERT_EQ(12, g->get());
  ASSERT_EQ("HW", s->get());
}
TEST(MetricsCollectorTest, TestSingletonWithConcordMetrics) {
  auto aggregator = std::make_shared<Aggregator>();
  Component rep("replica", aggregator);
  Component st("bc_state_transfer", aggregator);

  ConcordBftMetricsComp::createReplicaComponent(rep, MetricsCollector::getInstance(0).getReplicaComp());
  ConcordBftMetricsComp::createStateTransferComponent(st, MetricsCollector::getInstance(0).getStateTransferComp());

  ComponentCollector& repComp = MetricsCollector::getInstance(0).getReplicaComp();
  ASSERT_EQ(0, aggregator->GetGauge("replica", "view").Get());
  ASSERT_EQ(0, aggregator->GetCounter("replica", "receivedInternalMsgs").Get());
  ASSERT_EQ("", aggregator->GetStatus("replica", "firstCommitPath").Get());

  repComp.takeMetric(ReplicaMetricsCode::REPLICA_VIEW, 100);
  repComp.takeMetric(ReplicaMetricsCode::REPLICA_RECEIVED_INTERNAL_MSGS);
  repComp.takeMetric(ReplicaMetricsCode::REPLICA_FIRST_COMMIT_PATH, "Slow");
  rep.UpdateAggregator();

  ASSERT_EQ(100, aggregator->GetGauge("replica", "view").Get());
  ASSERT_EQ(1, aggregator->GetCounter("replica", "receivedInternalMsgs").Get());
  ASSERT_EQ("Slow", aggregator->GetStatus("replica", "firstCommitPath").Get());

  ComponentCollector& stComp = MetricsCollector::getInstance(0).getStateTransferComp();

  ASSERT_EQ(0, aggregator->GetGauge("bc_state_transfer", "num_pending_item_data_msgs_").Get());
  ASSERT_EQ(0, aggregator->GetCounter("bc_state_transfer", "received_ask_for_checkpoint_summaries_msg").Get());
  ASSERT_EQ("", aggregator->GetStatus("bc_state_transfer", "pedantic_checks_enabled").Get());

  stComp.takeMetric(StateTransferMetricCode::BCST_RECEIVED_ASK_FOR_CHECKPOINT_SUMMARIES_MSG);
  stComp.takeMetric(StateTransferMetricCode::BCST_PEDANTIC_CHECKS_ENABLED, "true");
  stComp.takeMetric(StateTransferMetricCode::BCST_NUM_PENDING_ITEM_DATA_MSGS, 100);
  st.UpdateAggregator();

  ASSERT_EQ(100, aggregator->GetGauge("bc_state_transfer", "num_pending_item_data_msgs_").Get());
  ASSERT_EQ(1, aggregator->GetCounter("bc_state_transfer", "received_ask_for_checkpoint_summaries_msg").Get());
  ASSERT_EQ("true", aggregator->GetStatus("bc_state_transfer", "pedantic_checks_enabled").Get());
}

}  // namespace concordMetrics
