// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "gtest/gtest.h"
#include "performance_handler.h"

using namespace concord::diagnostics;

// 5 Minutes
static constexpr int64_t MAX_VALUE_MICROSECONDS = 1000 * 1000 * 60 * 5;

TEST(histogram_tests, snapshots) {
  const std::string component_name("test_replica");
  const std::string hist_name("some_histogram");
  auto recorder = std::make_shared<Recorder>(hist_name, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
  PerformanceHandler handler;

  handler.registerComponent(component_name, {recorder});
  auto data = handler.get(component_name);
  // The histograms are empty
  auto hist_data = data.at(hist_name);
  auto hist_data2 = handler.get(component_name, hist_name);
  ASSERT_EQ(0, hist_data.history.max);
  ASSERT_EQ(0, hist_data.last_snapshot.max);
  ASSERT_EQ(hist_data, hist_data2);
  auto snapshot_end = hist_data.snapshot_end;

  // Add a value to the histogram
  recorder->record(1);

  // We haven't taken a snapshot yet to synchronize with the recorder (another thread in production)

  hist_data = handler.get(component_name).at(hist_name);
  hist_data2 = handler.get(component_name, hist_name);
  ASSERT_EQ(0, hist_data.history.max);
  ASSERT_EQ(0, hist_data.last_snapshot.max);
  ASSERT_EQ(snapshot_end, hist_data.snapshot_end);
  ASSERT_EQ(hist_data, hist_data2);

  // Take a snapshot. This should not update the history.
  handler.snapshot(component_name);
  hist_data = handler.get(component_name).at(hist_name);
  hist_data2 = handler.get(component_name, hist_name);
  ASSERT_EQ(0, hist_data.history.max);
  ASSERT_EQ(0, hist_data.history.count);
  ASSERT_EQ(1, hist_data.last_snapshot.max);
  ASSERT_EQ(1, hist_data.last_snapshot.count);
  ASSERT_LT(snapshot_end, hist_data.snapshot_end);
  ASSERT_EQ(hist_data, hist_data2);

  snapshot_end = hist_data.snapshot_end;

  // Taking another snapshot should move the prior snapshot into history
  handler.snapshot(component_name);
  hist_data = handler.get(component_name).at(hist_name);
  hist_data2 = handler.get(component_name, hist_name);
  ASSERT_EQ(1, hist_data.history.max);
  ASSERT_EQ(0, hist_data.last_snapshot.max);
  ASSERT_LT(snapshot_end, hist_data.snapshot_end);
  ASSERT_EQ(hist_data, hist_data2);

  ASSERT_ANY_THROW(handler.get("bad_replica"));
  ASSERT_ANY_THROW(handler.get("test_replica", "bad_histogram"));
}
