// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "ControllerWithSimpleHistory.hpp"
#include "gtest/gtest.h"
#include <chrono>
#include <thread>

using namespace std;
using namespace bftEngine;

// Tests methods - insideActiveWindow, onBecomePrimary.
// Tests logic, which sets the range of sequence numbers that are valid for current window.
// The starting index is initialized to the next sequence number:
// that is a multiplication of EvaluationPeriod plus one.
// The end index is `start + EvaluationPeriod` excluded.
TEST(ControllerWithSimpleHistory, sequence_number_range_window) {
  // Test case - init seq is 0.
  // range should be (1,1 + ControllerWithSimpleHistory::EvaluationPeriod]
  {
    uint16_t C = 0;
    uint16_t F = 1;
    ReplicaId replicaId = 0;
    ViewNum initialView = 0;
    SeqNum initialSeq = 0;
    ControllerWithSimpleHistory cwsh{C, F, replicaId, initialView, initialSeq};
    // Should start from one
    ASSERT_EQ(false, cwsh.insideActiveWindow((SeqNum)0));
    // End at 1 + EvaluationPeriod, Excluded.
    ASSERT_EQ(true, cwsh.insideActiveWindow((SeqNum)(1)));
    // End at 1 + EvaluationPeriod, Excluded.
    ASSERT_EQ(true, cwsh.insideActiveWindow((SeqNum)(ControllerWithSimpleHistory::EvaluationPeriod / 2)));
    ASSERT_EQ(true, cwsh.insideActiveWindow((SeqNum)(ControllerWithSimpleHistory::EvaluationPeriod)));
    // End at 1 + EvaluationPeriod, Excluded.
    ASSERT_EQ(false, cwsh.insideActiveWindow((SeqNum)(1 + ControllerWithSimpleHistory::EvaluationPeriod)));
  }

  // Test case - init seq is 122.
  // range should be next multiplication of EvaluationPeriod plus one:
  // i.e. (129,129 + ControllerWithSimpleHistory::EvaluationPeriod]
  {
    uint16_t C = 0;
    uint16_t F = 1;
    ReplicaId replicaId = 0;
    ViewNum initialView = 0;
    SeqNum initialSeq = 122;
    ControllerWithSimpleHistory cwsh{C, F, replicaId, initialView, initialSeq};
    // Should start from 129
    ASSERT_EQ(false, cwsh.insideActiveWindow((SeqNum)122));
    // Should start from 129
    ASSERT_EQ(true, cwsh.insideActiveWindow((SeqNum)(129)));
    // End at 129 + EvaluationPeriod, Excluded.
    ASSERT_EQ(true, cwsh.insideActiveWindow((SeqNum)((129 - 1) + ControllerWithSimpleHistory::EvaluationPeriod)));
    ASSERT_EQ(false, cwsh.insideActiveWindow((SeqNum)((129) + ControllerWithSimpleHistory::EvaluationPeriod)));
  }
}

// Tests method - onNewView.
// Tests when replica == primary, side effects:
// 1)Resets slow path timer to default.
// 2)initializes a range of sequence numbers for the current window to handle.
// Logic:
// Run one cycle of EvaluationPeriod requests, which sets the slow path timer to a lower value:
// then perform onNewView to a view where replica is primary.
TEST(ControllerWithSimpleHistory, onNewView_is_primary) {
  uint16_t C = 0;
  uint16_t F = 1;
  ReplicaId replicaId = 0;
  ViewNum initialView = 0;
  SeqNum initialSeq = 0;
  ControllerWithSimpleHistory cwsh{C, F, replicaId, initialView, initialSeq};
  // Timer value is default
  auto slowThresh = cwsh.timeToStartSlowPathMilli();

  // Fill one EvaluationPeriod of requests:
  // Evaluates an average request duration, which is lower than the default.
  auto prePrepareTime = std::chrono::steady_clock::now() - std::chrono::milliseconds(10);
  for (auto i = (size_t)1; i <= ControllerWithSimpleHistory::EvaluationPeriod; ++i) {
    cwsh.onSendingPrePrepare((SeqNum)i, CommitPath::OPTIMISTIC_FAST, prePrepareTime);
    cwsh.onNewSeqNumberExecution((SeqNum)i);
  }

  // Take timer value after the cycle.
  auto slowThreshAfter = cwsh.timeToStartSlowPathMilli();
  // Test that timer values are as expected
  ASSERT_LT(slowThreshAfter, slowThresh);
  // New view, replica should be primary.
  cwsh.onNewView(4, 122);
  auto slowThreshOnView = cwsh.timeToStartSlowPathMilli();
  // Both values should equal to defualt value.
  ASSERT_EQ(slowThreshOnView, slowThresh);
  // Should start from 129
  ASSERT_EQ(false, cwsh.insideActiveWindow((SeqNum)122));
  ASSERT_EQ(true, cwsh.insideActiveWindow((SeqNum)(129)));

  // End at 129 + EvaluationPeriod, Excluded.
  ASSERT_EQ(true, cwsh.insideActiveWindow((SeqNum)((129 - 1) + ControllerWithSimpleHistory::EvaluationPeriod)));
  ASSERT_EQ(false, cwsh.insideActiveWindow((SeqNum)((129) + ControllerWithSimpleHistory::EvaluationPeriod)));
}

// Tests method - onNewView.
// Tests when replica is not primary.
// Logic:
// Run one cycle of EvaluationPeriod requests:
// Sets `slow path timer` and `sequence number range` to value other than the default:
// Perform onNewView where replica is not primary, therefore values does not change according to the onNewView
// protocol.
TEST(ControllerWithSimpleHistory, onNewView_not_primary) {
  uint16_t C = 0;
  uint16_t F = 1;
  ReplicaId replicaId = 0;
  ViewNum initialView = 0;
  SeqNum initialSeq = 0;
  ControllerWithSimpleHistory cwsh{C, F, replicaId, initialView, initialSeq};
  auto slowThresh = cwsh.timeToStartSlowPathMilli();
  // Fill one EvaluationPeriod of requests:
  // Evaluates an average request duration, which is lower than the default.
  // After the execution the next sequence number is (EvaluationPeriod + 1).
  auto prePrepareTime = std::chrono::steady_clock::now() - std::chrono::milliseconds(10);
  for (auto i = (size_t)1; i <= ControllerWithSimpleHistory::EvaluationPeriod; ++i) {
    cwsh.onSendingPrePrepare((SeqNum)i, CommitPath::OPTIMISTIC_FAST, prePrepareTime);
    cwsh.onNewSeqNumberExecution((SeqNum)i);
  }

  auto slowThreshAfter = cwsh.timeToStartSlowPathMilli();
  ASSERT_LT(slowThreshAfter, slowThresh);

  // Call with view numer '3' which does not set the current replica to be the primary.
  cwsh.onNewView(3, 830);
  auto slowThreshOnView = cwsh.timeToStartSlowPathMilli();
  // Assert that after new view, `slow path timer` did not change back to default.
  ASSERT_EQ(slowThreshOnView, slowThreshAfter);

  // Test that next sequence number is starts from  from (EvaluationPeriod + 1), not from the first available after 830
  auto sn = (SeqNum)(ControllerWithSimpleHistory::EvaluationPeriod + 1);
  ASSERT_EQ(true, cwsh.insideActiveWindow(sn));
  ASSERT_EQ(true, cwsh.insideActiveWindow(((sn - 1) + ControllerWithSimpleHistory::EvaluationPeriod)));
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}