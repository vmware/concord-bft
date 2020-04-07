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
#include "messages/SignedShareMsgs.hpp"
#include "threshsign/IThresholdSigner.h"
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

////////////////////////////Path upgrade/downgrade scenarios/////////////////////

/// Downgrade scenarios

// Test - Downgrade from OPTIMISTIC_FAST to FAST_WITH_THRESHOLD
// Logic:
// 1)Construct controller with C > 0 i.e. enable FAST_WITH_THRESHOLD path.
// 2)Run loop for EvaluationPeriod times:
//  - call onStartingSlowCommit for more than factor * EvaluationPeriod.
TEST(ControllerWithSimpleHistory, downgrade_from_optimistic_to_threshold) {
  uint16_t C = 1;
  uint16_t F = 1;
  ReplicaId replicaId = 0;
  ViewNum initialView = 0;
  SeqNum initialSeq = 0;
  ControllerWithSimpleHistory cwsh{C, F, replicaId, initialView, initialSeq};

  ASSERT_EQ(CommitPath::OPTIMISTIC_FAST, cwsh.getCurrentFirstPath());

  // Factor + 1, in order to trigger the degradation.
  auto slowStartCount =
      ControllerWithSimpleHistory_debugDowngradeFactor * ControllerWithSimpleHistory::EvaluationPeriod + 1;
  auto prePrepareTime = std::chrono::steady_clock::now() - std::chrono::milliseconds(10);
  bool changed{false};
  for (auto i = (size_t)1; i <= ControllerWithSimpleHistory::EvaluationPeriod; ++i) {
    cwsh.onSendingPrePrepare((SeqNum)i, CommitPath::OPTIMISTIC_FAST, prePrepareTime);
    changed = cwsh.onNewSeqNumberExecution((SeqNum)i);
    if (slowStartCount-- > 0) cwsh.onStartingSlowCommit((SeqNum)i);
  }
  ASSERT_EQ(changed, true);
  ASSERT_EQ(CommitPath::FAST_WITH_THRESHOLD, cwsh.getCurrentFirstPath());
}

// Test - Downgrade from OPTIMISTIC_FAST to SLOW
// Logic:
// 1)Construct controller with C == 0 i.e. disable FAST_WITH_THRESHOLD path.
// 2)Run loop for EvaluationPeriod times:
//  - call onStartingSlowCommit for more than factor * EvaluationPeriod.
TEST(ControllerWithSimpleHistory, downgrade_from_optimistic_to_slow) {
  uint16_t C = 0;
  uint16_t F = 1;
  ReplicaId replicaId = 0;
  ViewNum initialView = 0;
  SeqNum initialSeq = 0;
  ControllerWithSimpleHistory cwsh{C, F, replicaId, initialView, initialSeq};

  ASSERT_EQ(CommitPath::OPTIMISTIC_FAST, cwsh.getCurrentFirstPath());

  // Factor + 1, in order to trigger the degradation.
  auto slowStartCount =
      ControllerWithSimpleHistory_debugDowngradeFactor * ControllerWithSimpleHistory::EvaluationPeriod + 1;
  auto prePrepareTime = std::chrono::steady_clock::now() - std::chrono::milliseconds(10);
  bool changed{false};
  for (auto i = (size_t)1; i <= ControllerWithSimpleHistory::EvaluationPeriod; ++i) {
    cwsh.onSendingPrePrepare((SeqNum)i, CommitPath::OPTIMISTIC_FAST, prePrepareTime);
    changed = cwsh.onNewSeqNumberExecution((SeqNum)i);
    if (slowStartCount-- > 0) cwsh.onStartingSlowCommit((SeqNum)i);
  }
  ASSERT_EQ(changed, true);
  ASSERT_EQ(CommitPath::SLOW, cwsh.getCurrentFirstPath());
}

// Test - Downgrade from OPTIMISTIC_FAST to FAST_WITH_THRESHOLD and then to SLOW
// Logic:
// 1)Construct controller with C > 0 i.e. enable FAST_WITH_THRESHOLD path.
// 2)Run loop for EvaluationPeriod times:
//  - call to onStartingSlowCommit for more than factor * EvaluationPeriod.
// 3)Run loop for EvaluationPeriod times:
//  - call to onStartingSlowCommit for more than factor * EvaluationPeriod.
TEST(ControllerWithSimpleHistory, downgrade_from_optimistic_to_thresh_then_slow) {
  uint16_t C = 1;
  uint16_t F = 1;
  ReplicaId replicaId = 0;
  ViewNum initialView = 0;
  SeqNum initialSeq = 0;
  ControllerWithSimpleHistory cwsh{C, F, replicaId, initialView, initialSeq};

  ASSERT_EQ(CommitPath::OPTIMISTIC_FAST, cwsh.getCurrentFirstPath());

  // Factor + 1, in order to trigger the degradation.
  auto slowStartCount =
      ControllerWithSimpleHistory_debugDowngradeFactor * ControllerWithSimpleHistory::EvaluationPeriod + 1;
  auto prePrepareTime = std::chrono::steady_clock::now() - std::chrono::milliseconds(10);
  bool changed{false};
  auto i = (size_t)1;
  for (; i <= ControllerWithSimpleHistory::EvaluationPeriod; ++i) {
    cwsh.onSendingPrePrepare((SeqNum)i, CommitPath::OPTIMISTIC_FAST, prePrepareTime);
    changed = cwsh.onNewSeqNumberExecution((SeqNum)i);
    if (slowStartCount-- > 0) cwsh.onStartingSlowCommit((SeqNum)i);
  }

  ASSERT_EQ(changed, true);
  ASSERT_EQ(CommitPath::FAST_WITH_THRESHOLD, cwsh.getCurrentFirstPath());

  // Start from next window
  for (++i; i <= 2 * ControllerWithSimpleHistory::EvaluationPeriod; ++i) {
    cwsh.onSendingPrePrepare((SeqNum)i, CommitPath::OPTIMISTIC_FAST, prePrepareTime);
    changed = cwsh.onNewSeqNumberExecution((SeqNum)i);
    cwsh.onStartingSlowCommit((SeqNum)i);
  }

  ASSERT_EQ(changed, true);
  ASSERT_EQ(CommitPath::SLOW, cwsh.getCurrentFirstPath());
}

// Test - no degradation
// Logic:
// 1)Construct controller with C > 0 i.e. enable FAST_WITH_THRESHOLD path.
// 2)Run loop for EvaluationPeriod times:
//  - call to onStartingSlowCommit for less than (EvaluationPeriod - factor * EvaluationPeriod)
TEST(ControllerWithSimpleHistory, no_degradation) {
  uint16_t C = 1;
  uint16_t F = 1;
  ReplicaId replicaId = 0;
  ViewNum initialView = 0;
  SeqNum initialSeq = 0;
  ControllerWithSimpleHistory cwsh{C, F, replicaId, initialView, initialSeq};

  ASSERT_EQ(CommitPath::OPTIMISTIC_FAST, cwsh.getCurrentFirstPath());
  bool changed{true};

  // Less than needed to trigger degradation
  auto slowStartCount =
      ControllerWithSimpleHistory::EvaluationPeriod * (1 - ControllerWithSimpleHistory_debugDowngradeFactor) - 1;

  auto prePrepareTime = std::chrono::steady_clock::now() - std::chrono::milliseconds(10);
  for (auto i = (size_t)1; i <= ControllerWithSimpleHistory::EvaluationPeriod; ++i) {
    cwsh.onSendingPrePrepare((SeqNum)i, CommitPath::OPTIMISTIC_FAST, prePrepareTime);
    changed = cwsh.onNewSeqNumberExecution((SeqNum)i);
    if (slowStartCount-- > 0) cwsh.onStartingSlowCommit((SeqNum)i);
  }
  ASSERT_EQ(changed, false);
  ASSERT_EQ(CommitPath::OPTIMISTIC_FAST, cwsh.getCurrentFirstPath());
}

// Upgrade scenarios

// Mocks
class IShareSecretKeyDummy : public IShareSecretKey {
 public:
  string toString() const override { return "IShareSecretKeyDummy"; }
};

class IShareVerificationKeyDummy : public IShareVerificationKey {
 public:
  string toString() const override { return "IShareVerificationKeyDummy"; }
};

class ThreshSigMock : public IThresholdSigner {
 public:
  IShareSecretKeyDummy is;
  IShareVerificationKeyDummy isv;
  virtual int requiredLengthForSignedData() const { return 5; };
  virtual void signData(const char *hash, int hashLen, char *outSig, int outSigLen){};

  virtual const IShareSecretKey &getShareSecretKey() { return is; };
  virtual const IShareVerificationKey &getShareVerificationKey() const { return isv; };
  const std::string getVersion() const { return "v"; }
  void serializeDataMembers(std::ostream &) const {}
  void deserializeDataMembers(std::istream &) {}
  const IShareSecretKey &getShareSecretKey() const { return is; }
};

// Test - Downgrade from OPTIMISTIC_FAST to FAST_WITH_THRESHOLD then upgrade to OPTIMISTIC_FAST
// E.L Logic: Not Possible, Bug ?

// Test - Downgrade from OPTIMISTIC_FAST to SLOW then upgrade to OPTIMISTIC_FAST again.
// Logic:
// 1)Construct controller with C == 0 i.e. disable FAST_WITH_THRESHOLD path.
// 2)Run loop for EvaluationPeriod times:
//  - call to onStartingSlowCommit for more than factor * EvaluationPeriod.
// 3)Run loop for EvaluationPeriod times:
//  - call onMessage for each replica to satisfy full cooporation.
TEST(ControllerWithSimpleHistory, upgrade_slow_to_optimistic) {
  uint16_t C = 0;
  uint16_t F = 1;
  ReplicaId replicaId = 0;
  ViewNum initialView = 0;
  SeqNum initialSeq = 0;
  ControllerWithSimpleHistory cwsh{C, F, replicaId, initialView, initialSeq};

  ASSERT_EQ(CommitPath::OPTIMISTIC_FAST, cwsh.getCurrentFirstPath());

  // Factor + 1, in order to trigger the degradation.
  auto slowStartCount =
      ControllerWithSimpleHistory_debugDowngradeFactor * ControllerWithSimpleHistory::EvaluationPeriod + 1;
  bool changed{false};
  auto prePrepareTime = std::chrono::steady_clock::now() - std::chrono::milliseconds(10);
  auto i = (size_t)1;
  for (; i <= ControllerWithSimpleHistory::EvaluationPeriod; ++i) {
    cwsh.onSendingPrePrepare((SeqNum)i, CommitPath::OPTIMISTIC_FAST, prePrepareTime);
    changed = cwsh.onNewSeqNumberExecution((SeqNum)i);
    if (slowStartCount-- > 0) cwsh.onStartingSlowCommit((SeqNum)i);
  }

  ASSERT_EQ(true, changed);
  ASSERT_EQ(CommitPath::SLOW, cwsh.getCurrentFirstPath());

  char buf[5] = {'m', 'o', 's', 'h', 'e'};
  Digest d{buf, 5};
  ThreshSigMock th;
  for (++i; i <= 2 * ControllerWithSimpleHistory::EvaluationPeriod; ++i) {
    cwsh.onSendingPrePrepare((SeqNum)i, CommitPath::OPTIMISTIC_FAST, prePrepareTime);
    changed = cwsh.onNewSeqNumberExecution((SeqNum)i);
    for (auto id : {1, 2, 3}) {
      auto p = impl::PreparePartialMsg::create(0, i, id, d, &th);
      cwsh.onMessage(p);
      delete p;
    }
  }

  ASSERT_EQ(true, changed);
  ASSERT_EQ(CommitPath::OPTIMISTIC_FAST, cwsh.getCurrentFirstPath());
}

// Test - Downgrade from OPTIMISTIC_FAST to FAST_WITH_THRESHOLD to SLOW then upgrade to FAST_WITH_THRESHOLD
// Logic:
// 1)Construct controller with C  > 0 i.e. enable FAST_WITH_THRESHOLD path.
// 2)Run loop for EvaluationPeriod times:
//  - call to onStartingSlowCommit for more than factor * EvaluationPeriod.
// 3)Run loop for EvaluationPeriod times:
//  - call to onStartingSlowCommit for more than factor * EvaluationPeriod.
// 4)Run loop for EvaluationPeriod times:
//  - call onMessage for 3*f+C (partial set) replicas to satisfy full cooporation.
TEST(ControllerWithSimpleHistory, upgrade_slow_to_threshold) {
  uint16_t C = 1;
  uint16_t F = 1;
  ReplicaId replicaId = 0;
  ViewNum initialView = 0;
  SeqNum initialSeq = 0;
  ControllerWithSimpleHistory cwsh{C, F, replicaId, initialView, initialSeq};

  ASSERT_EQ(CommitPath::OPTIMISTIC_FAST, cwsh.getCurrentFirstPath());

  // Factor + 1, in order to trigger the degradation.
  auto slowStartCount =
      ControllerWithSimpleHistory_debugDowngradeFactor * ControllerWithSimpleHistory::EvaluationPeriod + 1;
  bool changed{false};
  auto prePrepareTime = std::chrono::steady_clock::now() - std::chrono::milliseconds(10);
  auto i = (size_t)1;
  for (; i <= ControllerWithSimpleHistory::EvaluationPeriod; ++i) {
    cwsh.onSendingPrePrepare((SeqNum)i, CommitPath::OPTIMISTIC_FAST, prePrepareTime);
    changed = cwsh.onNewSeqNumberExecution((SeqNum)i);
    if (slowStartCount-- > 0) cwsh.onStartingSlowCommit((SeqNum)i);
  }

  ASSERT_EQ(true, changed);
  ASSERT_EQ(CommitPath::FAST_WITH_THRESHOLD, cwsh.getCurrentFirstPath());

  for (++i; i <= 2 * ControllerWithSimpleHistory::EvaluationPeriod; ++i) {
    cwsh.onSendingPrePrepare((SeqNum)i, CommitPath::OPTIMISTIC_FAST, prePrepareTime);
    changed = cwsh.onNewSeqNumberExecution((SeqNum)i);
    cwsh.onStartingSlowCommit((SeqNum)i);
  }

  ASSERT_EQ(true, changed);
  ASSERT_EQ(CommitPath::SLOW, cwsh.getCurrentFirstPath());

  char buf[5] = {'m', 'o', 's', 'h', 'e'};
  Digest d{buf, 5};
  ThreshSigMock th;
  for (++i; i <= 3 * ControllerWithSimpleHistory::EvaluationPeriod; ++i) {
    cwsh.onSendingPrePrepare((SeqNum)i, CommitPath::OPTIMISTIC_FAST, prePrepareTime);
    changed = cwsh.onNewSeqNumberExecution((SeqNum)i);
    for (auto id : {1, 2, 3, 4}) {
      auto p = impl::PreparePartialMsg::create(0, i, id, d, &th);
      cwsh.onMessage(p);
      delete p;
    }
  }

  ASSERT_EQ(true, changed);
  ASSERT_EQ(CommitPath::FAST_WITH_THRESHOLD, cwsh.getCurrentFirstPath());
}

// Test - no upgrade
// Logic:
// 1)Construct controller with C == 0 i.e. disable FAST_WITH_THRESHOLD path.
// 2)Run loop for EvaluationPeriod times:
//  - call to onStartingSlowCommit for more than factor * EvaluationPeriod
// 3)Run loop for EvaluationPeriod times:
//  - Don't call onMessage i.e. no cooporation from other replicas.
TEST(ControllerWithSimpleHistory, no_upgrade) {
  uint16_t C = 0;
  uint16_t F = 1;
  ReplicaId replicaId = 0;
  ViewNum initialView = 0;
  SeqNum initialSeq = 0;
  ControllerWithSimpleHistory cwsh{C, F, replicaId, initialView, initialSeq};

  ASSERT_EQ(CommitPath::OPTIMISTIC_FAST, cwsh.getCurrentFirstPath());

  // Factor + 1, in order to trigger the degradation.
  auto slowStartCount =
      ControllerWithSimpleHistory_debugDowngradeFactor * ControllerWithSimpleHistory::EvaluationPeriod + 1;
  bool changed{false};
  auto prePrepareTime = std::chrono::steady_clock::now() - std::chrono::milliseconds(10);
  auto i = (size_t)1;
  for (; i <= ControllerWithSimpleHistory::EvaluationPeriod; ++i) {
    cwsh.onSendingPrePrepare((SeqNum)i, CommitPath::OPTIMISTIC_FAST, prePrepareTime);
    changed = cwsh.onNewSeqNumberExecution((SeqNum)i);
    if (slowStartCount-- > 0) cwsh.onStartingSlowCommit((SeqNum)i);
  }

  ASSERT_EQ(true, changed);
  ASSERT_EQ(CommitPath::SLOW, cwsh.getCurrentFirstPath());

  for (++i; i <= 2 * ControllerWithSimpleHistory::EvaluationPeriod; ++i) {
    cwsh.onSendingPrePrepare((SeqNum)i, CommitPath::OPTIMISTIC_FAST, prePrepareTime);
    changed = cwsh.onNewSeqNumberExecution((SeqNum)i);
  }

  ASSERT_EQ(false, changed);
  ASSERT_EQ(CommitPath::SLOW, cwsh.getCurrentFirstPath());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}