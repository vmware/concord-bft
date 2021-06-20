// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <chrono>
#include <set>

#include "ControllerBase.hpp"
#include "SysConsts.hpp"
#include "SequenceWithActiveWindow.hpp"
#include "RollingAvgAndVar.hpp"
#include "TimeUtils.hpp"

namespace bftEngine {
namespace impl {

// Used by the Primary replica for commit path management -
// Determines current commit path.
// Evaluates whether to upgrade/downgrade current path.
// learns the average duration(with some fine tuning) of block execution.
class ControllerWithSimpleHistory : public ControllerBase {
 public:
  static constexpr size_t EvaluationPeriod = 64;

  // Parameters for adaptive tuning of slow path timer
  static constexpr uint32_t MinTimeToStartSlowPathMilli = 20;
  static constexpr uint32_t MaxTimeToStartSlowPathMilli = 2000;
  static constexpr uint32_t ResetOnSeqNum = 1000;
  static constexpr float MaxUpdateInTimeToStartSlowPath = 0.20F;
  static constexpr uint32_t defaultTimeToStartSlowPathMilli = 150;

  ControllerWithSimpleHistory(uint16_t C, uint16_t F, ReplicaId replicaId, ViewNum initialView, SeqNum initialSeq);

  // getter methods

  virtual CommitPath getCurrentFirstPath() override;
  virtual uint32_t timeToStartSlowPathMilli() override;
  // Timer that determines the frequency of checking whether to downgrade active requests to slow path
  virtual uint32_t slowPathsTimerMilli() override;

  // events

  // Initialize controller, in case replica is Primary in new view
  virtual void onNewView(ViewNum v, SeqNum s) override;

  // Called after executing all transactions in a block:
  //  1)Measures duration of execution where start time is PrePrepare.
  //  2)Calculates the mean and variance of block executions.
  //  3)in case its the end of EvaluationPeriod(water mark), calls onEndOfEvaluationPeriod.
  // Returns true if path was changed (can be true only at the end of EvaluationPeriod).
  virtual bool onNewSeqNumberExecution(SeqNum n) override;

  // Sets PrePrepare sending timepoint
  virtual void onSendingPrePrepare(SeqNum n, CommitPath commitPath) override;
  void onSendingPrePrepare(SeqNum n, CommitPath commitPath, const Time& timePoint);

  // Marks request that was downgraded to slow path
  virtual void onStartingSlowCommit(SeqNum n) override;

  // Adds replica to a set of replicas, that have replied to a PrePrepare msg with a corresponding PreparePartialMsg.
  virtual void onMessage(const PreparePartialMsg* m) override;
  // End - events

  // Returns true in case a sequence number is within the range of the current window.
  bool insideActiveWindow(const SeqNum& n);

  // Returns the duration in milli, from the sending of the preprepare msg.
  virtual int durationSincePrePrepare(SeqNum n) override;

  // Returns lower or upper bound, in case val is out of range, otherwise returns val.
  template <typename T>
  static const T& normalizeToRange(const T& lower, const T& upper, const T& val) {
    if (val < lower) return lower;
    if (val > upper) return upper;
    return val;
  }

  // Defines a lower bound from a value (multiplied by `1-factor`).
  template <typename T>
  static float relativeLowerBound(const float& factor, const T& val) {
    return ((1 - factor) * val);
  }

  // Defines an upper bound from a value (multiplied by `1+factor`).
  template <typename T>
  static float relativeUpperBound(const float& factor, const T& val) {
    return ((1 + factor) * val);
  }

  // Holds essential data about a request -
  //  - Timepoint of PrePrepare sending.
  //  - Request execution duration.
  //  - Whether request was downgraded to slow path.
  //  - Set of replicas, that participated in the slow path execution.
  class SeqNoInfo {
   public:
    SeqNoInfo() : switchToSlowPath(false), prePrepareTime(MinTime), durationMicro_(std::chrono::milliseconds::zero()) {}

    void resetAndFree() {
      switchToSlowPath = false;
      prePrepareTime = MinTime;
      durationMicro_ = std::chrono::microseconds::zero();
      replicas.clear();
    }

   private:
    bool switchToSlowPath;

    Time prePrepareTime;
    std::chrono::microseconds durationMicro_;

    // used when currentFirstPath == SLOW
    std::set<ReplicaId> replicas;  // replicas that have responded for this sequence number

    friend class ControllerWithSimpleHistory;

   public:
    // methods for SequenceWithActiveWindow
    static void init(SeqNoInfo& i, void* d) {}

    static void free(SeqNoInfo& i) { i.resetAndFree(); }

    static void reset(SeqNoInfo& i) { i.resetAndFree(); }
  };

 protected:
  const bool onlyOptimisticFast;
  const size_t c;
  const size_t f;
  const size_t numOfReplicas;
  const ReplicaId myId;

  // Holds a range of requests, starting from SeqNum to SeqNum+EvaluationPeriod
  SequenceWithActiveWindow<EvaluationPeriod, 1, SeqNum, SeqNoInfo, SeqNoInfo> recentActivity;

  CommitPath currentFirstPath;
  ViewNum currentView;
  bool isPrimary;

  RollingAvgAndVar avgAndStdOfExecTime;

  uint32_t currentTimeToStartSlowPathMilli;

  // Initializes this controller when replica becomes primary
  // Resets to default the threshold duration (currentTimeToStartSlowPathMilli).
  // When this threshold exceeds, request gets downgraded to slow path.
  void onBecomePrimary(ViewNum v, SeqNum s);

  // Changes primary execution path (e.g. SLOW_PATH, FAST_PATH), based on
  // current execution path and the success rate ratio of previous requests.
  // Also tunes the threshold duration for downgrading to slow path,
  // Returns true if path was changed
  bool onEndOfEvaluationPeriod();
};

}  // namespace impl
}  // namespace bftEngine
