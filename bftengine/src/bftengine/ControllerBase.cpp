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

#include "ControllerBase.hpp"

namespace bftEngine {
namespace impl {

///////////////////////////////////////////////////////////////////////////////
// SimpleController
///////////////////////////////////////////////////////////////////////////////

SimpleController::SimpleController(size_t degradationThreshold, int C)
    : dThreshold(degradationThreshold), onlyOptimisticFast(C == 0) {
  currentFirstPath = CommitPath::OPTIMISTIC_FAST;
  numberOfSlowCommitPaths = 0;
}

SimpleController::~SimpleController() {}

CommitPath SimpleController::getCurrentFirstPath() { return currentFirstPath; }

uint32_t SimpleController::timeToStartSlowPathMilli() { return 150; }

uint32_t SimpleController::slowPathsTimerMilli() { return 150; }

int SimpleController::durationSincePrePrepare(SeqNum n) { return -1; }

void SimpleController::onStartingSlowCommit(SeqNum n) {
  numberOfSlowCommitPaths++;

  if (onlyOptimisticFast) {
    if (currentFirstPath == CommitPath::OPTIMISTIC_FAST && numberOfSlowCommitPaths == (dThreshold))
      currentFirstPath = CommitPath::SLOW;
  } else {
    if (currentFirstPath == CommitPath::OPTIMISTIC_FAST && numberOfSlowCommitPaths == dThreshold)
      currentFirstPath = CommitPath::FAST_WITH_THRESHOLD;
    else if (currentFirstPath == CommitPath::FAST_WITH_THRESHOLD && numberOfSlowCommitPaths == (dThreshold * 2))
      currentFirstPath = CommitPath::SLOW;
  }
}

}  // namespace impl
}  // namespace bftEngine
