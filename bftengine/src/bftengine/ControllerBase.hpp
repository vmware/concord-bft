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

#include <cstddef>
#include <stdint.h>

#include "PrimitiveTypes.hpp"

namespace bftEngine {
namespace impl {

class PreparePartialMsg;

///////////////////////////////////////////////////////////////////////////////
// ControllerBase
///////////////////////////////////////////////////////////////////////////////

class ControllerBase {
 public:
  virtual ~ControllerBase() {}

  // getter methods

  virtual CommitPath getCurrentFirstPath() = 0;
  virtual uint32_t timeToStartSlowPathMilli() = 0;
  virtual uint32_t slowPathsTimerMilli() = 0;

  // events (used to pass information to the controller)

  virtual void onNewView(ViewNum v, SeqNum s) {}
  virtual bool onNewSeqNumberExecution(SeqNum n) { return false; }

  virtual void onSendingPrePrepare(SeqNum n, CommitPath commitPath) {}
  virtual void onStartingSlowCommit(SeqNum n) {}
  virtual void onMessage(const PreparePartialMsg* m) {}

  // TODO(GG): add more methods that may be useful  by controllers
  virtual int durationSincePrePrepare(SeqNum n) = 0;
};

///////////////////////////////////////////////////////////////////////////////
// SimpleController
///////////////////////////////////////////////////////////////////////////////

class SimpleController : public ControllerBase {
 public:
  SimpleController(size_t degradationThreshold, int C);
  virtual ~SimpleController();

  // getter methods

  virtual CommitPath getCurrentFirstPath() override;
  virtual uint32_t timeToStartSlowPathMilli() override;
  virtual uint32_t slowPathsTimerMilli() override;

  // events (used to pass information to the controller)

  virtual void onStartingSlowCommit(SeqNum n) override;

  // Not implemented returns -1
  virtual int durationSincePrePrepare(SeqNum n) override;

 private:
  CommitPath currentFirstPath;
  size_t numberOfSlowCommitPaths;

  const size_t dThreshold;
  const bool onlyOptimisticFast;
};

}  // namespace impl
}  // namespace bftEngine
