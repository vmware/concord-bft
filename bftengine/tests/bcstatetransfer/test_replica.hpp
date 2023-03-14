// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
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

#include <cstdlib>
#include <cstring>
#include <deque>
#include <memory>

#include "IStateTransfer.hpp"
#include "messages/MessageBase.hpp"

namespace bftEngine {

namespace bcst {

// A state transfer message
class Msg {
 public:
  Msg(char* data, uint32_t size, uint16_t destReplicaId) : data_{new char[size]}, len_(size), to_(destReplicaId) {
    memcpy(data_.get(), data, size);
  }
  Msg() = delete;

  std::unique_ptr<char[]> data_;
  uint32_t len_;
  uint16_t to_;
};

class TestReplica : public IReplicaForStateTransfer {
 public:
  TestReplica() : onTransferringCompleteCalled_(false), oneShotTimerDurationMilli_{0}, oneShotTimerTriggered_{false} {};
  ///////////////////////////////////////////////////////////////////////////
  // IReplicaForStateTransfer methods
  ///////////////////////////////////////////////////////////////////////////
  void onTransferringComplete(uint64_t checkpointNumberOfNewState) override { onTransferringCompleteCalled_ = true; };

  void freeStateTransferMsg(char* msg) override {
    // Same behavior as in ReplicaForStateTransfer
    std::free(msg - sizeof(MessageBase::Header));
  }

  void sendStateTransferMessage(char* m, uint32_t size, uint16_t replicaId) override {
    sent_messages_.emplace_back(Msg(m, size, replicaId));
  }

  void changeStateTransferTimerPeriod(uint32_t timerPeriodMilli) override{};

  std::pair<bool, uint32_t> popOneShotTimerDurationMilli() {
    std::pair<bool, uint32_t> p{oneShotTimerTriggered_, oneShotTimerDurationMilli_};
    oneShotTimerTriggered_ = false;
    oneShotTimerDurationMilli_ = 0;
    return p;
  }
  concordUtil::Timers::Handle addOneShotTimer(uint32_t timeoutMilli) override {
    oneShotTimerDurationMilli_ = timeoutMilli;
    oneShotTimerTriggered_ = true;
    return concordUtil::Timers::Handle();
  }
  ///////////////////////////////////////////////////////////////////////////
  // Data - All public on purpose, so that it can be accessed by tests
  ///////////////////////////////////////////////////////////////////////////

  // All messages sent by the state transfer module
  std::deque<Msg> sent_messages_;
  bool onTransferringCompleteCalled_;
  uint32_t oneShotTimerDurationMilli_;  // 0 if not triggered
  bool oneShotTimerTriggered_;
};

}  // namespace bcst

}  // namespace bftEngine
