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
#include <vector>
#include <memory>
#include "IStateTransfer.hpp"
#include "messages/MessageBase.hpp"

namespace bftEngine {

namespace bcst {

// A state transfer message
struct Msg {
  std::unique_ptr<char[]> msg_;
  uint32_t len_;
  uint16_t to_;
};

class TestReplica : public IReplicaForStateTransfer {
 public:
  ///////////////////////////////////////////////////////////////////////////
  // IReplicaForStateTransfer methods
  ///////////////////////////////////////////////////////////////////////////
  void onTransferringComplete(uint64_t checkpointNumberOfNewState) override{};

  void freeStateTransferMsg(char* m) override {
    char* p = (m - sizeof(bftEngine::impl::MessageBase::Header));
    std::free(p);
  }

  void sendStateTransferMessage(char* m, uint32_t size, uint16_t replicaId) override {
    std::unique_ptr<char[]> msg{new char[size]};
    memcpy(msg.get(), m, size);

    sent_messages_.push_back(Msg{std::move(msg), size, replicaId});
  }

  void changeStateTransferTimerPeriod(uint32_t timerPeriodMilli) override{};

  concordUtil::Timers::Handle addOneShotTimer(uint32_t timeoutMilli) override { return concordUtil::Timers::Handle(); }
  ///////////////////////////////////////////////////////////////////////////
  // Data - All public on purpose, so that it can be accessed by tests
  ///////////////////////////////////////////////////////////////////////////

  // All messages sent by the state transfer module
  std::vector<Msg> sent_messages_;
};

}  // namespace bcst

}  // namespace bftEngine
