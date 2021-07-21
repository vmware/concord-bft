// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <cstdint>
#include <vector>
#include <functional>

namespace concord::client::reconfiguration {
struct State {
  uint64_t blockid;
  std::vector<uint8_t> data;
};

struct WriteState {
  std::vector<uint8_t> data;
  std::function<void()> callBack = nullptr;
};

class IStateClient {
 public:
  virtual State getNextState(uint64_t lastKnownBlockId) const = 0;
  virtual State getLatestClientUpdate(uint16_t clientId) const = 0;
  virtual bool updateStateOnChain(const WriteState& state) = 0;
  virtual void start(uint64_t lastKnownBlock) = 0;
  virtual void stop() = 0;
  virtual ~IStateClient() = default;
};

class IStateHandler {
 public:
  virtual bool validate(const State&) const = 0;
  virtual bool execute(const State&, WriteState&) = 0;
  virtual ~IStateHandler() = default;
};

}  // namespace concord::client::reconfiguration