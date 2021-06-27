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
#include <map>
#include <mutex>
#include <condition_variable>
#include <thread>
#include "bftclient/bft_client.h"
namespace cre::state {
struct State {
  uint64_t block;
  std::vector<uint8_t> data;
};

class IStateClient {
 public:
  virtual State getNextState(uint64_t lastKnownBlockId) = 0;
  virtual ~IStateClient() = default;
};

class PullBasedStateClient : public IStateClient {
 public:
  PullBasedStateClient(std::shared_ptr<bft::client::Client> client,
                       uint64_t interval_timeout_ms,
                       uint64_t last_known_block,
                       const uint16_t id_);
  State getNextState(uint64_t lastKnownBlockId) override;
  ~PullBasedStateClient();

 private:
  State getNewStateImpl(uint64_t lastKnownBlockId);
  std::shared_ptr<bft::client::Client> bftclient_;
  uint16_t id_;
  uint64_t interval_timeout_ms_;
  uint64_t last_known_block_;
  std::map<uint64_t, State> updates_;
  std::mutex lock_;
  std::condition_variable new_updates_;
  std::atomic_bool stopped{true};
  std::thread consumer_;
};

}  // namespace cre::state