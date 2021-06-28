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
#include "bftclient/seq_num_generator.h"

namespace cre::state {
struct State {
  uint64_t block;
  std::vector<uint8_t> data;
};

class IStateClient {
 public:
  virtual State getNextState(uint64_t lastKnownBlockId) = 0;
  virtual State getLatestClientUpdate(uint16_t clientId) = 0;
  virtual bool updateStateOnChain(const State& state) = 0;
  virtual void start() = 0;
  virtual void stop() = 0;
  virtual ~IStateClient() = default;
};

class PollBasedStateClient : public IStateClient {
 public:
  PollBasedStateClient(bft::client::Client* client,
                       uint64_t interval_timeout_ms,
                       uint64_t last_known_block,
                       const uint16_t id_);
  State getNextState(uint64_t lastKnownBlockId) override;
  State getLatestClientUpdate(uint16_t clientId) override;
  bool updateStateOnChain(const State& state) override;
  ~PollBasedStateClient();
  void start() override;
  void stop() override;

 private:
  State getNewStateImpl(uint64_t lastKnownBlockId);
  std::unique_ptr<bft::client::Client> bftclient_;
  uint16_t id_;
  uint64_t interval_timeout_ms_;
  uint64_t last_known_block_;
  std::queue<State> updates_;
  bft::client::SeqNumberGenerator sn_gen_;
  std::mutex lock_;
  std::mutex bftclient_lock_;
  std::condition_variable new_updates_;
  std::atomic_bool stopped{true};
  std::thread consumer_;
};

}  // namespace cre::state