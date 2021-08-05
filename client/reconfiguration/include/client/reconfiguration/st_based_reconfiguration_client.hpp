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
#include "cre_interfaces.hpp"
#include "bftclient/bft_client.h"
#include "bftclient/seq_num_generator.h"
#include "concord.cmf.hpp"

namespace concord::client::reconfiguration {

class STBasedReconfigurationClient : public IStateClient {
 public:
  STBasedReconfigurationClient(std::function<void(const std::vector<uint8_t>&)> updateStateCb,
                               const uint64_t& blockId,
                               uint64_t intervalTimeoutMs = 1000);
  State getNextState(uint64_t lastKnownBlockId) const override;
  State getLatestClientUpdate(uint16_t clientId) const override;
  bool updateState(const WriteState& state) override;
  void start(uint64_t lastKnownBlock) override { stopped_ = false; }
  void stop() override { stopped_ = true; }
  void pushUpdate(std::vector<State>&) override;

 protected:
  mutable std::queue<State> updates_;
  mutable std::mutex lock_;
  mutable std::condition_variable new_updates_;
  std::atomic_bool stopped_{true};
  std::function<void(const std::vector<uint8_t>&)> storeReconfigBlockToMdtCb_;
  uint64_t lastKnownReconfigurationCmdBlockId_ = 0;
  uint64_t interval_timeout_ms_ = 1000;
};
}  // namespace concord::client::reconfiguration