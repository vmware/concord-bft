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
  PullBasedStateClient(std::shared_ptr<bft::client::Client> client, uint32_t id_);
  State getNextState(uint64_t lastKnownBlockId) override;

 private:
  std::shared_ptr<bft::client::Client> bftclient_;
  uint64_t lastKnownBlock_;
  uint32_t id_;
};

}  // namespace cre::state