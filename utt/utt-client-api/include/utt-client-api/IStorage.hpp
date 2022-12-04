// UTT Client API
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once
#include <vector>

#include "commitment.hpp"
#include "coin.hpp"

namespace utt::client {
class IStorage {
 public:
  virtual void setLastExecutedSn(uint64_t) = 0;
  virtual void setRegistrationPartialCommitment(const libutt::api::Commitment&) = 0;
  virtual void setRegistrationCommitment(const libutt::api::Commitment&) = 0;
  virtual void setCoin(const libutt::api::Coin&) = 0;
  virtual void removeCoin(const libutt::api::Coin&) = 0;
  virtual void startTransaction() = 0;
  virtual void commit() = 0;

  virtual uint64_t getLastExecutedSn() = 0;
  virtual libutt::api::Commitment getRegistrationCommitment() = 0;
  virtual std::vector<libutt::api::Coin> getCoins() = 0;
};
}  // namespace utt::client