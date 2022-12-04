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
#include <unordered_map>
#include <mutex>
#include <utt-client-api/IStorage.hpp>

namespace utt::client {
class InMemoryUserStorage : public IStorage {
 public:
  void setLastExecutedSn(uint64_t) override;
  void setRegistrationPartialCommitment(const libutt::api::Commitment&) override;
  void setRegistrationCommitment(const libutt::api::Commitment&) override;
  void setCoin(const libutt::api::Coin&) override;
  void removeCoin(const libutt::api::Coin&) override;
  void startTransaction() override;
  void commit() override;

  uint64_t getLastExecutedSn() override;
  libutt::api::Commitment getRegistrationCommitment() override;
  std::vector<libutt::api::Coin> getCoins() override;

 private:
  uint64_t lastExecutedSn_;
  libutt::api::Commitment rcm1_;
  libutt::api::Commitment rcm_;
  std::unordered_map<std::string, libutt::api::Coin> coins_;
  std::mutex lock_;
};
}  // namespace utt::client