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

#include "inmemUserStorage.hpp"

namespace utt::client {
void InMemoryUserStorage::setLastExecutedSn(uint64_t sn) {
  if (sn < lastExecutedSn_) throw std::runtime_error("trying to write an incorrect sequence number");
  lastExecutedSn_ = sn;
}
void InMemoryUserStorage::setRegistrationPartialCommitment(const libutt::api::Commitment& rcm1) { rcm1_ = rcm1; }
void InMemoryUserStorage::setRegistrationCommitment(const libutt::api::Commitment& rcm) { rcm_ = rcm; }
void InMemoryUserStorage::setCoin(const libutt::api::Coin& coin) {
  if (coins_.find(coin.getNullifier()) != coins_.end())
    throw std::runtime_error("trying to add an already exist coin to storage");
  coins_[coin.getNullifier()] = coin;
}
void InMemoryUserStorage::removeCoin(const libutt::api::Coin& coin) {
  if (coins_.find(coin.getNullifier()) != coins_.end())
    throw std::runtime_error("trying to remove an non existed coin to storage");
  coins_.erase(coin.getNullifier());
};
void InMemoryUserStorage::startTransaction() { lock_.lock(); }
void InMemoryUserStorage::commit() { lock_.unlock(); }

uint64_t InMemoryUserStorage::getLastExecutedSn() { return lastExecutedSn_; }
libutt::api::Commitment InMemoryUserStorage::getRegistrationCommitment() { return rcm1_; }
std::vector<libutt::api::Coin> InMemoryUserStorage::getCoins() {
  std::vector<libutt::api::Coin> coins;
  for (const auto& [_, c] : coins_) coins.push_back(c);
  return coins;
}
}  // namespace utt::client
