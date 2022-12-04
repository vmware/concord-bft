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
bool InMemoryUserStorage::isNewStorage() { return true; };
void InMemoryUserStorage::setKeyPair(const std::pair<std::string, std::string>& keyPair) { keyPair_ = keyPair; }
void InMemoryUserStorage::setLastExecutedSn(uint64_t sn) {
  if (sn < lastExecutedSn_) throw std::runtime_error("trying to write an incorrect sequence number");
  lastExecutedSn_ = sn;
}
void InMemoryUserStorage::setClientSideSecret(const libutt::api::types::CurvePoint& s1) { s1_ = s1; }
void InMemoryUserStorage::setSystemSideSecret(const libutt::api::types::CurvePoint& s2) { s2_ = s2; };
void InMemoryUserStorage::setRcmSignature(const libutt::api::types::Signature& rcm_sig) { rcm_sig_ = rcm_sig; }
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
void InMemoryUserStorage::startTransaction() {}
void InMemoryUserStorage::commit() {}

uint64_t InMemoryUserStorage::getLastExecutedSn() { return lastExecutedSn_; }
libutt::api::types::CurvePoint InMemoryUserStorage::getClientSideSecret() { return s1_; }
libutt::api::types::CurvePoint InMemoryUserStorage::getSystemSideSecret() { return s2_; }
libutt::api::types::Signature InMemoryUserStorage::getRcmSignature() { return rcm_sig_; }
std::vector<libutt::api::Coin> InMemoryUserStorage::getCoins() {
  std::vector<libutt::api::Coin> coins;
  for (const auto& [_, c] : coins_) coins.push_back(c);
  return coins;
}
std::pair<std::string, std::string> InMemoryUserStorage::getKeyPair() { return keyPair_; }
}  // namespace utt::client
