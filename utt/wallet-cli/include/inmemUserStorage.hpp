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
#include <utt-client-api/IStorage.hpp>

namespace utt::client {
class InMemoryUserStorage : public IStorage {
 public:
  bool isNewStorage() override;
  void setKeyPair(const std::pair<std::string, std::string>&) override;
  void setLastExecutedSn(uint64_t) override;
  void setClientSideSecret(const libutt::api::types::CurvePoint&) override;
  void setSystemSideSecret(const libutt::api::types::CurvePoint&) override;
  void setRcmSignature(const libutt::api::types::Signature&) override;
  void setCoin(const libutt::api::Coin&) override;
  void removeCoin(const libutt::api::Coin&) override;
  void startTransaction() override;
  void commit() override;

  uint64_t getLastExecutedSn() override;
  libutt::api::types::CurvePoint getClientSideSecret() override;
  libutt::api::types::CurvePoint getSystemSideSecret() override;
  libutt::api::types::Signature getRcmSignature() override;
  std::vector<libutt::api::Coin> getCoins() override;
  std::pair<std::string, std::string> getKeyPair() override;

 private:
  uint64_t lastExecutedSn_;
  libutt::api::types::CurvePoint s1_;
  libutt::api::types::CurvePoint s2_;
  libutt::api::types::Signature rcm_sig_;
  std::unordered_map<std::string, libutt::api::Coin> coins_;
  std::pair<std::string, std::string> keyPair_;
};
}  // namespace utt::client