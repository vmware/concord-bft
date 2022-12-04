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
#include "coin.hpp"

namespace utt::client {
class IStorage {
 public:
  class guard {
   public:
    guard(IStorage& s) : s_{s} { s_.startTransaction(); }
    ~guard() { s_.commit(); }

   private:
    IStorage& s_;
  };
  virtual ~IStorage() = default;
  virtual bool isNewStorage() = 0;
  virtual void setKeyPair(const std::pair<std::string, std::string>&) = 0;
  virtual void setLastExecutedSn(uint64_t) = 0;
  virtual void setClientSideSecret(const libutt::api::types::CurvePoint&) = 0;
  virtual void setSystemSideSecret(const libutt::api::types::CurvePoint&) = 0;
  virtual void setRcmSignature(const libutt::api::types::Signature&) = 0;
  virtual void setCoin(const libutt::api::Coin&) = 0;
  virtual void removeCoin(const libutt::api::Coin&) = 0;
  virtual void startTransaction() = 0;
  virtual void commit() = 0;

  virtual uint64_t getLastExecutedSn() = 0;
  virtual libutt::api::types::CurvePoint getClientSideSecret() = 0;
  virtual libutt::api::types::CurvePoint getSystemSideSecret() = 0;
  virtual libutt::api::types::Signature getRcmSignature() = 0;
  virtual std::vector<libutt::api::Coin> getCoins() = 0;
  virtual std::pair<std::string, std::string> getKeyPair() = 0;
};
}  // namespace utt::client