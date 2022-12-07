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

#include "IStorage.hpp"

namespace utt::client {
class ITransactionalStorage : public IStorage {
 public:
  class guard {
   public:
    guard(ITransactionalStorage& storage) : storage_{storage} { storage_.startTransaction(); }
    ~guard() { storage_.commit(); }

   private:
    ITransactionalStorage& storage_;
  };

  ITransactionalStorage(std::unique_ptr<IStorage> storage) : storage_{std::move(storage)} {}
  bool isNewStorage() override;
  void setKeyPair(const std::pair<std::string, std::string>&) override;
  void setLastExecutedSn(uint64_t) override;
  void setClientSideSecret(const libutt::api::types::CurvePoint&) override;
  void setSystemSideSecret(const libutt::api::types::CurvePoint&) override;
  void setRcmSignature(const libutt::api::types::Signature&) override;
  void setCoin(const libutt::api::Coin&) override;
  void removeCoin(const libutt::api::Coin&) override;

  uint64_t getLastExecutedSn() override;
  libutt::api::types::CurvePoint getClientSideSecret() override;
  libutt::api::types::CurvePoint getSystemSideSecret() override;
  libutt::api::types::Signature getRcmSignature() override;
  std::vector<libutt::api::Coin> getCoins() override;
  std::pair<std::string, std::string> getKeyPair() override;
  virtual void startTransaction() = 0;
  virtual void commit() = 0;

 protected:
  std::unique_ptr<IStorage> storage_;
};
}  // namespace utt::client