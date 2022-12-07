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

#include <storage/ITransactionalStorage.hpp>
#include <mutex>
#include <optional>
#include <nlohmann/json.hpp>

using json = nlohmann::json;
namespace utt::client {
struct FileBasedUserStorageState {
  json state_;
} class FileBasedUserStorage : public IStorage {
 public:
  FileBasedUserStorage(std::shared_ptr<FileBasedUserStorageState> state) : state_{state} {}
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
  std::shared_ptr<FileBasedUserStorageState> state_;
};

class FileBasedTransactionalStorage : public ITransactionalStorage {
 public:
  FileBasedTransactionalStorage(std::unique_ptr<IStorage> storage,
                                std::shared_ptr<FileBasedUserStorageState> state,
                                const std::string& path);
  void startTransaction() override;
  void commit() override;
  static std::unique_ptr<FileBasedTransactionalStorage> create(const std::string& path);

 private:
  std::string state_path_;
  std::string pending_path_;
  std::string lock_path_;
  std::shared_ptr<FileBasedUserStorageState> state_;
}
}  // namespace utt::client