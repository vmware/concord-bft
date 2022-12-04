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

#if __has_include(<filesystem>)
#include <filesystem>
namespace fs = std::filesystem;
#elif __has_include(<experimental/filesystem>)
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#else
#error "Missing filesystem support"
#endif
#include <utt/Serialization.h>
#include <fstream>
#include <sstream>
#include "fileBasedUserStorage.hpp"
#include "serialization.hpp"

namespace utt::client {
FileBasedUserStorage::FileBasedUserStorage(const std::string& path)
    : state_path_{path + "/.state.json"}, pending_path_{path + "/.pending.json"} {
  if (fs::exists(pending_path_)) {
    fs::copy(pending_path_, state_path_);
    fs::remove(pending_path_);
  }
  if (fs::exists(state_path_)) {
    std::ifstream f(state_path_);
    current_state_ = json::parse(f);
  }
}

void FileBasedUserStorage::startTransaction() {
  if (!current_state_.contains("initialized")) current_state_["initialized"] = true;
}

void FileBasedUserStorage::commit() {
  std::ofstream out_state(pending_path_);
  out_state << current_state_ << std::endl;
  fs::copy(pending_path_, state_path_);
  fs::remove(pending_path_);
}

bool FileBasedUserStorage::isNewStorage() { return current_state_.contains("initialized"); }

void FileBasedUserStorage::setKeyPair(const std::pair<std::string, std::string>& keyPair) {
  current_state_["key_pair"] = {{"sk", keyPair.first}, {"pk", keyPair.second}};
}

void FileBasedUserStorage::setLastExecutedSn(uint64_t sn) { current_state_["last_executed_sn"] = sn; }

void FileBasedUserStorage::setClientSideSecret(const libutt::api::types::CurvePoint& s1) {
  std::stringstream ss;
  libutt::serializeVector(ss, s1);
  current_state_["s1"] = ss.str();
}

void FileBasedUserStorage::setSystemSideSecret(const libutt::api::types::CurvePoint& s2) {
  std::stringstream ss;
  libutt::serializeVector(ss, s2);
  current_state_["s2"] = ss.str();
}

void FileBasedUserStorage::setRcmSignature(const libutt::api::types::Signature& rcm_sig) {
  std::stringstream ss;
  libutt::serializeVector(ss, rcm_sig);
  current_state_["rcm_sig"] = ss.str();
}

void FileBasedUserStorage::setCoin(const libutt::api::Coin& c) {
  current_state_["coins"][c.getNullifier()] = libutt::api::serialize(c);
}

void FileBasedUserStorage::removeCoin(const libutt::api::Coin& c) { current_state_["coins"].erase(c.getNullifier()); }

uint64_t FileBasedUserStorage::getLastExecutedSn() { return current_state_["last_executed_sn"]; }

libutt::api::types::CurvePoint FileBasedUserStorage::getClientSideSecret() {
  std::stringstream ss;
  ss.str(current_state_["s1"]);
  libutt::api::types::CurvePoint ret;
  libutt::deserializeVector<uint64_t>(ss, ret);
  return ret;
}

libutt::api::types::CurvePoint FileBasedUserStorage::getSystemSideSecret() {
  std::stringstream ss;
  ss.str(current_state_["s2"]);
  libutt::api::types::CurvePoint ret;
  libutt::deserializeVector<uint64_t>(ss, ret);
  return ret;
}

libutt::api::types::Signature FileBasedUserStorage::getRcmSignature() {
  std::stringstream ss;
  ss.str(current_state_["rcm_sig"]);
  libutt::api::types::Signature ret;
  libutt::deserializeVector<uint8_t>(ss, ret);
  return ret;
}

std::vector<libutt::api::Coin> FileBasedUserStorage::getCoins() {
  std::vector<libutt::api::Coin> coins;
  for (const auto& serialized_coin : current_state_["coins"])
    coins.push_back(libutt::api::deserialize<libutt::api::Coin>(serialized_coin));
  return coins;
}

std::pair<std::string, std::string> FileBasedUserStorage::getKeyPair() {
  return {current_state_["key_pair"]["sk"], current_state_["key_pair"]["pk"]};
}
}  // namespace utt::client