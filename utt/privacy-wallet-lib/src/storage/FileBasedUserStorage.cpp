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
#include <xutils/Log.h>
#include <storage/FileBasedUserStorage.hpp>
#include "serialization.hpp"

namespace utt::client {
std::string bytesToHex(const std::string& bytes) {
  std::stringstream s;
  for (size_t i = 0; i < bytes.size(); i++) {
    // Convert from signed char to std::uint8_t and then to an unsigned non-char type so that it prints as an integer.
    const auto u = static_cast<std::uint8_t>(bytes[i]);
    s << std::hex << std::setw(2) << std::setfill('0') << static_cast<std::uint16_t>(u);
  }
  return s.str();
}

std::string bytesToHex(const std::vector<uint8_t>& bytes) {
  std::stringstream s;
  for (size_t i = 0; i < bytes.size(); i++) {
    s << std::hex << std::setw(2) << std::setfill('0') << static_cast<std::uint16_t>(bytes[i]);
  }
  return s.str();
}

std::vector<uint8_t> hexStringToBytes(const std::string& hex) {
  std::vector<uint8_t> bytes;

  for (unsigned int i = 0; i < hex.length(); i += 2) {
    std::string byteString = hex.substr(i, 2);
    uint8_t byte = (uint8_t)strtol(byteString.c_str(), NULL, 16);
    bytes.push_back(byte);
  }

  return bytes;
}

FileBasedUserStorage::FileBasedUserStorage(const std::string& path)
    : state_path_{path + "/.state.json"}, pending_path_{path + "/.pending.json"}, lock_path_{path + "/.LOCK"} {
  fs::create_directories(path);
  if (fs::exists(lock_path_)) {
    // If we have a lock file, then we have a pending file that we need to write to the actual storage.
    if (!fs::exists(pending_path_)) throw std::runtime_error("storage is corrupted");
    fs::copy(pending_path_, state_path_, fs::copy_options::overwrite_existing);
    fs::remove(lock_path_);
    fs::remove(pending_path_);
  }
  if (fs::exists(state_path_)) {
    std::ifstream f(state_path_);
    state_ = json::parse(f);
  }
  if (!state_.contains("last_executed_sn")) state_["last_executed_sn"] = 0;
}

void FileBasedUserStorage::startTransaction() {
  if (!state_.contains("initialized")) state_["initialized"] = true;
}

void FileBasedUserStorage::commit() {
  std::ofstream out_state(pending_path_);
  out_state << state_ << std::endl;
  out_state.close();
  // Creating the lockfile marks that are ready to copy the content of pending to the actual state
  std::ofstream lockfile(lock_path_);
  lockfile.close();
  fs::copy(pending_path_, state_path_, fs::copy_options::overwrite_existing);
  // Remove the lock only after a successful copy
  fs::remove(lock_path_);
  fs::remove(pending_path_);
}

bool FileBasedUserStorage::isNewStorage() { return !state_.contains("initialized"); }

void FileBasedUserStorage::setKeyPair(const std::pair<std::string, std::string>& keyPair) {
  state_["key_pair"] = {{"sk", bytesToHex(keyPair.first)}, {"pk", bytesToHex(keyPair.second)}};
}

void FileBasedUserStorage::setLastExecutedSn(uint64_t sn) { state_["last_executed_sn"] = sn; }

void FileBasedUserStorage::setClientSideSecret(const libutt::api::types::CurvePoint& s1) {
  std::stringstream ss;
  libutt::serializeVector(ss, s1);
  state_["s1"] = bytesToHex(ss.str());
}

void FileBasedUserStorage::setSystemSideSecret(const libutt::api::types::CurvePoint& s2) {
  std::stringstream ss;
  libutt::serializeVector(ss, s2);
  state_["s2"] = bytesToHex(ss.str());
}

void FileBasedUserStorage::setRcmSignature(const libutt::api::types::Signature& rcm_sig) {
  state_["rcm_sig"] = bytesToHex(rcm_sig);
}

void FileBasedUserStorage::setCoin(const libutt::api::Coin& c) {
  state_["coins"][bytesToHex(c.getNullifier())] = bytesToHex(libutt::api::serialize(c));
}

void FileBasedUserStorage::removeCoin(const libutt::api::Coin& c) {
  state_["coins"].erase(bytesToHex(c.getNullifier()));
}

uint64_t FileBasedUserStorage::getLastExecutedSn() { return state_["last_executed_sn"]; }

libutt::api::types::CurvePoint FileBasedUserStorage::getClientSideSecret() {
  if (!state_.contains("s1")) return {};
  std::stringstream ss;
  auto bytes = hexStringToBytes(state_["s1"]);
  ss.str(std::string(bytes.begin(), bytes.end()));
  libutt::api::types::CurvePoint ret;
  libutt::deserializeVector<uint64_t>(ss, ret);
  return ret;
}

libutt::api::types::CurvePoint FileBasedUserStorage::getSystemSideSecret() {
  if (!state_.contains("s2")) return {};
  std::stringstream ss;
  auto bytes = hexStringToBytes(state_["s2"]);
  ss.str(std::string(bytes.begin(), bytes.end()));
  libutt::api::types::CurvePoint ret;
  libutt::deserializeVector<uint64_t>(ss, ret);
  return ret;
}

libutt::api::types::Signature FileBasedUserStorage::getRcmSignature() {
  if (!state_.contains("rcm_sig")) return {};
  return hexStringToBytes(state_["rcm_sig"]);
}

std::vector<libutt::api::Coin> FileBasedUserStorage::getCoins() {
  std::vector<libutt::api::Coin> coins;
  for (const auto& serialized_coin : state_["coins"]) {
    coins.push_back(libutt::api::deserialize<libutt::api::Coin>(hexStringToBytes(serialized_coin)));
  }
  return coins;
}

std::pair<std::string, std::string> FileBasedUserStorage::getKeyPair() {
  if (!state_.contains("key_pair")) return {"", ""};
  auto sk = hexStringToBytes(state_["key_pair"]["sk"]);
  auto pk = hexStringToBytes(state_["key_pair"]["pk"]);
  return {std::string(sk.begin(), sk.end()), std::string(pk.begin(), pk.end())};
}
}  // namespace utt::client