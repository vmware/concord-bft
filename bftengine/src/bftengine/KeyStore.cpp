// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "KeyStore.h"

#include "assertUtils.hpp"

namespace bftEngine::impl {

uint16_t ClusterKeyStore::loadAllReplicasKeyStoresFromReservedPages() {
  clusterKeys_.clear();
  for (uint16_t i = 0; i < clusterSize_; i++) {
    auto repKeys = loadReplicaKeyStoreFromReserevedPages(i);

    if (!repKeys.has_value()) continue;
    clusterKeys_[i] = std::move(repKeys.value());
  }
  log();
  return clusterKeys_.size();
}

std::optional<ClusterKeyStore::PublicKeys> ClusterKeyStore::loadReplicaKeyStoreFromReserevedPages(
    const uint16_t& repID) {
  if (!loadReservedPage(repID, buffer_.size(), buffer_.data())) {
    LOG_INFO(KEY_EX_LOG, "Failed to load reserved page for replica " << repID << ", first start?");
    return {};
  }
  try {
    std::istringstream iss(buffer_);
    PublicKeys ks;
    PublicKeys::deserialize(iss, ks);
    for (auto [sn, pk] : ks.keys) LOG_DEBUG(KEY_EX_LOG, "rid: " << repID << " seqnum: " << sn << " pubkey: " << pk);
    return ks;
  } catch (const std::exception& e) {
    LOG_FATAL(KEY_EX_LOG,
              "Failed to deserialize replica key store [" << repID << "] from reserved pages, reason: " << e.what());
    ConcordAssert(false);
  }
}

void ClusterKeyStore::saveReplicaKeyStoreToReserevedPages(const uint16_t& repID) {
  PublicKeys clusterKey;
  try {
    clusterKey = clusterKeys_.at(repID);
  } catch (const std::out_of_range& e) {
    LOG_FATAL(KEY_EX_LOG, "clusterKeys_.at() failed for " << KVLOG(repID) << e.what());
    throw;
  }

  std::ostringstream oss;
  concord::serialize::Serializable::serialize(oss, clusterKey);
  auto rkStr = oss.str();
  saveReservedPage(repID, rkStr.size(), rkStr.c_str());
}

// Save clients keys to res pages and sets `published` to true.
void ClientKeyStore::save(const std::string& keys) {
  auto hashed_keys = concord::util::SHA3_256().digest(keys.c_str(), keys.size());
  auto strHashed_keys = std::string(hashed_keys.begin(), hashed_keys.end());
  ConcordAssertEQ(strHashed_keys.size(), concord::util::SHA3_256::SIZE_IN_BYTES);
  saveReservedPage(0, strHashed_keys.size(), strHashed_keys.c_str());
  published_ = true;
  LOG_INFO(KEY_EX_LOG, "Clients keys were updated, size " << keys.size());
}

std::string ClientKeyStore::load() {
  std::string res_page_version(concord::util::SHA3_256::SIZE_IN_BYTES, '\0');
  loadReservedPage(0, res_page_version.length(), res_page_version.data());
  return res_page_version == std::string(concord::util::SHA3_256::SIZE_IN_BYTES, '\0') ? "" : res_page_version;
}

}  // namespace bftEngine::impl
