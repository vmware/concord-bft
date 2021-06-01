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

#pragma once
#include "Serializable.h"
#include "IReservedPages.hpp"
#include "ReservedPagesClient.hpp"
#include "KeyExchangeMsg.hpp"
#include <map>
#include <optional>
#include "sha_hash.hpp"

namespace bftEngine::impl {

typedef int64_t SeqNum;  // TODO [TK] redefinition

/**
 *  Holds and persists public keys of all replicas.
 */
class ClusterKeyStore : public ResPagesClient<ClusterKeyStore, 2> {
 public:
  /**
   * Persistent public keys store
   */
  struct PublicKeys : public concord::serialize::SerializableFactory<PublicKeys> {
    void push(const std::string& pub, const SeqNum& sn) {
      auto res = keys.insert(std::make_pair(sn, pub));
      if (!res.second) ConcordAssert(pub == res.first->second)  // if existed expect same key
    }
    void serializeDataMembers(std::ostream& outStream) const { serialize(outStream, keys); }
    void deserializeDataMembers(std::istream& inStream) { deserialize(inStream, keys); }
    std::map<SeqNum, std::string> keys;
  };

  ClusterKeyStore(uint32_t size) : clusterSize_(size), buffer_(sizeOfReservedPage(), 0) {
    ConcordAssertGT(sizeOfReservedPage(), 0);
    loadAllReplicasKeyStoresFromReservedPages();
  }

  void push(const KeyExchangeMsg& kem, const SeqNum& sn) {
    LOG_INFO(KEY_EX_LOG, kem.toString() << " seqnum: " << sn);
    clusterKeys_[kem.repID].push(kem.pubkey, sn);
    saveReplicaKeyStoreToReserevedPages(kem.repID);
  }

  const std::string getKey(const uint16_t& repId, const SeqNum& sn) const {
    try {
      return clusterKeys_.at(repId).keys.at(sn);
    } catch (const std::out_of_range& e) {
      LOG_FATAL(KEY_EX_LOG, "key not found for replica: " << repId << " seqnum: " << sn);
      ConcordAssert(false);
    }
  }

  const uint32_t numOfExchangedReplicas() const { return clusterKeys_.size(); }

  bool keyExists(uint16_t repId) const { return clusterKeys_.find(repId) != clusterKeys_.end(); }

  PublicKeys keys(uint16_t repId) const {
    try {
      return clusterKeys_.at(repId);
    } catch (const std::out_of_range& e) {
      LOG_FATAL(KEY_EX_LOG, "clusterKeys_.at() has failed for " << KVLOG(repId) << e.what());
      throw;
    }
  }

  void log() const {
    LOG_INFO(KEY_EX_LOG, "Cluster Public Keys (size " << clusterSize_ << "):");
    for (auto [repid, PKs] : clusterKeys_)
      for (auto [sn, pubkey] : PKs.keys)
        LOG_INFO(KEY_EX_LOG, "repId:" << repid << "\tseqnum: " << sn << "\tpubkey: " << pubkey);
  }

  // Reserved Pages
  /**
   * @return number of replicas with keys
   */
  uint16_t loadAllReplicasKeyStoresFromReservedPages();
  std::optional<PublicKeys> loadReplicaKeyStoreFromReserevedPages(const uint16_t& repID);

  void saveAllReplicasKeyStoresToReservedPages() {
    for (uint16_t i = 0; i < (uint16_t)clusterKeys_.size(); i++) saveReplicaKeyStoreToReserevedPages(i);
  }
  void saveReplicaKeyStoreToReserevedPages(const uint16_t& repID);

 private:
  // replica id -> public keys
  std::map<uint16_t, PublicKeys> clusterKeys_;
  const uint32_t clusterSize_;

  std::string buffer_;
};

// Manages the BFT state of the clients public keys:
// It stores the digest of the keys in reserved pages.
// On construction it compares the input keys (replica keys) against the reserved pages and setst the published flag
// to true in case of a match, otherwise it set to false and the replica won't process external requests until
// publish.
class ClientKeyStore : public ResPagesClient<ClientKeyStore, 4, 1> {
 public:
  bool published_{false};

  ClientKeyStore(const std::string& keys) { checkAndSetState(keys); }

  ClientKeyStore() = delete;

  // Save digest of client keys to res pages and sets a flag to true.
  void commit(const std::string& keys) {
    auto hashed_keys = concord::util::SHA3_256().digest(keys.c_str(), keys.size());
    auto strHashed_keys = std::string(hashed_keys.begin(), hashed_keys.end());
    ConcordAssertEQ(strHashed_keys.size(), concord::util::SHA3_256::SIZE_IN_BYTES);
    saveReservedPage(0, strHashed_keys.size(), strHashed_keys.c_str());
    published_ = true;
    LOG_INFO(ON_CHAIN_LOG, "Clients keys were updated");
  }

  bool compareAgainstReservedPage(const std::string& keys) {
    auto hashed_keys = concord::util::SHA3_256().digest(keys.c_str(), keys.size());
    auto strHashed_keys = std::string(hashed_keys.begin(), hashed_keys.end());
    std::string res_page_version(concord::util::SHA3_256::SIZE_IN_BYTES, '\0');
    loadReservedPage(0, res_page_version.length(), res_page_version.data());
    return strHashed_keys == res_page_version;
  }

  void saveToReservedPages(const std::string& keys) {
    auto hashed_keys = concord::util::SHA3_256().digest(keys.c_str(), keys.size());
    auto strHashed_keys = std::string(hashed_keys.begin(), hashed_keys.end());
    saveReservedPage(0, strHashed_keys.size(), strHashed_keys.c_str());
  }

  // Checks if the input param keys (the replica keys) is equal to the keys stored in reserved pages.
  // and sets the publish flag accordingly.
  void checkAndSetState(const std::string& keys) {
    if (!compareAgainstReservedPage(keys)) {
      LOG_WARN(ON_CHAIN_LOG, "Clients keys are not dated or empty, will not accept msgs until an update");
      published_ = false;
      return;
    }
    LOG_DEBUG(ON_CHAIN_LOG, "Clients keys were publish");
    published_ = true;
  }
};
}  // namespace bftEngine::impl
