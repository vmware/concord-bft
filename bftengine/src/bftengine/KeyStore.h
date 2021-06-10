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
namespace bftEngine::impl {

typedef int64_t SeqNum;  // TODO [TK] redefinition

/**
 *  Holds and persists public keys of all replicas.
 */
class ClusterKeyStore : public ResPagesClient<ClusterKeyStore> {
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
    LOG_INFO(KEY_EX_LOG, "size: " << size);
    ConcordAssertGT(sizeOfReservedPage(), 0);
    loadAllReplicasKeyStoresFromReservedPages();
  }

  void push(const KeyExchangeMsg& kem, const SeqNum& sn) {
    LOG_INFO(KEY_EX_LOG, kem.toString() << " seqnum: " << sn);
    clusterKeys_[kem.repID].push(kem.pubkey, sn);
    saveReplicaKeyStoreToReserevedPages(kem.repID);
    log();
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
}  // namespace bftEngine::impl
