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
#include "deque"
#include "IReservedPages.hpp"
#include "ReservedPages.hpp"
#include "KeyExchangeMsg.hpp"
namespace bftEngine::impl {

// A replica's key store.
// A queue with limit on its size.
// Queue's object is the key msg and its corresponding seq num
class ReplicaKeyStore : public concord::serialize::SerializableFactory<ReplicaKeyStore> {
 public:
  struct ReplicaKey : public concord::serialize::SerializableFactory<ReplicaKey> {
    KeyExchangeMsg msg;
    uint64_t seqnum{};

    ReplicaKey(const KeyExchangeMsg& other, const uint64_t& seqnum);
    ReplicaKey() {}

   protected:
    const std::string getVersion() const;
    void serializeDataMembers(std::ostream& outStream) const;
    void deserializeDataMembers(std::istream& inStream);
  };

  bool push(const KeyExchangeMsg& kem, const uint64_t& sn);
  void pop();
  inline void setKeysLimit(const uint16_t& l) { numOfKeysLimit_ = l; };
  inline uint16_t numKeys() const { return keys_.size(); };

  // Return by value, since reference might be invalidated.
  ReplicaKey current() const;
  // Advance the queue if conditions are met.
  bool rotate(const uint64_t& chknum);

  static ReplicaKeyStore deserializeReplicaKeyStore(const char* serializedRepStore, const int& size);

 protected:
  const std::string getVersion() const;
  void serializeDataMembers(std::ostream& outStream) const;
  void deserializeDataMembers(std::istream& inStream);

 private:
  std::deque<ReplicaKey> keys_;
  uint16_t numOfKeysLimit_{2};
  uint16_t seqNumsPerChkPoint_{150};  // TODO init from config
  uint16_t checkPointsForRotation_{2};
};

// Holds all replicas key store.
// Perform operations like rotation and push.
// Responsible on reserved pages operations.
class ClusterKeyStore : public ResPagesClient<ClusterKeyStore, 2> {
 public:
  ClusterKeyStore(const uint32_t& clusterSize, IReservedPages& reservedPages, const uint32_t& sizeOfReservedPage);
  bool push(const KeyExchangeMsg& kem, const uint64_t& sn);
  // iterate on all replcias
  std::vector<uint16_t> rotate(const uint64_t& chknum);
  KeyExchangeMsg getReplicaPublicKey(const uint16_t& repID) const;
  uint16_t numKeys(const uint16_t& repID) const { return clusterKeys_[repID].numKeys(); }

  // Reserved Pages
  bool loadAllReplicasKeyStoresFromReservedPages();
  std::optional<ReplicaKeyStore> loadReplicaKeyStoreFromReserevedPages(const uint16_t& repID);

  void saveAllReplicasKeyStoresToReservedPages();
  void saveReplicaKeyStoreToReserevedPages(const uint16_t& repID);
  std::set<uint16_t> exchangedReplicas;

 private:
  std::vector<ReplicaKeyStore> clusterKeys_;
  IReservedPages& reservedPages_;
  std::string buffer_;
};
}  // namespace bftEngine::impl
