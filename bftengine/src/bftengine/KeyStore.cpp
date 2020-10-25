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

///////////////////////////REPLICA KEY STORE//////////////////////////////////

bool ReplicaKeyStore::push(const KeyExchangeMsg& kem, const uint64_t& sn) {
  if (keys_.size() >= numOfKeysLimit_) {
    LOG_ERROR(KEY_EX_LOG, "Keys limit for replica exceeds, limit " << numOfKeysLimit_);
    return false;
  }

  keys_.emplace_back(kem, sn);
  return true;
}

ReplicaKeyStore::ReplicaKey ReplicaKeyStore::current() const {
  if (keys_.empty()) {
    LOG_FATAL(KEY_EX_LOG, "Replica key store is empty");
    ConcordAssertNE(keys_.empty(), true);
  }

  return keys_.front();
}

void ReplicaKeyStore::pop() {
  if (keys_.empty()) return;
  keys_.pop_front();
}

const std::string ReplicaKeyStore::getVersion() const { return "1"; }

void ReplicaKeyStore::serializeDataMembers(std::ostream& outStream) const {
  serialize(outStream, numOfKeysLimit_);
  serialize(outStream, keys_);
}

void ReplicaKeyStore::deserializeDataMembers(std::istream& inStream) {
  deserialize(inStream, numOfKeysLimit_);
  deserialize(inStream, keys_);
}

ReplicaKeyStore ReplicaKeyStore::deserializeReplicaKeyStore(const char* serializedRepStore, const int& size) {
  ConcordAssertGT(size, 0);
  std::stringstream ss;
  ReplicaKeyStore ks;
  ss.write(serializedRepStore, std::streamsize(size));
  deserialize(ss, ks);
  return ks;
}

bool ReplicaKeyStore::rotate(const uint64_t& chknum) {
  if (keys_.size() < 2) return false;
  auto chekPointSeqNum = chknum * seqNumsPerChkPoint_;
  // Since it's FIFO we need to check the second element only.
  uint16_t seqNumsSinceKeyExchangeMsg = chekPointSeqNum - keys_[1].seqnum;
  ConcordAssertGE(seqNumsSinceKeyExchangeMsg, 0);
  // if checkpoint is less than desired
  if (seqNumsSinceKeyExchangeMsg < (checkPointsForRotation_ - 1) * seqNumsPerChkPoint_) {
    return false;
  }
  // if somehow rotation wasn't performed on desired checkpoint.
  ConcordAssertLT(seqNumsSinceKeyExchangeMsg, checkPointsForRotation_ * seqNumsPerChkPoint_);
  LOG_DEBUG(KEY_EX_LOG,
            "Key rotation for replica " << keys_[1].msg.repID << " recieved on seqnum " << keys_[1].seqnum
                                        << " rotated on " << chekPointSeqNum);
  keys_.pop_front();
  return true;
}

////////////////////////REPLCIA KEY///////////////////////////

const std::string ReplicaKeyStore::ReplicaKey::getVersion() const { return "1"; }

void ReplicaKeyStore::ReplicaKey::serializeDataMembers(std::ostream& outStream) const {
  serialize(outStream, msg);
  serialize(outStream, seqnum);
}

void ReplicaKeyStore::ReplicaKey::deserializeDataMembers(std::istream& inStream) {
  deserialize(inStream, msg);
  deserialize(inStream, seqnum);
}

ReplicaKeyStore::ReplicaKey::ReplicaKey(const KeyExchangeMsg& other, const uint64_t& seqnum)
    : msg(other), seqnum(seqnum) {}

//////////////////////CLUSTER KEY STORE////////////////////////

ClusterKeyStore::ClusterKeyStore(const uint32_t& clusterSize,
                                 IReservedPages& reservedPages,
                                 const uint32_t& sizeOfReservedPage)
    : clusterKeys_(clusterSize), reservedPages_(reservedPages), buffer_(sizeOfReservedPage, 0) {
  ConcordAssertGT(sizeOfReservedPage, 0);
  loadAllReplicasKeyStoresFromReservedPages();
}

bool ClusterKeyStore::loadAllReplicasKeyStoresFromReservedPages() {
  exchangedReplicas.clear();
  for (uint16_t i = 0; i < (uint16_t)clusterKeys_.size(); i++) {
    auto repKeys = loadReplicaKeyStoreFromReserevedPages(i);

    if (!repKeys.has_value()) continue;
    exchangedReplicas.insert(i);
    clusterKeys_[i] = std::move(repKeys.value());
  }

  if (exchangedReplicas.size() < clusterKeys_.size()) {
    // If not first start, all replicas should be deseriaized correctly
    // Unless for some reason we crashed before completing full exchange
    // TODO decide how we want to handle this error.
    if (exchangedReplicas.size() > 0) {
      LOG_WARN(KEY_EX_LOG, "Partial set of replicas keys were loaded from reserved pages");
      return false;
    }

    LOG_INFO(KEY_EX_LOG, "No replicas keys were loaded from reserved pages");
    return false;
  }

  LOG_INFO(KEY_EX_LOG, "All replicas keys were loaded from reserved pages");
  return true;
}

std::optional<ReplicaKeyStore> ClusterKeyStore::loadReplicaKeyStoreFromReserevedPages(const uint16_t& repID) {
  // TODO: The check of the return value doesn't discriminate between a missing reserved page + the last checkpoint
  // being 0 and a failure to load. However, the check is still needed as a failure to load might leave the buffer_
  // variable in a wrong state. Additionally, we need to be sure that returning an empty optional is the right thing to
  // do.
  if (!reservedPages_.loadReservedPage(resPageOffset() + repID, buffer_.size(), buffer_.data())) {
    LOG_INFO(KEY_EX_LOG, "Couldn't load replica key store [" << repID << "] from reserved pages, first start?");
    return {};
  }
  try {
    return ReplicaKeyStore::deserializeReplicaKeyStore(buffer_.c_str(), buffer_.size());
  } catch (std::exception& e) {
    LOG_INFO(KEY_EX_LOG, "Couldn't deserialize replica key store [" << repID << "] from reserved pages, first start?");
    return {};
  }
}

void ClusterKeyStore::saveAllReplicasKeyStoresToReservedPages() {
  for (uint16_t i = 0; i < (uint16_t)clusterKeys_.size(); i++) {
    saveReplicaKeyStoreToReserevedPages(i);
  }
}

void ClusterKeyStore::saveReplicaKeyStoreToReserevedPages(const uint16_t& repID) {
  std::stringstream ss;
  concord::serialize::Serializable::serialize(ss, clusterKeys_.at(repID));
  auto rkStr = ss.str();
  reservedPages_.saveReservedPage(resPageOffset() + repID, rkStr.size(), rkStr.c_str());
}

bool ClusterKeyStore::push(const KeyExchangeMsg& kem, const uint64_t& sn) {
  if (kem.repID >= clusterKeys_.size()) {
    LOG_ERROR(KEY_EX_LOG, "Replica id is out of range " << kem.repID);
    return false;
  }

  if (!clusterKeys_[kem.repID].push(kem, sn)) return false;

  LOG_DEBUG(KEY_EX_LOG, "pushed key " << kem.toString());
  saveReplicaKeyStoreToReserevedPages(kem.repID);
  return true;
}

std::vector<uint16_t> ClusterKeyStore::rotate(const uint64_t& chknum) {
  std::vector<uint16_t> ret;
  uint16_t idx = 0;
  for (auto& replicaKeyStore : clusterKeys_) {
    if (!replicaKeyStore.rotate(chknum)) continue;
    ret.push_back(idx);

    saveReplicaKeyStoreToReserevedPages(idx);
    ++idx;
  }
  return ret;
}

KeyExchangeMsg ClusterKeyStore::getReplicaPublicKey(const uint16_t& repID) const {
  if (repID >= clusterKeys_.size()) {
    LOG_ERROR(KEY_EX_LOG, "Replica id is out of range " << repID);
    ConcordAssertGE(repID, clusterKeys_.size());
  }
  return clusterKeys_[repID].current().msg;
}

}  // namespace bftEngine::impl