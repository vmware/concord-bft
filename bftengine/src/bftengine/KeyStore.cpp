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

////////////////////////////// KEY EXCHANGE MSG//////////////////////////////

const std::string KeyExchangeMsg::getVersion() const { return "1"; }

KeyExchangeMsg KeyExchangeMsg::deserializeMsg(const char* serializedMsg, const int& size) {
  std::stringstream ss;
  KeyExchangeMsg ke;
  ss.write(serializedMsg, std::streamsize(size));
  deserialize(ss, ke);
  return ke;
}

void KeyExchangeMsg::serializeDataMembers(std::ostream& outStream) const {
  serialize(outStream, key);
  serialize(outStream, signature);
  serialize(outStream, repID);
}

void KeyExchangeMsg::deserializeDataMembers(std::istream& inStream) {
  deserialize(inStream, key);
  deserialize(inStream, signature);
  deserialize(inStream, repID);
}

std::string KeyExchangeMsg::toString() const {
  std::stringstream ss;
  ss << "key [" << key << "] signature [" << signature << "] replica id [" << repID << "]";
  return ss.str();
}

///////////////////////////REPLICA KEY STORE//////////////////////////////////

bool ReplicaKeyStore::push(const KeyExchangeMsg& kem, const uint64_t& sn) {
  if (keys_.size() >= numOfKeysLimit_) {
    LOG_ERROR(GL, "KEY EXCHANGE: keys limit for replica exceeds, limit " << numOfKeysLimit_);
    return false;
  }

  keys_.emplace_back(kem, sn);
  return true;
}

ReplicaKeyStore::ReplicaKey ReplicaKeyStore::current() const {
  if (keys_.empty()) {
    LOG_FATAL(GL, "KEY EXCHANGE: replica key store is empty");
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
  LOG_DEBUG(GL,
            "KEY EXCHANGE:: Key rotation for replica " << keys_[1].msg.repID << " recieved on seqnum "
                                                       << keys_[1].seqnum << " rotated on " << chekPointSeqNum);
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

ClusterKeyStore::ClusterKeyStore(const uint32_t& clusterSize) : clusterKeys_(clusterSize) {}

bool ClusterKeyStore::push(const KeyExchangeMsg& kem,
                           const uint64_t& sn,
                           const std::vector<IKeyExchanger*>& registryToExchange) {
  if (kem.repID >= clusterKeys_.size()) {
    LOG_ERROR(GL, "KEY EXCHANGE: replica id is out of range " << kem.repID);
    return false;
  }

  if (!clusterKeys_[kem.repID].push(kem, sn)) return false;

  for (auto ike : registryToExchange) {
    ike->onNewKey(clusterKeys_[kem.repID].current().msg);
  }
  return true;
}

bool ClusterKeyStore::rotate(const uint64_t& chknum, const std::vector<IKeyExchanger*>& registryToExchange) {
  bool ret{};
  auto idx = 0;
  for (auto& replicaKeyStore : clusterKeys_) {
    if (!replicaKeyStore.rotate(chknum)) continue;
    ret = true;
    // Notify registry on exchange for replica
    LOG_DEBUG(GL, "KEY EXCHANGE: notifying registry for exchange for replica " << idx << " at checkoint " << chknum);
    for (auto ike : registryToExchange) {
      ike->onExchange(replicaKeyStore.current().msg);
    }
    ++idx;
    // TODO Save replica reserved pages, function for UT
  }
  return ret;
}

KeyExchangeMsg ClusterKeyStore::replicaKey(const uint16_t& repID) const {
  if (repID >= clusterKeys_.size()) {
    LOG_ERROR(GL, "KEY EXCHANGE: replica id is out of range " << repID);
    return {};
  }
  return clusterKeys_[repID].current().msg;
}
