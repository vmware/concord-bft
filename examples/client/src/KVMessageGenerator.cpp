// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "assertUtils.hpp"
#include "common.hpp"
#include "block_metadata.hpp"
#include "KVMessageGenerator.hpp"

using namespace concord::osexample;
using namespace kv::messages;
const size_t kMaxKVSizeToUse = sizeof(uint64_t);

void KVMessageGenerator::addExpectedWriteReply(bool foundConflict) {
  KVReply reply;
  reply.reply = KVWriteReply();
  KVWriteReply& write_rep = std::get<KVWriteReply>(reply.reply);
  if (foundConflict) {
    write_rep.success = false;
    write_rep.latest_block = lastBlockId_;
  } else {
    write_rep.success = true;
    write_rep.latest_block = lastBlockId_ + 1;
  }

  replies_.push_back(reply);
}

void KVMessageGenerator::addExpectedReadReply(size_t numberOfKeysToRead, KVReadRequest& readReq) {
  // Compute expected reply
  KVReply reply;
  reply.reply = KVReadReply();
  KVReadReply& read_rep = std::get<KVReadReply>(reply.reply);
  read_rep.reads.resize(numberOfKeysToRead);

  for (size_t i = 0; i < numberOfKeysToRead; i++) {
    read_rep.reads[i].first = readReq.keys[i];
    std::pair<std::vector<uint8_t>, uint64_t> kidPair(readReq.keys[i], readReq.read_version);

    KeyBlockIdToValueMap::const_iterator it = allKeysToValueMap_.lower_bound(kidPair);
    if (it != allKeysToValueMap_.end() && (readReq.read_version >= it->first.second) &&
        (it->first.first == readReq.keys[i])) {
      read_rep.reads[i].second = it->second;
    } else {
      // Make the value an empty bytestring for a non-found key.
      read_rep.reads[i].second.clear();
    }
  }
  // Add reply to m_replies
  replies_.push_back(reply);
}

bool KVMessageGenerator::lookForConflicts(uint64_t readVersion,
                                          const std::vector<std::vector<uint8_t>>& readKeysArray) {
  bool foundConflict = false;
  uint64_t i;
  for (i = readVersion + 1; (i <= lastBlockId_) && !foundConflict; i++) {
    const KVBlock& currBlock = internalBlockchain_[i];
    for (size_t a = 0; (a < readKeysArray.size()) && !foundConflict; a++) {
      const std::vector<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>>& items = currBlock.items;
      for (size_t b = 0; (b < currBlock.items.size()) && !foundConflict; b++) {
        if (readKeysArray[a] == items[b].first) foundConflict = true;
      }
    }
  }
  return foundConflict;
}

void KVMessageGenerator::addNewBlock(
    const std::vector<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>>& writesKVArray) {
  ++lastBlockId_;
  KVBlock new_block;
  new_block.items = writesKVArray;

  new_block.id = lastBlockId_;
  LOG_INFO(getLogger(), "adding new block " << KVLOG(new_block.id));

  for (size_t i = 0; i < writesKVArray.size(); i++) {
    allKeysToValueMap_[std::pair<std::vector<uint8_t>, uint64_t>(writesKVArray[i].first, lastBlockId_)] =
        writesKVArray[i].second;
  }
  internalBlockchain_[lastBlockId_] = new_block;
}

bft::client::Msg KVMessageGenerator::createWriteRequest() {
  // Create request
  KVRequest request;
  bft::client::Msg msg;
  uint64_t readVersion = lastBlockId_;

  if (lastBlockId_ > prevLastBlockId_ + CommonConstants::CONFLICT_DISTANCE) {
    readVersion = 0;
    while (readVersion < prevLastBlockId_) readVersion = lastBlockId_ - (rand() % CommonConstants::CONFLICT_DISTANCE);
  }

  size_t numOfWrites = msgConfig_->numOfWrites;
  size_t numOfKeysInReadSet = msgConfig_->numOfKeysInReadSet;

  request.request = KVWriteRequest();
  KVWriteRequest& write_req = std::get<KVWriteRequest>(request.request);

  write_req.readset.resize(numOfKeysInReadSet);
  write_req.writeset.resize(numOfWrites);
  write_req.read_version = readVersion;

  for (size_t i = 0; i < numOfKeysInReadSet; i++) {
    uint64_t key = 0;
    do {
      key = rand() % CommonConstants::NUMBER_OF_KEYS;
    } while (key == concord::kvbc::IBlockMetadata::kBlockMetadataKey);
    write_req.readset[i].resize(kMaxKVSizeToUse);

    uint8_t* key_first_byte = reinterpret_cast<uint8_t*>(&key);
    uint8_t* key_last_byte = key_first_byte + sizeof(key);
    std::copy(key_first_byte, key_last_byte, write_req.readset[i].data());
  }

  std::set<uint64_t> usedKeys;
  for (size_t i = 0; i < numOfWrites; i++) {
    uint64_t key = 0;
    do {  // Avoid duplications
      key = rand() % CommonConstants::NUMBER_OF_KEYS;
    } while (usedKeys.count(key) > 0 || key == concord::kvbc::IBlockMetadata::kBlockMetadataKey);
    usedKeys.insert(key);

    uint64_t value = rand();

    write_req.writeset[i].first.resize(kMaxKVSizeToUse);
    uint8_t* key_first_byte = reinterpret_cast<uint8_t*>(&key);
    uint8_t* key_last_byte = key_first_byte + sizeof(key);
    std::copy(key_first_byte, key_last_byte, write_req.writeset[i].first.data());

    write_req.writeset[i].second.resize(kMaxKVSizeToUse);
    uint8_t* value_first_byte = reinterpret_cast<uint8_t*>(&value);
    uint8_t* value_last_byte = value_first_byte + sizeof(value);
    std::copy(value_first_byte, value_last_byte, write_req.writeset[i].second.data());
  }

  // Add request to m_requests
  requests_.push_back(request);

  // akash delete this
  LOG_INFO(getLogger(), "writeset size, " << KVLOG(write_req.writeset.size()));

  bool foundConflict = lookForConflicts(readVersion, write_req.readset);

  // Add expected reply
  addExpectedWriteReply(foundConflict);

  // If needed, add new block into the blockchain
  if (!foundConflict) {
    addNewBlock(write_req.writeset);
  }

  kv::messages::serialize(msg, request);
  return msg;
}

bft::client::Msg KVMessageGenerator::createReadRequest() {
  // Create request
  KVRequest request;
  bft::client::Msg msg;

  uint64_t readVersion = 0;
  if (prevLastBlockId_ == lastBlockId_) {
    readVersion = lastBlockId_;
  } else {  // New blocks have been written to the DB during this run.
    while (readVersion <= prevLastBlockId_) readVersion = (rand() % (lastBlockId_ + 1));
  }

  size_t numberOfKeysToRead = msgConfig_->numOfWrites;
  request.request = KVReadRequest();
  KVReadRequest& read_req = std::get<KVReadRequest>(request.request);
  read_req.keys.resize(numberOfKeysToRead);
  read_req.read_version = readVersion;

  for (size_t i = 0; i < numberOfKeysToRead; i++) {
    uint64_t key = 0;
    do {
      key = rand() % CommonConstants::NUMBER_OF_KEYS;
    } while (key == concord::kvbc::IBlockMetadata::kBlockMetadataKey);
    read_req.keys[i].resize(kMaxKVSizeToUse);

    uint8_t* key_first_byte = reinterpret_cast<uint8_t*>(&key);
    uint8_t* key_last_byte = key_first_byte + sizeof(key);
    std::copy(key_first_byte, key_last_byte, read_req.keys[i].data());
  }

  // Add request to requests_
  requests_.push_back(request);

  // Add expected reply
  addExpectedReadReply(numberOfKeysToRead, read_req);

  kv::messages::serialize(msg, request);
  return msg;
}
