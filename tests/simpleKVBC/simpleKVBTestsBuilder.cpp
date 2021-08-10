// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "boost/detail/endian.hpp"
#include "assertUtils.hpp"
#include <chrono>
#include <map>
#include <set>
#include "basicRandomTestsRunner.hpp"
#include "commonKVBTests.hpp"
#include "DbMetadataStorage.cpp"
#include "storage/db_types.h"
#include "block_metadata.hpp"

using std::set;
using std::chrono::seconds;

using concord::kvbc::BlockId;
using concord::kvbc::IClient;
using skvbc::messages::SKVBCGetLastBlockReply;
using skvbc::messages::SKVBCGetLastBlockRequest;
using skvbc::messages::SKVBCReadReply;
using skvbc::messages::SKVBCReadRequest;
using skvbc::messages::SKVBCReply;
using skvbc::messages::SKVBCRequest;
using skvbc::messages::SKVBCWriteReply;
using skvbc::messages::SKVBCWriteRequest;

const int NUMBER_OF_KEYS = 200;
const int CONFLICT_DISTANCE = 49;
const int MAX_WRITES_IN_REQ = 12;
const int MAX_READ_SET_SIZE_IN_REQ = 10;
const int MAX_READS_IN_REQ = 12;

const size_t kMaxKVSizeToUse = sizeof(uint64_t);

static_assert(
    sizeof(uint8_t) == 1,
    "code in this file for packing data of integer types into byte strings may assume uint8_t is a 1-byte type");

namespace BasicRandomTests {

TestsBuilder::TestsBuilder(logging::Logger& logger, IClient& client) : logger_(logger), client_(client) {
  prevLastBlockId_ = getInitialLastBlockId();
  lastBlockId_ = prevLastBlockId_;
  LOG_INFO(logger, "TestsBuilder: initialBlockId_=" << prevLastBlockId_);
}

TestsBuilder::~TestsBuilder() { LOG_INFO(logger_, "TestsBuilder: The last DB block is " << lastBlockId_); }

// When working with persistent KVB, we need to retrieve current last block-id
// and all written keys before starting.
BlockId TestsBuilder::getInitialLastBlockId() {
  SKVBCRequest request;
  request.request = SKVBCGetLastBlockRequest();
  vector<uint8_t> serialized_request;
  serialize(serialized_request, request);

  SKVBCReply reply;
  reply.reply = SKVBCGetLastBlockReply();
  vector<uint8_t> serialized_reply;
  serialize(serialized_reply, reply);
  size_t expected_reply_size = serialized_reply.size();
  uint32_t actual_reply_size = 0;

  static_assert(
      (sizeof(*(serialized_request.data())) == sizeof(char)) && (sizeof(*(serialized_reply.data())) == sizeof(char)),
      "Byte pointer type used by concord::kvbc::IClient interface is not compatible with byte pointer type used by "
      "CMF.");
  auto res = client_.invokeCommandSynch(reinterpret_cast<char*>(serialized_request.data()),
                                        serialized_request.size(),
                                        true,
                                        seconds(5),
                                        expected_reply_size,
                                        reinterpret_cast<char*>(serialized_reply.data()),
                                        &actual_reply_size);
  ConcordAssert(res.isOK());

  LOG_INFO(logger_, "Actual reply size = " << actual_reply_size << ", expected reply size = " << expected_reply_size);
  ConcordAssert(actual_reply_size == expected_reply_size);
  deserialize(serialized_reply, reply);
  ConcordAssert(holds_alternative<SKVBCGetLastBlockReply>(reply.reply));

  return (get<SKVBCGetLastBlockReply>(reply.reply)).latest_block;
}

void TestsBuilder::retrieveExistingBlocksFromKVB() {
  if (prevLastBlockId_ == BasicRandomTests::FIRST_KVB_BLOCK)
    // KVB contains only the genesis block
    return;

  // KVB is not empty. Read existing blocks and save in the memory.
  SKVBCRequest request;
  request.request = SKVBCReadRequest();
  SKVBCReadRequest& read_req = get<SKVBCReadRequest>(request.request);
  read_req.read_version = prevLastBlockId_;
  read_req.keys.resize(NUMBER_OF_KEYS);

  SKVBCReply reply;

  // Note we make the assumption that the serialization size for a CMF message is not dependent on the value of any
  // fixed-length data within the message, and that the serialization size will never decrease as the length of a vector
  // within the message increases.
  SKVBCReadReply maximally_sized_reply;
  maximally_sized_reply.reads.resize(NUMBER_OF_KEYS);
  for (auto& kvp : maximally_sized_reply.reads) {
    kvp.first.resize(kMaxKVSizeToUse);
    kvp.second.resize(kMaxKVSizeToUse);
  }
  reply.reply = maximally_sized_reply;
  vector<uint8_t> serialized_reply;
  serialize(serialized_reply, reply);
  size_t expected_max_reply_size = serialized_reply.size();
  uint32_t actualReplySize = 0;

  for (uint64_t key = 0; key < NUMBER_OF_KEYS; ++key) {
    read_req.keys[key].resize(kMaxKVSizeToUse);

    uint8_t* key_first_byte = reinterpret_cast<uint8_t*>(&key);
    uint8_t* key_last_byte = key_first_byte + sizeof(key);

#ifdef BOOST_BIG_ENDIAN
    copy(key_first_byte, key_last_byte, read_req.keys[key].data());
#else  // BOOST_BIG_ENDIAN not defined
#ifdef BOOST_LITTLE_ENDIAN
    reverse_copy(key_first_byte, key_last_byte, read_req.keys[key].data());
#else   // BOOST_LITTLE_ENDIAN not defined
    static_assert(false, "failed to determine the endianness being compiled for");
#endif  // if BOOST_LITTLE_ENDIAN defined/else
#endif  // if BOOST_BIG_ENDIAN defined/else
  }

  vector<uint8_t> serialized_request;
  serialize(serialized_request, request);

  static_assert(
      (sizeof(*(serialized_request.data())) == sizeof(char)) && (sizeof(*(serialized_reply.data())) == sizeof(char)),
      "Byte pointer type used by concord::kvbc::IClient interface is not compatible with byte pointer type used by "
      "CMF.");
  // Infinite timeout
  auto res = client_.invokeCommandSynch(reinterpret_cast<char*>(serialized_request.data()),
                                        serialized_request.size(),
                                        true,
                                        seconds(0),
                                        expected_max_reply_size,
                                        reinterpret_cast<char*>(serialized_reply.data()),
                                        &actualReplySize);
  ConcordAssert(res.isOK());

  ConcordAssert(actualReplySize <= expected_max_reply_size);
  serialized_reply.resize(actualReplySize);
  deserialize(serialized_reply, reply);
  ConcordAssert(holds_alternative<SKVBCReadReply>(reply.reply));
  const SKVBCReadReply& read_rep = get<SKVBCReadReply>(reply.reply);
  ConcordAssert(read_rep.reads.size() == NUMBER_OF_KEYS);

  for (int key = 0; key < NUMBER_OF_KEYS; key++) {
    pair<vector<uint8_t>, uint64_t> simpleKIDPair(read_rep.reads[key].first, read_req.read_version);
    allKeysToValueMap_[simpleKIDPair] = read_rep.reads[key].second;
  }
}

void TestsBuilder::createRandomTest(size_t numOfRequests, size_t seed) {
  retrieveExistingBlocksFromKVB();
  create(numOfRequests, seed);
  for (auto elem : internalBlockchain_) {
    elem.second = SimpleBlock();
  }
}

void TestsBuilder::create(size_t numOfRequests, size_t seed) {
  srand(seed);
  for (size_t i = 0; i < numOfRequests; i++) {
    int percent = rand() % 100 + 1;
    if (percent <= 50)
      createAndInsertRandomRead();
    else if (percent <= 95)
      createAndInsertRandomConditionalWrite();
    else if (percent <= 100)
      createAndInsertGetLastBlock();
    else
      ConcordAssert(0);
  }

  for (const auto& elem : internalBlockchain_) {
    BlockId blockId = elem.first;
    const SimpleBlock& block = elem.second;
    ConcordAssert(blockId == block.id);
  }
}

void TestsBuilder::addExpectedWriteReply(bool foundConflict) {
  SKVBCReply reply;
  reply.reply = SKVBCWriteReply();
  SKVBCWriteReply& write_rep = get<SKVBCWriteReply>(reply.reply);
  if (foundConflict) {
    write_rep.success = false;
    write_rep.latest_block = lastBlockId_;
  } else {
    write_rep.success = true;
    write_rep.latest_block = lastBlockId_ + 1;
  }
  replies_.push_back(reply);
}

bool TestsBuilder::lookForConflicts(uint64_t readVersion, const vector<vector<uint8_t>>& readKeysArray) {
  bool foundConflict = false;
  uint64_t i;
  for (i = readVersion + 1; (i <= lastBlockId_) && !foundConflict; i++) {
    const SimpleBlock& currBlock = internalBlockchain_[i];
    for (size_t a = 0; (a < readKeysArray.size()) && !foundConflict; a++) {
      const vector<pair<vector<uint8_t>, vector<uint8_t>>>& items = currBlock.items;
      for (size_t b = 0; (b < currBlock.items.size()) && !foundConflict; b++) {
        if (readKeysArray[a] == items[b].first) foundConflict = true;
      }
    }
  }
  return foundConflict;
}

void TestsBuilder::addNewBlock(const vector<pair<vector<uint8_t>, vector<uint8_t>>>& writesKVArray) {
  ++lastBlockId_;
  SimpleBlock new_block;
  new_block.items = writesKVArray;

  new_block.id = lastBlockId_;

  for (size_t i = 0; i < writesKVArray.size(); i++) {
    allKeysToValueMap_[pair<vector<uint8_t>, uint64_t>(writesKVArray[i].first, lastBlockId_)] = writesKVArray[i].second;
  }
  internalBlockchain_[lastBlockId_] = new_block;
}

void TestsBuilder::createAndInsertRandomConditionalWrite() {
  // Create request
  uint64_t readVersion = lastBlockId_;
  if (lastBlockId_ > prevLastBlockId_ + CONFLICT_DISTANCE) {
    readVersion = 0;
    while (readVersion < prevLastBlockId_) readVersion = lastBlockId_ - (rand() % CONFLICT_DISTANCE);
  }

  size_t numOfWrites = (rand() % (MAX_WRITES_IN_REQ - 1)) + 1;
  size_t numOfKeysInReadSet = (rand() % MAX_READ_SET_SIZE_IN_REQ);

  SKVBCRequest request;
  request.request = SKVBCWriteRequest();
  SKVBCWriteRequest& write_req = get<SKVBCWriteRequest>(request.request);
  write_req.readset.resize(numOfKeysInReadSet);
  write_req.writeset.resize(numOfWrites);
  write_req.read_version = readVersion;

  for (size_t i = 0; i < numOfKeysInReadSet; i++) {
    uint64_t key = 0;
    do {
      key = rand() % NUMBER_OF_KEYS;
    } while (key == concord::kvbc::IBlockMetadata::kBlockMetadataKey);
    write_req.readset[i].resize(kMaxKVSizeToUse);

    uint8_t* key_first_byte = reinterpret_cast<uint8_t*>(&key);
    uint8_t* key_last_byte = key_first_byte + sizeof(key);

#ifdef BOOST_BIG_ENDIAN
    copy(key_first_byte, key_last_byte, write_req.readset[i].data());
#else  // BOOST_BIG_ENDIAN not defined
#ifdef BOOST_LITTLE_ENDIAN
    reverse_copy(key_first_byte, key_last_byte, write_req.readset[i].data());
#else   // BOOST_LITTLE_ENDIAN not defined
    static_assert(false, "failed to determine the endianness being compiled for");
#endif  // if BOOST_LITTLE_ENDIAN defined/else
#endif  // if BOOST_BIG_ENDIAN defined/else
  }

  std::set<uint64_t> usedKeys;
  for (size_t i = 0; i < numOfWrites; i++) {
    uint64_t key = 0;
    do {  // Avoid duplications
      key = rand() % NUMBER_OF_KEYS;
    } while (usedKeys.count(key) > 0 || key == concord::kvbc::IBlockMetadata::kBlockMetadataKey);
    usedKeys.insert(key);

    uint64_t value = rand();

    write_req.writeset[i].first.resize(kMaxKVSizeToUse);
    uint8_t* key_first_byte = reinterpret_cast<uint8_t*>(&key);
    uint8_t* key_last_byte = key_first_byte + sizeof(key);
#ifdef BOOST_BIG_ENDIAN
    copy(key_first_byte, key_last_byte, write_req.writeset[i].first.data());
#else  // BOOST_BIG_ENDIAN not defined
#ifdef BOOST_LITTLE_ENDIAN
    reverse_copy(key_first_byte, key_last_byte, write_req.writeset[i].first.data());
#else   // BOOST_LITTLE_ENDIAN not defined
    static_assert(false, "failed to determine the endianness being compiled for");
#endif  // if BOOST_LITTLE_ENDIAN defined/else
#endif  // if BOOST_BIG_ENDIAN defined/else

    write_req.writeset[i].second.resize(kMaxKVSizeToUse);
    uint8_t* value_first_byte = reinterpret_cast<uint8_t*>(&value);
    uint8_t* value_last_byte = value_first_byte + sizeof(value);
#ifdef BOOST_BIG_ENDIAN
    copy(value_first_byte, value_last_byte, write_req.writeset[i].second.data());
#else  // BOOST_BIG_ENDIAN not defined
#ifdef BOOST_LITTLE_ENDIAN
    reverse_copy(value_first_byte, value_last_byte, write_req.writeset[i].second.data());
#else   // BOOST_LITTLE_ENDIAN not defined
    static_assert(false, "failed to determine the endianness being compiled for");
#endif  // if BOOST_LITTLE_ENDIAN defined/else
#endif  // if BOOST_BIG_ENDIAN defined/else
  }

  // Add request to m_requests
  requests_.push_back(request);

  bool foundConflict = lookForConflicts(readVersion, write_req.readset);

  // Add expected reply to m_replies
  addExpectedWriteReply(foundConflict);

  // If needed, add new block into the blockchain
  if (!foundConflict) {
    addNewBlock(write_req.writeset);
  }
}

void TestsBuilder::createAndInsertRandomRead() {
  // Create request
  uint64_t readVersion = 0;
  if (prevLastBlockId_ == lastBlockId_) {
    readVersion = lastBlockId_;
  } else {  // New blocks have been written to the DB during this run.
    while (readVersion <= prevLastBlockId_) readVersion = (rand() % (lastBlockId_ + 1));
  }
  size_t numberOfKeysToRead = (rand() % (MAX_READS_IN_REQ - 1)) + 1;
  SKVBCRequest request;
  request.request = SKVBCReadRequest();
  SKVBCReadRequest& read_req = get<SKVBCReadRequest>(request.request);
  read_req.keys.resize(numberOfKeysToRead);
  read_req.read_version = readVersion;

  for (size_t i = 0; i < numberOfKeysToRead; i++) {
    uint64_t key = 0;
    do {
      key = rand() % NUMBER_OF_KEYS;
    } while (key == concord::kvbc::IBlockMetadata::kBlockMetadataKey);
    read_req.keys[i].resize(kMaxKVSizeToUse);

    uint8_t* key_first_byte = reinterpret_cast<uint8_t*>(&key);
    uint8_t* key_last_byte = key_first_byte + sizeof(key);

#ifdef BOOST_BIG_ENDIAN
    copy(key_first_byte, key_last_byte, read_req.keys[i].data());
#else  // BOOST_BIG_ENDIAN not defined
#ifdef BOOST_LITTLE_ENDIAN
    reverse_copy(key_first_byte, key_last_byte, read_req.keys[i].data());
#else   // BOOST_LITTLE_ENDIAN not defined
    static_assert(false, "failed to determine the endianness being compiled for");
#endif  // if BOOST_LITTLE_ENDIAN defined/else
#endif  // if BOOST_BIG_ENDIAN defined/else
  }

  // Add request to m_requests
  requests_.push_back(request);

  // Compute expected reply
  SKVBCReply reply;
  reply.reply = SKVBCReadReply();
  SKVBCReadReply& read_rep = get<SKVBCReadReply>(reply.reply);
  read_rep.reads.resize(numberOfKeysToRead);

  for (size_t i = 0; i < numberOfKeysToRead; i++) {
    read_rep.reads[i].first = read_req.keys[i];
    pair<vector<uint8_t>, uint64_t> simpleKIDPair(read_req.keys[i], read_req.read_version);

    KeyBlockIdToValueMap::const_iterator it = allKeysToValueMap_.lower_bound(simpleKIDPair);
    if (it != allKeysToValueMap_.end() && (read_req.read_version >= it->first.second) &&
        (it->first.first == read_req.keys[i])) {
      read_rep.reads[i].second = it->second;
    } else {
      // Make the value an empty bytestring for a non-found key.
      read_rep.reads[i].second.clear();
    }
  }
  // Add reply to m_replies
  replies_.push_back(reply);
}

void TestsBuilder::createAndInsertGetLastBlock() {
  // Create request
  SKVBCRequest request;
  request.request = SKVBCGetLastBlockRequest();

  // Add request to m_requests
  requests_.push_back(request);

  // compute expected reply
  SKVBCReply reply;
  reply.reply = SKVBCGetLastBlockReply();
  (get<SKVBCGetLastBlockReply>(reply.reply)).latest_block = lastBlockId_;

  // Add reply to m_replies
  replies_.push_back(reply);
}

}  // namespace BasicRandomTests
