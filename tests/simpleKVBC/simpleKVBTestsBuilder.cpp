// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
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
#include <chrono>
#include <map>
#include <set>
#include "basicRandomTestsRunner.hpp"
#include "commonKVBTests.hpp"
#include "DbMetadataStorage.cpp"
#include "storage/db_types.h"
#include "block_metadata.hpp"

#ifndef _WIN32
#include <unistd.h>
#endif

using std::set;
using std::chrono::seconds;

using concord::kvbc::BlockId;
using concord::kvbc::IClient;

const int NUMBER_OF_KEYS = 200;
const int CONFLICT_DISTANCE = 49;
const int MAX_WRITES_IN_REQ = 12;
const int MAX_READ_SET_SIZE_IN_REQ = 10;
const int MAX_READS_IN_REQ = 12;

namespace BasicRandomTests {

TestsBuilder::TestsBuilder(logging::Logger &logger, IClient &client) : logger_(logger), client_(client) {
  prevLastBlockId_ = getInitialLastBlockId();
  lastBlockId_ = prevLastBlockId_;
  LOG_INFO(logger, "TestsBuilder: initialBlockId_=" << prevLastBlockId_);
}

TestsBuilder::~TestsBuilder() {
  LOG_INFO(logger_, "TestsBuilder: The last DB block is " << lastBlockId_);
  for (auto elem : requests_) delete[] elem;
  for (auto elem : replies_) delete[] elem;
}

// When working with persistent KVB, we need to retrieve current last block-id
// and all written keys before starting.
BlockId TestsBuilder::getInitialLastBlockId() {
  auto *request = SimpleGetLastBlockRequest::alloc();
  request->header.type = GET_LAST_BLOCK;

  size_t expectedReplySize = sizeof(SimpleReply_GetLastBlock);
  std::vector<char> reply(expectedReplySize);
  uint32_t actualReplySize = 0;

  auto res = client_.invokeCommandSynch((char *)request,
                                        sizeof(SimpleGetLastBlockRequest),
                                        true,
                                        seconds(5),
                                        expectedReplySize,
                                        reply.data(),
                                        &actualReplySize);
  ConcordAssert(res.isOK());

  auto *replyObj = (SimpleReply_GetLastBlock *)reply.data();
  LOG_INFO(logger_, "Actual reply size = " << actualReplySize << ", expected reply size = " << expectedReplySize);
  ConcordAssert(actualReplySize == expectedReplySize);
  ConcordAssert(replyObj->header.type == GET_LAST_BLOCK);
  SimpleGetLastBlockRequest::free(request);
  return replyObj->latestBlock;
}

void TestsBuilder::retrieveExistingBlocksFromKVB() {
  if (prevLastBlockId_ == BasicRandomTests::FIRST_KVB_BLOCK)
    // KVB contains only the genesis block
    return;

  // KVB is not empty. Read existing blocks and save in the memory.
  auto *request = SimpleReadRequest::alloc(NUMBER_OF_KEYS);
  request->header.type = READ;
  request->readVersion = prevLastBlockId_;
  request->numberOfKeysToRead = NUMBER_OF_KEYS;
  SimpleKey *requestKeys = request->keys;

  size_t expectedReplySize = SimpleReply_Read::getSize(NUMBER_OF_KEYS);
  std::vector<char> reply(expectedReplySize);
  uint32_t actualReplySize = 0;

  for (int key = 0; key < NUMBER_OF_KEYS; key++) memcpy(requestKeys[key].key, &key, sizeof(key));

  // Infinite timeout
  auto res = client_.invokeCommandSynch(
      (char *)request, request->getSize(), true, seconds(0), expectedReplySize, reply.data(), &actualReplySize);
  ConcordAssert(res.isOK());

  auto *replyObj = (SimpleReply_Read *)reply.data();
  __attribute__((unused)) size_t numOfItems = replyObj->numOfItems;
  ConcordAssert(actualReplySize == expectedReplySize);
  ConcordAssert(replyObj->header.type == READ);
  ConcordAssert(numOfItems == NUMBER_OF_KEYS);

  for (int key = 0; key < NUMBER_OF_KEYS; key++) {
    SimpleKeyBlockIdPair simpleKIDPair(replyObj->items[key].simpleKey, request->readVersion);
    allKeysToValueMap_[simpleKIDPair] = replyObj->items[key].simpleValue;
  }
  SimpleReadRequest::free(request);
}

void TestsBuilder::createRandomTest(size_t numOfRequests, size_t seed) {
  retrieveExistingBlocksFromKVB();
  create(numOfRequests, seed);
  for (auto elem : internalBlockchain_) {
    free(elem.second);
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

  for (__attribute__((unused)) auto elem : internalBlockchain_) {
    __attribute__((unused)) BlockId blockId = elem.first;
    __attribute__((unused)) SimpleBlock *block = elem.second;
    ConcordAssert(blockId == block->id);
  }
}

void TestsBuilder::addExpectedWriteReply(bool foundConflict) {
  auto *reply = SimpleReply_ConditionalWrite::alloc();
  if (foundConflict) {
    reply->success = false;
    reply->latestBlock = lastBlockId_;
  } else {
    reply->success = true;
    reply->latestBlock = lastBlockId_ + 1;
  }
  reply->header.type = COND_WRITE;
  replies_.push_back((SimpleReply *)reply);
}

bool TestsBuilder::lookForConflicts(BlockId readVersion, size_t numOfKeysInReadSet, SimpleKey *readKeysArray) {
  bool foundConflict = false;
  BlockId i;
  for (i = readVersion + 1; (i <= lastBlockId_) && !foundConflict; i++) {
    SimpleBlock *currBlock = internalBlockchain_[i];
    for (size_t a = 0; (a < numOfKeysInReadSet) && !foundConflict; a++) {
      SimpleKV *items = currBlock->items;
      for (size_t b = 0; (b < currBlock->numOfItems) && !foundConflict; b++) {
        if (memcmp(readKeysArray[a].key, items[b].simpleKey.key, KV_LEN) == 0) foundConflict = true;
      }
    }
  }
  return foundConflict;
}

void TestsBuilder::addNewBlock(size_t numOfWrites, SimpleKV *writesKVArray) {
  ++lastBlockId_;
  auto *newBlock = SimpleBlock::alloc(numOfWrites);

  newBlock->id = lastBlockId_;
  newBlock->numOfItems = numOfWrites;

  SimpleKV *items = newBlock->items;
  for (size_t i = 0; i < numOfWrites; i++) {
    items[i] = writesKVArray[i];

    SimpleKey simpleKey;
    memcpy(simpleKey.key, writesKVArray[i].simpleKey.key, KV_LEN);

    SimpleValue simpleValue;
    memcpy(simpleValue.value, writesKVArray[i].simpleValue.value, KV_LEN);

    allKeysToValueMap_[SimpleKeyBlockIdPair(simpleKey, lastBlockId_)] = simpleValue;
  }
  internalBlockchain_[lastBlockId_] = newBlock;
}

void TestsBuilder::createAndInsertRandomConditionalWrite() {
  // Create request
  BlockId readVersion = lastBlockId_;
  if (lastBlockId_ > prevLastBlockId_ + CONFLICT_DISTANCE) {
    readVersion = 0;
    while (readVersion < prevLastBlockId_) readVersion = lastBlockId_ - (rand() % CONFLICT_DISTANCE);
  }

  size_t numOfWrites = (rand() % (MAX_WRITES_IN_REQ - 1)) + 1;
  size_t numOfKeysInReadSet = (rand() % MAX_READ_SET_SIZE_IN_REQ);

  auto *request = SimpleCondWriteRequest::alloc(numOfKeysInReadSet, numOfWrites);
  request->header.type = COND_WRITE;
  request->readVersion = readVersion;
  request->numOfKeysInReadSet = numOfKeysInReadSet;
  request->numOfWrites = numOfWrites;
  SimpleKey *readKeysArray = request->readSetArray();
  SimpleKV *writesKVArray = request->keyValueArray();

  for (size_t i = 0; i < numOfKeysInReadSet; i++) {
    size_t key = 0;
    do {
      key = rand() % NUMBER_OF_KEYS;
    } while (key == concord::kvbc::IBlockMetadata::kBlockMetadataKey);
    memcpy(readKeysArray[i].key, &key, sizeof(key));
  }

  std::set<size_t> usedKeys;
  for (size_t i = 0; i < numOfWrites; i++) {
    size_t key = 0;
    do {  // Avoid duplications
      key = rand() % NUMBER_OF_KEYS;
    } while (usedKeys.count(key) > 0 || key == concord::kvbc::IBlockMetadata::kBlockMetadataKey);
    usedKeys.insert(key);

    size_t value = rand();
    memcpy(writesKVArray[i].simpleKey.key, &key, sizeof(key));
    memcpy(writesKVArray[i].simpleValue.value, &value, sizeof(value));
  }

  // Add request to m_requests
  requests_.push_back((SimpleRequest *)request);

  bool foundConflict = lookForConflicts(readVersion, numOfKeysInReadSet, readKeysArray);

  // Add expected reply to m_replies
  addExpectedWriteReply(foundConflict);

  // If needed, add new block into the blockchain
  if (!foundConflict) {
    addNewBlock(request->numOfWrites, writesKVArray);
  }
}

void TestsBuilder::createAndInsertRandomRead() {
  // Create request
  BlockId readVersion = 0;
  if (prevLastBlockId_ == lastBlockId_) {
    readVersion = lastBlockId_;
  } else {  // New blocks have been written to the DB during this run.
    while (readVersion <= prevLastBlockId_) readVersion = (rand() % (lastBlockId_ + 1));
  }
  size_t numberOfKeysToRead = (rand() % (MAX_READS_IN_REQ - 1)) + 1;
  auto *request = SimpleReadRequest::alloc(numberOfKeysToRead);
  request->header.type = READ;
  request->readVersion = readVersion;
  request->numberOfKeysToRead = numberOfKeysToRead;

  SimpleKey *requestKeys = request->keys;
  for (size_t i = 0; i < numberOfKeysToRead; i++) {
    size_t key = 0;
    do {
      key = rand() % NUMBER_OF_KEYS;
    } while (key == concord::kvbc::IBlockMetadata::kBlockMetadataKey);
    memcpy(requestKeys[i].key, &key, sizeof(key));
  }

  // Add request to m_requests
  requests_.push_back((SimpleRequest *)request);

  // Compute expected reply
  auto *reply = SimpleReply_Read::alloc(numberOfKeysToRead);
  reply->header.type = READ;
  reply->numOfItems = numberOfKeysToRead;

  SimpleKV *replyItems = reply->items;
  for (size_t i = 0; i < numberOfKeysToRead; i++) {
    memcpy(replyItems[i].simpleKey.key, requestKeys[i].key, KV_LEN);
    SimpleKeyBlockIdPair simpleKIDPair(requestKeys[i], request->readVersion);

    KeyBlockIdToValueMap::const_iterator it = allKeysToValueMap_.lower_bound(simpleKIDPair);
    if (it != allKeysToValueMap_.end() && (request->readVersion >= it->first.blockId) &&
        (memcmp(it->first.key.key, requestKeys[i].key, KV_LEN) == 0)) {
      memcpy(replyItems[i].simpleValue.value, it->second.value, KV_LEN);
    } else {
      // Fill value by zeroes for a non-found key.
      memset(replyItems[i].simpleValue.value, 0, KV_LEN);
    }
  }
  // Add reply to m_replies
  replies_.push_back((SimpleReply *)reply);
}

void TestsBuilder::createAndInsertGetLastBlock() {
  // Create request
  auto *request = SimpleGetLastBlockRequest::alloc();
  request->header.type = GET_LAST_BLOCK;

  // Add request to m_requests
  requests_.push_back((SimpleRequest *)request);

  // compute expected reply
  auto *reply = SimpleReply_GetLastBlock::alloc();
  reply->header.type = GET_LAST_BLOCK;
  reply->latestBlock = lastBlockId_;

  // Add reply to m_replies
  replies_.push_back((SimpleReply *)reply);
}

size_t TestsBuilder::sizeOfRequest(SimpleRequest *request) {
  switch (request->type) {
    case COND_WRITE:
      return ((SimpleCondWriteRequest *)request)->getSize();
    case READ:
      return ((SimpleReadRequest *)request)->getSize();
    case GET_LAST_BLOCK:
      return sizeof(SimpleRequest);
    default:
      ConcordAssert(0);
  }
  return 0;
}

size_t TestsBuilder::sizeOfReply(SimpleReply *reply) {
  switch (reply->type) {
    case COND_WRITE:
      return sizeof(SimpleReply_ConditionalWrite);
    case READ:
      return ((SimpleReply_Read *)reply)->getSize();
    case GET_LAST_BLOCK:
      return sizeof(SimpleReply_GetLastBlock);
    default:
      ConcordAssert(0);
  }
  return 0;
}

}  // namespace BasicRandomTests
