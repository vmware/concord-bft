// Concord
//
// Copyright (c) 2018-2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <list>
#include <map>
#include <cstring>
#include <sstream>
#include "kv_types.hpp"
#include "KVBCInterfaces.h"
#include "Logger.hpp"

namespace BasicRandomTests {

const int KV_LEN = 21;

#pragma pack(push, 1)

struct SimpleKey {
  char key[KV_LEN];
};

struct SimpleValue {
  char value[KV_LEN];
};

struct SimpleKV {
  SimpleKey simpleKey;
  SimpleValue simpleValue;
};

struct SimpleBlock {
  concord::kvbc::BlockId id = 0;
  size_t numOfItems = 0;
  SimpleKV items[1];

  static SimpleBlock* alloc(size_t items) {
    size_t blockSize = sizeof(SimpleBlock) + sizeof(SimpleKV) * (items - 1);
    char* buf = new char[blockSize];
    memset(buf, 0, blockSize);
    return (SimpleBlock*)buf;
  }

  static void free(SimpleBlock* buf) { delete[] buf; }
};

enum RequestType : char {
  NONE = 0,
  READ = 1,
  COND_WRITE = 2,
  GET_LAST_BLOCK = 3,
  GET_BLOCK_DATA = 4,
  LONG_EXEC_COND_WRITE = 5,
};

struct SimpleRequest {
  RequestType type = {NONE};
};

struct SimpleGetLastBlockRequest {
  static SimpleGetLastBlockRequest* alloc() {
    size_t reqSize = sizeof(SimpleGetLastBlockRequest);
    char* buf = new char[reqSize];
    memset(buf, 0, reqSize);
    return (SimpleGetLastBlockRequest*)buf;
  }

  static void free(SimpleGetLastBlockRequest* buf) { delete[] buf; }

  SimpleRequest header;
};

// A SimpleGetBlockDataRequest returns a read response, except
// all keys are for the specific block requested.
struct SimpleGetBlockDataRequest {
  static SimpleGetBlockDataRequest* alloc() {
    size_t size = sizeof(SimpleGetBlockDataRequest);
    char* pBuf = new char[size];
    memset(pBuf, 0, size);
    return (SimpleGetBlockDataRequest*)(pBuf);
  }
  static void free(SimpleGetBlockDataRequest* p) { delete[] p; }
  static size_t size() { return sizeof(SimpleGetBlockDataRequest); }

  SimpleRequest h;
  concord::kvbc::BlockId block_id;
};

struct SimpleCondWriteRequest {
  static SimpleCondWriteRequest* alloc(size_t numOfKeysInReadSet, size_t numOfWrites) {
    size_t reqSize = getSize(numOfKeysInReadSet, numOfWrites);
    char* buf = new char[reqSize];
    memset(buf, 0, reqSize);
    return (SimpleCondWriteRequest*)buf;
  }

  static void free(SimpleCondWriteRequest* buf) { delete[] buf; }

  static size_t getSize(size_t numOfKeysInReadSet, size_t numOfWrites) {
    return sizeof(SimpleCondWriteRequest) + numOfKeysInReadSet * sizeof(SimpleKey) + numOfWrites * sizeof(SimpleKV);
  }

  size_t getSize() { return getSize(numOfKeysInReadSet, numOfWrites); }

  SimpleKey* readSetArray() { return (SimpleKey*)(((char*)this) + sizeof(SimpleCondWriteRequest)); }

  SimpleKV* keyValueArray() {
    return (SimpleKV*)(((char*)this) + sizeof(SimpleCondWriteRequest) + numOfKeysInReadSet * sizeof(SimpleKey));
  }

  SimpleRequest header;
  concord::kvbc::BlockId readVersion = 0;
  size_t numOfKeysInReadSet = 0;
  size_t numOfWrites = 0;
};

struct SimpleReadRequest {
  static SimpleReadRequest* alloc(size_t numOfKeysToRead) {
    size_t reqSize = sizeof(SimpleReadRequest) + (sizeof(SimpleKey) * (numOfKeysToRead - 1));
    char* buf = new char[reqSize];
    memset(buf, 0, reqSize);
    return (SimpleReadRequest*)buf;
  }

  static void free(SimpleReadRequest* buf) { delete[] buf; }

  static size_t getSize(size_t numOfKeysToRead) {
    return sizeof(SimpleReadRequest) + (numOfKeysToRead - 1) * sizeof(SimpleKey);
  }

  size_t getSize() { return getSize(numberOfKeysToRead); }
  SimpleKey* keysArray() { return ((SimpleKey*)keys); }

  SimpleRequest header;
  concord::kvbc::BlockId readVersion = 0;  // If 0, read from the latest version
  size_t numberOfKeysToRead = 0;
  SimpleKey keys[1];
};

struct SimpleReply {
  RequestType type = {NONE};
};

struct SimpleReply_ConditionalWrite {
  static SimpleReply_ConditionalWrite* alloc() {
    size_t repSize = sizeof(SimpleReply_ConditionalWrite);
    char* buf = new char[repSize];
    memset(buf, 0, repSize);
    return (SimpleReply_ConditionalWrite*)buf;
  }

  bool isEquiv(SimpleReply_ConditionalWrite& other, std::ostringstream& error) {
    if (header.type != other.header.type) {
      error << "*** Write: Wrong message type: " << other.header.type;
    } else if (latestBlock != other.latestBlock) {
      error << "*** Write: Wrong latestBlock: " << other.latestBlock << ", expected: " << latestBlock;
    } else if (success != other.success) {
      error << "*** Write: Wrong result: " << other.success;
    } else {
      return true;
    }
    return false;
  }

  static void free(SimpleReply_ConditionalWrite* buf) { delete[] buf; }

  SimpleReply header;
  bool success = false;
  concord::kvbc::BlockId latestBlock = 0;
};

struct SimpleReply_Read {
  static size_t getSize(size_t numOfItems) {
    size_t size = sizeof(SimpleReply_Read) + (sizeof(SimpleKV) * (numOfItems - 1));
    return size;
  }

  size_t getSize() { return getSize(numOfItems); }

  static SimpleReply_Read* alloc(size_t numOfItems) {
    size_t size = sizeof(SimpleReply_Read) + (sizeof(SimpleKV) * (numOfItems - 1));
    char* buf = new char[size];
    memset(buf, 0, size);
    return (SimpleReply_Read*)buf;
  }

  bool isEquiv(SimpleReply_Read& other, std::ostringstream& error) {
    if (header.type != other.header.type) {
      error << "*** READ: Wrong message type: " << other.header.type;
      return false;
    }
    if (numOfItems != other.numOfItems) {
      error << "*** READ: Wrong numOfItems: " << other.numOfItems << ", expected: " << numOfItems;
      return false;
    }
    SimpleKV* itemPtr = items;
    SimpleKV* otherItemPtr = other.items;
    for (size_t i = 0; i < numOfItems; i++) {
      if (memcmp(itemPtr->simpleKey.key, otherItemPtr->simpleKey.key, sizeof(itemPtr->simpleKey.key)) != 0) {
        error << "*** READ: Key for item number " << i << " is wrong";
        return false;
      }
      if (memcmp(itemPtr->simpleValue.value, otherItemPtr->simpleValue.value, sizeof(itemPtr->simpleValue.value)) !=
          0) {
        error << "*** READ: Value for item number " << i << " is wrong";
        return false;
      }
      ++itemPtr;
      ++otherItemPtr;
    }
    return true;
  }

  static void free(SimpleReply_Read* buf) { delete[] buf; }

  SimpleReply header;
  size_t numOfItems = 0;
  SimpleKV items[1];
};

struct SimpleReply_GetLastBlock {
  static SimpleReply_GetLastBlock* alloc() {
    size_t repSize = sizeof(SimpleReply_GetLastBlock);
    char* buf = new char[repSize];
    memset(buf, 0, repSize);
    return (SimpleReply_GetLastBlock*)buf;
  }

  bool isEquiv(SimpleReply_GetLastBlock& other, std::ostringstream& error) {
    if (header.type != other.header.type) {
      error << "*** GetLastBlock: Wrong message type: " << other.header.type;
    } else if (latestBlock != other.latestBlock) {
      error << "*** GetLastBlock: Wrong latestBlock: " << other.latestBlock << ", expected: " << latestBlock;
    } else {
      return true;
    }
    return false;
  }

  static void free(SimpleReply_GetLastBlock* buf) { delete[] buf; }

  SimpleReply header;
  concord::kvbc::BlockId latestBlock = 0;
};

struct SimpleReply_HaveYouStopped {
  size_t getSize() { return sizeof(SimpleReply_HaveYouStopped); }

  static SimpleReply_Read* alloc(size_t numOfItems) {
    size_t size = sizeof(SimpleReply_Read);
    char* buf = new char[size];
    memset(buf, 0, size);
    return (SimpleReply_Read*)buf;
  }

  bool isEquiv(SimpleReply_HaveYouStopped& other, std::ostringstream& error) {
    if (header.type != other.header.type) {
      error << "*** WEDGE: Wrong message type: " << other.header.type;
      return false;
    }
    if (stopped != other.stopped) {
      error << "*** WEDGE: Wrong stopeed indication: " << other.stopped;
      return false;
    }

    return true;
  }

  static void free(SimpleReply_HaveYouStopped* buf) { delete[] buf; }

  SimpleReply header;
  int64_t stopped;
};

#pragma pack(pop)

class SimpleKeyBlockIdPair  // Represents <key, blockId>
{
 public:
  const SimpleKey key;
  const concord::kvbc::BlockId blockId;

  SimpleKeyBlockIdPair(const SimpleKey& simpleKey, concord::kvbc::BlockId bId) : key(simpleKey), blockId(bId) {}

  bool operator<(const SimpleKeyBlockIdPair& other) const {
    int c = memcmp((char*)&this->key, (char*)&other.key, sizeof(SimpleKey));
    if (c == 0)
      return this->blockId > other.blockId;
    else
      return (c < 0);
  }

  bool operator==(const SimpleKeyBlockIdPair& other) const {
    if (this->blockId != other.blockId) return false;
    int c = memcmp((char*)&this->key, (char*)&other.key, sizeof(SimpleKey));
    return (c == 0);
  }
};

typedef std::map<SimpleKeyBlockIdPair, SimpleValue> KeyBlockIdToValueMap;
typedef std::list<SimpleRequest*> RequestsList;
typedef std::list<SimpleReply*> RepliesList;

class TestsBuilder {
 public:
  explicit TestsBuilder(logging::Logger& logger, concord::kvbc::IClient& client);
  ~TestsBuilder();

  static size_t sizeOfRequest(SimpleRequest* req);
  static size_t sizeOfReply(SimpleReply* rep);

  void createRandomTest(size_t numOfRequests, size_t seed);
  RequestsList getRequests() { return requests_; }
  RepliesList getReplies() { return replies_; }

 private:
  void create(size_t numOfRequests, size_t seed);
  void createAndInsertRandomConditionalWrite();
  void createAndInsertRandomRead();
  void createAndInsertGetLastBlock();
  void addExpectedWriteReply(bool foundConflict);
  bool lookForConflicts(concord::kvbc::BlockId readVersion, size_t numOfKeysInReadSet, SimpleKey* readKeysArray);
  void addNewBlock(size_t numOfWrites, SimpleKV* writesKVArray);
  void retrieveExistingBlocksFromKVB();
  concord::kvbc::BlockId getInitialLastBlockId();

 private:
  logging::Logger& logger_;
  concord::kvbc::IClient& client_;
  RequestsList requests_;
  RepliesList replies_;
  std::map<concord::kvbc::BlockId, SimpleBlock*> internalBlockchain_;
  KeyBlockIdToValueMap allKeysToValueMap_;
  concord::kvbc::BlockId prevLastBlockId_ = 0;
  concord::kvbc::BlockId lastBlockId_ = 0;
};

}  // namespace BasicRandomTests
