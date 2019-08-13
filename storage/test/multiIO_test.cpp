// Copyright 2019 VMware, all rights reserved
/**
 * Test multi* functions for RocksDBClient class.
 */

#define USE_ROCKSDB 1

#include "Logger.hpp"
#include "hash_defs.h"
#include "gtest/gtest.h"
#include "comparators.h"
#include "rocksdb_client.h"
#include "kv_types.hpp"

using namespace std;

using concordUtils::Status;
using concordUtils::Sliver;
using concordUtils::KeysVector;
using concordUtils::KeyValuePair;
using concordUtils::SetOfKeyValuePairs;
using concordStorage::rocksdb::RocksDBClient;
using concordStorage::blockchain::RocksKeyComparator;

namespace {

RocksDBClient *dbClient = nullptr;
const uint16_t blocksNum = 50;
const uint16_t keyLen = 120;
const uint16_t valueLen = 500;

uint8_t *createAndFillBuf(size_t length) {
  auto *buffer = new uint8_t[length];
  srand(static_cast<uint>(time(nullptr)));
  for (size_t i = 0; i < length; i++) {
    buffer[i] = static_cast<uint8_t>(rand() % 256);
  }
  return buffer;
}

void verifyMultiGet(KeysVector &keys, Sliver inValues[blocksNum],
                    KeysVector &outValues) {
  ASSERT_TRUE(dbClient->multiGet(keys, outValues) == Status::OK());
  ASSERT_TRUE(outValues.size() == blocksNum);
  for (int i = 0; i < blocksNum; i++) {
    ASSERT_TRUE(inValues[i] == outValues[i]);
  }
}

void verifyMultiDel(KeysVector &keys) {
  const Status expectedStatus = Status::NotFound("Not Found");
  for (const auto &it : keys) {
    Sliver outValue;
    ASSERT_TRUE(dbClient->get(it, outValue) == expectedStatus);
  }
}

void launchMultiPut(KeysVector &keys, Sliver inValues[blocksNum],
                    SetOfKeyValuePairs &keyValueMap) {
  for (auto i = 0; i < blocksNum; i++) {
    keys[i] = Sliver(createAndFillBuf(keyLen), keyLen);
    inValues[i] = Sliver(createAndFillBuf(valueLen), valueLen);
    keyValueMap.insert(KeyValuePair(keys[i], inValues[i]));
  }
  ASSERT_TRUE(dbClient->multiPut(keyValueMap).isOK());
}

TEST(multiIO_test, single_put) {
  Sliver key(createAndFillBuf(keyLen), keyLen);
  Sliver inValue(createAndFillBuf(valueLen), valueLen);
  Status status = dbClient->put(key, inValue);
  ASSERT_TRUE(status.isOK());
  Sliver outValue;
  status = dbClient->get(key, outValue);
  ASSERT_TRUE(status.isOK());
  ASSERT_TRUE(inValue == outValue);
}

TEST(multiIO_test, multi_put) {
  KeysVector keys(blocksNum);
  Sliver inValues[blocksNum];
  SetOfKeyValuePairs keyValueMap;
  KeysVector outValues;
  launchMultiPut(keys, inValues, keyValueMap);
  verifyMultiGet(keys, inValues, outValues);
}

TEST(multiIO_test, multi_del) {
  KeysVector keys(blocksNum);
  Sliver inValues[blocksNum];
  SetOfKeyValuePairs keyValueMap;
  KeysVector outValues;
  launchMultiPut(keys, inValues, keyValueMap);
  ASSERT_TRUE(dbClient->multiDel(keys).isOK());
  verifyMultiDel(keys);
}

}  // end namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  const string dbPath = "./rocksdb_test";
  dbClient = new RocksDBClient(dbPath, new RocksKeyComparator());
  dbClient->init();
  int res = RUN_ALL_TESTS();
  return res;
}
