// Copyright 2019 VMware, all rights reserved
/**
 * Test public functions for RocksDBMetadataStorage class.
 */

#define USE_ROCKSDB 1

#include "Logger.hpp"
#include "hash_defs.h"
#include "gtest/gtest.h"
#include "rocksdb/key_comparator.h"
#include "rocksdb/client.h"
#include "storage/db_metadata_storage.h"
#include "blockchain/db_types.h"
#include "blockchain/db_adapter.h"
#include "bftengine/MetadataStorage.hpp"

using namespace std;

using concord::storage::blockchain::ObjectId;
using concord::storage::blockchain::KeyManipulator;
using concord::storage::rocksdb::KeyComparator;
using concord::storage::rocksdb::Client;
using concord::storage::DBMetadataStorage;

namespace {

DBMetadataStorage *metadataStorage = nullptr;
const ObjectId initialObjectId = 2;
const uint32_t initialObjDataSize = 80;
const uint32_t maxObjDataSize = 200;
const uint16_t objectsNum = 100;

uint8_t *createAndFillBuf(size_t length) {
  auto *buffer = new uint8_t[length];
  srand(static_cast<uint>(time(nullptr)));
  for (size_t i = 0; i < length; i++) {
    buffer[i] = static_cast<uint8_t>(rand() % 256);
  }
  return buffer;
}

uint8_t *writeRandomData(const ObjectId &objectId, const uint32_t &dataLen) {
  uint8_t *data = createAndFillBuf(dataLen);
  metadataStorage->atomicWrite(objectId, (char *)data, dataLen);
  return data;
}

uint8_t *writeInTransaction(const ObjectId &objectId, const uint32_t &dataLen) {
  uint8_t *data = createAndFillBuf(dataLen);
  metadataStorage->writeInTransaction(objectId, (char *)data, dataLen);
  return data;
}

bool is_match(const uint8_t *exp, const uint8_t *actual, const size_t len) {
  for (size_t i = 0; i < len; i++) {
    if (exp[i] != actual[i]) {
      return false;
    }
  }
  return true;
}

TEST(metadataStorage_test, single_read) {
  auto *inBuf = writeRandomData(initialObjectId, initialObjDataSize);
  auto *outBuf = new uint8_t[initialObjDataSize];
  uint32_t realSize = 0;
  metadataStorage->read(initialObjectId, initialObjDataSize, (char *)outBuf, realSize);
  ASSERT_TRUE(initialObjDataSize == realSize);
  ASSERT_TRUE(is_match(inBuf, outBuf, realSize));
  delete[] inBuf;
  delete[] outBuf;
}

TEST(metadataStorage_test, multi_write) {
  metadataStorage->beginAtomicWriteOnlyTransaction();
  uint8_t *inBuf[objectsNum];
  uint8_t *outBuf[objectsNum];
  uint32_t objectsDataSize[objectsNum];
  for (ObjectId i = initialObjectId; i < objectsNum; ++i)
    objectsDataSize[i] = initialObjDataSize;
  for (ObjectId i = initialObjectId; i < objectsNum; i++) {
    objectsDataSize[i] += i;
    inBuf[i] = writeInTransaction(i, objectsDataSize[i]);
    outBuf[i] = new uint8_t[objectsDataSize[i]];
  }
  metadataStorage->commitAtomicWriteOnlyTransaction();
  uint32_t realSize = 0;
  for (ObjectId i = initialObjectId; i < objectsNum; i++) {
    metadataStorage->read(i, objectsDataSize[i], (char *)outBuf[i], realSize);
    ASSERT_TRUE(objectsDataSize[i] == realSize);
    ASSERT_TRUE(is_match(inBuf[i], outBuf[i], realSize));
    delete[] inBuf[i];
    delete[] outBuf[i];
  }
}

}  // end namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  KeyComparator* kk = new KeyComparator(new KeyManipulator);
  const string dbPath = "./metadataStorage_test_db";
  remove(dbPath.c_str());
  Client *dbClient = new Client(dbPath, kk);
  dbClient->init();
  metadataStorage = new DBMetadataStorage(dbClient, KeyManipulator::generateMetadataKey);
  bftEngine::MetadataStorage::ObjectDesc objectDesc[objectsNum];
  for (uint32_t id = initialObjectId; id < objectsNum; ++id) {
    objectDesc[id].id = id;
    objectDesc[id].maxSize = maxObjDataSize;
  }
  metadataStorage->initMaxSizeOfObjects(objectDesc, objectsNum);
  int res = RUN_ALL_TESTS();
  return res;
}
