// Copyright 2019 VMware, all rights reserved
/**
 * Test public functions for RocksDBMetadataStorage class.
 */

#define USE_ROCKSDB 1

#include "Logger.hpp"
#include "gtest/gtest.h"
#include "rocksdb/key_comparator.h"
#include "rocksdb/client.h"
#include "DbMetadataStorage.hpp"
#include "direct_kv_db_adapter.h"
#include "storage/direct_kv_key_manipulator.h"

#include <experimental/filesystem>

using namespace std;

using concord::storage::ObjectId;
using concord::kvbc::v1DirectKeyValue::DBKeyComparator;
using concord::storage::v1DirectKeyValue::MetadataKeyManipulator;
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

uint8_t *writeRandomData(const ObjectId &objectId, const uint32_t &dataLen, DBMetadataStorage *db = nullptr) {
  if (db == nullptr) {
    db = metadataStorage;
  }
  uint8_t *data = createAndFillBuf(dataLen);
  db->atomicWrite(objectId, (char *)data, dataLen);
  return data;
}

uint8_t *writeInTransaction(const ObjectId &objectId, const uint32_t &dataLen) {
  uint8_t *data = createAndFillBuf(dataLen);
  metadataStorage->writeInBatch(objectId, (char *)data, dataLen);
  return data;
}

DBMetadataStorage *initiateMetadataStorage(Client *dbClient, const std::string &dbPath, bool remove_old_file) {
  if (remove_old_file) {
    std::experimental::filesystem::remove_all(dbPath.c_str());
  }
  DBMetadataStorage *metadataStorage_ =
      new DBMetadataStorage(dbClient, std::make_unique<concord::storage::v1DirectKeyValue::MetadataKeyManipulator>());
  bftEngine::MetadataStorage::ObjectDesc objectDesc[objectsNum];
  for (uint32_t id = initialObjectId; id < objectsNum; ++id) {
    objectDesc[id].id = id;
    objectDesc[id].maxSize = maxObjDataSize;
  }
  metadataStorage_->initMaxSizeOfObjects(objectDesc, objectsNum);
  return metadataStorage_;
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
  metadataStorage->beginAtomicWriteOnlyBatch();
  uint8_t *inBuf[objectsNum];
  uint8_t *outBuf[objectsNum];
  uint32_t objectsDataSize[objectsNum];
  for (ObjectId i = initialObjectId; i < objectsNum; i++) {
    objectsDataSize[i] = initialObjDataSize + i;
    inBuf[i] = writeInTransaction(i, objectsDataSize[i]);
    outBuf[i] = new uint8_t[objectsDataSize[i]];
  }
  metadataStorage->commitAtomicWriteOnlyBatch();
  uint32_t realSize = 0;
  for (ObjectId i = initialObjectId; i < objectsNum; i++) {
    metadataStorage->read(i, objectsDataSize[i], (char *)outBuf[i], realSize);
    ASSERT_TRUE(objectsDataSize[i] == realSize);
    ASSERT_TRUE(is_match(inBuf[i], outBuf[i], realSize));
    delete[] inBuf[i];
    delete[] outBuf[i];
  }
}

uint8_t *createUpdateAndCloseDB(Client *client, bool clear_db = false) {
  auto db = initiateMetadataStorage(client, "./metadataStorage_test_db_recover", true);
  auto *inBuf = writeRandomData(initialObjectId, initialObjDataSize, db);
  if (clear_db) {
    db->eraseData();
  }
  delete db;
  return inBuf;
}

TEST(metadataStorage_test, recovering_test) {
  Client *dbClient_ = new Client("./metadataStorage_test_db_recover", make_unique<KeyComparator>(new DBKeyComparator));
  dbClient_->init();
  // We create a db in a different path and close it after writing a single object.
  auto inBuf = createUpdateAndCloseDB(dbClient_);

  // We will now verify that the written object is available if we load the DB from the file.
  auto db = initiateMetadataStorage(dbClient_, "./metadataStorage_test_db_recover", false);

  auto *outBuf = new uint8_t[initialObjDataSize];
  uint32_t realSize = 0;
  db->read(initialObjectId, initialObjDataSize, (char *)outBuf, realSize);
  ASSERT_TRUE(initialObjDataSize == realSize);
  ASSERT_TRUE(is_match(inBuf, outBuf, realSize));

  delete[] outBuf;
  delete[] inBuf;
  delete db;
  delete dbClient_;
}

TEST(metadataStorage_test, clear_db_test) {
  Client *dbClient_ = new Client("./metadataStorage_test_db_recover", make_unique<KeyComparator>(new DBKeyComparator));
  dbClient_->init();
  // We create a db in a different path and close it after writing a single object. Also, we tell the DB to delete its
  // metadata before closing.
  auto inBuf = createUpdateAndCloseDB(dbClient_, true);

  // We will now verify that the written object is not available if we load the DB from the file.
  auto db = initiateMetadataStorage(dbClient_, "./metadataStorage_test_db_recover", false);

  auto *outBuf = new uint8_t[initialObjDataSize];
  uint32_t realSize = 0;
  db->read(initialObjectId, initialObjDataSize, (char *)outBuf, realSize);
  ASSERT_TRUE(realSize == 0);

  delete[] outBuf;
  delete[] inBuf;
  delete db;
  delete dbClient_;
}
}  // end namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  Client *dbClient = new Client("./metadataStorage_test_db", make_unique<KeyComparator>(new DBKeyComparator));
  dbClient->init();
  metadataStorage = initiateMetadataStorage(dbClient, "./metadataStorage_test_db", true);
  int res = RUN_ALL_TESTS();
  return res;
}
