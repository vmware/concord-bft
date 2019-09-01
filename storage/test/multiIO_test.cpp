 // Copyright 2019 VMware, all rights reserved
/**
 * Test multi* functions for RocksDBClient class.
 */

#define USE_ROCKSDB 1

#include "Logger.hpp"
#include "hash_defs.h"
#include "gtest/gtest.h"
#include "rocksdb/key_comparator.h"
#include "rocksdb/client.h"
#include "kv_types.hpp"
#include "blockchain/db_adapter.h"

using namespace std;

using concordUtils::Status;
using concordUtils::Sliver;
using concordUtils::KeysVector;
using concordUtils::KeyValuePair;
using concordUtils::SetOfKeyValuePairs;
using concord::storage::rocksdb::Client;
using concord::storage::rocksdb::KeyComparator;
using concord::storage::ITransaction;
using concord::storage::blockchain::KeyManipulator;
namespace {

Client *dbClient = nullptr;
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
TEST(multiIO_test, basic_transaction)
{
  std::string key1_("basic_transaction::key1");
  Sliver key1(key1_);
  std::string key2_("basic_transaction::key2");
  Sliver key2(key2_);
  std::string val1_("basic_transaction::val1");
  Sliver inValue1(val1_);
  std::string val2_("basic_transaction::val2");
  Sliver inValue2(val2_);

  { // transaction scope
    ITransaction::Guard g(dbClient->beginTransaction());
    g.txn->put(key1, inValue1);
    g.txn->put(key2, inValue2);
    g.txn->remove(key1);
    std::string val1 = g.txn->get(key1);
    ASSERT_TRUE(val1.empty());
    g.txn->put(key1, inValue1);
    val1 = g.txn->get(key1);
    ASSERT_TRUE(inValue1 == Sliver(val1.data(), val1.size()));
  }
  Sliver outValue;
  Status status = dbClient->get(key1, outValue);
  ASSERT_TRUE(status.isOK());
  ASSERT_TRUE(inValue1 == outValue);
  status = dbClient->get(key2, outValue);
  ASSERT_TRUE(status.isOK());
  ASSERT_TRUE(inValue2 == outValue);
}

TEST(multiIO_test, no_commit_during_exception)
{
  std::string key_("no_commit_during_exception::key");
  Sliver key(key_);
  std::string val_("no_commit_during_exception::val");
  Sliver inValue(val_);
  try{
    { // transaction scope
      ITransaction::Guard g(dbClient->beginTransaction());
      g.txn->put(key, inValue);
      g.txn->remove(key);
      std::string val = g.txn->get(key);
      ASSERT_TRUE(val.empty());
      g.txn->put(key, inValue);
      val = g.txn->get(key);
      ASSERT_TRUE(inValue == Sliver(val.data(), val.size()));
      throw std::runtime_error("oops");
    }
  }catch(std::exception& e){}
  Sliver outValue;
  Status status = dbClient->get(key, outValue);
  ASSERT_FALSE(status.isOK());
}



}  // end namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  const string dbPath = "./rocksdb_test";
  dbClient = new Client(dbPath, new KeyComparator(new KeyManipulator()));
  dbClient->init();
  int res = RUN_ALL_TESTS();
  return res;
}
