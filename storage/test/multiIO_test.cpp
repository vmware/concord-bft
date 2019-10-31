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
using concordUtils::BlockId;
using concord::storage::rocksdb::Client;
using concord::storage::rocksdb::KeyComparator;
using concord::storage::ITransaction;
using concord::storage::blockchain::KeyManipulator;

namespace {

const uint16_t blocksNum = 50;
const uint16_t keyLen = 120;
const uint16_t valueLen = 500;

std::unique_ptr<Client> dbClient;

uint8_t *createAndFillBuf(size_t length) {
  auto *buffer = new uint8_t[length];
  srand(static_cast<uint>(time(nullptr)));
  for (size_t i = 0; i < length; i++) {
    buffer[i] = static_cast<uint8_t>(rand() % 256);
  }
  return buffer;
}

void verifyMultiGet(KeysVector &keys, Sliver inValues[blocksNum], KeysVector &outValues) {
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

void launchMultiPut(KeysVector &keys, Sliver inValues[blocksNum], SetOfKeyValuePairs &keyValueMap) {
  std::unique_ptr<KeyManipulator> key_manip_(new KeyManipulator);
  for (auto i = 0; i < blocksNum; i++) {
    keys[i] = key_manip_->genDataDbKey(Sliver(createAndFillBuf(keyLen), keyLen), i);
    inValues[i] = Sliver(createAndFillBuf(valueLen), valueLen);
    keyValueMap.insert(KeyValuePair(keys[i], inValues[i]));
  }
  ASSERT_TRUE(dbClient->multiPut(keyValueMap).isOK());
}

class multiIO_test : public ::testing::Test {
 protected:
  void SetUp() override {
    key_manipulator_.reset(new KeyManipulator());
    comparator_ = new KeyComparator(new KeyManipulator());
    dbClient.reset(new Client(dbPath_, comparator_));
    dbClient->init();
  }

  void TearDown() override {
    dbClient.reset();
    delete comparator_;
    string cmd = string("rm -rf ") + dbPath_;
    if (system(cmd.c_str())) {
      ASSERT_TRUE(false);
    }
  }

  const string dbPath_ = "./rocksdb_test";
  KeyComparator *comparator_;

  // comparator_ owns the manipulator passed to its constructor
  // This is a useful copy for generating keys
  std::unique_ptr<KeyManipulator> key_manipulator_;
};

TEST_F(multiIO_test, single_put) {
  BlockId block_id = 0;
  Sliver datakey(createAndFillBuf(keyLen), keyLen);
  Sliver key = key_manipulator_->genDataDbKey(datakey, block_id);
  Sliver inValue(createAndFillBuf(valueLen), valueLen);
  Status status = dbClient->put(key, inValue);
  ASSERT_TRUE(status.isOK());
  Sliver outValue;
  status = dbClient->get(key, outValue);
  ASSERT_TRUE(status.isOK());
  ASSERT_TRUE(inValue == outValue);
}

TEST_F(multiIO_test, multi_put) {
  KeysVector keys(blocksNum);
  Sliver inValues[blocksNum];
  SetOfKeyValuePairs keyValueMap;
  KeysVector outValues;
  launchMultiPut(keys, inValues, keyValueMap);
  verifyMultiGet(keys, inValues, outValues);
}

TEST_F(multiIO_test, multi_del) {
  KeysVector keys(blocksNum);
  Sliver inValues[blocksNum];
  SetOfKeyValuePairs keyValueMap;
  KeysVector outValues;
  launchMultiPut(keys, inValues, keyValueMap);
  ASSERT_TRUE(dbClient->multiDel(keys).isOK());
  verifyMultiDel(keys);
}
TEST_F(multiIO_test, basic_transaction) {
  std::string key1_("basic_transaction::key1");
  Sliver key1(key1_);
  std::string key2_("basic_transaction::key2");
  Sliver key2(key2_);
  std::string val1_("basic_transaction::val1");
  Sliver inValue1(val1_);
  std::string val2_("basic_transaction::val2");
  Sliver inValue2(val2_);

  key1 = key_manipulator_->genDataDbKey(key1, 0);
  key2 = key_manipulator_->genDataDbKey(key2, 0);

  {  // transaction scope
    ITransaction::Guard g(dbClient->beginTransaction());
    g.txn()->put(key1, inValue1);
    g.txn()->put(key2, inValue2);
    g.txn()->del(key1);
    std::string val1 = g.txn()->get(key1);
    ASSERT_TRUE(val1.empty());
    g.txn()->put(key1, inValue1);
    val1 = g.txn()->get(key1);
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

TEST_F(multiIO_test, no_commit_during_exception) {
  std::string key_("no_commit_during_exception::key");
  Sliver key(key_);
  std::string val_("no_commit_during_exception::val");
  Sliver inValue(val_);
  key = key_manipulator_->genDataDbKey(key, 0);
  try {
    {  // transaction scope
      ITransaction::Guard g(dbClient->beginTransaction());
      g.txn()->put(key, inValue);
      g.txn()->del(key);
      std::string val = g.txn()->get(key);
      ASSERT_TRUE(val.empty());
      g.txn()->put(key, inValue);
      val = g.txn()->get(key);
      ASSERT_TRUE(inValue == Sliver(val.data(), val.size()));
      throw std::runtime_error("oops");
    }
  } catch (std::exception &e) {
  }
  Sliver outValue;
  Status status = dbClient->get(key, outValue);
  ASSERT_FALSE(status.isOK());
}

}  // end namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  int res = RUN_ALL_TESTS();
  return res;
}
