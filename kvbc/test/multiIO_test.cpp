// Copyright 2019 VMware, all rights reserved
/**
 * Test multi* functions for RocksDBClient class.
 */

#define USE_ROCKSDB 1

#include "Logger.hpp"
#include "gtest/gtest.h"
#include "rocksdb/key_comparator.h"
#include "rocksdb/client.h"
#include "kv_types.hpp"
#include "direct_kv_db_adapter.h"

using namespace std;

using concordUtils::Status;
using concordUtils::Sliver;
using concord::kvbc::KeysVector;
using concord::kvbc::KeyValuePair;
using concord::kvbc::SetOfKeyValuePairs;
using concord::kvbc::BlockId;
using concord::storage::rocksdb::Client;
using concord::storage::rocksdb::KeyComparator;
using concord::storage::ITransaction;
using concord::kvbc::v1DirectKeyValue::DBKeyComparator;

namespace {

constexpr uint16_t blocksNum = 50;
constexpr uint16_t keyLen = 120;
constexpr uint16_t valueLen = 500;

const auto blockValue = std::string{"block"};

std::unique_ptr<Client> dbClient;

char *createAndFillBuf(size_t length) {
  auto *buffer = new char[length];
  srand(static_cast<uint>(time(nullptr)));
  for (size_t i = 0; i < length; i++) {
    buffer[i] = static_cast<char>(rand() % 256);
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

class multiIO_test : public ::testing::Test {
 protected:
  void SetUp() override {
    keyGen_.reset(new concord::kvbc::v1DirectKeyValue::RocksKeyGenerator);
    dbClient.reset(new Client(dbPath_, std::make_unique<KeyComparator>(new DBKeyComparator{})));
    dbClient->init();
  }

  void TearDown() override {
    dbClient.reset();
    string cmd = string("rm -rf ") + dbPath_;
    if (system(cmd.c_str())) {
      ASSERT_TRUE(false);
    }
  }

  void launchMultiPut(KeysVector &keys, Sliver inValues[blocksNum], SetOfKeyValuePairs &keyValueMap) {
    for (auto i = 0; i < blocksNum; i++) {
      keys[i] = keyGen_->dataKey(Sliver(createAndFillBuf(keyLen), keyLen), i);
      inValues[i] = Sliver(createAndFillBuf(valueLen), valueLen);
      keyValueMap.insert(KeyValuePair(keys[i], inValues[i]));
    }
    ASSERT_TRUE(dbClient->multiPut(keyValueMap).isOK());
  }

  void addBlockRange(BlockId begin, BlockId end) const {
    for (auto i = begin; i < end; ++i) {
      ASSERT_TRUE(dbClient->put(keyGen_->blockKey(i), Sliver{blockValue + std::to_string(i)}).isOK());
    }
  }

  ::testing::AssertionResult blockRangeIsEmpty(BlockId begin, BlockId end) const {
    for (auto i = begin; i < end; ++i) {
      auto v = Sliver{};
      if (!dbClient->get(keyGen_->blockKey(i), v).isNotFound()) {
        return ::testing::AssertionFailure() << "Block range[" << begin << ", " << end << ") is not empty";
      }
    }
    return ::testing::AssertionSuccess();
  }

  ::testing::AssertionResult blockRangeIsSet(BlockId begin, BlockId end) const {
    for (auto i = begin; i < end; ++i) {
      auto v = Sliver{};
      if (!dbClient->get(keyGen_->blockKey(i), v).isOK() || v != Sliver{blockValue + std::to_string(i)}) {
        return ::testing::AssertionFailure() << "Block range[" << begin << ", " << end << ") is not set";
      }
    }
    return ::testing::AssertionSuccess();
  }

  std::unique_ptr<concord::kvbc::v1DirectKeyValue::IDataKeyGenerator> keyGen_;
  const string dbPath_ = "./rocksdb_test";
};

TEST_F(multiIO_test, single_put) {
  BlockId block_id = 0;
  Sliver datakey(createAndFillBuf(keyLen), keyLen);
  Sliver key = keyGen_->dataKey(datakey, block_id);
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
  Sliver key1("basic_transaction::key1");
  Sliver key2("basic_transaction::key2");
  Sliver inValue1("basic_transaction::val1");
  Sliver inValue2("basic_transaction::val2");

  key1 = keyGen_->dataKey(key1, 0);
  key2 = keyGen_->dataKey(key2, 0);

  {  // transaction scope
    ITransaction::Guard g(dbClient->beginTransaction());
    g.txn()->put(key1, inValue1);
    g.txn()->put(key2, inValue2);
    g.txn()->del(key1);
    std::string val1 = g.txn()->get(key1);
    ASSERT_TRUE(val1.empty());
    g.txn()->put(key1, inValue1);
    val1 = g.txn()->get(key1);
    ASSERT_TRUE(inValue1 == Sliver(std::move(val1)));
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
  Sliver key("no_commit_during_exception::key");
  Sliver inValue("no_commit_during_exception::val");
  key = keyGen_->dataKey(key, 0);
  try {
    {  // transaction scope
      ITransaction::Guard g(dbClient->beginTransaction());
      g.txn()->put(key, inValue);
      g.txn()->del(key);
      std::string val = g.txn()->get(key);
      ASSERT_TRUE(val.empty());
      g.txn()->put(key, inValue);
      val = g.txn()->get(key);
      ASSERT_TRUE(inValue == Sliver(std::move(val)));
      throw std::runtime_error("oops");
    }
  } catch (std::exception &e) {
  }
  Sliver outValue;
  Status status = dbClient->get(key, outValue);
  ASSERT_FALSE(status.isOK());
}

// Note1: rangeDel() tests below rely on the fact that block keys are ordered in ascending order.
// Note2: rangeDel() tests below require the blocksNum value to be above a certain threshold for them to be correct.
static_assert(blocksNum > 20);

TEST_F(multiIO_test, delete_whole_range) {
  addBlockRange(0, blocksNum);
  ASSERT_TRUE(dbClient->rangeDel(keyGen_->blockKey(0), keyGen_->blockKey(blocksNum)).isOK());
  ASSERT_TRUE(blockRangeIsEmpty(0, blocksNum));
}

TEST_F(multiIO_test, delete_range_from_begin_to_less_than_end) {
  addBlockRange(0, blocksNum);
  ASSERT_TRUE(dbClient->rangeDel(keyGen_->blockKey(0), keyGen_->blockKey(blocksNum - 5)).isOK());
  ASSERT_TRUE(blockRangeIsEmpty(0, blocksNum - 5));
  ASSERT_TRUE(blockRangeIsSet(blocksNum - 5, blocksNum));
}

TEST_F(multiIO_test, delete_range_from_after_begin_to_end) {
  addBlockRange(0, blocksNum);
  ASSERT_TRUE(dbClient->rangeDel(keyGen_->blockKey(5), keyGen_->blockKey(blocksNum)).isOK());
  ASSERT_TRUE(blockRangeIsEmpty(5, blocksNum));
  ASSERT_TRUE(blockRangeIsSet(0, 4));
}

TEST_F(multiIO_test, delete_range_in_the_middle) {
  addBlockRange(0, blocksNum);
  ASSERT_TRUE(dbClient->rangeDel(keyGen_->blockKey(5), keyGen_->blockKey(blocksNum - 10)).isOK());
  ASSERT_TRUE(blockRangeIsEmpty(5, blocksNum - 10));
  ASSERT_TRUE(blockRangeIsSet(0, 4));
  ASSERT_TRUE(blockRangeIsSet(blocksNum - 10, blocksNum));
}

TEST_F(multiIO_test, delete_range_non_existent_keys) {
  addBlockRange(10, blocksNum);
  addBlockRange(blocksNum + 10, blocksNum + 10 + 2);
  ASSERT_TRUE(dbClient->rangeDel(keyGen_->blockKey(8), keyGen_->blockKey(blocksNum + 3)).isOK());
  ASSERT_TRUE(blockRangeIsEmpty(8, blocksNum));
  ASSERT_TRUE(blockRangeIsSet(blocksNum + 10, blocksNum + 10 + 2));
}

TEST_F(multiIO_test, delete_empty_range) {
  addBlockRange(0, blocksNum);
  ASSERT_TRUE(dbClient->rangeDel(keyGen_->blockKey(5), keyGen_->blockKey(5)).isOK());
  ASSERT_TRUE(blockRangeIsSet(0, blocksNum));
}

}  // end namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  int res = RUN_ALL_TESTS();
  return res;
}
