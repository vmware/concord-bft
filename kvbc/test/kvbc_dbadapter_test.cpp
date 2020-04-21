// Copyright 2019 VMware, all rights reserved
/**
 * Test multi* functions for RocksDBClient class.
 */

#include "Logger.hpp"
#include "gtest/gtest.h"
#include "kv_types.hpp"
#include "direct_kv_db_adapter.h"

#ifdef USE_ROCKSDB
#include "rocksdb/client.h"
#include "rocksdb/key_comparator.h"
#endif

using namespace std;

using concordUtils::Status;
using concordUtils::Sliver;
using concord::kvbc::KeysVector;
using concord::kvbc::KeyValuePair;
using concord::kvbc::SetOfKeyValuePairs;
using concord::kvbc::BlockId;
// using concord::storage::rocksdb::Client;
// using concord::storage::rocksdb::KeyComparator;
using concord::storage::ITransaction;
using concord::kvbc::v1DirectKeyValue::DBKeyManipulator;
using concord::kvbc::v1DirectKeyValue::DBKeyComparator;

namespace {

std::unique_ptr<concord::storage::IDBClient> dbClient;

class kvbc_dbadapter_test : public ::testing::Test {
 protected:
  void SetUp() override {
    //    keyGen_.reset(new concord::kvbc::KeyGenerator);
    //    comparator_ = new KeyComparator(new DBKeyComparator());
    //    dbClient.reset(new Client(dbPath_, comparator_));
    //    dbClient->init();
  }

  void TearDown() override {
    //    dbClient.reset();
    //    delete comparator_;
    //    string cmd = string("rm -rf ") + dbPath_;
    //    if (system(cmd.c_str())) {
    //      ASSERT_TRUE(false);
    //    }
  }

  std::unique_ptr<concord::kvbc::v1DirectKeyValue::IDataKeyGenerator> keyGen_;
  const string dbPath_ = "./rocksdb_test";
  //  KeyComparator *comparator_;
};

TEST_F(kvbc_dbadapter_test, basic) {
  //  auto config = get_dbadapter_configuration();
  //  IDbAdapter* dbAdapter = new concord::kvbc::DBAdapter(std::shared_ptr<IDBClient>(std::get<0>(config)),
  //                                                       std::shared_ptr<IDBClient>(std::get<1>(config)),
  //                                                       std::unique_ptr<IDataKeyGenerator>(std::get<2>(config)));
  //  dbAdapter->getDb();
}

}  // end namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  int res = RUN_ALL_TESTS();
  return res;
}
