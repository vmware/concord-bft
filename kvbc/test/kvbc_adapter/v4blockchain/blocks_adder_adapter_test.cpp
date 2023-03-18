// Concord
//
// Copyright (c) 2023 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "categorization/db_categories.h"
#include "kvbc_adapter/replica_adapter.hpp"
#include "kvbc_key_types.hpp"
#include "storage/test/storage_test_common.h"
#include "block_metadata.hpp"
#include "block_header.hpp"

#include <map>

namespace {
using namespace ::testing;
using namespace concord::kvbc;
using namespace concord::kvbc::categorization;
using concord::kvbc::adapter::ReplicaBlockchain;
using concord::storage::rocksdb::NativeClient;

class BlocksAdderAdapterTest : public Test {
  void SetUp() override {
    destroyDb();
    db_ = TestRocksDb::createNative();
    bftEngine::ReplicaConfig::instance().kvBlockchainVersion = BLOCKCHAIN_VERSION::V4_BLOCKCHAIN;
    bftEngine::ReplicaConfig::instance().extraHeader = true;
    kvbc_ = std::make_unique<ReplicaBlockchain>(
        db_,
        false,
        std::map<std::string, CATEGORY_TYPE>{{kExecutionProvableCategory, CATEGORY_TYPE::block_merkle},
                                             {kConcordInternalCategoryId, CATEGORY_TYPE::versioned_kv},
                                             {kExecutionPrivateCategory, CATEGORY_TYPE::versioned_kv}});
  }

  void TearDown() override { destroyDb(); }

  void destroyDb() {
    db_.reset();
    ASSERT_EQ(0, db_.use_count());
    cleanup();
  }

 public:
  // read block header
  BlockHeader read_block(BlockId block_id) {
    auto key = BlockHeader::blockNumAsKeyToBlockHeader((uint8_t *)&block_id, sizeof(block_id));
    auto opt_val = kvbc_->getLatest(kExecutionPrivateCategory, key);
    auto val = std::get<VersionedValue>(*opt_val);
    return BlockHeader::deserialize(val.data);
  }

 protected:
  std::shared_ptr<NativeClient> db_;
  std::unique_ptr<ReplicaBlockchain> kvbc_;
};

TEST_F(BlocksAdderAdapterTest, validate_header) {
  // write two blocks
  for (size_t i = 1; i <= 2; i++) {
    auto updates = Updates{};
    auto ver_updates = VersionedUpdates{};
    ver_updates.addUpdate(std::string{BlockMetadata::kBlockMetadataKeyStr}, concordUtils::toBigEndianStringBuffer(i));
    updates.add(kConcordInternalCategoryId, std::move(ver_updates));
    ASSERT_EQ(kvbc_->add(std::move(updates)), i);
  }
  ASSERT_EQ(kvbc_->getLastReachableBlockNum(), 2);

  // compare computed hash with the one read from storage
  auto parent_header = read_block(0);
  auto hash = parent_header.hash();
  auto header = read_block(1);
  ASSERT_EQ(hash, header.data.parent_hash);
}

}  // end namespace

int main(int argc, char **argv) {
  logging::initLogger("logging.properties");
  ::testing::InitGoogleTest(&argc, argv);
  int res = RUN_ALL_TESTS();
  return res;
}