// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "db_editor_tests_base.h"
#include "kv_blockchain_db_editor.hpp"

namespace {

using namespace concord::kvbc::tools::db_editor;

const auto kTestName = kToolName + "_test";
const auto kCategoryMerkle = "merkle"s;
const auto kCategoryVersioned = "versioned"s;
const auto kCategoryImmutable = "immutable"s;

class DbEditorTests : public DbEditorTestsBase {
 public:
  void CreateBlockchain(std::size_t db_id, BlockId blocks, std::optional<BlockId> mismatch_at = std::nullopt) override {
    auto db = TestRocksDb::create(db_id);
    auto adapter = KeyValueBlockchain{
        concord::storage::rocksdb::NativeClient::fromIDBClient(db),
        true,
        std::map<std::string, CATEGORY_TYPE>{
            {kCategoryMerkle, CATEGORY_TYPE::block_merkle},
            {kCategoryVersioned, CATEGORY_TYPE::versioned_kv},
            {kCategoryImmutable, CATEGORY_TYPE::immutable},
            {concord::kvbc::categorization::kConcordInternalCategoryId, CATEGORY_TYPE::versioned_kv}}};

    const auto mismatch_kv = std::make_pair(getSliver(std::numeric_limits<unsigned>::max()), getSliver(42));

    for (auto i = 1u; i <= blocks; ++i) {
      Updates updates;
      BlockMerkleUpdates merkle_updates;
      VersionedUpdates ver_updates;
      ImmutableUpdates immutable_updates;
      if (empty_block_id_ != i) {
        for (auto j = 0u; j < num_keys_; ++j) {
          merkle_updates.addUpdate(getSliver((j + i - 1) * kv_multiplier_).toString(),
                                   getSliver((j + i - 1) * 2 * kv_multiplier_).toString());
          ver_updates.addUpdate("vkey"s + std::to_string((j + i - 1) * kv_multiplier_),
                                "vval"s + std::to_string((j + i - 1) * 2 * kv_multiplier_));
        }
      }

      // Add a key to simulate a mismatch.
      if (mismatch_at.has_value() && *mismatch_at == i) {
        merkle_updates.addUpdate(mismatch_kv.first.toString(), mismatch_kv.second.toString());
        ver_updates.addUpdate(mismatch_kv.first.toString(), mismatch_kv.second.toString());
      }
      updates.add(kCategoryMerkle, std::move(merkle_updates));
      updates.add(kCategoryVersioned, std::move(ver_updates));
      if (i == 1u) {
        immutable_updates.addUpdate("immutable_key1", {"immutable_val1", {"1", "2"}});
        updates.add("immutable", std::move(immutable_updates));
      }
      ASSERT_NO_THROW(adapter.addBlock(std::move(updates)));
    }

    const auto status = db->multiPut(generateMetadata());
    ASSERT_TRUE(status.isOK());

    {  // Generate ST metadata
      using bftEngine::bcst::impl::DataStore;
      using bftEngine::bcst::impl::DBDataStore;
      using bftEngine::bcst::impl::DataStoreTransaction;
      using concord::storage::v2MerkleTree::STKeyManipulator;
      std::unique_ptr<DataStore> ds =
          std::make_unique<DBDataStore>(db, 1024 * 4, std::make_shared<STKeyManipulator>(), true);
      DataStoreTransaction::Guard g(ds->beginTransaction());
      g.txn()->setReplicas({0, 1, 2, 3});
      g.txn()->setMyReplicaId(0);
      g.txn()->setFVal(1);
      g.txn()->setMaxNumOfStoredCheckpoints(3);
      g.txn()->setNumberOfReservedPages(75);
      g.txn()->setLastStoredCheckpoint(0);
      g.txn()->setFirstStoredCheckpoint(0);
      g.txn()->setIsFetchingState(false);
      g.txn()->setFirstRequiredBlock(0);
      g.txn()->setLastRequiredBlock(0);
      g.txn()->setAsInitialized();
    }
  }

  void DeleteBlocksUntil(std::size_t db_id, BlockId until_block_id) override {
    auto db = TestRocksDb::createNative(db_id);
    auto adapter = KeyValueBlockchain{db, true};

    for (auto i = 1ull; i < until_block_id; ++i) {
      adapter.deleteBlock(i);
    }
  }

 protected:
  static constexpr unsigned num_categories_{2};
};

TEST_F(DbEditorTests, no_arguments) {
  ASSERT_EQ(EXIT_FAILURE, run(CommandLineArguments{{kTestName}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Usage:"));
}

TEST_F(DbEditorTests, invalid_command) {
  ASSERT_EQ(EXIT_FAILURE, run(CommandLineArguments{{kTestName, "unknownCommand"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Usage:"));
}

TEST_F(DbEditorTests, get_genesis_block_id) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getGenesisBlockID"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_EQ("{\n  \"genesisBlockID\": \"1\"\n}\n", out_.str());
}

TEST_F(DbEditorTests, get_last_reachable_block_id) {
  ASSERT_EQ(
      EXIT_SUCCESS,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getLastReachableBlockID"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_EQ("{\n  \"lastReachableBlockID\": \""s + std::to_string(num_blocks_) + "\"\n}\n", out_.str());
}

TEST_F(DbEditorTests, get_last_block_id) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getLastBlockID"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_EQ("{\n  \"lastBlockID\": \""s + std::to_string(num_blocks_) + "\"\n}\n", out_.str());
}

TEST_F(DbEditorTests, get_raw_block) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getRawBlock", "5"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_THAT(out_.str(), StartsWith("{\n  \"rawBlock\": \"0x"));
}

TEST_F(DbEditorTests, get_raw_block_missing_block_id) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getRawBlock"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getRawBlock], reason: Missing BLOCK-ID argument"));
}

TEST_F(DbEditorTests, get_raw_block_invalid_block_id) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getRawBlock", "5ab"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getRawBlock], reason: Invalid BLOCK-ID: 5ab"));
}

TEST_F(DbEditorTests, get_raw_block_range_full_range) {
  ASSERT_EQ(
      EXIT_SUCCESS,
      run(
          CommandLineArguments{
              {kTestName, rocksDbPath(main_path_db_id_), "getRawBlockRange", "1", std::to_string(num_blocks_ + 1)}},
          out_,
          err_));
  ASSERT_TRUE(err_.str().empty());
  for (auto i = 1u; i <= num_blocks_; ++i) {
    ASSERT_THAT(out_.str(), HasSubstr("  \"rawBlock" + std::to_string(i) + "\": \"0x"));
  }
}

TEST_F(DbEditorTests, get_raw_block_range_except_last) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(
                CommandLineArguments{
                    {kTestName, rocksDbPath(main_path_db_id_), "getRawBlockRange", "1", std::to_string(num_blocks_)}},
                out_,
                err_));
  ASSERT_TRUE(err_.str().empty());
  for (auto i = 1u; i < num_blocks_; ++i) {
    ASSERT_THAT(out_.str(), HasSubstr("  \"rawBlock" + std::to_string(i) + "\": \"0x"));
  }
  ASSERT_THAT(out_.str(), Not(HasSubstr("  \"rawBlock" + std::to_string(num_blocks_) + "\": \"0x")));
}

TEST_F(DbEditorTests, get_raw_block_range_whole_blockchain) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{kTestName,
                                      rocksDbPath(main_path_db_id_),
                                      "getRawBlockRange",
                                      "1",
                                      std::to_string(std::numeric_limits<BlockId>::max())}},
                out_,
                err_));
  ASSERT_TRUE(err_.str().empty());
  for (auto i = 1u; i <= num_blocks_; ++i) {
    ASSERT_THAT(out_.str(), HasSubstr("  \"rawBlock" + std::to_string(i) + "\": \"0x"));
  }
}

TEST_F(DbEditorTests, get_raw_block_range_single_block) {
  ASSERT_EQ(
      EXIT_SUCCESS,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getRawBlockRange", "2", "3"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_THAT(out_.str(), HasSubstr("  \"rawBlock2\": \"0x"));
}

TEST_F(DbEditorTests, get_raw_block_range_non_existent) {
  ASSERT_EQ(
      EXIT_FAILURE,
      run(
          CommandLineArguments{
              {kTestName, rocksDbPath(main_path_db_id_), "getRawBlockRange", "0", std::to_string(num_blocks_ + 1)}},
          out_,
          err_));
  ASSERT_TRUE(out_.str().empty());
}

TEST_F(DbEditorTests, get_raw_block_range_missing_range) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getRawBlockRange", "1"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(),
              StartsWith("Failed to execute command [getRawBlockRange], reason: Missing or invalid block range"));
}

TEST_F(DbEditorTests, get_raw_block_range_invalid_range) {
  ASSERT_EQ(
      EXIT_FAILURE,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getRawBlockRange", "2", "1"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getRawBlockRange], reason: Invalid block range"));
}

TEST_F(DbEditorTests, get_raw_block_range_invalid_block_id_end) {
  ASSERT_EQ(
      EXIT_FAILURE,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getRawBlockRange", "2", "0"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(),
              StartsWith("Failed to execute command [getRawBlockRange], reason: Invalid BLOCK-ID-END value"));
}

TEST_F(DbEditorTests, get_raw_block_range_invalid_range_stard_end_equal) {
  ASSERT_EQ(
      EXIT_FAILURE,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getRawBlockRange", "2", "2"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getRawBlockRange], reason: Invalid block range"));
}

TEST_F(DbEditorTests, compare_to_self) {
  ASSERT_EQ(
      EXIT_SUCCESS,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "compareTo", rocksDbPath(main_path_db_id_)}},
          out_,
          err_));
  ASSERT_THAT(out_.str(), HasSubstr("\"mainGenesisBlockId\": \"" + std::to_string(INITIAL_GENESIS_BLOCK_ID) + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"otherGenesisBlockId\": \"" + std::to_string(INITIAL_GENESIS_BLOCK_ID) + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"mainLastReachableBlockId\": \"" + std::to_string(num_blocks_) + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"otherLastReachableBlockId\": \"" + std::to_string(num_blocks_) + "\""));
  ASSERT_THAT(out_.str(),
              HasSubstr("\"comparedRangeFirstBlockId\": \"" + std::to_string(INITIAL_GENESIS_BLOCK_ID) + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"comparedRangeLastBlockId\": \"" + std::to_string(num_blocks_) + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"result\": \"equivalent\""));
  ASSERT_TRUE(err_.str().empty());
}

TEST_F(DbEditorTests, compare_to_partial_match) {
  CreateBlockchain(other_path_db_id_, more_num_blocks_);
  DeleteBlocksUntil(other_path_db_id_, INITIAL_GENESIS_BLOCK_ID + 1);
  ASSERT_EQ(
      EXIT_SUCCESS,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "compareTo", rocksDbPath(other_path_db_id_)}},
          out_,
          err_));
  ASSERT_THAT(out_.str(), HasSubstr("\"mainGenesisBlockId\": \"1\""));
  ASSERT_THAT(out_.str(),
              HasSubstr("\"otherGenesisBlockId\": \"" + std::to_string(INITIAL_GENESIS_BLOCK_ID + 1) + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"mainLastReachableBlockId\": \"" + std::to_string(num_blocks_) + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"otherLastReachableBlockId\": \"" + std::to_string(more_num_blocks_) + "\""));
  ASSERT_THAT(out_.str(),
              HasSubstr("\"comparedRangeFirstBlockId\": \"" + std::to_string(INITIAL_GENESIS_BLOCK_ID + 1) + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"comparedRangeLastBlockId\": \"" + std::to_string(num_blocks_) + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"result\": \"equivalent\""));
  ASSERT_TRUE(err_.str().empty());
}

TEST_F(DbEditorTests, compare_to_mismatch_middle) {
  CreateBlockchain(other_path_db_id_, num_blocks_, first_mismatch_block_id_);
  ASSERT_EQ(
      EXIT_SUCCESS,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "compareTo", rocksDbPath(other_path_db_id_)}},
          out_,
          err_));
  ASSERT_THAT(out_.str(), HasSubstr("\"mainGenesisBlockId\": \"" + std::to_string(INITIAL_GENESIS_BLOCK_ID) + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"otherGenesisBlockId\": \"" + std::to_string(INITIAL_GENESIS_BLOCK_ID) + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"mainLastReachableBlockId\": \"" + std::to_string(num_blocks_) + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"otherLastReachableBlockId\": \"" + std::to_string(num_blocks_) + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"result\": \"mismatch\""));
  ASSERT_THAT(out_.str(),
              HasSubstr("\"firstMismatchingBlockId\": \"" + std::to_string(first_mismatch_block_id_) + "\""));
  ASSERT_TRUE(err_.str().empty());
}

TEST_F(DbEditorTests, compare_to_mismatch_genesis) {
  CreateBlockchain(other_path_db_id_, num_blocks_, INITIAL_GENESIS_BLOCK_ID);
  ASSERT_EQ(
      EXIT_SUCCESS,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "compareTo", rocksDbPath(other_path_db_id_)}},
          out_,
          err_));
  ASSERT_THAT(out_.str(), HasSubstr("\"mainGenesisBlockId\": \"" + std::to_string(INITIAL_GENESIS_BLOCK_ID) + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"otherGenesisBlockId\": \"" + std::to_string(INITIAL_GENESIS_BLOCK_ID) + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"mainLastReachableBlockId\": \"" + std::to_string(num_blocks_) + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"otherLastReachableBlockId\": \"" + std::to_string(num_blocks_) + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"result\": \"mismatch\""));
  ASSERT_THAT(out_.str(),
              HasSubstr("\"firstMismatchingBlockId\": \"" + std::to_string(INITIAL_GENESIS_BLOCK_ID) + "\""));
  ASSERT_TRUE(err_.str().empty());
}

TEST_F(DbEditorTests, compare_to_mismatch_last_reachable) {
  CreateBlockchain(other_path_db_id_, num_blocks_, num_blocks_);
  ASSERT_EQ(
      EXIT_SUCCESS,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "compareTo", rocksDbPath(other_path_db_id_)}},
          out_,
          err_));
  ASSERT_THAT(out_.str(), HasSubstr("\"mainGenesisBlockId\": \"" + std::to_string(INITIAL_GENESIS_BLOCK_ID) + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"otherGenesisBlockId\": \"" + std::to_string(INITIAL_GENESIS_BLOCK_ID) + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"mainLastReachableBlockId\": \"" + std::to_string(num_blocks_) + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"otherLastReachableBlockId\": \"" + std::to_string(num_blocks_) + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"result\": \"mismatch\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"firstMismatchingBlockId\": \"" + std::to_string(num_blocks_) + "\""));
  ASSERT_TRUE(err_.str().empty());
}

TEST_F(DbEditorTests, compare_to_no_overlap) {
  CreateBlockchain(other_path_db_id_, more_num_blocks_);
  DeleteBlocksUntil(other_path_db_id_, num_blocks_ + 1);
  ASSERT_EQ(
      EXIT_SUCCESS,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "compareTo", rocksDbPath(other_path_db_id_)}},
          out_,
          err_));
  ASSERT_THAT(out_.str(), HasSubstr("\"mainGenesisBlockId\": \"" + std::to_string(INITIAL_GENESIS_BLOCK_ID) + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"otherGenesisBlockId\": \"" + std::to_string(num_blocks_ + 1) + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"mainLastReachableBlockId\": \"" + std::to_string(num_blocks_) + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"otherLastReachableBlockId\": \"" + std::to_string(more_num_blocks_) + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"result\": \"no-overlap\""));
  ASSERT_TRUE(err_.str().empty());
}

TEST_F(DbEditorTests, compare_to_missing_path) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "compareTo"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(),
              StartsWith("Failed to execute command [compareTo], reason: Missing PATH-TO-OTHER-DB argument"));
}

TEST_F(DbEditorTests, get_genesis_block_id_non_existent_dir) {
  ASSERT_EQ(
      EXIT_FAILURE,
      run(CommandLineArguments{{kTestName, rocksDbPath(non_existent_path_db_id_), "getGenesisBlockID"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), HasSubstr("RocksDB directory path doesn't exist at"));
}

TEST_F(DbEditorTests, compare_to_non_existent_main_dir) {
  ASSERT_EQ(EXIT_FAILURE,
            run(
                CommandLineArguments{
                    {kTestName, rocksDbPath(non_existent_path_db_id_), "compareTo", rocksDbPath(other_path_db_id_)}},
                out_,
                err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), HasSubstr("RocksDB directory path doesn't exist at"));
}

TEST_F(DbEditorTests, compare_to_non_existent_other_dir) {
  ASSERT_EQ(EXIT_FAILURE,
            run(
                CommandLineArguments{
                    {kTestName, rocksDbPath(main_path_db_id_), "compareTo", rocksDbPath(non_existent_path_db_id_)}},
                out_,
                err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), HasSubstr("RocksDB directory path doesn't exist at"));
}

TEST_F(DbEditorTests, get_last_state_transfer_block_id_na) {
  ASSERT_EQ(
      EXIT_SUCCESS,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getLastStateTransferBlockID"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_EQ("{\n  \"lastStateTransferBlockID\": \"n/a\"\n}\n", out_.str());
}

TEST_F(DbEditorTests, get_block_info) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getBlockInfo", "5"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_THAT(out_.str(), HasSubstr("  \"parentBlockDigest\": \"0x"));
  ASSERT_THAT(out_.str(),
              HasSubstr("  \"keyValueTotalCount\": \"" + std::to_string(num_keys_ * num_categories_) + '\"'));
  ASSERT_THAT(out_.str(), HasSubstr("  \"categoriesCount\": \"" + std::to_string(num_categories_) + '\"'));
  ASSERT_THAT(
      out_.str(),
      HasSubstr("    \"" + kCategoryMerkle + "\": {\n      \"keyValueCount\": \"" + std::to_string(num_keys_) + '\"'));
  ASSERT_THAT(out_.str(),
              HasSubstr("    \"" + kCategoryVersioned + "\": {\n      \"keyValueCount\": \"" +
                        std::to_string(num_keys_) + '\"'));
  ASSERT_THAT(out_.str(),
              Not(HasSubstr("    \"" + kCategoryImmutable + "\": {\n      \"keyValueCount\": \"" +
                            std::to_string(num_keys_) + '\"')));
}

TEST_F(DbEditorTests, get_block_info_with_immutable) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getBlockInfo", "1"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_THAT(out_.str(), HasSubstr("  \"parentBlockDigest\": \"0x"));
  ASSERT_THAT(out_.str(),
              HasSubstr("  \"keyValueTotalCount\": \"" + std::to_string(num_keys_ * num_categories_ + 1) + '\"'));
  ASSERT_THAT(out_.str(), HasSubstr("  \"categoriesCount\": \"" + std::to_string(num_categories_ + 1) + '\"'));
  ASSERT_THAT(
      out_.str(),
      HasSubstr("    \"" + kCategoryMerkle + "\": {\n      \"keyValueCount\": \"" + std::to_string(num_keys_) + '\"'));
  ASSERT_THAT(out_.str(),
              HasSubstr("    \"" + kCategoryVersioned + "\": {\n      \"keyValueCount\": \"" +
                        std::to_string(num_keys_) + '\"'));
  ASSERT_THAT(out_.str(), HasSubstr("    \"" + kCategoryImmutable + "\": {\n      \"keyValueCount\": \"1\""));
}

TEST_F(DbEditorTests, get_block_info_missing_block_id) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getBlockInfo"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getBlockInfo], reason: Missing BLOCK-ID argument"));
}

TEST_F(DbEditorTests, get_block_key_values) {
  ASSERT_EQ(
      EXIT_SUCCESS,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getBlockKeyValues", "5"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_THAT(out_.str(), StartsWith("{\n\"" + kCategoryMerkle + "\": {\n  \"0x"));
  ASSERT_THAT(out_.str(), HasSubstr("\"0x0000003c\": \"0x00000078\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"0x00000028\": \"0x00000050\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"0x00000032\": \"0x00000064\""));
  ASSERT_THAT(out_.str(), EndsWith("\n}\n"));
}

TEST_F(DbEditorTests, get_earliest_category_updates_merkle) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(
                CommandLineArguments{
                    {kTestName, rocksDbPath(main_path_db_id_), "getEarliestCategoryUpdates", kCategoryMerkle}},
                out_,
                err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_THAT(out_.str(), StartsWith("{\n  \"blockID\":"));
  ASSERT_THAT(out_.str(), HasSubstr("\"category\": \"" + kCategoryMerkle + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"0x00000000\": \"0x00000000\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"0x0000000a\": \"0x00000014\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"0x00000014\": \"0x00000028\""));
  ASSERT_THAT(out_.str(), EndsWith("\n}\n"));
}

TEST_F(DbEditorTests, get_earliest_category_updates_immutable) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(
                CommandLineArguments{
                    {kTestName, rocksDbPath(main_path_db_id_), "getEarliestCategoryUpdates", kCategoryImmutable}},
                out_,
                err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_THAT(out_.str(), StartsWith("{\n  \"blockID\":"));
  ASSERT_THAT(out_.str(), HasSubstr("\"category\": \"" + kCategoryImmutable + "\""));
  // Assert that we have the immutable_key1 and its value in the blockchain
  ASSERT_THAT(out_.str(), HasSubstr("\"0x696d6d757461626c655f6b657931\": \"0x696d6d757461626c655f76616c31\""));
  ASSERT_THAT(out_.str(), EndsWith("\n}\n"));
}

TEST_F(DbEditorTests, get_earliest_category_updates_versioned) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(
                CommandLineArguments{
                    {kTestName, rocksDbPath(main_path_db_id_), "getEarliestCategoryUpdates", kCategoryVersioned}},
                out_,
                err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_THAT(out_.str(), StartsWith("{\n  \"blockID\":"));
  ASSERT_THAT(out_.str(), HasSubstr("\"category\": \"" + kCategoryVersioned + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"0x766b657930\": \"0x7676616c30\""));
  ASSERT_THAT(out_.str(), EndsWith("\n}\n"));
}

TEST_F(DbEditorTests, get_category_earliest_stale_immutable) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(
                CommandLineArguments{
                    {kTestName, rocksDbPath(main_path_db_id_), "getCategoryEarliestStale", kCategoryImmutable}},
                out_,
                err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_THAT(out_.str(), StartsWith("{\n  \"blockID\":"));
  ASSERT_THAT(out_.str(), HasSubstr("\"category\": \"" + kCategoryImmutable + "\""));
  ASSERT_THAT(out_.str(), HasSubstr("[\"0x696d6d757461626c655f6b657931\"]"));
  ASSERT_THAT(out_.str(), EndsWith("\n}\n"));
}

TEST_F(DbEditorTests, get_summary_stale_keys_invalid_argumets) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getStaleKeysSummary", "0", "5"}},
                out_,
                err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), HasSubstr("BLOCK-VERSION-FROM is incorrect: current genesis: 1"));
  ASSERT_THAT(err_.str(), HasSubstr("BLOCK-VERSION-FROM: 0"));

  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getStaleKeysSummary", "1", "1000"}},
                out_,
                err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), HasSubstr("latest reachable block: 10"));
  ASSERT_THAT(err_.str(), HasSubstr("BLOCK-VERSION-TO: 1000"));

  ASSERT_EQ(
      EXIT_FAILURE,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getStaleKeysSummary", "1000"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), HasSubstr("latest reachable block: 10"));
  ASSERT_THAT(err_.str(), HasSubstr("BLOCK-VERSION-TO: 1000"));
}

TEST_F(DbEditorTests, get_summary_stale_keys) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getStaleKeysSummary"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_THAT(out_.str(), HasSubstr("\"block_merkle\": \"16\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"immutable\": \"1\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"versioned_kv\": \"16\""));
  ASSERT_THAT(out_.str(), EndsWith("\n}\n"));

  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getStaleKeysSummary", "1", "10"}},
                out_,
                err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_THAT(out_.str(), HasSubstr("\"block_merkle\": \"16\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"immutable\": \"1\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"versioned_kv\": \"16\""));
  ASSERT_THAT(out_.str(), EndsWith("\n}\n"));

  ASSERT_EQ(
      EXIT_SUCCESS,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getStaleKeysSummary", "10"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_THAT(out_.str(), HasSubstr("\"block_merkle\": \"16\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"immutable\": \"1\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"versioned_kv\": \"16\""));
  ASSERT_THAT(out_.str(), EndsWith("\n}\n"));

  ASSERT_EQ(
      EXIT_SUCCESS,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getStaleKeysSummary", "1"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_THAT(out_.str(), HasSubstr("\"block_merkle\": \"2\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"immutable\": \"1\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"versioned_kv\": \"2\""));
  ASSERT_THAT(out_.str(), EndsWith("\n}\n"));

  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getStaleKeysSummary", "2", "3"}},
                out_,
                err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_THAT(out_.str(), HasSubstr("\"block_merkle\": \"4\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"immutable\": \"0\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"versioned_kv\": \"4\""));
  ASSERT_THAT(out_.str(), EndsWith("\n}\n"));
}

TEST_F(DbEditorTests, get_empty_block_key_values) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(
                CommandLineArguments{
                    {kTestName, rocksDbPath(main_path_db_id_), "getBlockKeyValues", std::to_string(empty_block_id_)}},
                out_,
                err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_EQ("{\n\"" + kCategoryMerkle + "\": {\n},\n\"" + kCategoryVersioned + "\": {\n}\n}\n", out_.str());
}

TEST_F(DbEditorTests, get_block_key_values_missing_block_id) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getBlockKeyValues"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(),
              StartsWith("Failed to execute command [getBlockKeyValues], reason: Missing BLOCK-ID argument"));
}

TEST_F(DbEditorTests, get_value_latest) {
  ASSERT_EQ(
      EXIT_SUCCESS,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getValue", kCategoryMerkle, "0x00000028"}},
          out_,
          err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_EQ("{\n  \"value\": \"0x00000050\"\n}\n", out_.str());
}

TEST_F(DbEditorTests, get_value_with_block_version) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(
                CommandLineArguments{
                    {kTestName, rocksDbPath(main_path_db_id_), "getValue", kCategoryMerkle, "0x00000028", "5"}},
                out_,
                err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_EQ("{\n  \"value\": \"0x00000050\"\n}\n", out_.str());
}

TEST_F(DbEditorTests, get_value_with_block_version_non_existent) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName,
                                      rocksDbPath(main_path_db_id_),
                                      "getValue",
                                      kCategoryMerkle,
                                      "0x00000028",
                                      std::to_string(num_blocks_ + 1)}},
                out_,
                err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getValue], reason: "));
}

TEST_F(DbEditorTests, get_value_key_without_0x) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(
                CommandLineArguments{
                    {kTestName, rocksDbPath(main_path_db_id_), "getValue", kCategoryMerkle, "00000028", "5"}},
                out_,
                err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_EQ("{\n  \"value\": \"0x00000050\"\n}\n", out_.str());
}

TEST_F(DbEditorTests, get_value_empty_key) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getValue", kCategoryMerkle, "", "5"}},
                out_,
                err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getValue], reason: "));
  ASSERT_THAT(err_.str(), Not(HasSubstr("Invalid hex string:")));
}

TEST_F(DbEditorTests, get_value_with_version_0) {
  ASSERT_EQ(EXIT_FAILURE,
            run(
                CommandLineArguments{
                    {kTestName, rocksDbPath(main_path_db_id_), "getValue", kCategoryMerkle, "0x00000028", "0"}},
                out_,
                err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getValue], reason: "));
}

TEST_F(DbEditorTests, get_value_wrong_category) {
  ASSERT_EQ(EXIT_FAILURE,
            run(
                CommandLineArguments{
                    {kTestName, rocksDbPath(main_path_db_id_), "getValue", kCategoryVersioned, "0x00000028"}},
                out_,
                err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getValue], reason: "));
}

TEST_F(DbEditorTests, get_value_non_existant_category) {
  ASSERT_EQ(EXIT_FAILURE,
            run(
                CommandLineArguments{
                    {kTestName, rocksDbPath(main_path_db_id_), "getValue", "non_existant_category", "0x00000028"}},
                out_,
                err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getValue], reason: "));
}

TEST_F(DbEditorTests, get_value_missing_key) {
  ASSERT_EQ(
      EXIT_FAILURE,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getValue", kCategoryMerkle}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(),
              StartsWith("Failed to execute command [getValue], reason: Missing CATEGORY and HEX-KEY arguments"));
}

TEST_F(DbEditorTests, get_value_invalid_key_size) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getValue", kCategoryMerkle, "0xabc"}},
                out_,
                err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getValue], reason: Invalid hex string: 0xabc"));
}

TEST_F(DbEditorTests, get_value_invalid_key_chars_at_start) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getValue", kCategoryMerkle, "0xjj"}},
                out_,
                err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getValue], reason: Invalid hex string: 0xjj"));
}

TEST_F(DbEditorTests, get_value_invalid_key_chars_at_end) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getValue", kCategoryMerkle, "0xabcx"}},
                out_,
                err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getValue], reason: Invalid hex string: 0xabcx"));
}

TEST_F(DbEditorTests, get_value_invalid_key_chars_no_0x) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getValue", kCategoryMerkle, "abck"}},
                out_,
                err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getValue], reason: Invalid hex string: abck"));
}

TEST_F(DbEditorTests, get_categories) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getCategories"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_EQ("{\n  \"" + std::string(concord::kvbc::categorization::kConcordInternalCategoryId) +
                "\": \"versioned_kv\",\n  \"" + kCategoryImmutable + "\": \"immutable\",\n  \"" + kCategoryMerkle +
                "\": \"block_merkle\",\n  \"" + kCategoryVersioned + "\": \"versioned_kv\"\n}\n",
            out_.str());
}

TEST_F(DbEditorTests, get_categories_by_block_with_immutable) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getCategories", "1"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_EQ("{\n  \"" + kCategoryImmutable + "\": \"immutable\",\n  \"" + kCategoryMerkle +
                "\": \"block_merkle\",\n  \"" + kCategoryVersioned + "\": \"versioned_kv\"\n}\n",
            out_.str());
}

TEST_F(DbEditorTests, get_categories_by_block_no_immutable) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getCategories", "3"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_EQ(
      "{\n  \"" + kCategoryMerkle + "\": \"block_merkle\",\n  \"" + kCategoryVersioned + "\": \"versioned_kv\"\n}\n",
      out_.str());
}

TEST_F(DbEditorTests, remove_metadata) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "removeMetadata"}}, out_, err_));
  ASSERT_EQ("{\n  \"result\": \"true\",\n  \"epoch\": \"1\"\n}\n", out_.str());
  ASSERT_TRUE(err_.str().empty());

  const auto adapter = getAdapter(rocksDbPath(main_path_db_id_));
  const auto& kvp = generateMetadata();
  for (const auto& kv : kvp) {
    ASSERT_TRUE(adapter.db()->asIDBClient()->has(kv.first).isNotFound());
  }

  ASSERT_NO_THROW(adapter.getRawBlock(5));
  ASSERT_NE(adapter.getLastReachableBlockId(), 0);
  ASSERT_NE(adapter.getLastReachableBlockId(), concord::kvbc::INITIAL_GENESIS_BLOCK_ID);
  ASSERT_EQ(concord::kvbc::INITIAL_GENESIS_BLOCK_ID, adapter.getGenesisBlockId());
}

TEST_F(DbEditorTests, get_st_metadata) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getSTMetadata"}}, out_, err_));
  ASSERT_THAT(out_.str(), HasSubstr("\"NumberOfReservedPages\": \"75\""));
  ASSERT_TRUE(err_.str().empty());
}

}  // namespace

int main(int argc, char* argv[]) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
