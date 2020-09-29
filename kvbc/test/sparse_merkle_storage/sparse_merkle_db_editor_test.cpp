// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "db_adapter_interface.h"
#include "endianness.hpp"
#include "kv_types.hpp"
#include "merkle_tree_db_adapter.h"
#include "PersistentStorageImp.hpp"
#include "sliver.hpp"
#include "sparse_merkle_db_editor.hpp"
#include "storage_test_common.h"
#include "storage/db_types.h"
#include "storage/merkle_tree_key_manipulator.h"

#include <cstdlib>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>

namespace {

using namespace concord::kvbc::tools::sparse_merkle_db;
using namespace concord::kvbc::v2MerkleTree;
using concord::kvbc::BlockId;
using concord::kvbc::INITIAL_GENESIS_BLOCK_ID;
using concord::kvbc::SetOfKeyValuePairs;
using concordUtils::toBigEndianStringBuffer;
using concordUtils::Sliver;

using testing::EndsWith;
using testing::HasSubstr;
using testing::InitGoogleTest;
using testing::Not;
using testing::StartsWith;
using testing::Test;

using namespace std::string_literals;

const auto kTestName = kToolName + "_test";

Sliver getSliver(unsigned value) { return toBigEndianStringBuffer(value); }

SetOfKeyValuePairs generateMetadataAndStateTransfer() {
  // Create a full range of metadata objects
  constexpr auto num_metadata_object_ids = MAX_METADATA_PARAMS_NUM - INITIALIZED_FLAG;
  auto updates = SetOfKeyValuePairs{};
  concord::storage::v2MerkleTree::MetadataKeyManipulator manipulator;
  for (concord::storage::ObjectId i = INITIALIZED_FLAG; i < num_metadata_object_ids; ++i) {
    updates[manipulator.generateMetadataKey(i)] = getSliver(i);
  }

  concord::storage::v2MerkleTree::STKeyManipulator st_manipulator;
  updates[st_manipulator.generateStateTransferKey(1)] = std::string{"val"};
  updates[st_manipulator.generateSTPendingPageKey(1)] = std::string{"val"};
  updates[st_manipulator.generateSTCheckpointDescriptorKey(1)] = std::string{"val"};
  updates[st_manipulator.generateSTReservedPageDynamicKey(1, 1)] = std::string{"val"};
  return updates;
}

class SparseMerkleDbEditorTests : public Test {
 public:
  void SetUp() override {
    CleanDbs();
    CreateBlockchain(main_path_db_id_, num_blocks_);
  }

  void TearDown() override { CleanDbs(); }

  void CleanDbs() {
    TestRocksDb::cleanup(main_path_db_id_);
    TestRocksDb::cleanup(other_path_db_id_);
    TestRocksDb::cleanup(non_existent_path_db_id_);
    TestRocksDb::cleanup(empty_path_db_id_);
  }

  void CreateBlockchain(std::size_t db_id, BlockId blocks, std::optional<BlockId> mismatch_at = std::nullopt) {
    auto db = TestRocksDb::create(db_id);
    auto adapter = DBAdapter{db};

    const auto mismatch_kv = std::make_pair(getSliver(std::numeric_limits<unsigned>::max()), getSliver(42));

    for (auto i = 1u; i <= blocks; ++i) {
      auto updates = SetOfKeyValuePairs{};
      if (empty_block_id_ != i) {
        for (auto j = 0u; j < num_keys_; ++j) {
          updates[getSliver((j + i - 1) * kv_multiplier_)] = getSliver((j + i - 1) * 2 * kv_multiplier_);
        }
      }
      // Add a key to simulate a mismatch.
      if (mismatch_at.has_value() && *mismatch_at == i) {
        updates.insert(mismatch_kv);
      }
      ASSERT_NO_THROW(adapter.addBlock(updates));
    }

    const auto status = db->multiPut(generateMetadataAndStateTransfer());
    ASSERT_TRUE(status.isOK());
  }

  void DeleteBlocksUntil(std::size_t db_id, BlockId until_block_id) {
    auto db = TestRocksDb::create(db_id);
    auto adapter = DBAdapter{db};

    for (auto i = 1ull; i < until_block_id; ++i) {
      adapter.deleteBlock(i);
    }
  }

 protected:
  static constexpr std::size_t main_path_db_id_{defaultDbId};
  static constexpr std::size_t other_path_db_id_{main_path_db_id_ + 1};
  static constexpr std::size_t non_existent_path_db_id_{other_path_db_id_ + 1};
  static constexpr std::size_t empty_path_db_id_{non_existent_path_db_id_ + 1};
  static constexpr unsigned kv_multiplier_{10};
  static constexpr BlockId empty_block_id_{10};
  static constexpr BlockId num_blocks_{10};
  static constexpr BlockId more_num_blocks_{16};
  static constexpr BlockId first_mismatch_block_id_{7};
  static constexpr unsigned num_keys_{3};

  std::stringstream out_;
  std::stringstream err_;
};

TEST_F(SparseMerkleDbEditorTests, no_arguments) {
  ASSERT_EQ(EXIT_FAILURE, run(CommandLineArguments{{kTestName}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Usage:"));
}

TEST_F(SparseMerkleDbEditorTests, invalid_command) {
  ASSERT_EQ(EXIT_FAILURE, run(CommandLineArguments{{kTestName, "unknownCommand"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Usage:"));
}

TEST_F(SparseMerkleDbEditorTests, get_genesis_block_id) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getGenesisBlockID"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_EQ("{\n  \"genesisBlockID\": \"1\"\n}\n", out_.str());
}

TEST_F(SparseMerkleDbEditorTests, get_last_reachable_block_id) {
  ASSERT_EQ(
      EXIT_SUCCESS,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getLastReachableBlockID"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_EQ("{\n  \"lastReachableBlockID\": \""s + std::to_string(num_blocks_) + "\"\n}\n", out_.str());
}

TEST_F(SparseMerkleDbEditorTests, get_last_block_id) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getLastBlockID"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_EQ("{\n  \"lastBlockID\": \""s + std::to_string(num_blocks_) + "\"\n}\n", out_.str());
}

TEST_F(SparseMerkleDbEditorTests, get_raw_block) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getRawBlock", "5"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_THAT(out_.str(), StartsWith("{\n  \"rawBlock\": \"0x"));
}

TEST_F(SparseMerkleDbEditorTests, get_raw_block_missing_block_id) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getRawBlock"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getRawBlock], reason: Missing BLOCK-ID argument"));
}

TEST_F(SparseMerkleDbEditorTests, get_raw_block_invalid_block_id) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getRawBlock", "5ab"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getRawBlock], reason: Invalid BLOCK-ID: 5ab"));
}

TEST_F(SparseMerkleDbEditorTests, get_raw_block_range_full_range) {
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

TEST_F(SparseMerkleDbEditorTests, get_raw_block_range_except_last) {
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

TEST_F(SparseMerkleDbEditorTests, get_raw_block_range_whole_blockchain) {
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

TEST_F(SparseMerkleDbEditorTests, get_raw_block_range_single_block) {
  ASSERT_EQ(
      EXIT_SUCCESS,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getRawBlockRange", "2", "3"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_THAT(out_.str(), HasSubstr("  \"rawBlock2\": \"0x"));
}

TEST_F(SparseMerkleDbEditorTests, get_raw_block_range_non_existent) {
  ASSERT_EQ(
      EXIT_FAILURE,
      run(
          CommandLineArguments{
              {kTestName, rocksDbPath(main_path_db_id_), "getRawBlockRange", "0", std::to_string(num_blocks_ + 1)}},
          out_,
          err_));
  ASSERT_TRUE(out_.str().empty());
}

TEST_F(SparseMerkleDbEditorTests, get_raw_block_range_missing_range) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getRawBlockRange", "1"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(),
              StartsWith("Failed to execute command [getRawBlockRange], reason: Missing or invalid block range"));
}

TEST_F(SparseMerkleDbEditorTests, get_raw_block_range_invalid_range) {
  ASSERT_EQ(
      EXIT_FAILURE,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getRawBlockRange", "2", "1"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getRawBlockRange], reason: Invalid block range"));
}

TEST_F(SparseMerkleDbEditorTests, get_raw_block_range_invalid_block_id_end) {
  ASSERT_EQ(
      EXIT_FAILURE,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getRawBlockRange", "2", "0"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(),
              StartsWith("Failed to execute command [getRawBlockRange], reason: Invalid BLOCK-ID-END value"));
}

TEST_F(SparseMerkleDbEditorTests, get_raw_block_range_invalid_range_stard_end_equal) {
  ASSERT_EQ(
      EXIT_FAILURE,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getRawBlockRange", "2", "2"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getRawBlockRange], reason: Invalid block range"));
}

TEST_F(SparseMerkleDbEditorTests, get_block_info) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getBlockInfo", "5"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_THAT(out_.str(), HasSubstr("  \"sparseMerkleRootHash\": \"0x"));
  ASSERT_THAT(out_.str(), HasSubstr("  \"parentBlockDigest\": \"0x"));
  ASSERT_THAT(out_.str(), HasSubstr("  \"keyValueCount\": \"" + std::to_string(num_keys_) + '\"'));
}

TEST_F(SparseMerkleDbEditorTests, get_block_info_missing_block_id) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getBlockInfo"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getBlockInfo], reason: Missing BLOCK-ID argument"));
}

TEST_F(SparseMerkleDbEditorTests, get_block_key_values) {
  ASSERT_EQ(
      EXIT_SUCCESS,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getBlockKeyValues", "5"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_THAT(out_.str(), StartsWith("{\n  \"0x"));
  ASSERT_THAT(out_.str(), HasSubstr("\"0x0000003c\": \"0x00000078\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"0x00000028\": \"0x00000050\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"0x00000032\": \"0x00000064\""));
  ASSERT_THAT(out_.str(), EndsWith("\"\n}\n"));
}

TEST_F(SparseMerkleDbEditorTests, get_empty_block_key_values) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(
                CommandLineArguments{
                    {kTestName, rocksDbPath(main_path_db_id_), "getBlockKeyValues", std::to_string(empty_block_id_)}},
                out_,
                err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_EQ("{\n}\n", out_.str());
}

TEST_F(SparseMerkleDbEditorTests, get_block_key_values_missing_block_id) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getBlockKeyValues"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(),
              StartsWith("Failed to execute command [getBlockKeyValues], reason: Missing BLOCK-ID argument"));
}

TEST_F(SparseMerkleDbEditorTests, get_value_latest) {
  ASSERT_EQ(
      EXIT_SUCCESS,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getValue", "0x00000028"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_EQ("{\n  \"blockVersion\": \"5\",\n  \"value\": \"0x00000050\"\n}\n", out_.str());
}

TEST_F(SparseMerkleDbEditorTests, get_value_with_block_version) {
  ASSERT_EQ(
      EXIT_SUCCESS,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getValue", "0x00000028", "5"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_EQ("{\n  \"blockVersion\": \"5\",\n  \"value\": \"0x00000050\"\n}\n", out_.str());
}

TEST_F(SparseMerkleDbEditorTests, get_value_key_without_0x) {
  ASSERT_EQ(
      EXIT_SUCCESS,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getValue", "00000028", "5"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_EQ("{\n  \"blockVersion\": \"5\",\n  \"value\": \"0x00000050\"\n}\n", out_.str());
}

TEST_F(SparseMerkleDbEditorTests, get_value_empty_key) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getValue", "", "5"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getValue], reason: "));
  ASSERT_THAT(err_.str(), Not(HasSubstr("Invalid hex string:")));
}

TEST_F(SparseMerkleDbEditorTests, get_value_with_version_0) {
  ASSERT_EQ(
      EXIT_FAILURE,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getValue", "0x00000028", "0"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getValue], reason: "));
}

TEST_F(SparseMerkleDbEditorTests, get_value_missing_key) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getValue"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getValue], reason: Missing HEX-KEY argument"));
}

TEST_F(SparseMerkleDbEditorTests, get_value_invalid_key_size) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getValue", "0xabc"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getValue], reason: Invalid hex string: 0xabc"));
}

TEST_F(SparseMerkleDbEditorTests, get_value_invalid_key_chars_at_start) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getValue", "0xjj"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getValue], reason: Invalid hex string: 0xjj"));
}

TEST_F(SparseMerkleDbEditorTests, get_value_invalid_key_chars_at_end) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getValue", "0xabcx"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getValue], reason: Invalid hex string: 0xabcx"));
}

TEST_F(SparseMerkleDbEditorTests, get_value_invalid_key_chars_no_0x) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "getValue", "abck"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getValue], reason: Invalid hex string: abck"));
}

TEST_F(SparseMerkleDbEditorTests, compare_to_self) {
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

TEST_F(SparseMerkleDbEditorTests, compare_to_partial_match) {
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

TEST_F(SparseMerkleDbEditorTests, compare_to_mismatch_middle) {
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

TEST_F(SparseMerkleDbEditorTests, compare_to_mismatch_genesis) {
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

TEST_F(SparseMerkleDbEditorTests, compare_to_mismatch_last_reachable) {
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

TEST_F(SparseMerkleDbEditorTests, compare_to_no_overlap) {
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

TEST_F(SparseMerkleDbEditorTests, compare_to_missing_path) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "compareTo"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(),
              StartsWith("Failed to execute command [compareTo], reason: Missing PATH-TO-OTHER-DB argument"));
}

TEST_F(SparseMerkleDbEditorTests, get_genesis_block_id_non_existent_dir) {
  ASSERT_EQ(
      EXIT_FAILURE,
      run(CommandLineArguments{{kTestName, rocksDbPath(non_existent_path_db_id_), "getGenesisBlockID"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), HasSubstr("RocksDB directory path doesn't exist at"));
}

TEST_F(SparseMerkleDbEditorTests, get_genesis_block_id_empty_db) {
  // Create an empty DB that exists on disk.
  TestRocksDb::create(empty_path_db_id_);

  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{kTestName, rocksDbPath(empty_path_db_id_), "getGenesisBlockID"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), HasSubstr("RocksDB database is empty at path"));
}

TEST_F(SparseMerkleDbEditorTests, compare_to_non_existent_main_dir) {
  ASSERT_EQ(EXIT_FAILURE,
            run(
                CommandLineArguments{
                    {kTestName, rocksDbPath(non_existent_path_db_id_), "compareTo", rocksDbPath(other_path_db_id_)}},
                out_,
                err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), HasSubstr("RocksDB directory path doesn't exist at"));
}

TEST_F(SparseMerkleDbEditorTests, compare_to_empty_main_db) {
  // Create an empty DB that exists on disk.
  TestRocksDb::create(empty_path_db_id_);

  ASSERT_EQ(EXIT_FAILURE,
            run(
                CommandLineArguments{
                    {kTestName, rocksDbPath(empty_path_db_id_), "compareTo", rocksDbPath(other_path_db_id_)}},
                out_,
                err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), HasSubstr("RocksDB database is empty at path"));
}

TEST_F(SparseMerkleDbEditorTests, compare_to_non_existent_other_dir) {
  ASSERT_EQ(EXIT_FAILURE,
            run(
                CommandLineArguments{
                    {kTestName, rocksDbPath(main_path_db_id_), "compareTo", rocksDbPath(non_existent_path_db_id_)}},
                out_,
                err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), HasSubstr("RocksDB directory path doesn't exist at"));
}

TEST_F(SparseMerkleDbEditorTests, compare_to_empty_other_db) {
  // Create an empty DB that exists on disk.
  TestRocksDb::create(empty_path_db_id_);

  ASSERT_EQ(
      EXIT_FAILURE,
      run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "compareTo", rocksDbPath(empty_path_db_id_)}},
          out_,
          err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), HasSubstr("RocksDB database is empty at path"));
}

TEST_F(SparseMerkleDbEditorTests, remove_metadata) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{kTestName, rocksDbPath(main_path_db_id_), "removeMetadata"}}, out_, err_));
  ASSERT_THAT(out_.str(), StartsWith(toJson(std::string{"result"}, std::string{"true"})));
  ASSERT_TRUE(err_.str().empty());

  const auto adapter = getAdapter(rocksDbPath(main_path_db_id_));
  const auto& kvp = generateMetadataAndStateTransfer();
  for (const auto& kv : kvp) {
    ASSERT_TRUE(adapter.getDb()->has(kv.first).isNotFound());
  }

  ASSERT_NO_THROW(adapter.getRawBlock(5));
  ASSERT_NE(adapter.getLastReachableBlockId(), 0);
  ASSERT_NE(adapter.getLastReachableBlockId(), concord::kvbc::INITIAL_GENESIS_BLOCK_ID);
  ASSERT_EQ(concord::kvbc::INITIAL_GENESIS_BLOCK_ID, adapter.getGenesisBlockId());
}

}  // namespace

int main(int argc, char* argv[]) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
