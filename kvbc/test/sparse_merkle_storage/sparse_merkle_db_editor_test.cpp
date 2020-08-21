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

#include "storage/db_types.h"
#include "sparse_merkle_db_editor.hpp"
#include "PersistentStorageImp.hpp"

#include "storage/merkle_tree_key_manipulator.h"
#include "storage_test_common.h"

#include "endianness.hpp"
#include "kv_types.hpp"
#include "merkle_tree_db_adapter.h"
#include "sliver.hpp"

#include <cstdlib>
#include <limits>
#include <memory>
#include <sstream>

namespace {

using namespace concord::kvbc::tools::sparse_merkle_db;
using namespace concord::kvbc::v2MerkleTree;
using concord::kvbc::BlockId;
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
  updates[st_manipulator.generateSTReservedPageStaticKey(1, 1)] = std::string{"val"};
  updates[st_manipulator.generateSTReservedPageDynamicKey(1, 1)] = std::string{"val"};
  return updates;
}

class SparseMerkleDbInspectorTests : public Test {
 public:
  void SetUp() override {
    auto db = TestRocksDb::create();
    auto adapter = DBAdapter{db};

    for (auto i = 0u; i < num_blocks_ - 1; ++i) {
      auto updates = SetOfKeyValuePairs{};
      for (auto j = 0u; j < num_keys_; ++j) {
        updates[getSliver((j + i) * num_blocks_)] = getSliver((j + i) * 2 * num_blocks_);
      }
      ASSERT_NO_THROW(adapter.addBlock(updates));
    }
    // Add an empty block.
    ASSERT_NO_THROW(adapter.addBlock(SetOfKeyValuePairs{}));
    const auto status = db->multiPut(generateMetadataAndStateTransfer());
    ASSERT_TRUE(status.isOK());
  }

  void TearDown() override { cleanup(); }

 protected:
  const unsigned num_blocks_{10};
  const unsigned num_keys_{3};
  std::stringstream out_;
  std::stringstream err_;
};

TEST_F(SparseMerkleDbInspectorTests, no_arguments) {
  ASSERT_EQ(EXIT_FAILURE, run(CommandLineArguments{{"sparse_merkle_db_editor_test"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Usage:"));
}

TEST_F(SparseMerkleDbInspectorTests, invalid_command) {
  ASSERT_EQ(EXIT_FAILURE, run(CommandLineArguments{{"sparse_merkle_db_editor_test", "unknownCommand"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Usage:"));
}

TEST_F(SparseMerkleDbInspectorTests, get_genesis_block_id) {
  ASSERT_EQ(
      EXIT_SUCCESS,
      run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getGenesisBlockID"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_EQ("{\n  \"genesisBlockID\": \"1\"\n}\n", out_.str());
}

TEST_F(SparseMerkleDbInspectorTests, get_last_reachable_block_id) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getLastReachableBlockID"}},
                out_,
                err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_EQ("{\n  \"lastReachableBlockID\": \""s + std::to_string(num_blocks_) + "\"\n}\n", out_.str());
}

TEST_F(SparseMerkleDbInspectorTests, get_last_block_id) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getLastBlockID"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_EQ("{\n  \"lastBlockID\": \""s + std::to_string(num_blocks_) + "\"\n}\n", out_.str());
}

TEST_F(SparseMerkleDbInspectorTests, get_raw_block) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getRawBlock", "5"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_THAT(out_.str(), StartsWith("{\n  \"rawBlock\": \"0x"));
}

TEST_F(SparseMerkleDbInspectorTests, get_raw_block_missing_block_id) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getRawBlock"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getRawBlock], reason: Missing BLOCK-ID argument"));
}

TEST_F(SparseMerkleDbInspectorTests, get_raw_block_invalid_block_id) {
  ASSERT_EQ(
      EXIT_FAILURE,
      run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getRawBlock", "5ab"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getRawBlock], reason: Invalid BLOCK-ID: 5ab"));
}

TEST_F(SparseMerkleDbInspectorTests, get_raw_block_range_full_range) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{"sparse_merkle_db_editor_test",
                                      rocksDbPath(),
                                      "getRawBlockRange",
                                      "1",
                                      std::to_string(num_blocks_ + 1)}},
                out_,
                err_));
  ASSERT_TRUE(err_.str().empty());
  for (auto i = 1u; i <= num_blocks_; ++i) {
    ASSERT_THAT(out_.str(), HasSubstr("  \"rawBlock" + std::to_string(i) + "\": \"0x"));
  }
}

TEST_F(SparseMerkleDbInspectorTests, get_raw_block_range_except_last) {
  ASSERT_EQ(
      EXIT_SUCCESS,
      run(
          CommandLineArguments{
              {"sparse_merkle_db_editor_test", rocksDbPath(), "getRawBlockRange", "1", std::to_string(num_blocks_)}},
          out_,
          err_));
  ASSERT_TRUE(err_.str().empty());
  for (auto i = 1u; i < num_blocks_; ++i) {
    ASSERT_THAT(out_.str(), HasSubstr("  \"rawBlock" + std::to_string(i) + "\": \"0x"));
  }
  ASSERT_THAT(out_.str(), Not(HasSubstr("  \"rawBlock" + std::to_string(num_blocks_) + "\": \"0x")));
}

TEST_F(SparseMerkleDbInspectorTests, get_raw_block_range_whole_blockchain) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{"sparse_merkle_db_editor_test",
                                      rocksDbPath(),
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

TEST_F(SparseMerkleDbInspectorTests, get_raw_block_range_single_block) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getRawBlockRange", "2", "3"}},
                out_,
                err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_THAT(out_.str(), HasSubstr("  \"rawBlock2\": \"0x"));
}

TEST_F(SparseMerkleDbInspectorTests, get_raw_block_range_non_existent) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{"sparse_merkle_db_editor_test",
                                      rocksDbPath(),
                                      "getRawBlockRange",
                                      "0",
                                      std::to_string(num_blocks_ + 1)}},
                out_,
                err_));
  ASSERT_TRUE(out_.str().empty());
}

TEST_F(SparseMerkleDbInspectorTests, get_raw_block_range_missing_range) {
  ASSERT_EQ(
      EXIT_FAILURE,
      run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getRawBlockRange", "1"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(),
              StartsWith("Failed to execute command [getRawBlockRange], reason: Missing or invalid block range"));
}

TEST_F(SparseMerkleDbInspectorTests, get_raw_block_range_invalid_range) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getRawBlockRange", "2", "1"}},
                out_,
                err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getRawBlockRange], reason: Invalid block range"));
}

TEST_F(SparseMerkleDbInspectorTests, get_raw_block_range_invalid_block_id_end) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getRawBlockRange", "2", "0"}},
                out_,
                err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(),
              StartsWith("Failed to execute command [getRawBlockRange], reason: Invalid BLOCK-ID-END value"));
}

TEST_F(SparseMerkleDbInspectorTests, get_raw_block_range_invalid_range_stard_end_equal) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getRawBlockRange", "2", "2"}},
                out_,
                err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getRawBlockRange], reason: Invalid block range"));
}

TEST_F(SparseMerkleDbInspectorTests, get_block_info) {
  ASSERT_EQ(
      EXIT_SUCCESS,
      run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getBlockInfo", "5"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_THAT(out_.str(), HasSubstr("  \"sparseMerkleRootHash\": \"0x"));
  ASSERT_THAT(out_.str(), HasSubstr("  \"parentBlockDigest\": \"0x"));
  ASSERT_THAT(out_.str(), HasSubstr("  \"keyValueCount\": \"" + std::to_string(num_keys_) + '\"'));
}

TEST_F(SparseMerkleDbInspectorTests, get_block_info_missing_block_id) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getBlockInfo"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getBlockInfo], reason: Missing BLOCK-ID argument"));
}

TEST_F(SparseMerkleDbInspectorTests, get_block_key_values) {
  ASSERT_EQ(
      EXIT_SUCCESS,
      run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getBlockKeyValues", "5"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_THAT(out_.str(), StartsWith("{\n  \"0x"));
  ASSERT_THAT(out_.str(), HasSubstr("\"0x0000003c\": \"0x00000078\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"0x00000028\": \"0x00000050\""));
  ASSERT_THAT(out_.str(), HasSubstr("\"0x00000032\": \"0x00000064\""));
  ASSERT_THAT(out_.str(), EndsWith("\"\n}\n"));
}

TEST_F(SparseMerkleDbInspectorTests, get_empty_block_key_values) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getBlockKeyValues", "10"}},
                out_,
                err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_EQ("{\n}\n", out_.str());
}

TEST_F(SparseMerkleDbInspectorTests, get_block_key_values_missing_block_id) {
  ASSERT_EQ(
      EXIT_FAILURE,
      run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getBlockKeyValues"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(),
              StartsWith("Failed to execute command [getBlockKeyValues], reason: Missing BLOCK-ID argument"));
}

TEST_F(SparseMerkleDbInspectorTests, get_value_latest) {
  ASSERT_EQ(
      EXIT_SUCCESS,
      run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getValue", "0x00000028"}}, out_, err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_EQ("{\n  \"blockVersion\": \"5\",\n  \"value\": \"0x00000050\"\n}\n", out_.str());
}

TEST_F(SparseMerkleDbInspectorTests, get_value_with_block_version) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getValue", "0x00000028", "5"}},
                out_,
                err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_EQ("{\n  \"blockVersion\": \"5\",\n  \"value\": \"0x00000050\"\n}\n", out_.str());
}

TEST_F(SparseMerkleDbInspectorTests, get_value_key_without__0x) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getValue", "00000028", "5"}},
                out_,
                err_));
  ASSERT_TRUE(err_.str().empty());
  ASSERT_EQ("{\n  \"blockVersion\": \"5\",\n  \"value\": \"0x00000050\"\n}\n", out_.str());
}

TEST_F(SparseMerkleDbInspectorTests, get_value_empty_key) {
  ASSERT_EQ(
      EXIT_FAILURE,
      run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getValue", "", "5"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getValue], reason: "));
  ASSERT_THAT(err_.str(), Not(HasSubstr("Invalid hex string:")));
}

TEST_F(SparseMerkleDbInspectorTests, get_value_with_version_0) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getValue", "0x00000028", "0"}},
                out_,
                err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getValue], reason: "));
}

TEST_F(SparseMerkleDbInspectorTests, get_value_missing_key) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getValue"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getValue], reason: Missing HEX-KEY argument"));
}

TEST_F(SparseMerkleDbInspectorTests, get_value_invalid_key_size) {
  ASSERT_EQ(
      EXIT_FAILURE,
      run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getValue", "0xabc"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getValue], reason: Invalid hex string: 0xabc"));
}

TEST_F(SparseMerkleDbInspectorTests, get_value_invalid_key_chars_at_start) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getValue", "0xjj"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getValue], reason: Invalid hex string: 0xjj"));
}

TEST_F(SparseMerkleDbInspectorTests, get_value_invalid_key_chars_at_end) {
  ASSERT_EQ(
      EXIT_FAILURE,
      run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getValue", "0xabcx"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getValue], reason: Invalid hex string: 0xabcx"));
}

TEST_F(SparseMerkleDbInspectorTests, get_value_invalid_key_chars_no_0x) {
  ASSERT_EQ(EXIT_FAILURE,
            run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "getValue", "abck"}}, out_, err_));
  ASSERT_TRUE(out_.str().empty());
  ASSERT_THAT(err_.str(), StartsWith("Failed to execute command [getValue], reason: Invalid hex string: abck"));
}

TEST_F(SparseMerkleDbInspectorTests, remove_metadata) {
  ASSERT_EQ(EXIT_SUCCESS,
            run(CommandLineArguments{{"sparse_merkle_db_editor_test", rocksDbPath(), "removeMetadata"}}, out_, err_));
  ASSERT_THAT(out_.str(), StartsWith(toJson(std::string{"result"}, std::string{"true"})));
  ASSERT_TRUE(err_.str().empty());

  auto db = std::make_shared<::concord::storage::rocksdb::Client>(rocksDbPath());
  db->init(true);
  const auto& kvp = generateMetadataAndStateTransfer();
  for (const auto& kv : kvp) {
    ASSERT_TRUE(db->has(kv.first).isNotFound());
  }

  auto adapter = DBAdapter{db};
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
