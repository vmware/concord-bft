// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "endianness.hpp"
#include "kvbc_storage_test_common.h"
#include "kv_types.hpp"
#include "merkle_tree_db_adapter.h"
#include "PersistentStorageImp.hpp"
#include "sliver.hpp"
#include "storage/db_types.h"
#include "storage/merkle_tree_key_manipulator.h"
#include "categorization/updates.h"

namespace {

using namespace concord::kvbc::v2MerkleTree;
using concord::kvbc::BlockId;
using concord::kvbc::categorization::Updates;
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

class DbEditorTestsBase : public Test {
 public:
  virtual void SetUp() override {
    CleanDbs();
    CreateBlockchain(main_path_db_id_, num_blocks_);
  }

  virtual void TearDown() override { CleanDbs(); }

  virtual void CleanDbs() {
    TestRocksDb::cleanup(main_path_db_id_);
    TestRocksDb::cleanup(other_path_db_id_);
    TestRocksDb::cleanup(non_existent_path_db_id_);
    TestRocksDb::cleanup(empty_path_db_id_);
  }

  virtual void CreateBlockchain(std::size_t db_id,
                                BlockId blocks,
                                std::optional<BlockId> mismatch_at = std::nullopt,
                                bool override_keys = false) = 0;

  virtual void DeleteBlocksUntil(std::size_t db_id, BlockId until_block_id) = 0;
  virtual BlockId AddBlock(std::size_t db_id, Updates&& updates) = 0;
  Sliver getSliver(unsigned value) { return toBigEndianStringBuffer(value); }

  SetOfKeyValuePairs generateMetadata() {
    // Create a full range of metadata objects
    constexpr auto num_metadata_object_ids = MAX_METADATA_PARAMS_NUM - INITIALIZED_FLAG;
    auto updates = SetOfKeyValuePairs{};
    concord::storage::v2MerkleTree::MetadataKeyManipulator manipulator;
    for (concord::storage::ObjectId i = INITIALIZED_FLAG; i < num_metadata_object_ids; ++i) {
      updates[manipulator.generateMetadataKey(i)] = getSliver(i);
    }
    return updates;
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

}  // namespace
