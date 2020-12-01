// Copyright 2018 VMware, all rights reserved
/**
 * The following test suite tests ordering of KeyValuePairs
 */

#include "gtest/gtest.h"
#include "categorization/updates.h"
#include "categorization/kv_blockchain.h"
#include <iostream>
#include <string>
#include <vector>

using namespace concord::kvbc::categorization;
using namespace concord::kvbc;

namespace {

TEST(categorized_updates, merkle_update) {
  std::string key{"key"};
  std::string val{"val"};
  MerkleUpdates mu;
  mu.addUpdate(std::move(key), std::move(val));
  ASSERT_TRUE(mu.getData().kv.size() == 1);
}

TEST(categorized_updates, add_block) {
  KeyValueBlockchain block_chain;
  Updates updates;
  ASSERT_TRUE(block_chain.addBlock(updates) == (BlockId)8);
}

}  // end namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
