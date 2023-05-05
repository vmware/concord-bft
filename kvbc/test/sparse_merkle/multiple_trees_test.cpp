// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#include <memory>
#include <set>

#include "gtest/gtest.h"
#include "sparse_merkle/tree.h"
#include "sparse_merkle/proof_path_processor.h"

#include "serialized_test_db.h"

#include <iostream>
using namespace std;

using namespace concordUtils;
using namespace concord::kvbc;
using namespace concord::kvbc::sparse_merkle;
using namespace proof_path_processor;

void db_put(const shared_ptr<SerializedTestDB>& db, const UpdateBatch& batch) {
  for (const auto& [key, val] : batch.internal_nodes) {
    db->put(key, val);
  }
  for (const auto& [key, val] : batch.leaf_nodes) {
    db->put(key, val);
  }
  // remove the stale keys/values (we only need the latest for this test)
  for (auto& internal_key : batch.stale.internal_keys) {
    db->del(internal_key);
  }
  for (auto& leaf_key : batch.stale.leaf_keys) {
    db->del(leaf_key);
  }
}

std::string random_string(size_t length) {
  auto randchar = []() -> char {
    const char charset[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    const size_t max_index = (sizeof(charset) - 1);
    return charset[rand() % max_index];
  };
  std::string str(length, 0);
  std::generate_n(str.begin(), length, randchar);
  return str;
}

TEST(tree_tests, fill_trees_then_extract_proof_paths_and_verify_them) {
  auto seed = time(0);
  srand(seed);
  cout << "Test seed: " << seed << endl;
  std::shared_ptr<SerializedTestDB> db(new SerializedTestDB);
  std::vector<std::string> addresses;
  addresses.push_back("");
  for (int i = 0; i < 200; i++) {
    addresses.push_back("MY_ETH_ADDR_" + std::to_string(i));
  }

  std::vector<std::map<std::string, std::string>> all;
  all.resize(addresses.size());

  for (int i = 1; i <= 1500; i++) {
    for (size_t address = 0; address < addresses.size(); address++) {
      Tree tree(db, addresses[address]);
      SetOfKeyValuePairs updates;
      for (int j = 1; j <= 10; j++) {
        auto key = random_string(256);
        auto val = random_string(256);
        all[address][key] = val;
        updates.insert({Sliver(std::move(key)), Sliver(std::move(val))});
      }
      auto batch = tree.update(updates);
      db_put(db, batch);
    }
  }
  for (size_t address = 0; address < addresses.size(); address++) {
    Tree tree(db, addresses[address]);
    auto rootHash = tree.get_root_hash();
    for (const auto& v : all[address]) {
      Sliver key{Sliver(std::string(v.first))};
      ASSERT_TRUE(
          verifyProofPath(key, Sliver(std::string(v.second)), getProofPath(key, db, addresses[address]), rootHash));
    }
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  int res = RUN_ALL_TESTS();
  return res;
}
