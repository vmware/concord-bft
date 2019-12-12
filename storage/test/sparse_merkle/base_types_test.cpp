// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#include "gtest/gtest.h"
#include "sparse_merkle/base_types.h"

using namespace concord::storage::sparse_merkle;

TEST(nibble_test, check_bit_operations) {
  Nibble full(15);
  ASSERT_TRUE(full.get_bit(0));
  ASSERT_TRUE(full.get_bit(1));
  ASSERT_TRUE(full.get_bit(2));
  ASSERT_TRUE(full.get_bit(3));

  Nibble empty(0);
  ASSERT_FALSE(empty.get_bit(0));
  ASSERT_FALSE(empty.get_bit(1));
  ASSERT_FALSE(empty.get_bit(2));
  ASSERT_FALSE(empty.get_bit(3));
}

TEST(nibble_DeathTest, bits_outside_range) { ASSERT_DEATH(Nibble bad(16), ""); }

TEST(nibble_path_test, append_and_get) {
  NibblePath path;
  path.append(15);
  ASSERT_EQ(path.length(), 1);
  path.append(2);
  ASSERT_EQ(path.length(), 2);
  path.append(0);
  ASSERT_EQ(path.length(), 3);

  ASSERT_EQ(Nibble(15), path.get(0));
  ASSERT_EQ(Nibble(2), path.get(1));
  ASSERT_EQ(Nibble(0), path.get(2));
}

TEST(nibble_path_DeathTest, range_errors) {
  NibblePath path;
  for (size_t i = 0; i < Hash::MAX_NIBBLES - 1; i++) {
    path.append(0);
  }
  ASSERT_DEATH(path.append(0), "");
  ASSERT_DEATH(path.get(Hash::MAX_NIBBLES), "");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  int res = RUN_ALL_TESTS();
  return res;
}
