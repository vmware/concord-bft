// Concord
//
// Copyright (c) 2019-2020 VMware, Inc. All Rights Reserved.
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

using namespace concord::kvbc::sparse_merkle;

TEST(nibble_test, check_bit_operations) {
  Nibble full(15);
  ASSERT_TRUE(full.getBit(0));
  ASSERT_TRUE(full.getBit(1));
  ASSERT_TRUE(full.getBit(2));
  ASSERT_TRUE(full.getBit(3));

  Nibble empty(0);
  ASSERT_FALSE(empty.getBit(0));
  ASSERT_FALSE(empty.getBit(1));
  ASSERT_FALSE(empty.getBit(2));
  ASSERT_FALSE(empty.getBit(3));
}

TEST(nibble_DeathTest, bits_outside_range) { ASSERT_DEATH(Nibble bad(16), ""); }

TEST(nibble_path_test, append_and_get_and_popBack) {
  NibblePath path;
  path.append(7);
  ASSERT_EQ(path.length(), 1);
  path.append(2);
  ASSERT_EQ(path.length(), 2);
  path.append(0);
  ASSERT_EQ(path.length(), 3);

  ASSERT_EQ(Nibble(7), path.get(0));
  ASSERT_EQ(Nibble(2), path.get(1));
  ASSERT_EQ(Nibble(0), path.get(2));
  ASSERT_EQ(3, path.length());

  ASSERT_EQ(Nibble(0), path.popBack());
  ASSERT_EQ(2, path.length());
  ASSERT_EQ(Nibble(2), path.popBack());
  ASSERT_EQ(1, path.length());
  ASSERT_EQ(Nibble(7), path.popBack());

  ASSERT_TRUE(path.empty());
}

TEST(nibble_path_DeathTest, range_errors) {
  NibblePath path;
  for (size_t i = 0; i < Hash::MAX_NIBBLES - 1; i++) {
    path.append(0);
  }
  ASSERT_DEATH(path.append(0), "");
  ASSERT_DEATH(path.get(Hash::MAX_NIBBLES), "");
}

TEST(hash_test, prefix_bits_in_common) {
  // A hash compared with itself should have all bits in common.
  ASSERT_EQ(Hash::SIZE_IN_BITS, PLACEHOLDER_HASH.prefix_bits_in_common(PLACEHOLDER_HASH));

  // Make the hash differ in the last byte
  std::array<uint8_t, Hash::SIZE_IN_BYTES> array2 = Hash::EMPTY_BUF;
  array2[Hash::SIZE_IN_BYTES - 1] = 0xF0;
  Hash hash2 = Hash(array2);

  ASSERT_EQ(Hash::SIZE_IN_BITS - 8, PLACEHOLDER_HASH.prefix_bits_in_common(hash2));

  // Check that we only have 4 bits in common when we only compare from depth of
  // MAX_NIBBLES - 3;
  ASSERT_EQ(Nibble::SIZE_IN_BITS, PLACEHOLDER_HASH.prefix_bits_in_common(hash2, Hash::MAX_NIBBLES - 3));

  // Check that when we exclude all bytes up to the last byte we have no bits in
  // common.
  ASSERT_EQ(0, PLACEHOLDER_HASH.prefix_bits_in_common(hash2, Hash::MAX_NIBBLES - 2));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  int res = RUN_ALL_TESTS();
  return res;
}
