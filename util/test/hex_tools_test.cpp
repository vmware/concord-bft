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
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include "gtest/gtest.h"

#include "hex_tools.h"

#include <stdexcept>
#include <string>

namespace {

using namespace concordUtils;

using testing::InitGoogleTest;

TEST(hex_tools, hex_to_sliver_empty) { ASSERT_TRUE(hexToSliver("").empty()); }

TEST(hex_tools, hex_to_sliver_lower_0x_empty) { ASSERT_TRUE(hexToSliver("0x").empty()); }

TEST(hex_tools, hex_to_sliver_capital_0X_empty) { ASSERT_TRUE(hexToSliver("0X").empty()); }

TEST(hex_tools, hex_to_sliver_odd_size) { ASSERT_THROW(hexToSliver("123"), std::invalid_argument); }

TEST(hex_tools, hex_to_sliver_invalid_char_with_0x) { ASSERT_THROW(hexToSliver("0x12ck"), std::invalid_argument); }

TEST(hex_tools, hex_to_sliver_invalid_char_without_0x) { ASSERT_THROW(hexToSliver("12ck"), std::invalid_argument); }

TEST(hex_tools, hex_to_sliver_lower_0x_in_the_middle) { ASSERT_THROW(hexToSliver("126a0x"), std::invalid_argument); }

TEST(hex_tools, hex_to_sliver_capital_0X_in_the_middle) { ASSERT_THROW(hexToSliver("126a0X"), std::invalid_argument); }

TEST(hex_tools, hex_to_sliver_capital_with_lower_0x) {
  const auto sliver = hexToSliver("0x61646A");
  ASSERT_EQ(3, sliver.length());
  ASSERT_EQ('a', sliver[0]);
  ASSERT_EQ('d', sliver[1]);
  ASSERT_EQ('j', sliver[2]);
}

TEST(hex_tools, hex_to_sliver_capital_with_capital_0X) {
  const auto sliver = hexToSliver("0X61646A");
  ASSERT_EQ(3, sliver.length());
  ASSERT_EQ('a', sliver[0]);
  ASSERT_EQ('d', sliver[1]);
  ASSERT_EQ('j', sliver[2]);
}

TEST(hex_tools, hex_to_sliver_lower_with_lower_0x) {
  const auto sliver = hexToSliver("0x61646a");
  ASSERT_EQ(3, sliver.length());
  ASSERT_EQ('a', sliver[0]);
  ASSERT_EQ('d', sliver[1]);
  ASSERT_EQ('j', sliver[2]);
}

TEST(hex_tools, hex_to_sliver_lower_with_capital_0X) {
  const auto sliver = hexToSliver("0X61646a");
  ASSERT_EQ(3, sliver.length());
  ASSERT_EQ('a', sliver[0]);
  ASSERT_EQ('d', sliver[1]);
  ASSERT_EQ('j', sliver[2]);
}

TEST(hex_tools, hex_to_sliver_capital_without_0x) {
  const auto sliver = hexToSliver("61646A");
  ASSERT_EQ(3, sliver.length());
  ASSERT_EQ('a', sliver[0]);
  ASSERT_EQ('d', sliver[1]);
  ASSERT_EQ('j', sliver[2]);
}

TEST(hex_tools, hex_to_sliver_lower_without_0x) {
  const auto sliver = hexToSliver("61646a");
  ASSERT_EQ(3, sliver.length());
  ASSERT_EQ('a', sliver[0]);
  ASSERT_EQ('d', sliver[1]);
  ASSERT_EQ('j', sliver[2]);
}

}  // namespace

int main(int argc, char *argv[]) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
