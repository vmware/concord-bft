// Copyright 2018 VMware, all rights reserved
/**
 * The following test suite tests ordering of KeyValuePairs
 */

#include "gtest/gtest.h"
#include "kv_types.hpp"
#include "sliver.hpp"

#include <iostream>
#include <string>
#include <vector>

#include <string.h>

using namespace std;
using concord::kvbc::order;
using concord::kvbc::OrderedSetOfKeyValuePairs;
using concord::kvbc::SetOfKeyValuePairs;
using concordUtils::Sliver;

namespace {

/**
 * Test that Sliver's comparison operator maintains unique keys in OrderedSetOfKeyValuePairs .
 */
TEST(sliver_test, unique_keys_in_ordered_set_of_kv_pairs) {
  const auto abc1 = Sliver{"abc"};
  const auto abc2 = Sliver{"abc"};
  const auto value = Sliver{"value"};

  const auto ordered = OrderedSetOfKeyValuePairs{{abc1, value}, {abc2, value}, {abc1, value}};
  ASSERT_EQ(ordered.size(), 1);
  ASSERT_TRUE(ordered.begin()->first == abc1);
  ASSERT_TRUE(ordered.begin()->second == value);
}

/**
 * Test that slivers are correctly ordered from SetOfKeyValuePairs (unordered) to OrderedSetOfKeyValuePairs .
 */
TEST(sliver_test, unordered_to_ordered_set_of_kv_pairs) {
  const auto empty = Sliver{};
  const auto abc = Sliver{"abc"};
  const auto abcd = Sliver{"abcd"};
  const auto adc = Sliver{"adc"};
  const auto bbcd = Sliver{"bbcd"};

  const auto value = Sliver{"value"};

  // Ordered reference.
  const auto reference = std::vector<Sliver>{{empty, abc, abcd, adc, bbcd}};

  // Insert in an arbitrary order.
  const auto unordered = SetOfKeyValuePairs{{bbcd, value}, {abcd, value}, {adc, value}, {abc, value}, {empty, value}};

  // Order.
  const auto ordered = order<SetOfKeyValuePairs>(unordered);

  ASSERT_EQ(reference.size(), ordered.size());
  auto i = 0u;
  for (const auto& [k, v] : ordered) {
    ASSERT_TRUE(reference[i] == k);
    ASSERT_TRUE(value == v);
    ++i;
  }
}

}  // end namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
