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
//

#include "gtest/gtest.h"
#include "lru_cache.hpp"

using namespace concord::util;

struct moveable {
  bool moved{false};
  std::string member{"member"};
  moveable(moveable&& o) : member(std::move(o.member)) { o.moved = true; }
  moveable(const moveable& o) : member(o.member) {}
  moveable() {}
  moveable& operator=(const moveable& m) { return *this; }
  moveable& operator=(moveable&& m) {
    moved = true;
    return *this;
  }
};

bool operator==(const moveable& m, const moveable& o) { return o.member == m.member; }

template <>
class std::hash<moveable> {
 public:
  size_t operator()(const moveable& m) const { return std::hash<std::string>{}(m.member); }
};

TEST(LruTest, basic) {
  auto cache = LruCache<int, int>(3);

  // Fill the cache and check all values along the way.
  cache.put(1, 100);
  ASSERT_EQ(100, *cache.get(1));
  cache.put(2, 200);
  ASSERT_EQ(100, *cache.get(1));
  ASSERT_EQ(200, *cache.get(2));
  cache.put(3, 300);
  ASSERT_EQ(100, *cache.get(1));
  ASSERT_EQ(200, *cache.get(2));
  ASSERT_EQ(300, *cache.get(3));

  // Remove the LRU key (1)
  cache.put(4, 400);
  ASSERT_EQ(std::nullopt, cache.get(1));
  ASSERT_EQ(200, *cache.get(2));
  ASSERT_EQ(300, *cache.get(3));
  ASSERT_EQ(400, *cache.get(4));

  // Update a value
  cache.put(3, 3000);
  ASSERT_EQ(200, *cache.get(2));
  ASSERT_EQ(3000, *cache.get(3));
  ASSERT_EQ(400, *cache.get(4));

  // Update the LRU value and insert removes the value not updated
  // 3 is the LRU value after the put.
  cache.put(2, 2000);
  // Force 3 out of the cache
  cache.put(5, 500);
  ASSERT_EQ(std::nullopt, cache.get(3));
  ASSERT_EQ(2000, *cache.get(2));
  ASSERT_EQ(400, *cache.get(4));
  ASSERT_EQ(500, *cache.get(5));

  ASSERT_EQ(3, cache.size());
}

TEST(LruTest, lots_of_puts) {
  auto cache = LruCache<int, int>(3);
  cache.put(1, 1);
  cache.put(2, 2);
  cache.put(1, 1);
  cache.put(3, 3);
  cache.put(4, 4);
  cache.put(2, 2);
  cache.put(5, 5);

  ASSERT_EQ(3, cache.size());

  // The last 3 put values should be in the cache
  ASSERT_EQ(4, *cache.get(4));
  ASSERT_EQ(2, *cache.get(2));
  ASSERT_EQ(5, *cache.get(5));

  cache.put(1, 10000);
  cache.put(1, 100);
  ASSERT_EQ(3, cache.size());
  ASSERT_EQ(2, *cache.get(2));
  ASSERT_EQ(5, *cache.get(5));
  ASSERT_EQ(100, *cache.get(1));
}

TEST(LruTest, moves) {
  moveable key;
  moveable value;
  std::string skey = key.member;
  std::string svalue = value.member;
  auto cache = LruCache<moveable, moveable>(1);
  cache.put(std::move(key), std::move(value));
  ASSERT_TRUE(key.moved);
  ASSERT_TRUE(value.moved);

  ASSERT_EQ(svalue, (*cache.get(moveable{})).member);
}

TEST(LruTest, const_ref_not_moved) {
  moveable key;
  moveable value;
  auto cache = LruCache<moveable, moveable>(1);
  cache.put(key, value);
  ASSERT_FALSE(key.moved);
  ASSERT_FALSE(value.moved);

  ASSERT_EQ(value.member, (*cache.get(key)).member);
}

TEST(LruTest, move_only_key) {
  moveable key;
  moveable value;
  auto cache = LruCache<moveable, moveable>(1);
  cache.put(std::move(key), value);
  ASSERT_TRUE(key.moved);
  ASSERT_FALSE(value.moved);

  ASSERT_EQ(value.member, (*cache.get(moveable{})).member);
}

TEST(LruTest, move_only_value) {
  moveable key;
  moveable value;
  auto cache = LruCache<moveable, moveable>(1);
  cache.put(key, std::move(value));
  ASSERT_FALSE(key.moved);
  ASSERT_TRUE(value.moved);

  ASSERT_EQ(moveable{}.member, (*cache.get(key)).member);
}
