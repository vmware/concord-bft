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
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include <memory>
#include <vector>

#include "gtest/gtest.h"

#include "SimpleMemoryPool.hpp"

namespace {
using namespace std;
using namespace concord::util;

TEST(SimpleMemoryPoolTest, check_input_maxNumElements) {
  EXPECT_THROW(SimpleMemoryPool<int> pool(0), std::invalid_argument);
  EXPECT_NO_THROW(SimpleMemoryPool<int> pool1(1));
  EXPECT_NO_THROW(SimpleMemoryPool<int> pool2(1000));
}

TEST(SimpleMemoryPoolTest, basic_test) {
  constexpr size_t poolSize = 100;
  SimpleMemoryPool<int> pool(poolSize);
  std::vector<std::shared_ptr<int>> vec1;

  // before alloc
  ASSERT_EQ(pool.numFreeElements(), poolSize);
  ASSERT_EQ(pool.numAllocatedElements(), 0);
  ASSERT_FALSE(pool.empty());
  ASSERT_TRUE(pool.full());

  // alloc
  for (size_t i{1}; i <= poolSize; ++i) {
    vec1.emplace_back(pool.alloc());
    ASSERT_EQ(pool.numFreeElements(), poolSize - i);
    ASSERT_EQ(pool.numAllocatedElements(), i);
  }

  // after alloc, before free
  ASSERT_TRUE(pool.empty());
  ASSERT_FALSE(pool.full());

  // free
  size_t i{0};
  for (auto &element : vec1) {
    pool.free(element);
    ++i;
    ASSERT_EQ(pool.numFreeElements(), i);
    ASSERT_EQ(pool.numAllocatedElements(), poolSize - i);
  }

  // after free
  ASSERT_FALSE(pool.empty());
  ASSERT_TRUE(pool.full());
}

TEST(SimpleMemoryPoolTest, test_callbacks) {
  int a = 10, b = 20, c = 30;
  SimpleMemoryPool<int> pool(
      100, [&](std::shared_ptr<int> &) { ++a; }, [&](std::shared_ptr<int> &) { ++b; }, [&]() { ++c; });

  ASSERT_EQ(a, 10);
  ASSERT_EQ(b, 20);
  ASSERT_EQ(c, 31);

  auto element = pool.alloc();

  ASSERT_EQ(a, 11);
  ASSERT_EQ(b, 20);
  ASSERT_EQ(c, 31);

  pool.free(element);

  ASSERT_EQ(a, 11);
  ASSERT_EQ(b, 21);
  ASSERT_EQ(c, 31);
}

TEST(SimpleMemoryPoolTest, test_alloc_free_errors) {
  constexpr size_t poolSize = 100;
  SimpleMemoryPool<int> pool(poolSize);
  std::vector<std::shared_ptr<int>> vec1;

  for (size_t i{0}; i < poolSize; ++i) vec1.emplace_back(pool.alloc());

  ASSERT_TRUE(pool.empty());
  ASSERT_FALSE(pool.full());

  // no more elements in pool
  EXPECT_THROW(pool.alloc(), std::runtime_error);

  // try to return element which does not belong to pool
  auto element = std::make_shared<int>();
  EXPECT_THROW(pool.free(element), std::runtime_error);

  // free all elements and try double free
  for (auto &element : vec1) {
    pool.free(element);
  }
  ASSERT_FALSE(pool.empty());
  ASSERT_TRUE(pool.full());
  EXPECT_THROW(pool.free(vec1[0]), std::runtime_error);
}

}  // namespace
