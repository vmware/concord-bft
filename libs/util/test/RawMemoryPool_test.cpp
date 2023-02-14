// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
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

#include "util/RawMemoryPool.hpp"

namespace {
using namespace std;
using namespace concordUtil;

Timers timers;
const uint32_t chunkSize = 1034;
const uint32_t initialChunks = 3;
const uint32_t maxChunks = 20;

TEST(RawMemoryPoolTest, checkInput) {
  RawMemoryPool pool(chunkSize, timers);
  EXPECT_THROW(pool.allocatePool(maxChunks + 1, maxChunks), std::invalid_argument);
  EXPECT_THROW(pool.allocatePool(initialChunks, -1), std::invalid_argument);
  EXPECT_THROW(pool.allocatePool(-1, maxChunks), std::invalid_argument);
  EXPECT_NO_THROW(pool.allocatePool(initialChunks, initialChunks));
}

TEST(RawMemoryPoolTest, testAllocFree) {
  RawMemoryPool pool(chunkSize, timers);
  pool.allocatePool(initialChunks, maxChunks);

  ASSERT_EQ(pool.getNumOfAvailableChunks(), initialChunks);
  ASSERT_EQ(pool.getNumOfAllocatedChunks(), initialChunks);
  ASSERT_FALSE(pool.isPoolFull());

  // One chunk is in use
  char* chunk = pool.getChunk();
  ASSERT_TRUE(chunk);
  ASSERT_EQ(pool.getNumOfAvailableChunks(), initialChunks - 1);
  ASSERT_EQ(pool.getNumOfAllocatedChunks(), initialChunks);
  ASSERT_FALSE(pool.isPoolFull());

  pool.returnChunk(chunk);  // No pruning expected
  ASSERT_EQ(pool.getNumOfAvailableChunks(), initialChunks);
  ASSERT_EQ(pool.getNumOfAllocatedChunks(), initialChunks);
  ASSERT_FALSE(pool.isPoolFull());

  // Allocate all chunks up to maxChunks number through getChunk() function + fill chunks[] array
  char* chunks[maxChunks];
  for (uint32_t i = 0; i < maxChunks; i++) {
    chunks[i] = pool.getChunk();
    ASSERT_TRUE(chunks[i]);
  }
  ASSERT_EQ(pool.getNumOfAvailableChunks(), 0);
  ASSERT_EQ(pool.getNumOfAllocatedChunks(), maxChunks);
  ASSERT_TRUE(pool.isPoolFull());

  // Return all chunks - pruning gets triggered
  for (uint32_t i = 0; i < maxChunks; i++) {
    ASSERT_TRUE(chunks[i]);
    pool.returnChunk(chunks[i]);
  }
  ASSERT_EQ(pool.getNumOfAvailableChunks(), pool.getNumOfAvailableChunks());
  ASSERT_EQ(pool.getNumOfAllocatedChunks(), pool.getNumOfAvailableChunks());
  ASSERT_FALSE(pool.isPoolFull());
}

TEST(RawMemoryPoolTest, testAllocFreeErrors) {
  RawMemoryPool pool(chunkSize, timers);
  pool.allocatePool(maxChunks, maxChunks);
  ASSERT_TRUE(pool.isPoolFull());

  // Return more chunks than were allocated
  char chunk[chunkSize];
  EXPECT_THROW(pool.returnChunk(chunk), std::runtime_error);
}

}  // namespace
