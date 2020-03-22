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

#include "gtest/gtest.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <exception>
#include <iterator>

#include "blockchain_view.h"

namespace {

using namespace concord::kvbc;

using BlockTimestamp = std::uint64_t;

class TimestampedBlockInfo : public BaseBlockInfo {
 public:
  static constexpr auto timestampOffset = 100;

  TimestampedBlockInfo(concord::kvbc::BlockId id) : BaseBlockInfo{id} {}
  TimestampedBlockInfo(concord::kvbc::BlockId id, BlockTimestamp ts) : BaseBlockInfo{id}, timestamp_{ts} {}

  BlockTimestamp timestamp() const { return timestamp_; }
  void loadIndices() { timestamp_ = id() + timestampOffset; }
  void loadData() {}

 private:
  BlockTimestamp timestamp_{0};
};

const auto TimestampCompare = [](const TimestampedBlockInfo& lhs, const TimestampedBlockInfo& rhs) {
  return lhs.timestamp() < rhs.timestamp();
};

class LoadException : public std::exception {
 public:
  LoadException(const std::string& msg) : msg_{msg} {}
  const char* what() const noexcept override { return msg_.c_str(); }

 private:
  std::string msg_;
};

class SucceedOnceBlockInfo : public BaseBlockInfo {
 public:
  SucceedOnceBlockInfo(concord::kvbc::BlockId id) : BaseBlockInfo{id} {}

  void loadIndices() {
    if (fail_) {
      throw LoadException{"Simulate failure"};
    }
    fail_ = true;
  }
  void loadData() {}

 private:
  static inline auto fail_ = false;
};

class InitIntBlockInfo : public BaseBlockInfo {
 public:
  InitIntBlockInfo(concord::kvbc::BlockId id, int init) : BaseBlockInfo{id}, init_{init} {}

  void loadIndices() {}
  void loadData() {}

  int init() const { return init_; };

 private:
  int init_{0};
};

class InitPtrBlockInfo : public BaseBlockInfo {
 public:
  InitPtrBlockInfo(concord::kvbc::BlockId id, const int* init) : BaseBlockInfo{id}, init_{init} {}

  void loadIndices() {}
  void loadData() {}

  const int* init() const { return init_; };

 private:
  const int* init_{nullptr};
};

class MovableBlockInfo : public BaseBlockInfo {
 public:
  MovableBlockInfo(concord::kvbc::BlockId id) : BaseBlockInfo{id} {}
  MovableBlockInfo(MovableBlockInfo&&) = default;
  MovableBlockInfo& operator=(MovableBlockInfo&&) = default;
  MovableBlockInfo(const MovableBlockInfo&) = delete;
  MovableBlockInfo& operator=(const MovableBlockInfo&) = delete;

  void loadIndices() {}
  void loadData() {}
};

TEST(blockchain_view_tests, construction) {
  ASSERT_NO_THROW({
    auto view1 = BlockchainView<BaseBlockInfo>(0, 1);
    auto view2 = BlockchainView<BaseBlockInfo>(0, MAX_BLOCKCHAIN_VIEW_SIZE);
  });

  ASSERT_THROW({ auto view = BlockchainView<BaseBlockInfo>(0, MAX_BLOCKCHAIN_VIEW_SIZE + 1ul); }, std::length_error)
      << "BlockchainView constructor: count exceeds MAX_BLOCKCHAIN_VIEW_SIZE";

  ASSERT_THROW({ auto view = BlockchainView<BaseBlockInfo>(1, MAX_BLOCKCHAIN_VIEW_SIZE); }, std::invalid_argument)
      << "BlockchainView constructor: genesis + count > MAX_BLOCKCHAIN_VIEW_SIZE";
}

TEST(blockchain_view_tests, empty) {
  const auto empty = BlockchainView<BaseBlockInfo>{42, 0};

  ASSERT_EQ(empty.size(), 0);
  ASSERT_TRUE(empty.empty());
  ASSERT_TRUE(std::begin(empty) == std::end(empty));
  ASSERT_TRUE(std::rbegin(empty) == std::rend(empty));
}

TEST(blockchain_view_tests, front_back) {
  constexpr auto genesisBlockId = 1ul;
  constexpr auto size = 5;
  const auto view = BlockchainView<BaseBlockInfo>{genesisBlockId, size};

  ASSERT_EQ(view.front().id(), 1);
  ASSERT_EQ(view.back().id(), 5);
}

TEST(blockchain_view_tests, iteration) {
  constexpr auto genesisBlockId = 1ul;
  constexpr auto size = 5;
  const auto view = BlockchainView<BaseBlockInfo>{genesisBlockId, size};

  // range-based for
  {
    auto id = 1;
    for (const auto& b : view) {
      ASSERT_EQ(b.id(), id);
      ++id;
    }
    ASSERT_EQ(id, size + 1);
  }

  // reverse iteration
  {
    auto id = size;
    std::for_each(std::rbegin(view), std::rend(view), [&id](auto& b) {
      ASSERT_EQ(b.id(), id);
      --id;
    });
    ASSERT_EQ(id, 0);
  }

  // raw loop - zero-indexed
  for (auto i = 0u; i < view.size(); ++i) {
    ASSERT_EQ(view[i].id(), i + 1);
  }
}

TEST(blockchain_view_tests, find) {
  constexpr auto genesisBlockId = 1ul;
  constexpr auto size = 5;
  const auto view = BlockchainView<BaseBlockInfo>{genesisBlockId, size};

  ASSERT_TRUE(view.find(0) == std::cend(view));
  ASSERT_TRUE(view.find(8) == std::cend(view));
  ASSERT_TRUE(view.find(6) == std::cend(view));
  ASSERT_TRUE(view.find(1) == std::cbegin(view));
  ASSERT_TRUE(view.find(3) == std::cbegin(view) + 2);
  ASSERT_TRUE(view.find(5) == std::cbegin(view) + 4);
}

TEST(blockchain_view_tests, linear_search_id) {
  constexpr auto genesisBlockId = 1ul;
  constexpr auto size = 10;
  const auto view = BlockchainView<BaseBlockInfo>{genesisBlockId, size};
  constexpr auto val = 4;

  auto it = std::find(std::begin(view), std::end(view), BaseBlockInfo{val});
  ASSERT_TRUE(it != std::end(view));
  ASSERT_EQ(it->id(), val);
  ASSERT_EQ(view.cacheSize(), val);
}

TEST(blockchain_view_tests, binary_search_timestamp) {
  constexpr auto genesisBlockId = 1ul;
  constexpr auto size = 10 * 1000ul * 1000 * 1000;
  const auto log2Size = static_cast<std::uint32_t>(std::log2(size));
  constexpr auto timeFromLastBlock = 2;
  const auto view = BlockchainView<TimestampedBlockInfo>{genesisBlockId, size};
  const auto val = TimestampedBlockInfo{0, view.back().timestamp() - timeFromLastBlock};

  auto it = std::upper_bound(std::begin(view), std::end(view), val, TimestampCompare);
  ASSERT_TRUE(it != std::end(view));
  ASSERT_EQ(it->id(), size - 1);
  ASSERT_EQ(it->timestamp(), size - 1 + TimestampedBlockInfo::timestampOffset);
  ASSERT_LT(view.cacheSize(), log2Size * 5);  // provide a leeway factor of 5
}

TEST(blockchain_view_tests, clear_cache) {
  constexpr auto genesisBlockId = 1ul;
  constexpr auto size = 5;
  auto view = BlockchainView<BaseBlockInfo>{genesisBlockId, size};

  // fill the cache and verify its size
  auto id = 1;
  for (const auto& b : view) {
    ASSERT_EQ(b.id(), id);
    ++id;
  }
  ASSERT_EQ(id, size + 1);
  ASSERT_EQ(view.cacheSize(), size);

  // clear and verify it is cleared
  view.clearCache();
  ASSERT_EQ(view.cacheSize(), 0);

  // verify size doesn't change after clearing the cache
  ASSERT_FALSE(view.empty());
  ASSERT_EQ(view.size(), size);

  // re-fill the cache and verify
  id = 1;
  for (const auto& b : view) {
    ASSERT_EQ(b.id(), id);
    ++id;
  }
  ASSERT_EQ(id, size + 1);
  ASSERT_EQ(view.cacheSize(), size);
}

TEST(blockchain_view_tests, exceptions) {
  constexpr auto genesisBlockId = 1ul;
  constexpr auto size = 5;
  constexpr auto id = 2;
  auto view = BlockchainView<SucceedOnceBlockInfo>{genesisBlockId, size};

  auto it = view.find(id);
  ASSERT_TRUE(it != std::cend(view));
  ASSERT_EQ(it->id(), id);
  ASSERT_EQ(view.cacheSize(), 1);

  ASSERT_THROW({ ++it; }, LoadException);
  ASSERT_TRUE(it != std::cend(view));
  ASSERT_EQ(it->id(), id);
  ASSERT_EQ(view.cacheSize(), 1);

  ASSERT_THROW({ --it; }, LoadException);
  ASSERT_TRUE(it != std::cend(view));
  ASSERT_EQ(it->id(), id);
  ASSERT_EQ(view.cacheSize(), 1);

  ASSERT_THROW({ it += 2; }, LoadException);
  ASSERT_TRUE(it != std::cend(view));
  ASSERT_EQ(it->id(), id);
  ASSERT_EQ(view.cacheSize(), 1);
}

TEST(blockchain_view_tests, distance) {
  constexpr auto genesisBlockId = 1ul;
  constexpr auto size = 5;
  constexpr auto id = 3;
  auto view = BlockchainView<BaseBlockInfo>{genesisBlockId, size};

  ASSERT_EQ(std::distance(std::begin(view), std::end(view)), size);
  ASSERT_EQ(std::distance(std::begin(view), view.find(id)), size - id);

  ASSERT_EQ(std::distance(std::end(view), std::begin(view)), -size);
  ASSERT_EQ(std::distance(view.find(id), std::begin(view)), id - size);
}

TEST(blockchain_view_tests, init) {
  constexpr auto genesisBlockId = 1ul;
  constexpr auto size = 5;
  constexpr auto init1 = 42;
  const int* init2{nullptr};
  auto view1 = BlockchainView<InitIntBlockInfo, decltype(init1)>{genesisBlockId, size, init1};
  auto view2 = BlockchainView<InitPtrBlockInfo, decltype(init2)>{genesisBlockId, size, init2};

  for (const auto& b : view1) {
    ASSERT_EQ(b.init(), init1);
  }

  for (const auto& b : view2) {
    ASSERT_EQ(b.init(), init2);
  }
}

TEST(blockchain_view_tests, movable_block_info) {
  constexpr auto genesisBlockId = 1ul;
  constexpr auto size = 5;
  auto view = BlockchainView<MovableBlockInfo>{genesisBlockId, size};

  for (auto i = 0u; i < view.size(); ++i) {
    ASSERT_EQ(view[i].id(), i + 1);
  }
}

}  // anonymous namespace

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
