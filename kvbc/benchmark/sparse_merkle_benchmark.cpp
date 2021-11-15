// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

// This file contains a number of microbenchmarks of merkle tree storage components.

#include <benchmark/benchmark.h>

#include "endianness.hpp"
#include "Handoff.hpp"
#include "Logger.hpp"
#include "memorydb/client.h"
#include "merkle_tree_db_adapter.h"
#include "merkle_tree_key_manipulator.h"
#include "merkle_tree_serialization.h"
#include "sha_hash.hpp"
#include "sparse_merkle/base_types.h"
#include "sparse_merkle/internal_node.h"

#include <cstddef>
#include <cstdint>
#include <future>
#include <iterator>
#include <memory>
#include <random>
#include <string>
#include <type_traits>
#include <vector>
#include <utility>

namespace {

using namespace ::concord::kvbc::sparse_merkle;
using namespace ::concord::kvbc::v2MerkleTree;
using namespace ::concord::kvbc::v2MerkleTree::detail;
using namespace ::concord::storage::memorydb;
using namespace ::concord::util;

using ::concord::kvbc::OrderedKeysSet;
using ::concord::kvbc::SetOfKeyValuePairs;

using ::concordUtils::fromBigEndianBuffer;
using ::concordUtils::Sliver;
using ::concordUtils::toBigEndianStringBuffer;

Hash hash(const std::string &value) {
  auto hasher = Hasher{};
  return hasher.hash(value.data(), value.size());
}

Hash hash(const char *value) { return hash(std::string{value}); }

Hash hash(const Hash &value) {
  auto hasher = Hasher{};
  return hasher.hash(value.data(), value.size());
}

template <typename T>
Hash hash(const T &value) {
  static_assert(std::is_arithmetic_v<T>);
  auto hasher = Hasher{};
  return hasher.hash(&value, sizeof(value));
}

const auto internalChild = InternalChild{hash(42), 42};
const auto leafChild = LeafChild{hash("data1"), LeafKey{hash("data2"), 42}};

BatchedInternalNode createBatchedInternalNode() {
  constexpr auto internalCount = 15;
  constexpr auto leafCount = 16;
  auto children = BatchedInternalNode::Children{};
  static_assert(internalCount + leafCount == children.size());
  for (auto i = 0ull; i < internalCount; ++i) {
    children[i] = InternalChild{hash(i), i};
  }
  for (auto i = 0ull; i < leafCount; ++i) {
    const auto idx = internalCount + i;
    const auto hashChild = hash(idx);
    const auto hashKey = hash(hashChild);
    children[idx] = LeafChild{hashChild, LeafKey{hashKey, idx}};
  }
  return children;
}

const auto batchedInternalNode = createBatchedInternalNode();

const auto internalNodeKey =
    InternalNodeKey{42, NibblePath{32, {1, 2, 3, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}}};

std::vector<std::uint8_t> randomBuffer(std::size_t size) {
  auto rd = std::random_device{};
  auto gen = std::mt19937{rd()};
  auto dis = std::uniform_int_distribution<std::uint16_t>{0, 255};
  auto buf = std::vector<std::uint8_t>{};
  buf.reserve(size);
  for (auto i = 0ull; i < size; ++i) {
    buf.push_back(dis(gen));
  }
  return buf;
}

std::string randomString(std::size_t size) {
  const auto buf = randomBuffer(size);
  return std::string{std::cbegin(buf), std::cend(buf)};
}

void createSubsliver(benchmark::State &state) {
  const auto src = Sliver{std::string(state.range(0), 'a')};

  for (auto _ : state) {
    const auto copy = Sliver{src, 0, src.length()};
    benchmark::DoNotOptimize(copy);
  }
}

void fromBigEndian(benchmark::State &state) {
  const auto src = toBigEndianStringBuffer(std::uint64_t{42});

  for (auto _ : state) {
    const auto val = fromBigEndianBuffer<std::uint64_t>(src.data());
    benchmark::DoNotOptimize(val);
  }
}

void copyInternalChild(benchmark::State &state) {
  for (auto _ : state) {
    const auto copy = internalChild;
    benchmark::DoNotOptimize(copy);
  }
}

void copyLeafChild(benchmark::State &state) {
  for (auto _ : state) {
    const auto copy = leafChild;
    benchmark::DoNotOptimize(copy);
  }
}

void copyChildVariantContainingLeafChild(benchmark::State &state) {
  const Child src = leafChild;

  for (auto _ : state) {
    // NOLINTNEXTLINE(performance-unnecessary-copy-initialization)
    const auto copy = src;
    benchmark::DoNotOptimize(copy);
  }
}

void copyChildVariantContainingInteranlChild(benchmark::State &state) {
  const Child src = internalChild;

  for (auto _ : state) {
    // NOLINTNEXTLINE(performance-unnecessary-copy-initialization)
    const auto copy = src;
    benchmark::DoNotOptimize(copy);
  }
}

void assignLeafChildToChildVariant(benchmark::State &state) {
  for (auto _ : state) {
    const auto copy = Child{leafChild};
    benchmark::DoNotOptimize(copy);
  }
}

void assignInternalChildToChildVariant(benchmark::State &state) {
  for (auto _ : state) {
    const auto copy = Child{internalChild};
    benchmark::DoNotOptimize(copy);
  }
}

void copyBatchedInternalNode(benchmark::State &state) {
  for (auto _ : state) {
    const auto copy = batchedInternalNode;
    benchmark::DoNotOptimize(copy);
  }
}

void deserializeInternalChild(benchmark::State &state) {
  const auto ser = Sliver{serialize(internalChild)};

  for (auto _ : state) {
    const auto deser = deserialize<InternalChild>(ser);
    benchmark::DoNotOptimize(deser);
  }
}

void deserializeLeafChild(benchmark::State &state) {
  const auto ser = Sliver{serialize(leafChild)};

  for (auto _ : state) {
    const auto deser = deserialize<LeafChild>(ser);
    benchmark::DoNotOptimize(deser);
  }
}

void deserializeHash(benchmark::State &state) {
  const auto ser = Sliver{serialize(hash(42))};

  for (auto _ : state) {
    const auto deser = deserialize<Hash>(ser);
    benchmark::DoNotOptimize(deser);
  }
}

void deserializeLeafKey(benchmark::State &state) {
  const auto ser = Sliver{serialize(LeafKey{hash(42), 42})};

  for (auto _ : state) {
    const auto deser = deserialize<LeafKey>(ser);
    benchmark::DoNotOptimize(deser);
  }
}

void serializeBatchedInternalNode(benchmark::State &state) {
  for (auto _ : state) {
    const auto ser = serialize(batchedInternalNode);
    benchmark::DoNotOptimize(ser);
  }
}

void deserializeBatchedInternalNode(benchmark::State &state) {
  const auto ser = Sliver{serialize(batchedInternalNode)};

  for (auto _ : state) {
    const auto deser = deserialize<BatchedInternalNode>(ser);
    benchmark::DoNotOptimize(deser);
  }
}

void serializeInternalKey(benchmark::State &state) {
  for (auto _ : state) {
    const auto ser = serialize(internalNodeKey);
    benchmark::DoNotOptimize(ser);
  }
}

void serializeInternalKeyPlusOp(benchmark::State &state) {
  for (auto _ : state) {
    const auto ser = serializeImp(internalNodeKey.version().value()) + serializeImp(internalNodeKey.path());
    benchmark::DoNotOptimize(ser);
  }
}

void calculateSha2(benchmark::State &state) {
  const auto value = randomBuffer(state.range(0));
  auto hasher = SHA2_256{};

  for (auto _ : state) {
    const auto hash = hasher.digest(value.data(), value.size());
    benchmark::DoNotOptimize(hash);
  }
}

void calculateSha3(benchmark::State &state) {
  const auto value = randomBuffer(state.range(0));
  auto hasher = SHA3_256{};

  for (auto _ : state) {
    const auto hash = hasher.digest(value.data(), value.size());
    benchmark::DoNotOptimize(hash);
  }
}

void stdAsync(benchmark::State &state) {
  constexpr auto input = 41;

  for (auto _ : state) {
    auto fut = std::async(std::launch::async, []() { return input + 1; });
    const auto res = fut.get();
    benchmark::DoNotOptimize(res);
  }
}

void handoff(benchmark::State &state) {
  auto handoff = Handoff{0};
  constexpr auto input = 41;

  for (auto _ : state) {
    auto task = std::make_shared<std::packaged_task<decltype(input)()>>([] { return input + 1; });
    auto fut = task->get_future();
    handoff.push([task]() { (*task)(); });
    const auto res = fut.get();
    benchmark::DoNotOptimize(res);
  }
}

struct Blockchain : benchmark::Fixture {
  void SetUp(const benchmark::State &state) override {
    keyCount = state.range(0);
    keySize = state.range(1);
    valueSize = state.range(2);

    auto db = std::make_shared<Client>();
    db->init();
    adapter = std::make_unique<DBAdapter>(db);

    for (auto i = 0ull; i < blockCount; ++i) {
      adapter->addBlock(createBlockUpdates(keyCount, keySize, valueSize));
    }
  }

  SetOfKeyValuePairs createBlockUpdates(std::size_t keyCount, std::size_t keySize, std::size_t valueSize) {
    auto updates = SetOfKeyValuePairs{};
    for (auto j = 0ull; j < keyCount; ++j) {
      auto key = toBigEndianStringBuffer(currentKeyValue++);
      if (keySize > sizeof(currentKeyValue)) {
        key += std::string(keySize - sizeof(currentKeyValue), 'a');
      }
      updates[std::move(key)] = randomString(valueSize);
    }
    return updates;
  }

  void TearDown(const benchmark::State &) override { adapter.reset(); }

  std::uint64_t currentKeyValue{0};
  std::unique_ptr<DBAdapter> adapter;
  const std::uint64_t blockCount{256};
  std::int64_t keyCount{0};
  std::int64_t keySize{0};
  std::int64_t valueSize{0};
};

BENCHMARK_DEFINE_F(Blockchain, addBlock)(benchmark::State &state) {
  const auto updates = createBlockUpdates(keyCount, keySize, valueSize);

  for (auto _ : state) {
    benchmark::DoNotOptimize(adapter->addBlock(updates));
    state.PauseTiming();
    adapter->deleteLastReachableBlock();
    state.ResumeTiming();
  }
}

BENCHMARK_DEFINE_F(Blockchain, getInternalFromCache)(benchmark::State &state) {
  const auto updates = createBlockUpdates(keyCount, keySize, valueSize);
  const auto internalNodes = adapter->updateTree(updates, OrderedKeysSet{}).second.internalNodes();
  const auto nibblePath = (--internalNodes.cend())->first;

  for (auto _ : state) {
    auto it = internalNodes.find(nibblePath);
    benchmark::DoNotOptimize(it);
  }
}

BENCHMARK_DEFINE_F(Blockchain, getInternalFromDb)(benchmark::State &state) {
  const auto updates = createBlockUpdates(keyCount, keySize, valueSize);
  auto cache = adapter->updateTree(updates, OrderedKeysSet{}).second;
  auto &internalNodes = cache.internalNodes();

  // First, make sure the key is not in the cache.
  const auto lastIt = --internalNodes.cend();
  const auto [nibblePath, internalNode] = *lastIt;
  internalNodes.erase(lastIt);
  const auto internalNodeKey = InternalNodeKey{cache.version(), nibblePath};

  // Secondly, persist it.
  adapter->getDb()->put(DBKeyManipulator::genInternalDbKey(internalNodeKey), serialize(internalNode));

  for (auto _ : state) {
    // Look it up from the DB, knowing the cache has other elements, but not the requested one.
    const auto found = cache.getInternalNode(internalNodeKey);
    benchmark::DoNotOptimize(found);
  }
}

BENCHMARK_DEFINE_F(Blockchain, updateCachePut)(benchmark::State &state) {
  const auto updates = createBlockUpdates(keyCount, keySize, valueSize);
  auto cache = adapter->updateTree(updates, OrderedKeysSet{}).second;
  auto &internalNodes = cache.internalNodes();

  for (auto _ : state) {
    // Delete the last element.
    state.PauseTiming();
    auto lastIt = --internalNodes.cend();
    const auto [nibblePath, internalNode] = *lastIt;
    internalNodes.erase(lastIt);
    state.ResumeTiming();

    // Insert the last element.
    const auto res = internalNodes.emplace(nibblePath, internalNode);
    benchmark::DoNotOptimize(res);
  }
}

BENCHMARK_DEFINE_F(Blockchain, getRawBlock)(benchmark::State &state) {
  const auto blockId = adapter->getLastReachableBlockId();

  for (auto _ : state) {
    const auto block = adapter->getRawBlock(blockId);
    benchmark::DoNotOptimize(block);
  }
}

// Blockchain ranges for:
//  - key count
//  - key size
//  - value size
const auto blockchainRanges = std::vector<std::pair<std::int64_t, std::int64_t>>{{16, 256}, {4, 512}, {1024, 4 * 1024}};
constexpr auto blockchainRangeMultiplier = 2;

constexpr auto shaRangeStart = 8;
constexpr auto shaRangeEnd = 40 * 1024 * 1024;

}  // namespace

BENCHMARK(createSubsliver)->RangeMultiplier(blockchainRangeMultiplier)->Range(8, 8 * 1024);
BENCHMARK(fromBigEndian);
BENCHMARK(copyInternalChild);
BENCHMARK(copyLeafChild);
BENCHMARK(copyChildVariantContainingLeafChild);
BENCHMARK(copyChildVariantContainingInteranlChild);
BENCHMARK(assignInternalChildToChildVariant);
BENCHMARK(assignLeafChildToChildVariant);
BENCHMARK(copyBatchedInternalNode);
BENCHMARK(deserializeInternalChild);
BENCHMARK(deserializeLeafChild);
BENCHMARK(deserializeHash);
BENCHMARK(deserializeLeafKey);
BENCHMARK(serializeBatchedInternalNode);
BENCHMARK(deserializeBatchedInternalNode);
BENCHMARK(serializeInternalKey);
BENCHMARK(serializeInternalKeyPlusOp);
BENCHMARK(calculateSha2)->RangeMultiplier(blockchainRangeMultiplier)->Range(shaRangeStart, shaRangeEnd);
BENCHMARK(calculateSha3)->RangeMultiplier(blockchainRangeMultiplier)->Range(shaRangeStart, shaRangeEnd);
BENCHMARK(stdAsync);
BENCHMARK(handoff);
BENCHMARK_REGISTER_F(Blockchain, addBlock)->RangeMultiplier(blockchainRangeMultiplier)->Ranges(blockchainRanges);
BENCHMARK_REGISTER_F(Blockchain, getInternalFromCache)
    ->RangeMultiplier(blockchainRangeMultiplier)
    ->Ranges(blockchainRanges);
BENCHMARK_REGISTER_F(Blockchain, getInternalFromDb)
    ->RangeMultiplier(blockchainRangeMultiplier)
    ->Ranges(blockchainRanges);
BENCHMARK_REGISTER_F(Blockchain, updateCachePut)->RangeMultiplier(blockchainRangeMultiplier)->Ranges(blockchainRanges);
BENCHMARK_REGISTER_F(Blockchain, getRawBlock)->RangeMultiplier(blockchainRangeMultiplier)->Ranges(blockchainRanges);

BENCHMARK_MAIN();
