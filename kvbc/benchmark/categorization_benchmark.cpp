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
//

#include <benchmark/benchmark.h>

#include "categorization/base_types.h"
#include "categorization/details.h"
#include "categorized_kvbc_msgs.cmf.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <random>
#include <set>
#include <unordered_set>
#include <vector>

namespace {

using namespace concord::kvbc::categorization;
using namespace concord::kvbc::categorization::detail;

template <typename T>
Buffer serializeAlloc(const T &data) {
  auto out = Buffer{};
  serialize(out, data);
  return out;
}

template <typename T>
const Buffer &serializeTls(const T &data) {
  static thread_local auto out = Buffer{};
  out.clear();
  serialize(out, data);
  return out;
}

BenchmarkMessage message(std::size_t range) {
  auto msg = BenchmarkMessage{};
  auto str = std::string(range, 'a');
  msg.str = str;
  msg.map["k1"] = str;
  msg.map["k2"] = str;
  msg.map["k3"] = str;
  msg.map[str] = str;
  return msg;
}

std::vector<std::uint8_t> randomBuffer(std::size_t size) {
  static thread_local auto gen = std::mt19937{};
  auto dis = std::uniform_int_distribution<std::uint16_t>{0, 255};
  auto vec = std::vector<std::uint8_t>{};
  vec.reserve(size);
  for (auto i = 0ull; i < size; ++i) {
    vec.push_back(dis(gen));
  }
  return vec;
}

std::string randomString(std::size_t size) {
  const auto buf = randomBuffer(size);
  return std::string{std::cbegin(buf), std::cend(buf)};
}

std::vector<std::string> randomStringVector(std::size_t string_size, std::size_t count) {
  auto vec = std::vector<std::string>{};
  for (auto i = 0ull; i < count; ++i) {
    vec.push_back(randomString(string_size));
  }
  return vec;
}

std::vector<std::string> duplicates(std::size_t string_size, std::size_t count) {
  const auto r = randomStringVector(string_size / 2, count / 2);
  auto vec = std::vector<std::string>{r};
  vec.insert(vec.end(), r.begin(), r.end());
  return vec;
}

void serializeAlloc(benchmark::State &state) {
  const auto msg = message(state.range(0));
  for (auto _ : state) {
    const auto out = serializeAlloc(msg);
    benchmark::DoNotOptimize(out);
  }
}

void serializeTls(benchmark::State &state) {
  const auto msg = message(state.range(0));
  for (auto _ : state) {
    const auto &out = serializeTls(msg);
    benchmark::DoNotOptimize(out);
  }
}

void vectorSortAndUnique(benchmark::State &state) {
  const auto d = duplicates(state.range(0), state.range(1));
  for (auto _ : state) {
    state.PauseTiming();
    auto vec = d;
    state.ResumeTiming();

    std::sort(vec.begin(), vec.end());
    vec.erase(std::unique(vec.begin(), vec.end()), vec.end());
    benchmark::DoNotOptimize(vec);
  }
}

void vectorToSet(benchmark::State &state) {
  const auto d = duplicates(state.range(0), state.range(1));
  for (auto _ : state) {
    state.PauseTiming();
    auto vec = d;
    state.ResumeTiming();

    auto set = std::set<std::string>{std::make_move_iterator(vec.begin()), std::make_move_iterator(vec.end())};
    vec.assign(std::make_move_iterator(set.begin()), std::make_move_iterator(set.end()));
    benchmark::DoNotOptimize(set);
    benchmark::DoNotOptimize(vec);
  }
}

void vectorToUnorderedSet(benchmark::State &state) {
  const auto d = duplicates(state.range(0), state.range(1));
  for (auto _ : state) {
    state.PauseTiming();
    auto vec = d;
    state.ResumeTiming();

    auto set =
        std::unordered_set<std::string>{std::make_move_iterator(vec.begin()), std::make_move_iterator(vec.end())};
    vec.assign(std::make_move_iterator(set.begin()), std::make_move_iterator(set.end()));
    std::sort(vec.begin(), vec.end());
    benchmark::DoNotOptimize(set);
    benchmark::DoNotOptimize(vec);
  }
}

}  // namespace

const auto range_multiplier = 2;
const auto serialize_size_start = 8;
const auto serialize_size_end = 4096;

// Ranges for string vectors:
//   1. individual string size
//   2. strings count in the vector
const auto vector_ranges = std::vector<std::pair<std::int64_t, std::int64_t>>{{32, 192}, {8, 128}};

BENCHMARK(serializeAlloc)->RangeMultiplier(range_multiplier)->Range(serialize_size_start, serialize_size_end);
BENCHMARK(serializeTls)->RangeMultiplier(range_multiplier)->Range(serialize_size_start, serialize_size_end);
BENCHMARK(vectorSortAndUnique)->RangeMultiplier(range_multiplier)->Ranges(vector_ranges);
BENCHMARK(vectorToSet)->RangeMultiplier(range_multiplier)->Ranges(vector_ranges);
BENCHMARK(vectorToUnorderedSet)->RangeMultiplier(range_multiplier)->Ranges(vector_ranges);

BENCHMARK_MAIN();
