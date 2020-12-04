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

#include <cstddef>

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

}  // namespace

const auto range_multiplier = 2;
const auto serialize_size_start = 8;
const auto serialize_size_end = 4096;

BENCHMARK(serializeAlloc)->RangeMultiplier(range_multiplier)->Range(serialize_size_start, serialize_size_end);
BENCHMARK(serializeTls)->RangeMultiplier(range_multiplier)->Range(serialize_size_start, serialize_size_end);

BENCHMARK_MAIN();
