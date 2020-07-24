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

#include "callback_registry.hpp"

#include <exception>
#include <functional>
#include <utility>
#include <vector>

namespace {

using testing::InitGoogleTest;
using namespace concord::util;

constexpr auto answer = 42;
auto global_calls = 0;
void func(int, long) { ++global_calls; }

TEST(callback_registry, lambda_no_args) {
  auto calls = 0;
  auto reg = CallbackRegistry<>{};
  reg.registerCallback([&calls]() { ++calls; });
  reg.invokeAll();
  ASSERT_EQ(1, calls);
}

TEST(callback_registry, lambda_1_arg) {
  auto calls = 0;
  auto reg = CallbackRegistry<int>{};
  reg.registerCallback([&calls](int arg) {
    ++calls;
    ASSERT_EQ(answer, arg);
  });
  reg.invokeAll(answer);
  ASSERT_EQ(1, calls);
}

TEST(callback_registry, func_2_args) {
  global_calls = 0;
  auto reg = CallbackRegistry<int, long>{};
  auto handle = reg.registerCallback(func);
  handle.invoke(answer, answer);
  reg.invokeAll(answer, answer);
  ASSERT_EQ(2, global_calls);
}

TEST(callback_registry, std_function) {
  auto calls = 0;
  auto reg = CallbackRegistry<int>{};
  auto func = std::function<void(int)>{[&calls](int arg) {
    ++calls;
    ASSERT_EQ(answer, arg);
  }};
  reg.registerCallback(func);
  reg.invokeAll(answer);
  ASSERT_EQ(1, calls);
}

TEST(callback_registry, handle_invoke) {
  auto calls = 0;
  auto reg = CallbackRegistry<>{};
  auto handle = reg.registerCallback([&calls]() { ++calls; });
  handle.invoke();
  ASSERT_EQ(1, calls);
}

TEST(callback_registry, handle_invoke_and_invoke_all) {
  auto calls = 0;
  auto reg = CallbackRegistry<>{};
  auto handle = reg.registerCallback([&calls]() { ++calls; });
  reg.invokeAll();
  handle.invoke();
  ASSERT_EQ(2, calls);
}

TEST(callback_registry, handle_equality) {
  auto reg = CallbackRegistry<>{};
  auto handle1 = reg.registerCallback([]() {});
  auto handle2 = reg.registerCallback([]() {});
  ASSERT_EQ(handle1, handle1);
  ASSERT_EQ(handle2, handle2);
  ASSERT_NE(handle1, handle2);
}

TEST(callback_registry, multiple_callbacks) {
  auto calls = 0;
  const auto callback = [&calls] { ++calls; };
  auto reg = CallbackRegistry<>{};
  auto handles = std::vector<CallbackRegistry<>::CallbackHandle>{};
  const auto callback_count = 5;
  for (auto i = 0; i < callback_count; ++i) {
    handles.push_back(reg.registerCallback(callback));
  }
  reg.invokeAll();
  ASSERT_EQ(callback_count, calls);
}

TEST(callback_registry, deregister) {
  auto calls = 0;
  auto reg = CallbackRegistry<>{};
  const auto callback = [&calls] { ++calls; };
  auto handle1 = reg.registerCallback(callback);
  auto handle2 = reg.registerCallback(callback);
  // 2 calls
  reg.invokeAll();
  reg.deregisterCallback(std::move(handle1));
  // 1 call
  reg.invokeAll();
  ASSERT_EQ(3, calls);
  reg.deregisterCallback(std::move(handle2));
  // no calls
  reg.invokeAll();
  ASSERT_EQ(3, calls);
}

TEST(callback_registry, size) {
  auto reg = CallbackRegistry<>{};
  auto handle = reg.registerCallback([]() {});
  ASSERT_EQ(1, reg.size());
  ASSERT_FALSE(reg.empty());
  reg.deregisterCallback(std::move(handle));
  ASSERT_EQ(0, reg.size());
  ASSERT_TRUE(reg.empty());
}

TEST(callback_registry, propagate_exception) {
  auto reg = CallbackRegistry<>{};
  reg.registerCallback([]() { throw std::exception{}; });
  ASSERT_THROW(reg.invokeAll(), std::exception);
}

}  // namespace

int main(int argc, char *argv[]) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
