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

#include <functional>
#include <stdexcept>
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
  reg.add([&calls]() { ++calls; });
  reg.invokeAll();
  ASSERT_EQ(1, calls);
}

TEST(callback_registry, lambda_1_arg) {
  auto calls = 0;
  auto reg = CallbackRegistry<int>{};
  reg.add([&calls](int arg) {
    ++calls;
    ASSERT_EQ(answer, arg);
  });
  reg.invokeAll(answer);
  ASSERT_EQ(1, calls);
}

TEST(callback_registry, func_2_args) {
  global_calls = 0;
  auto reg = CallbackRegistry<int, long>{};
  auto handle = reg.add(func);
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
  reg.add(func);
  reg.invokeAll(answer);
  ASSERT_EQ(1, calls);
}

TEST(callback_registry, valid_handle_invoke) {
  auto calls = 0;
  auto reg = CallbackRegistry<>{};
  auto handle = reg.add([&calls]() { ++calls; });
  handle.invoke();
  ASSERT_EQ(1, calls);
}

TEST(callback_registry, moved_handles_invoke) {
  auto calls = 0;
  auto reg = CallbackRegistry<>{};
  auto handle1 = reg.add([&calls]() { ++calls; });
  auto handle2 = std::move(handle1);
  ASSERT_THROW(handle1.invoke(), std::logic_error);  // NOLINT(bugprone-use-after-move)
  ASSERT_NO_THROW(handle2.invoke());
  ASSERT_EQ(1, calls);
}

TEST(callback_registry, handle_invoke_and_invoke_all) {
  auto calls = 0;
  auto reg = CallbackRegistry<>{};
  auto handle = reg.add([&calls]() { ++calls; });
  reg.invokeAll();
  handle.invoke();
  ASSERT_EQ(2, calls);
}

TEST(callback_registry, valid_handle_equality) {
  auto reg = CallbackRegistry<>{};
  auto handle1 = reg.add([]() {});
  auto handle2 = reg.add([]() {});
  ASSERT_EQ(handle1, handle1);
  ASSERT_EQ(handle2, handle2);
  ASSERT_NE(handle1, handle2);
  ASSERT_NE(handle2, handle1);
}

TEST(callback_registry, invalid_handle_inequality) {
  auto reg = CallbackRegistry<>{};
  auto handle1 = reg.add([]() {});
  auto handle2 = reg.add([]() {});
  auto handle3 = std::move(handle1);
  auto handle4 = std::move(handle2);
  ASSERT_NE(handle1, handle2);  // NOLINT(bugprone-use-after-move)
  ASSERT_NE(handle1, handle3);  // NOLINT(bugprone-use-after-move)
  ASSERT_NE(handle1, handle4);  // NOLINT(bugprone-use-after-move)
  ASSERT_NE(handle2, handle3);  // NOLINT(bugprone-use-after-move)
  ASSERT_NE(handle2, handle4);  // NOLINT(bugprone-use-after-move)
  ASSERT_NE(handle3, handle4);
}

TEST(callback_registry, multiple_callbacks) {
  auto calls = 0;
  const auto callback = [&calls] { ++calls; };
  auto reg = CallbackRegistry<>{};
  auto handles = std::vector<CallbackRegistry<>::CallbackHandle>{};
  const auto callback_count = 5;
  for (auto i = 0; i < callback_count; ++i) {
    handles.push_back(reg.add(callback));
  }
  reg.invokeAll();
  ASSERT_EQ(callback_count, calls);
}

TEST(callback_registry, remove_valid_handle) {
  auto calls = 0;
  auto reg = CallbackRegistry<>{};
  const auto callback = [&calls] { ++calls; };
  auto handle1 = reg.add(callback);
  auto handle2 = reg.add(callback);
  // 2 calls
  reg.invokeAll();
  reg.remove(std::move(handle1));
  // 1 call
  reg.invokeAll();
  ASSERT_EQ(3, calls);
  reg.remove(std::move(handle2));
  // no calls
  reg.invokeAll();
  ASSERT_EQ(3, calls);
}

TEST(callback_registry, remove_invalid_handle) {
  auto reg = CallbackRegistry<>{};
  auto handle1 = reg.add([] {});
  [[maybe_unused]] auto handle2 = std::move(handle1);
  // NOLINTNEXTLINE(bugprone-use-after-move)
  ASSERT_THROW(reg.remove(std::move(handle1)), std::invalid_argument);
}

TEST(callback_registry, size) {
  auto reg = CallbackRegistry<>{};
  auto handle = reg.add([]() {});
  ASSERT_EQ(1, reg.size());
  ASSERT_FALSE(reg.empty());
  reg.remove(std::move(handle));
  ASSERT_EQ(0, reg.size());
  ASSERT_TRUE(reg.empty());
}

TEST(callback_registry, propagate_exception) {
  auto reg = CallbackRegistry<>{};
  reg.add([]() { throw std::runtime_error{""}; });
  ASSERT_THROW(reg.invokeAll(), std::runtime_error);
}

TEST(callback_registry, move_ctor_validity) {
  auto reg = CallbackRegistry<>{};
  auto handle1 = reg.add([]() {});
  auto handle2 = std::move(handle1);
  ASSERT_FALSE(handle1.valid());  // NOLINT(bugprone-use-after-move)
  ASSERT_TRUE(handle2.valid());
}

TEST(callback_registry, move_assign_validity) {
  auto reg = CallbackRegistry<>{};
  auto handle1 = reg.add([]() {});
  auto handle2 = reg.add([]() {});
  handle1 = std::move(handle2);
  ASSERT_TRUE(handle1.valid());
  ASSERT_FALSE(handle2.valid());  // NOLINT(bugprone-use-after-move)
}

TEST(callback_registry, move_ctor_invalid_argument) {
  auto reg = CallbackRegistry<>{};
  auto handle1 = reg.add([]() {});
  [[maybe_unused]] auto handle2 = std::move(handle1);
  // NOLINTNEXTLINE(bugprone-use-after-move)
  ASSERT_THROW([[maybe_unused]] auto handle3 = std::move(handle1), std::invalid_argument);
}

TEST(callback_registry, move_assign_invalid_argument) {
  auto reg = CallbackRegistry<>{};
  auto handle1 = reg.add([]() {});
  auto handle2 = std::move(handle1);
  // NOLINTNEXTLINE(bugprone-use-after-move)
  ASSERT_THROW(handle2 = std::move(handle1), std::invalid_argument);
}

TEST(callback_registry, move_assign_to_self_is_no_op) {
  auto reg = CallbackRegistry<>{};
  auto handle = reg.add([]() {});
  auto& handle_ref = handle;
  handle = std::move(handle_ref);
  ASSERT_TRUE(handle.valid());
  ASSERT_EQ(handle, handle);
}

}  // namespace

int main(int argc, char* argv[]) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
