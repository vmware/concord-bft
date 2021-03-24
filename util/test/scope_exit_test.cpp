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

#include "gtest/gtest.h"

#include "scope_exit.hpp"

#include <optional>

// NOLINTNEXTLINE(misc-unused-using-decls)
using concord::util::ScopeExit;

TEST(scope_exit, call_on_exit) {
  auto called = false;
  {
    auto s = ScopeExit{[&]() { called = true; }};
  }
  ASSERT_TRUE(called);
}

TEST(scope_exit, release_does_not_call) {
  auto called = false;
  {
    auto s = ScopeExit{[&]() { called = true; }};
    s.release();
  }
  ASSERT_FALSE(called);
}

TEST(scope_exit, move_calls_once_only) {
  auto called = 0;
  {
    auto s1 = ScopeExit{[&]() { ++called; }};
    auto s2 = ScopeExit{std::move(s1)};
  }
  ASSERT_EQ(1, called);
}

TEST(scope_exit, move_deactivates_source) {
  auto called = 0;
  {
    auto s1 = std::optional{ScopeExit{[&]() { ++called; }}};
    auto s2 = ScopeExit{std::move(*s1)};

    // Make sure that when the source is deactivated it doesn't call on destruction.
    s1.reset();
    ASSERT_EQ(0, called);
  }

  ASSERT_EQ(1, called);
}

TEST(scope_exit, move_from_inactive_does_not_call) {
  auto called = false;
  {
    auto s1 = ScopeExit{[&]() { called = true; }};
    s1.release();

    // s1 is inactive at that point.
    auto s2 = ScopeExit{std::move(s1)};
  }
  ASSERT_FALSE(called);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
