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

#include "synchronized_value.hpp"

#include <thread>

namespace {

using namespace concord::util;

TEST(synchronized_value, ctor_built_in_type_default_init) {
  auto v = SynchronizedValue<int>{};
  ASSERT_EQ(0, *v.access());
}

TEST(synchronized_value, ctor_user_type_default_init) {
  struct UserType {
    UserType() = default;
    UserType(int data) : data_{data} {}

    int data_{42};
  };

  auto v = SynchronizedValue<UserType>{};
  ASSERT_EQ(42, v.access()->data_);
}

TEST(synchronized_value, ctor_with_arguments) {
  auto v = SynchronizedValue<int>{42};
  ASSERT_EQ(42, *v.access());
}

TEST(synchronized_value, replace_built_in_type_default_init) {
  auto v = SynchronizedValue<int>{42};
  v.replace();
  ASSERT_EQ(0, *v.access());
}

TEST(synchronized_value, replace_user_type_default_init) {
  struct UserType {
    UserType() = default;
    UserType(int data) : data_{data} {}

    int data_{42};
  };

  auto v = SynchronizedValue<UserType>{7};
  v.replace();
  ASSERT_EQ(42, v.access()->data_);
}

TEST(synchronized_value, replace_with_arguments) {
  auto v = SynchronizedValue<int>{41};
  ASSERT_EQ(41, *v.access());
  v.replace(42);
  ASSERT_EQ(42, *v.access());
}

TEST(synchronized_value, accessor_changes_value) {
  auto v = SynchronizedValue<int>{42};
  auto a = v.access();
  ASSERT_EQ(42, *a);

  *a = 43;
  ASSERT_EQ(43, *a);
}

TEST(synchronized_value, ctor_creates_in_place) {
  struct NonCopyableAndNonMovable {
    NonCopyableAndNonMovable(int data) : data_{data} {}
    NonCopyableAndNonMovable(const NonCopyableAndNonMovable&) = delete;
    NonCopyableAndNonMovable& operator=(const NonCopyableAndNonMovable&) = delete;
    NonCopyableAndNonMovable(NonCopyableAndNonMovable&&) = delete;
    NonCopyableAndNonMovable& operator=(NonCopyableAndNonMovable&&) = delete;

    int data_{0};
  };

  auto v = SynchronizedValue<NonCopyableAndNonMovable>{42};
  ASSERT_EQ(42, v.access()->data_);
}

TEST(synchronized_value, multi_member_ctor) {
  struct MultiMember {
    MultiMember(int data1, int data2) : data1_{data1}, data2_{data2} {}
    int data1_{0};
    int data2_{0};
  };

  auto v = SynchronizedValue<MultiMember>{42, 43};
  auto a = v.access();
  ASSERT_EQ(42, a->data1_);
  ASSERT_EQ(43, a->data2_);
}

TEST(synchronized_value, two_threads_with_accessors) {
  auto v = SynchronizedValue<int>{7};

  auto t1 = std::thread{[&]() {
    auto a = v.access();
    *a = 42;
  }};

  auto t2 = std::thread{[&]() {
    auto a = v.access();
    *a = 43;
  }};

  t1.join();
  t2.join();

  auto a = v.access();
  ASSERT_TRUE((42 == *a || 43 == *a));
}

TEST(synchronized_value, two_threads_with_const_accessors) {
  auto v = SynchronizedValue<int>{42};

  auto t1 = std::thread{[&]() {
    auto a = v.constAccess();
    ASSERT_EQ(42, *a);
  }};

  auto t2 = std::thread{[&]() {
    auto a = v.constAccess();
    ASSERT_EQ(42, *a);
  }};

  t1.join();
  t2.join();
}

TEST(synchronized_value, one_thread_with_accessor_and_one_calls_replace) {
  auto v = SynchronizedValue<int>{7};

  auto t1 = std::thread{[&]() {
    auto a = v.access();
    *a = 42;
  }};

  auto t2 = std::thread{[&]() { v.replace(43); }};

  t1.join();
  t2.join();

  auto a = v.constAccess();
  ASSERT_TRUE((42 == *a || 43 == *a));
}

TEST(synchronized_value, two_threads_with_const_accessors_and_one_with_non_const) {
  auto v = SynchronizedValue<int>{7};

  auto t1 = std::thread{[&]() {
    auto a = v.access();
    *a = 42;
  }};

  auto t2 = std::thread{[&]() {
    auto a = v.access();
    *a = 43;
  }};

  auto t3 = std::thread{[&]() {
    auto a = v.constAccess();
    ASSERT_TRUE((7 == *a || 42 == *a || 43 == *a));
  }};

  t1.join();
  t2.join();
  t3.join();

  auto a = v.constAccess();
  ASSERT_TRUE((7 == *a || 42 == *a || 43 == *a));
}

}  // namespace
