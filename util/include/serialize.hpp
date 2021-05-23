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
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <cstring>
#include <type_traits>
#include "assertUtils.hpp"

// Please note that methods in this file do not take endianness into consideration

namespace concord::util {

template <typename T>
std::string serialize(const T& value) {
  static_assert(std::is_integral_v<T>);
  auto serialized = std::string(sizeof(value), 0);
  std::memcpy(serialized.data(), &value, sizeof(T));
  return serialized;
}

template <typename T>
T deserialize(const std::string& serialized) {
  static_assert(std::is_integral_v<T>);
  ConcordAssert(serialized.size() == sizeof(T));
  auto value = T{};
  std::memcpy(&value, serialized.data(), sizeof(T));
  return value;
}

template <typename T>
T deserialize(const char* begin, const char* end) {
  static_assert(std::is_integral_v<T>);
  ConcordAssertEQ((end - begin), sizeof(T));
  auto value = T{};
  std::memcpy(&value, begin, sizeof(T));
  return value;
}

}  // namespace concord::util
