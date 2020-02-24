
// Copyright 2020 VMware, all rights reserved

#pragma once

#include <arpa/inet.h>

#include <cstdint>
#include <type_traits>

namespace concordUtils {

template <typename T>
T swapByteOrder(T v) {
  static_assert(std::is_integral_v<T>);

  if constexpr (sizeof(v) == 2) {
    v = htons(v);
  } else if constexpr (sizeof(v) == 4) {
    v = htonl(v);
  } else if constexpr (sizeof(v) == 8) {
    const auto high = htonl(v >> 32);
    const auto low = htonl(v);
    v = (static_cast<std::uint64_t>(low) << 32) | high;
  }

  return v;
}

template <typename T>
T netToHost(T v) {
  return swapByteOrder(v);
}

template <typename T>
T hostToNet(T v) {
  return swapByteOrder(v);
}

}  // namespace concordUtils
