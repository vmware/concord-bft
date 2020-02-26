
// Copyright 2020 VMware, all rights reserved

#pragma once

#include <arpa/inet.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <iterator>
#include <string>
#include <type_traits>

namespace concordUtils {

template <typename T>
T hostToNet(T v) {
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
  return hostToNet(v);
}

template <typename T>
using isEndianConvertible = std::conjunction<std::is_integral<T>, std::negation<std::is_same<T, bool>>>;

// Convert integral types (except bool) to a std::string buffer in big endian (network) byte order.
template <typename T>
std::string toBigEndianStringBuffer(T v) {
  static_assert(isEndianConvertible<T>::value);

  v = concordUtils::hostToNet(v);

  const auto data = reinterpret_cast<const char *>(&v);
  return std::string{data, sizeof(v)};
}

template <typename T>
std::array<std::uint8_t, sizeof(T)> toBigEndianArrayBuffer(T v) {
  static_assert(isEndianConvertible<T>::value);

  v = concordUtils::hostToNet(v);

  std::array<std::uint8_t, sizeof(T)> ret;
  const auto data = reinterpret_cast<const std::uint8_t *>(&v);
  std::copy(data, data + sizeof(T), std::begin(ret));
  return ret;
}

// Buffer must be at least sizeof(T) bytes long.
template <typename T>
T fromBigEndianBuffer(const void *buf) {
  return netToHost(*reinterpret_cast<const T *>(buf));
}

}  // namespace concordUtils
