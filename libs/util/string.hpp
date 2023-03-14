// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
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

#include <string>
#include <algorithm>
#include <type_traits>
#include <iterator>
#include <sstream>

namespace concord {
namespace util {

template <typename T>
T to(const std::string& s) = delete;

template <>
inline float to<>(const std::string& s) {
  return std::stof(s);
}
template <>
inline double to<>(const std::string& s) {
  return std::stod(s);
}
template <>
inline bool to<>(const std::string& s) {
  return std::stoi(s);
}
template <>
inline std::uint16_t to<>(const std::string& s) {
  return static_cast<std::uint16_t>(std::stoi(s));
}
template <>
inline std::int32_t to<>(const std::string& s) {
  return std::stoi(s);
}
template <>
inline std::uint32_t to<>(const std::string& s) {
  return static_cast<std::uint32_t>(std::stoul(s));
}
template <>
inline long to<>(const std::string& s) {
  return std::stol(s);
}
template <>
inline unsigned long to<>(const std::string& s) {
  return std::stoul(s);
}
template <>
inline long long to<>(const std::string& s) {
  return std::stoll(s);
}
template <>
inline unsigned long long to<>(const std::string& s) {
  return std::stoull(s);
}
template <>
inline std::string to<>(const std::string& s) {
  return s;
}

inline std::string& ltrim_inplace(std::string& s) {
  s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](int ch) { return !std::isspace(ch); }));
  return s;
}
inline std::string& rtrim_inplace(std::string& s) {
  s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) { return !std::isspace(ch); }).base(), s.end());
  return s;
}
inline std::string& trim_inplace(std::string& str) { return ltrim_inplace(rtrim_inplace(str)); }
inline std::string ltrim(const std::string& s) {
  auto it = std::find_if(s.begin(), s.end(), [](char ch) { return !std::isspace(ch); });
  return std::string(it, s.end());
}
inline std::string rtrim(const std::string& s) {
  auto it = std::find_if(s.rbegin(), s.rend(), [](char ch) { return !std::isspace(ch); });
  return std::string(s.begin(), it.base());
}
inline std::string trim(const std::string& s) { return ltrim(rtrim(s)); }

template <typename E>
constexpr auto toChar(E e) {
  static_assert(std::is_enum_v<E>);
  static_assert(sizeof(E) <= sizeof(char));
  return static_cast<char>(e);
}

template <typename Container>
std::string toString(const Container& container, const char* const separator = "") {
  std::ostringstream out;
  using ElementType = typename Container::value_type;
  std::copy(container.begin(), container.end(), std::ostream_iterator<ElementType>(out, separator));
  return out.str();
}

inline bool isValidHexString(const std::string& str) {
  return str.length() % 2 == 0 && (str.find_first_not_of("0123456789abcdefABCDEF") == std::string::npos);
}

}  // namespace util
}  // namespace concord
