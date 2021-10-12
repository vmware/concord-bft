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
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "hex_tools.h"

#include <map>
#include <string>
#include <utility>
#include <vector>

namespace concordUtils {

template <typename KVContainer, typename Encoder>
inline std::string kvContainerToJson(const KVContainer &kv, const Encoder &enc) {
  auto out = std::string{"{\n"};
  for (const auto &[key, value] : kv) {
    out += ("  \"" + enc(key) + "\": \"" + enc(value) + "\",\n");
  }
  if (out.size() >= 2 && out[out.size() - 2] == ',') {
    out.erase(out.size() - 2, 1);
  }
  out += "}";
  return out;
}

template <typename KVContainer>
inline std::string kContainerToJson(const KVContainer &kv) {
  auto out = std::string{"{\n"};
  for (const auto &[key, value] : kv) {
    // NOLINTNEXTLINE(performance-inefficient-string-concatenation)
    out += ("  \"" + key + "\": \"" + value + "\",\n");
  }
  if (out.size() >= 2 && out[out.size() - 2] == ',') {
    out.erase(out.size() - 2, 1);
  }
  out += "}";
  return out;
}

inline std::string toJson(const std::unordered_map<Sliver, Sliver> &kv) {
  return kvContainerToJson(kv, [](const auto &arg) { return concordUtils::sliverToHex(arg); });
}

inline std::string toJson(const std::map<std::string, std::string> &kv) {
  return kvContainerToJson(kv, [](const auto &arg) { return arg; });
}

inline std::string toJson(const std::vector<std::pair<std::string, std::string>> &kv) {
  return kvContainerToJson(kv, [](const auto &arg) { return arg; });
}

inline std::string toJson(const std::string &key, const std::string &value) {
  return toJson(std::map<std::string, std::string>{std::make_pair(key, value)});
}

template <typename T>
inline std::string toJson(const std::string &key, const T &value) {
  return toJson(key, std::to_string(value));
}
template <typename T>
inline std::pair<std::string, std::string> toPair(const std::string &key, const T &value) {
  return std::make_pair(key, std::to_string(value));
}

inline std::pair<std::string, std::string> toPair(const std::string &key, const std::string &value) {
  return std::make_pair(key, value);
}

}  // namespace concordUtils
