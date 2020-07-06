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
#include "kv_types.hpp"

#include <map>
#include <string>

namespace concord::kvbc::tools::sparse_merkle_db {

template <typename Map, typename Encoder>
std::string mapToJson(const Map &kv, const Encoder &enc) {
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

inline std::string mapToJson(const SetOfKeyValuePairs &kv) {
  return mapToJson(kv, [](const auto &arg) { return concordUtils::sliverToHex(arg); });
}

inline std::string mapToJson(const std::map<std::string, std::string> &kv) {
  return mapToJson(kv, [](const auto &arg) { return arg; });
}

inline std::string toJson(const std::string &key, const std::string &value) {
  return mapToJson(std::map<std::string, std::string>{std::make_pair(key, value)});
}

template <typename T>
std::string toJson(const std::string &key, const T &value) {
  return toJson(key, std::to_string(value));
}

}  // namespace concord::kvbc::tools::sparse_merkle_db
