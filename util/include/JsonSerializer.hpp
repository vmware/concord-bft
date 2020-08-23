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

#include <map>
#include <string>
#include <utility>
#include <vector>

namespace concordUtils {

std::string ContainerToJson(const std::string key, const std::string value, const bool eof = false) {
  if (eof) return ("\"" + key + "\": \"" + value);
  return ("\"" + key + "\": \"" + value + "\",");
}

std::string ContainerToJson(const std::string key, const std::map<std::string, std::string> &kv) {
  auto out = ("\"" + key + "\": {\n");
  for (const auto &[k, v] : kv) {
    out += ContainerToJson(k, v);
    out += "\n";
  }
  if (out.size() >= 2 && out[out.size() - 2] == ',') {
    out.erase(out.size() - 2, 1);
  }
  out += "}";
  return out;
}

template <typename T>
std::pair<std::string, std::string> toPair(const std::string &key, const T &value) {
  return std::make_pair(key, std::to_string(value));
}

std::pair<std::string, std::string> toPair(const std::string &key, const std::string &value) {
  return std::make_pair(key, value);
}
}  // namespace concordUtils
