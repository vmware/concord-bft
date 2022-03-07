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

// Note: these templated functions are deprecated.  Use the BuildJson class below for new code.

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

// Build a JSON object.  Supports nested objects.
class BuildJson {
 public:
  // Start a root JSON object.
  void startJson() { str += "{\n"; }

  // End the root JSON object.
  void endJson() {
    if ((str.size() >= 2) && (str[str.size() - 2] == ',')) {
      // If the previous clause wasn't empty, we added a comma and a newline;
      // the comma needs to be removed since we are at the end of the generated JSON.
      str.erase(str.size() - 2, 1);
    }
    str += "}";
  }

  // Start a nested JSON object.
  void startNested(const std::string &key) { str += "  \"" + key + "\" : {\n"; }

  // End a nested JSON object.
  void endNested() {
    if ((str.size() >= 2) && (str[str.size() - 2] == ',')) {
      // If the previous clause wasn't empty, we added a comma and a newline;
      // the comma needs to be removed since we are at the end of the nested JSON clause.
      str.erase(str.size() - 2, 1);
    }
    str += "},\n";
  }

  // Add a key and a string value to the current object.
  void addKv(const std::string &key, const std::string &value) { str += "  \"" + key + "\" : \"" + value + "\",\n"; }

  // Add a key and a non-string value to the current object.
  template <typename T>
  void addKv(const std::string &key, const T &value) {
    str += "  \"" + key + "\" : \"" + std::to_string(value) + "\",\n";
  }

  // Add a nested JSON object.  The "json" parameter is expected to be a
  // quoted JSON object in braces with no key prefix.
  void addNestedJson(const std::string &key, const std::string &json) {
    str += "  \"" + key + "\" : ";
    str += json + ",\n";
  }

  // Return the generated JSON.  Should be called after endJson().
  std::string getJson() { return str; }

 private:
  std::string str;
};

}  // namespace concordUtils
