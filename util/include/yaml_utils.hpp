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
#include <vector>
#include <iostream>
#include "string.hpp"

namespace concord::util::yaml {
/**
 * Utils for parsing YAML-like elements.
 *
 * Not a YAML parser!
 *
 */

/**
 * name: value
 */
template <typename T>
inline T readValue(std::istream& input, const std::string& name) {
  std::string line;
  while (std::getline(input, line)) {
    concord::util::trim_inplace(line);
    // std::cout << __PRETTY_FUNCTION__ << "(name = " << name << ") line [" << line << "]" << std::endl;
    if (line.length() == 0) continue;  // empty line
    if (line[0] == '#') continue;      // comment
    if (size_t pos = line.find(':'); pos != line.npos) {
      std::string key = line.substr(0, pos);
      // NOLINTNEXTLINE(performance-inefficient-string-concatenation)
      if (key != name) throw std::runtime_error("expected key: " + name + std::string(" found: ") + key);
      std::string value = line.substr(pos + 1);
      concord::util::trim_inplace(value);
      return to<T>(value);
    } else
      throw std::runtime_error("invalid syntax: " + line);
  }
  return to<T>("");  // make compiler happy
}

/**
 * name:
 *   - value1
 *   - value2
 *   ...
 *   - valueN
 * [empty line]
 *
 * since it's not a real YAML parser, for simplicity list must be ended by an empty line
 *
 */
template <typename T>
inline std::vector<T> readCollection(std::istream& input, const std::string& name) {
  std::vector<T> result;
  std::string value = readValue<std::string>(input, name);
  if (value.length() > 0) throw std::runtime_error("wrong list syntax for " + name);
  std::string line;
  while (std::getline(input, line)) {
    concord::util::trim_inplace(line);
    // std::cout << __PRETTY_FUNCTION__ << "(name = " << name << ") line [" << line << "]" << std::endl;
    if (line.length() == 0) break;  // empty line
    if (line[0] == '#') continue;   // comment
    if (line[0] != '-')
      throw std::runtime_error("invalid syntax for list item, should start from \'-\'" + std::string(": ") + line);
    if (size_t pos = line.find(' '); pos != line.npos) {
      result.push_back(to<T>(line.substr(pos + 1)));
    } else {
      // NOLINTNEXTLINE(performance-inefficient-string-concatenation)
      throw std::runtime_error("invalid syntax for list " + name + std::string(": ") + line);
    }
  }
  return std::move(result);
}

}  // namespace concord::util::yaml
