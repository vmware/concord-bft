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
//
// This file provides functionality for YAML configuration file parsing.

#pragma once

#include "Logger.hpp"
#include <map>
#include <vector>
#include "string.hpp"
#include "util/filesystem.hpp"

namespace concord::util {

class ConfigFileParser {
  typedef std::multimap<std::string, std::string> ParamsMultiMap;
  typedef ParamsMultiMap::iterator ParamsMultiMapIt;

 public:
  friend class ParseError;
  class ParseError : public std::runtime_error {
   public:
    ParseError(const ConfigFileParser& parser, std::uint16_t line, const std::string& what)
        : std::runtime_error("parse error: " + parser.file_.string() + std::string(": ") + std::to_string(line) +
                             std::string(" reason: ") + what) {}
  };

  ConfigFileParser(logging::Logger& logger, fs::path file) : file_(file), logger_(logger) {}
  virtual ~ConfigFileParser() = default;

  void parse();

  // Returns the number of elements matching specific key.
  size_t count(const std::string& key);

  // Returns a range of values that match specified key.
  template <typename T>
  std::vector<T> get_values(const std::string& key) {
    std::vector<T> values;
    std::pair<ParamsMultiMapIt, ParamsMultiMapIt> range = parameters_map_.equal_range(key);
    LOG_TRACE(logger_, "key: " << key);
    if (range.first != parameters_map_.end()) {
      for (auto it = range.first; it != range.second; ++it) {
        values.push_back(to<T>(it->second));
        LOG_TRACE(logger_, "value: " << it->second);
      }
    }
    return values;
  }

  std::vector<std::string> splitValue(const std::string& value_to_split, const char* delimiter);

  void printAll();

  template <typename T>
  T get_optional_value(const std::string& key, const T& defaultValue) {
    std::vector<T> v = get_values<T>(key);
    if (v.size())
      return v[0];
    else
      return defaultValue;
  }
  template <typename T>
  T get_value(const std::string& key) {
    std::vector<T> v = get_values<T>(key);
    if (v.size())
      return v[0];
    else
      throw std::runtime_error("failed to get value for key :" + key);
  }

 protected:
  static const char key_delimiter_ = ':';
  static const char value_delimiter_ = '-';
  static const char comment_delimiter_ = '#';
  static const char end_of_line_ = '\n';

  fs::path file_;
  ParamsMultiMap parameters_map_;
  logging::Logger& logger_;
};

}  // namespace concord::util
