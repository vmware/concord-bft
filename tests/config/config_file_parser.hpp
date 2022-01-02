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
// This file provides functionality for configuration file parsing.

#pragma once

#include "Logger.hpp"
#include <map>
#include <vector>
#include "string.hpp"
#ifdef USE_S3_OBJECT_STORE
#include "s3/client.hpp"
#endif

namespace concord::tests::config {

class ConfigFileParser {
  typedef std::multimap<std::string, std::string> ParamsMultiMap;
  typedef ParamsMultiMap::iterator ParamsMultiMapIt;

 public:
  ConfigFileParser(logging::Logger& logger, std::string file_name)
      : file_name_(std::move(file_name)), logger_(logger) {}
  virtual ~ConfigFileParser() = default;

  // Returns 0 if passed successfully and 1 otherwise.
  bool Parse();

  // Returns the number of elements matching specific key.
  size_t Count(const std::string& key);

  // Returns a range of values that match specified key.
  std::vector<std::string> GetValues(const std::string& key);

  std::vector<std::string> SplitValue(const std::string& value_to_split, const char* delimiter);

  void printAll();

  const std::string getConfigFileName() const { return file_name_; }

 protected:
  static const char key_delimiter_ = ':';
  static const char value_delimiter_ = '-';
  static const char comment_delimiter_ = '#';
  static const char end_of_line_ = '\n';

  std::string file_name_;
  ParamsMultiMap parameters_map_;
  logging::Logger& logger_;
};

#ifdef USE_S3_OBJECT_STORE

class S3ConfigFileParser {
 public:
  S3ConfigFileParser(const std::string& s3ConfigFile) : parser_{logger_, s3ConfigFile} {}
  concord::storage::s3::StoreConfig parse();

 protected:
  template <typename T>
  T get_optional_value(const std::string& key, const T& defaultValue) {
    std::vector<std::string> v = parser_.GetValues(key);
    if (v.size())
      return concord::util::to<T>(v[0]);
    else
      return defaultValue;
  }
  template <typename T>
  T get_value(const std::string& key) {
    std::vector<std::string> v = parser_.GetValues(key);
    if (v.size())
      return concord::util::to<T>(v[0]);
    else
      throw std::runtime_error("failed to parse " + parser_.getConfigFileName() + ": " + key + " is not set.");
  }

  logging::Logger logger_ = logging::getLogger("concord.tests.config.s3");
  ConfigFileParser parser_;
};
#endif
}  // namespace concord::tests::config
