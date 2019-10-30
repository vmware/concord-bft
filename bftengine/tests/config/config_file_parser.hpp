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

#ifndef CONFIG_FILE_PARSER_HPP
#define CONFIG_FILE_PARSER_HPP

#include "Logger.hpp"

#include <map>
#include <vector>

typedef std::multimap<std::string, std::string> ParamsMultiMap;
typedef ParamsMultiMap::iterator ParamsMultiMapIt;

class ConfigFileParser {
 public:
  ConfigFileParser(concordlogger::Logger& logger, std::string file_name)
      : file_name_(std::move(file_name)), logger_(logger) {}
  virtual ~ConfigFileParser() = default;

  // Returns 0 if passed successfully and 1 otherwise.
  bool Parse();

  // Returns the number of elements matching specific key.
  size_t Count(std::string key);

  // Returns a range of values that match specified key.
  std::vector<std::string> GetValues(std::string key);

  std::vector<std::string> SplitValue(std::string value_to_split, const char* delimiter);

  void printAll();

 private:
  static const char key_delimiter_ = ':';
  static const char value_delimiter_ = '-';
  static const char comment_delimiter_ = '#';
  static const char end_of_line_ = '\n';

  std::string file_name_;
  ParamsMultiMap parameters_map_;
  concordlogger::Logger& logger_;
};

#endif
