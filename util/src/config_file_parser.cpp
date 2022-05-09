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
#include "config_file_parser.hpp"

#include <fstream>
#include <algorithm>
#include <cstring>

using std::string;
using std::getline;
using std::ifstream;
using std::pair;
using std::vector;

namespace concord::util {

void ConfigFileParser::parse() {
  ifstream stream(file_, std::ios::binary);
  if (!stream.is_open()) throw std::runtime_error("failed to open file: " + file_.string());

  string key;
  std::uint16_t line_no = 0;
  while (stream) {
    string value, line;
    line_no++;
    getline(stream, line, end_of_line_);
    // get rid of leading and trailing spaces
    concord::util::trim_inplace(line);
    if (line[0] == comment_delimiter_) {
      LOG_TRACE(logger_, "line:" << line_no << " COMMENT");
      continue;
    }

    if (line.empty()) {
      LOG_TRACE(logger_, "line:" << line_no << " EMPTY LINE");
      continue;
    }

    if (line[0] == value_delimiter_) {  // of the form '- value'
      LOG_TRACE(logger_, "line:" << line_no << " VALUE_DELIMETER");
      value = line.substr(1);
      concord::util::ltrim_inplace(value);
      LOG_TRACE(logger_, "line:" << line_no << " value: " << value);
      if (!key.empty())
        parameters_map_.insert(pair<string, string>(key, value));
      else
        throw ParseError(*this, line_no, "not found key for value: " + value);
      continue;
    }
    size_t keyDelimiterPos = line.find_first_of(key_delimiter_);
    if (keyDelimiterPos != string::npos) {
      LOG_TRACE(logger_, "line:" << line_no << " KEY_DELIMETER");
      key = line.substr(0, keyDelimiterPos);
      LOG_TRACE(logger_, "line:" << line_no << " key: " << key);
      if (line.size() > key.size() + 1) {  // simple key-value pair.
        value = line.substr(keyDelimiterPos + 1);
        concord::util::rtrim_inplace(key);
        concord::util::ltrim_inplace(value);
        LOG_TRACE(logger_, "line:" << line_no << " value: " << value);
        parameters_map_.insert(pair<string, string>(key, value));
        key = "";
      }
      continue;
    }
    throw ParseError(*this, line_no, "unrecognized format: " + line);
  }
  stream.close();
  LOG_DEBUG(logger_, "File: " << file_ << " successfully parsed.");
}

size_t ConfigFileParser::count(const string& key) {
  size_t res = parameters_map_.count(key);
  LOG_TRACE(logger_, "count() returns: " << res << " for key: " << key);
  return res;
}

std::vector<std::string> ConfigFileParser::splitValue(const std::string& value_to_split, const char* delimiter) {
  LOG_TRACE(logger_, "valueToSplit: " << value_to_split << ", delimiter: " << delimiter);
  char* rest = (char*)value_to_split.c_str();
  char* token;
  std::vector<std::string> values;
  while ((token = strtok_r(rest, delimiter, &rest))) {
    values.emplace_back(token);
    LOG_TRACE(logger_, "Value after split: " << token);
  }
  return values;
}

void ConfigFileParser::printAll() {
  LOG_TRACE(logger_, "\nKey/value pairs:");
  for (const auto& it : parameters_map_) {
    LOG_TRACE(logger_, it.first << ", " << it.second);
  }
}

}  // namespace concord::util
