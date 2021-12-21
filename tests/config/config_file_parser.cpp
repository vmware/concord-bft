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
// This file provides functionality for configuration (.ini) file parsing.
// Supported format (YAML like):
//
// # Sample configuration file for creation of a testing environment.
// replicas_config:
// - 127.0.0.1:3410
// - 127.0.0.1:3420
// - 127.0.0.1:3430
// - 127.0.0.1:3440
//
// clients_config: 127.0.0.1:4444

#include "config_file_parser.hpp"

#include <fstream>
#include <algorithm>
#include <cstring>
#include "string.hpp"

using std::string;
using std::getline;
using std::ifstream;
using std::pair;
using std::vector;

namespace concord::tests::config {

bool ConfigFileParser::Parse() {
  ifstream stream(file_name_, std::ios::binary);
  if (!stream.is_open()) {
    LOG_FATAL(logger_, "Failed to open file: " << file_name_);
    return false;
  }
  string key;
  while (stream) {
    string value, tmp;
    getline(stream, tmp, end_of_line_);
    // get rid of leading and trailing spaces
    concord::util::trim_inplace(tmp);
    if (tmp[0] == comment_delimiter_)  // Ignore comments.
      continue;

    if (tmp.empty())  // Skip empty lines.
      continue;

    if (tmp[0] == value_delimiter_) {  // of the form '- value'
      value = tmp.substr(tmp[1]);
      concord::util::ltrim_inplace(tmp);
      if (!key.empty())
        parameters_map_.insert(pair<string, string>(key, value));
      else {
        LOG_FATAL(logger_, "not found key for value " << value);
        return false;
      }
    }
    size_t keyDelimiterPos = tmp.find_first_of(key_delimiter_);
    if (keyDelimiterPos != string::npos) {
      key = tmp.substr(0, keyDelimiterPos);
      if (tmp.size() > key.size() + 1) {
        // Handle simple key-value pair.
        value = tmp.substr(keyDelimiterPos + 1);
        concord::util::rtrim_inplace(key);
        concord::util::ltrim_inplace(value);
        parameters_map_.insert(pair<string, string>(key, value));
      }
      continue;
    }
  }
  stream.close();
  LOG_DEBUG(logger_, "File: " << file_name_ << " successfully parsed.");
  return true;
}

size_t ConfigFileParser::Count(const string& key) {
  size_t res = parameters_map_.count(key);
  LOG_INFO(logger_, "count() returns: " << res << " for key: " << key);
  return res;
}

vector<string> ConfigFileParser::GetValues(const string& key) {
  vector<string> values;
  pair<ParamsMultiMapIt, ParamsMultiMapIt> range = parameters_map_.equal_range(key);
  LOG_DEBUG(logger_, "getValues() for key: " << key);
  if (range.first != parameters_map_.end()) {
    for (auto it = range.first; it != range.second; ++it) {
      values.push_back(it->second);
      LOG_DEBUG(logger_, "value: " << it->second);
    }
  }
  return values;
}

std::vector<std::string> ConfigFileParser::SplitValue(const std::string& value_to_split, const char* delimiter) {
  LOG_DEBUG(logger_, "valueToSplit: " << value_to_split << ", delimiter: " << delimiter);
  char* rest = (char*)value_to_split.c_str();
  char* token;
  std::vector<std::string> values;
  while ((token = strtok_r(rest, delimiter, &rest))) {
    values.emplace_back(token);
    LOG_DEBUG(logger_, "Value after split: " << token);
  }
  return values;
}

void ConfigFileParser::printAll() {
  LOG_DEBUG(logger_, "\nKey/value pairs:");
  for (const auto& it : parameters_map_) {
    LOG_DEBUG(logger_, it.first << ", " << it.second);
  }
}

#ifdef USE_S3_OBJECT_STORE
concord::storage::s3::StoreConfig S3ConfigFileParser::parse(const std::string& s3ConfigFile) {
  logging::Logger logger = logging::getLogger("concord.tests.config.s3");
  ConfigFileParser parser(logger, s3ConfigFile);
  if (!parser.Parse()) throw std::runtime_error("failed to parse" + s3ConfigFile);

  auto get_config_value = [&s3ConfigFile, &parser](const std::string& key) {
    std::vector<std::string> v = parser.GetValues(key);
    if (v.size()) {
      return v[0];
    } else {
      throw std::runtime_error("failed to parse " + s3ConfigFile + ": " + key + " is not set.");
    }
  };

  concord::storage::s3::StoreConfig config;
  config.bucketName = get_config_value("s3-bucket-name");
  config.accessKey = get_config_value("s3-access-key");
  config.protocol = get_config_value("s3-protocol");
  config.url = get_config_value("s3-url");
  config.secretKey = get_config_value("s3-secret-key");
  try {
    // TesterReplica is used for Apollo tests. Each test is executed against new blockchain, so we need brand new
    // bucket for the RO replica. To achieve this we use a hack - set the prefix to a uniqe value so each RO replica
    // writes in the same bucket but in different directory.
    // So if s3-path-prefix is NOT SET it is initialised to a unique value based on current time.
    config.pathPrefix = get_config_value("s3-path-prefix");
  } catch (std::runtime_error& e) {
    config.pathPrefix = std::to_string(std::chrono::high_resolution_clock::now().time_since_epoch().count());
  }
  return config;
}
#endif
}  // namespace concord::tests::config
