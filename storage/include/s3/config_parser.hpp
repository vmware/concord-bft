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
#include "s3/client.hpp"
#include "config_file_parser.hpp"

namespace concord::storage::s3 {

class ConfigFileParser {
 public:
  ConfigFileParser(const std::string& s3ConfigFile) : parser_{logger_, s3ConfigFile} {}
  StoreConfig parse();

 protected:
  logging::Logger logger_ = logging::getLogger("concord.storage.s3");
  util::ConfigFileParser parser_;
};
}  // namespace concord::storage::s3
