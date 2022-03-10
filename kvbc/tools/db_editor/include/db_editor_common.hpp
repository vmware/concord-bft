// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
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

#include "json_output.hpp"

#include "merkle_tree_block.h"
#include "storage/db_types.h"
#include "rocksdb/client.h"
#include "util/filesystem.hpp"

namespace concord::kvbc::tools::db_editor {

using namespace std::string_literals;
using concordUtils::toJson;

template <typename Tag>
struct Arguments {
  std::vector<std::string> values;
};

struct CommandLineArgumentsTag {};
struct CommandArgumentsTag {};

using CommandArguments = Arguments<CommandArgumentsTag>;
using CommandLineArguments = Arguments<CommandArgumentsTag>;

inline auto toBlockId(const std::string &s) {
  if (s.find_first_not_of("0123456789") != std::string::npos) {
    throw std::invalid_argument{"Invalid BLOCK-ID: " + s};
  }
  return kvbc::BlockId{std::stoull(s, nullptr)};
}

inline std::shared_ptr<storage::IDBClient> getDBClient(const std::string &path, bool read_only = false) {
  if (!fs::exists(path) || !fs::is_directory(path)) {
    throw std::invalid_argument{"RocksDB directory path doesn't exist at " + path};
  }

  std::shared_ptr<storage::IDBClient> db{std::make_shared<storage::rocksdb::Client>(path)};
  db->init(read_only);

  return db;
}

inline constexpr auto kMinCmdLineArguments = 3ull;

inline CommandLineArguments command_line_arguments(int argc, char *argv[]) {
  auto cmd_line_args = CommandLineArguments{};
  for (auto i = 0; i < argc; ++i) {
    cmd_line_args.values.push_back(argv[i]);
  }
  return cmd_line_args;
}

inline CommandArguments command_arguments(const CommandLineArguments &cmd_line_args) {
  auto cmd_args = CommandArguments{};
  for (auto i = kMinCmdLineArguments; i < cmd_line_args.values.size(); ++i) {
    cmd_args.values.push_back(cmd_line_args.values[i]);
  }
  return cmd_args;
}

}  // namespace concord::kvbc::tools::db_editor
