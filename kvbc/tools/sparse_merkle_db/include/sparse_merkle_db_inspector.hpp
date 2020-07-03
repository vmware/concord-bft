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

#include "json_output.hpp"

#include "hex_tools.h"
#include "merkle_tree_block.h"
#include "merkle_tree_db_adapter.h"
#include "rocksdb/client.h"
#include "sliver.hpp"

#include <cstdlib>
#include <exception>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace concord::kvbc::tools::sparse_merkle_db {

using namespace std::string_literals;

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

struct GetGenesisBlockID {
  std::string description() const {
    return "getGenesisBlockID\n"
           "  Returns the genesis block ID.";
  }
  std::string execute(const v2MerkleTree::DBAdapter &adapter, const CommandArguments &) const {
    return toJson("genesisBlockID", adapter.getGenesisBlockId());
  }
};

struct GetLastReachableBlockID {
  std::string description() const {
    return "getLastReachableBlockID\n"
           "  Returns the last reachable block ID";
  }
  std::string execute(const v2MerkleTree::DBAdapter &adapter, const CommandArguments &) const {
    return toJson("lastReachableBlockID", adapter.getLastReachableBlockId());
  }
};

struct GetLastBlockID {
  std::string description() const {
    return "getLastBlockID\n"
           " Returns the last block ID";
  }
  std::string execute(const v2MerkleTree::DBAdapter &adapter, const CommandArguments &) const {
    return toJson("lastBlockID", adapter.getLatestBlockId());
  }
};

struct GetRawBlock {
  std::string description() const {
    return "getRawBlock BLOCK-ID\n"
           "  Returns a serialized raw block (encoded in hex).";
  }
  std::string execute(const v2MerkleTree::DBAdapter &adapter, const CommandArguments &args) const {
    if (args.values.empty()) {
      throw std::invalid_argument{"Missing BLOCK-ID argument"};
    }
    const auto raw_block = adapter.getRawBlock(toBlockId(args.values.front()));
    return toJson("rawBlock", concordUtils::sliverToHex(raw_block));
  }
};

struct GetBlockInfo {
  std::string description() const {
    return "getBlockInfo BLOCK-ID\n"
           "  Returns information about the requested block (excluding its key-values).";
  }
  std::string execute(const v2MerkleTree::DBAdapter &adapter, const CommandArguments &args) const {
    if (args.values.empty()) {
      throw std::invalid_argument{"Missing BLOCK-ID argument"};
    }
    const auto raw_block = adapter.getRawBlock(toBlockId(args.values.front()));
    const auto state_hash = v2MerkleTree::block::detail::getStateHash(raw_block);
    const auto parent_digest = adapter.getParentDigest(raw_block);
    const auto key_values = adapter.getBlockData(raw_block);
    return mapToJson(std::map<std::string, std::string>{
        std::make_pair("sparseMerkleRootHash", concordUtils::bufferToHex(state_hash.data(), state_hash.size())),
        std::make_pair("parentBlockDigest", concordUtils::bufferToHex(parent_digest.data(), parent_digest.size())),
        std::make_pair("keyValueCount", std::to_string(key_values.size()))});
  }
};

struct GetBlockKeyValues {
  std::string description() const {
    return "getBlockKeyValues BLOCK-ID\n"
           "  Returns the block's key-values.";
  }
  std::string execute(const v2MerkleTree::DBAdapter &adapter, const CommandArguments &args) const {
    if (args.values.empty()) {
      throw std::invalid_argument{"Missing BLOCK-ID argument"};
    }
    const auto raw_block = adapter.getRawBlock(toBlockId(args.values.front()));
    return mapToJson(adapter.getBlockData(raw_block));
  }
};

struct GetValue {
  std::string description() const {
    return "getValue HEX-KEY [BLOCK-VERSION]\n"
           "  Gets a value by a hex-encoded key and (optionally) a block version.\n"
           "  If no BLOCK-VERSION is passed, the value for the latest one will be returned\n"
           "  (if existing). If the key doesn't exist at BLOCK-VERSION, but exists at an\n"
           "  earlier version, its value at the earlier version will be returned.";
  }
  std::string execute(const v2MerkleTree::DBAdapter &adapter, const CommandArguments &args) const {
    if (args.values.empty()) {
      throw std::invalid_argument{"Missing HEX-KEY argument"};
    }
    const auto key = concordUtils::hexToSliver(args.values.front());
    auto requested_block_version = adapter.getLastReachableBlockId();
    if (args.values.size() >= 2) {
      requested_block_version = toBlockId(args.values[1]);
    }
    const auto [value, block_version] = adapter.getValue(key, requested_block_version);
    return mapToJson(std::map<std::string, std::string>{std::make_pair("blockVersion", std::to_string(block_version)),
                                                        std::make_pair("value", concordUtils::sliverToHex(value))});
  }
};

using Command = std::variant<GetGenesisBlockID,
                             GetLastReachableBlockID,
                             GetLastBlockID,
                             GetRawBlock,
                             GetBlockInfo,
                             GetBlockKeyValues,
                             GetValue>;
inline const auto commands_map = std::map<std::string, Command>{
    std::make_pair("getGenesisBlockID", GetGenesisBlockID{}),
    std::make_pair("getLastReachableBlockID", GetLastReachableBlockID{}),
    std::make_pair("getLastBlockID", GetLastBlockID{}),
    std::make_pair("getRawBlock", GetRawBlock{}),
    std::make_pair("getBlockInfo", GetBlockInfo{}),
    std::make_pair("getBlockKeyValues", GetBlockKeyValues{}),
    std::make_pair("getValue", GetValue{}),
};

inline std::string usage() {
  auto ret =
      "Usage: sparse_merkle_db_inspector PATH-TO-DB COMMAND [ARGUMENTS]...\n"
      "Supported commands:\n"s;
  for (const auto &kv : commands_map) {
    std::visit([&ret](const auto &command) { ret += command.description(); }, kv.second);
    ret += "\n\n";
  }

  ret +=
      "Examples:\n"
      "  sparse_merkle_db_inspector /rocksdb-path getGenesisBlockID\n"
      "  sparse_merkle_db_inspector /rocksdb-path getRawBlock 42\n"
      "  sparse_merkle_db_inspector /rocksdb-path getValue 0x0a0b0c\n"
      "  sparse_merkle_db_inspector /rocksdb-path getValue 0x0a0b0c 42";

  return ret + '\n';
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

inline int run(const CommandLineArguments &cmd_line_args, std::ostream &out, std::ostream &err) {
  if (cmd_line_args.values.size() < kMinCmdLineArguments) {
    err << usage();
    return EXIT_FAILURE;
  }

  auto cmd_it = commands_map.find(cmd_line_args.values[2]);
  if (cmd_it == std::cend(commands_map)) {
    err << usage();
    return EXIT_FAILURE;
  }

  try {
    auto db = std::make_shared<storage::rocksdb::Client>(cmd_line_args.values[1]);
    db->init();
    auto adapter = v2MerkleTree::DBAdapter{db};
    const auto output =
        std::visit([&](const auto &command) { return command.execute(adapter, command_arguments(cmd_line_args)); },
                   cmd_it->second);
    out << output << std::endl;
  } catch (const std::exception &e) {
    err << "Failed to execute command [" << cmd_it->first << "], reason: " << e.what() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}

}  // namespace concord::kvbc::tools::sparse_merkle_db
