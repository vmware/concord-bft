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

#include "json_output.hpp"

#include <assertUtils.hpp>
#include "hex_tools.h"
#include "merkle_tree_block.h"
#include "merkle_tree_db_adapter.h"
#include "storage/db_types.h"
#include "rocksdb/client.h"
#include "sliver.hpp"

#include <algorithm>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace concord::kvbc::tools::sparse_merkle_db {

using namespace std::string_literals;

inline const auto kToolName = "sparse_merkle_db_editor"s;

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

inline v2MerkleTree::DBAdapter getAdapter(const std::string &path, bool read_only = false) {
  auto db = std::make_shared<storage::rocksdb::Client>(path);
  db->init(read_only);

  // Make sure we don't link the temporary ST chain as we don't want to change the DB in any way.
  const auto link_temp_st_chain = false;
  return v2MerkleTree::DBAdapter{db, link_temp_st_chain};
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

struct GetRawBlockRange {
  std::string description() const {
    return "getRawBlockRange BLOCK-ID-START BLOCK-ID-END\n"
           "  Returns a list of serialized raw blocks (encoded in hex) in the [BLOCK-ID-START, BLOCK-ID-END) range.";
  }

  std::string execute(const v2MerkleTree::DBAdapter &adapter, const CommandArguments &args) const {
    if (args.values.size() < 2) {
      throw std::invalid_argument{"Missing or invalid block range"};
    }
    const auto end = toBlockId(args.values[1]);
    if (end == 0) {
      throw std::invalid_argument{"Invalid BLOCK-ID-END value"};
    }
    const auto first = toBlockId(args.values[0]);
    const auto last = std::min(end - 1, adapter.getLatestBlockId());
    if (first > last) {
      throw std::invalid_argument{"Invalid block range"};
    }
    auto raw_blocks = std::vector<std::pair<std::string, std::string>>{};
    for (auto i = first; i <= last; ++i) {
      const auto raw_block = adapter.getRawBlock(i);
      raw_blocks.emplace_back("rawBlock" + std::to_string(i), concordUtils::sliverToHex(raw_block));
    }
    return toJson(raw_blocks);
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
    return toJson(std::map<std::string, std::string>{
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
    return toJson(adapter.getBlockData(raw_block));
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
    return toJson(std::map<std::string, std::string>{std::make_pair("blockVersion", std::to_string(block_version)),
                                                     std::make_pair("value", concordUtils::sliverToHex(value))});
  }
};

struct CompareTo {
  std::string description() const {
    return "compareTo PATH-TO-OTHER-DB\n"
           "  Compares the passed DB at PATH-TO-DB to the one in PATH-TO-OTHER-DB.\n"
           "  Returns the ID of the first mismatching block, if such exists, in the overlapping \n"
           "  range found in the two databases. If there is no overlapping range in the \n"
           "  databases, no comparison is made.";
  }

  std::string execute(const v2MerkleTree::DBAdapter &main_adapter, const CommandArguments &args) const {
    if (args.values.empty()) {
      throw std::invalid_argument{"Missing PATH-TO-OTHER-DB argument"};
    }

    const auto read_only = true;
    const auto other_adapter = getAdapter(args.values.front(), read_only);

    const auto main_genesis = main_adapter.getGenesisBlockId();
    const auto other_genesis = other_adapter.getGenesisBlockId();
    const auto compared_range_first_block_id = std::max(main_genesis, other_genesis);

    const auto main_last_reachable = main_adapter.getLastReachableBlockId();
    const auto other_last_reachable = other_adapter.getLastReachableBlockId();
    const auto compared_range_last_block_id = std::min(main_last_reachable, other_last_reachable);

    auto result = std::map<std::string, std::string>{
        std::make_pair("mainGenesisBlockId", std::to_string(main_genesis)),
        std::make_pair("otherGenesisBlockId", std::to_string(other_genesis)),
        std::make_pair("mainLastReachableBlockId", std::to_string(main_last_reachable)),
        std::make_pair("otherLastReachableBlockId", std::to_string(other_last_reachable))};

    if (compared_range_first_block_id > compared_range_last_block_id) {
      result["result"] = "no-overlap";
      return toJson(result);
    }

    result["comparedRangeFirstBlockId"] = std::to_string(compared_range_first_block_id);
    result["comparedRangeLastBlockId"] = std::to_string(compared_range_last_block_id);

    const auto mismatch =
        firstMismatch(compared_range_first_block_id, compared_range_last_block_id, main_adapter, other_adapter);
    if (mismatch) {
      result["result"] = "mismatch";
      result["firstMismatchingBlockId"] = std::to_string(*mismatch);
    } else {
      result["result"] = "equivalent";
    }

    return toJson(result);
  }

 private:
  static std::optional<BlockId> firstMismatch(BlockId left,
                                              BlockId right,
                                              const v2MerkleTree::DBAdapter &adapter1,
                                              const v2MerkleTree::DBAdapter &adapter2) {
    ConcordAssertGT(left, 0);
    ConcordAssertGE(right, left);
    auto mismatch = std::optional<BlockId>{};
    // Exploit the blockchain property - if a block is different, try to search for earlier differences to the left.
    // Otherwise, go right.
    while (left <= right) {
      auto current = (right - left) / 2 + left;
      const auto raw1 = adapter1.getRawBlock(current);
      const auto raw2 = adapter2.getRawBlock(current);
      if (raw1 != raw2) {
        mismatch = current;
        right = current - 1;
      } else {
        left = current + 1;
      }
    }
    return mismatch;
  }
};

struct RemoveMetadata {
  std::string description() const {
    return "removeMetadata\n"
           "  Removes metadata and state transfer data from RocksDB.";
  }

  std::string execute(const v2MerkleTree::DBAdapter &adapter, const CommandArguments &) const {
    using storage::v2MerkleTree::detail::EDBKeyType;

    static_assert(static_cast<uint8_t>(EDBKeyType::BFT) + 1 == static_cast<uint8_t>(EDBKeyType::Key),
                  "Key has to be after BFT, if not please review this functionality");

    const concordUtils::Sliver begin{std::string{static_cast<char>(EDBKeyType::BFT)}};
    const concordUtils::Sliver end{std::string{static_cast<char>(EDBKeyType::Key)}};

    const auto status = adapter.getDb()->rangeDel(begin, end);
    if (!status.isOK()) {
      throw std::runtime_error{"Failed to delete metadata and state transfer data: " + status.toString()};
    }
    return toJson(std::string{"result"}, std::string{"true"});
  }
};

using Command = std::variant<GetGenesisBlockID,
                             GetLastReachableBlockID,
                             GetLastBlockID,
                             GetRawBlock,
                             GetRawBlockRange,
                             GetBlockInfo,
                             GetBlockKeyValues,
                             GetValue,
                             CompareTo,
                             RemoveMetadata>;
inline const auto commands_map = std::map<std::string, Command>{
    std::make_pair("getGenesisBlockID", GetGenesisBlockID{}),
    std::make_pair("getLastReachableBlockID", GetLastReachableBlockID{}),
    std::make_pair("getLastBlockID", GetLastBlockID{}),
    std::make_pair("getRawBlock", GetRawBlock{}),
    std::make_pair("getRawBlockRange", GetRawBlockRange{}),
    std::make_pair("getBlockInfo", GetBlockInfo{}),
    std::make_pair("getBlockKeyValues", GetBlockKeyValues{}),
    std::make_pair("getValue", GetValue{}),
    std::make_pair("compareTo", CompareTo{}),
    std::make_pair("removeMetadata", RemoveMetadata{}),
};

inline std::string usage() {
  auto ret = "Usage: " + kToolName + " PATH-TO-DB COMMAND [ARGUMENTS]...\n\n";
  ret += "Supported commands:\n\n";

  for (const auto &kv : commands_map) {
    ret += std::visit([](const auto &command) { return command.description(); }, kv.second);
    ret += "\n\n";
  }

  ret += "Examples:\n";
  ret += "  " + kToolName + " /rocksdb-path getGenesisBlockID\n";
  ret += "  " + kToolName + " /rocksdb-path getRawBlock 42\n";
  ret += "  " + kToolName + " /rocksdb-path getValue 0x0a0b0c\n";
  ret += "  " + kToolName + " /rocksdb-path getValue 0x0a0b0c 42\n";

  return ret;
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
    auto adapter = getAdapter(cmd_line_args.values[1]);
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
