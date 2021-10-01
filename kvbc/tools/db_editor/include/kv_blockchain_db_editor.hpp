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

#include "kvbc_key_types.hpp"
#include "db_editor_common.hpp"
#include "categorization/kv_blockchain.h"
#include "execution_data.cmf.hpp"
#include "keys_and_signatures.cmf.hpp"
#include "db_interfaces.h"
#include "kvbc_key_types.h"
#include "categorization/db_categories.h"
#include "storage/merkle_tree_key_manipulator.h"
#include "bcstatetransfer/DBDataStore.hpp"
#include "bftengine/PersistentStorageImp.hpp"
#include "bftengine/DbMetadataStorage.hpp"
#include "crypto_utils.hpp"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#include <cryptopp/dll.h>
#pragma GCC diagnostic pop

namespace concord::kvbc::tools::db_editor {

using namespace categorization;

inline const auto kToolName = "kv_blockchain_db_editor"s;
inline KeyValueBlockchain getAdapter(const std::string &path, const bool read_only = false) {
  auto db = getDBClient(path, read_only);

  // Make sure we don't link the temporary ST chain as we don't want to change the DB in any way.
  const auto link_temp_st_chain = false;
  return KeyValueBlockchain{concord::storage::rocksdb::NativeClient::fromIDBClient(db), link_temp_st_chain};
}

inline BlockId getLatestBlockId(const KeyValueBlockchain &adapter) {
  auto blockId = adapter.getLastStatetransferBlockId();
  return blockId ? *blockId : adapter.getLastReachableBlockId();
}

inline const std::map<CATEGORY_TYPE, std::string> cat_type_str = {
    std::make_pair(CATEGORY_TYPE::block_merkle, "block_merkle"),
    std::make_pair(CATEGORY_TYPE::immutable, "immutable"),
    std::make_pair(CATEGORY_TYPE::versioned_kv, "versioned_kv")};

inline const std::string getCategoryType(const std::variant<BlockMerkleInput, VersionedInput, ImmutableInput> &c) {
  return std::visit(
      [](auto &&arg) {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, BlockMerkleInput>) {
          return cat_type_str.at(CATEGORY_TYPE::block_merkle);
        } else if constexpr (std::is_same_v<T, VersionedInput>) {
          return cat_type_str.at(CATEGORY_TYPE::versioned_kv);
        } else if constexpr (std::is_same_v<T, ImmutableInput>) {
          return cat_type_str.at(CATEGORY_TYPE::immutable);
        }
      },
      c);
}

struct GetGenesisBlockID {
  const bool read_only = true;
  std::string description() const {
    return "getGenesisBlockID\n"
           "  Returns the genesis block ID.";
  }
  std::string execute(const KeyValueBlockchain &adapter, const CommandArguments &) const {
    return toJson("genesisBlockID", adapter.getGenesisBlockId());
  }
};

struct GetLastReachableBlockID {
  const bool read_only = true;
  std::string description() const {
    return "getLastReachableBlockID\n"
           "  Returns the last reachable block ID.";
  }

  std::string execute(const KeyValueBlockchain &adapter, const CommandArguments &) const {
    return toJson("lastReachableBlockID", adapter.getLastReachableBlockId());
  }
};

struct GetLastStateTransferBlockID {
  const bool read_only = true;
  std::string description() const {
    return "getLastStateTransferBlockID\n"
           " Returns the last state transfer block ID. If there is no state transfer returns n/a.";
  }

  std::string execute(const KeyValueBlockchain &adapter, const CommandArguments &) const {
    auto blockId = adapter.getLastStatetransferBlockId();
    return toJson("lastStateTransferBlockID", blockId ? std::to_string(*blockId) : "n/a"s);
  }
};

struct GetLastBlockID {
  const bool read_only = true;
  std::string description() const {
    return "getLastBlockID\n"
           " Returns the last block ID. Either reachable or state transfer.";
  }

  std::string execute(const KeyValueBlockchain &adapter, const CommandArguments &) const {
    return toJson("lastBlockID", getLatestBlockId(adapter));
  }
};

struct GetRawBlock {
  const bool read_only = true;
  std::string description() const {
    return "getRawBlock BLOCK-ID\n"
           "  Returns a serialized raw block (encoded in hex).";
  }

  std::string execute(const KeyValueBlockchain &adapter, const CommandArguments &args) const {
    if (args.values.empty()) {
      throw std::invalid_argument{"Missing BLOCK-ID argument"};
    }
    const auto &blockId = args.values.front();
    const auto raw_block = adapter.getRawBlock(toBlockId(blockId));
    if (!raw_block) {
      throw NotFoundException{"Couldn't find a block by ID = "s + blockId};
    }
    return toJson("rawBlock", concordUtils::vectorToHex(categorization::RawBlock::serialize(*raw_block)));
  }
};

struct GetRawBlockRange {
  const bool read_only = true;
  std::string description() const {
    return "getRawBlockRange BLOCK-ID-START BLOCK-ID-END\n"
           "  Returns a list of serialized raw blocks (encoded in hex) in the [BLOCK-ID-START, BLOCK-ID-END) range.";
  }

  std::string execute(const KeyValueBlockchain &adapter, const CommandArguments &args) const {
    if (args.values.size() < 2) {
      throw std::invalid_argument{"Missing or invalid block range"};
    }
    const auto end = toBlockId(args.values[1]);
    if (end == 0) {
      throw std::invalid_argument{"Invalid BLOCK-ID-END value"};
    }
    const auto first = toBlockId(args.values[0]);
    const auto last = std::min(end - 1, getLatestBlockId(adapter));
    if (first > last) {
      throw std::invalid_argument{"Invalid block range"};
    }
    auto raw_blocks = std::vector<std::pair<std::string, std::string>>{};
    for (auto i = first; i <= last; ++i) {
      const auto raw_block = adapter.getRawBlock(i);
      if (!raw_block) {
        throw NotFoundException{"Couldn't find a block by ID = " + std::to_string(i)};
      }
      raw_blocks.emplace_back("rawBlock" + std::to_string(i),
                              concordUtils::vectorToHex(categorization::RawBlock::serialize(*raw_block)));
    }
    return toJson(raw_blocks);
  }
};

struct GetBlockInfo {
  const bool read_only = true;
  std::string description() const {
    return "getBlockInfo BLOCK-ID\n"
           "  Returns information about the requested block (excluding its key-values).";
  }

  std::string execute(const KeyValueBlockchain &adapter, const CommandArguments &args) const {
    if (args.values.empty()) {
      throw std::invalid_argument{"Missing BLOCK-ID argument"};
    }
    auto blockId = toBlockId(args.values.front());
    const auto parent_digest = adapter.parentDigest(blockId);
    const auto updates = adapter.getBlockUpdates(blockId);
    if (!updates) {
      throw NotFoundException{"Couldn't find a block by ID = " + std::to_string(blockId)};
    }
    ConcordAssert(parent_digest.has_value());
    auto catUpdates = updates->categoryUpdates();
    size_t keyValueTotalCount = 0;
    std::stringstream out;
    out << "{" << std::endl;
    out << "  \"parentBlockDigest\": \"" << concordUtils::bufferToHex(parent_digest->data(), parent_digest->size())
        << "\"," << std::endl;
    out << "  \"categoriesCount\": \"" << std::to_string(catUpdates.kv.size()) << "\"," << std::endl;
    out << "  \"categories\": {" << std::endl;

    for (auto it = catUpdates.kv.begin(); it != catUpdates.kv.end(); ++it) {
      const auto &s = std::visit([](auto &&arg) { return arg.kv.size(); }, it->second);
      keyValueTotalCount += s;

      out << "    \"" << it->first << "\": {" << std::endl;
      out << "      \"keyValueCount\": \"" << std::to_string(s) << "\"," << std::endl;
      out << "      \"type\": \"" << getCategoryType(it->second) << "\"" << std::endl;
      out << "    }";
      if (std::next(it) != catUpdates.kv.end()) out << ",";
      out << std::endl;
    }
    out << "  }," << std::endl;

    out << "  \"keyValueTotalCount\": \"" << std::to_string(keyValueTotalCount) << "\"" << std::endl;
    out << "}" << std::endl;

    return out.str();
  }
};

std::string persistencyType(const concord::messages::execution_data::EPersistecyType type) {
  std::string ret;
  switch (type) {
    case concord::messages::execution_data::EPersistecyType::RAW_ON_CHAIN:
      ret = "on-chain";
      break;
    case concord::messages::execution_data::EPersistecyType::SIG_ON_CHAIN:
      ret = "signature on-chain";
      break;
    case concord::messages::execution_data::EPersistecyType::OFF_CHAIN:
      ret = "request exceeds on-chain size threshold, signature is on chain";
      break;
    default:
      ret = "unknown";
      break;
  }
  return ret;
}

struct GetBlockRequests {
  const bool read_only = true;
  std::string description() const {
    return "getBlockRequests BLOCK-ID\n"
           "  Returns the requests that were executed as part of the block.";
  }

  std::string execute(const KeyValueBlockchain &adapter, const CommandArguments &args) const {
    using namespace CryptoPP;
    if (args.values.empty()) {
      throw std::invalid_argument{"Missing BLOCK-ID argument"};
    }
    auto blockId = toBlockId(args.values.front());
    auto key = concordUtils::toBigEndianStringBuffer(blockId);
    auto opt_val = adapter.get(concord::kvbc::categorization::kRequestsRecord, key, blockId);
    if (!opt_val) {
      std::stringstream out;
      out << "block [" << blockId << "] does not contain external client requests\n";
      return out.str();
    }
    auto imm_val = std::get<concord::kvbc::categorization::ImmutableValue>(*opt_val);
    concord::messages::execution_data::RequestsRecord record;
    std::vector<uint8_t> v{imm_val.data.begin(), imm_val.data.end()};
    concord::messages::execution_data::deserialize(v, record);
    std::stringstream out;
    out << "block [" << blockId << "] contains [" << record.requests.size() << "] requests\n";
    out << "Corresponding client keys are published at block [" << record.keys_version << "]\n";
    out << "{\n";
    out << "\"requests\": [\n";
    for (const auto &req : record.requests) {
      HexEncoder encoder;
      std::string hex_digest;
      encoder.Attach(new StringSink(hex_digest));
      encoder.Put(reinterpret_cast<const CryptoPP::byte *>(req.signature.c_str()), req.signature.size());
      encoder.MessageEnd();

      out << "\t{\n";
      out << "\t\t\"client_id\": " << req.clientId << ",\n";
      out << "\t\t\"cid\": \"" << req.cid << "\",\n";
      out << "\t\t\"sequence_number\": " << req.executionSequenceNum << ",\n";
      out << "\t\t\"persistency_type\": \"" << persistencyType(req.requestPersistencyType) << "\",\n";
      out << "\t\t\"signature_digest\": \"" << hex_digest << "\",\n";
      out << "\t},\n";
    }
    out << "]\n}\n";
    return out.str();
  }
};

struct VerifyBlockRequests {
  const bool read_only = true;
  std::string description() const {
    return "verifyBlockRequests BLOCK-ID\n"
           "  verifies the requests that were executed as part of the block.";
  }

  std::string execute(const KeyValueBlockchain &adapter, const CommandArguments &args) const {
    using namespace CryptoPP;
    if (args.values.empty()) {
      throw std::invalid_argument{"Missing BLOCK-ID argument"};
    }
    auto blockId = toBlockId(args.values.front());
    auto key = concordUtils::toBigEndianStringBuffer(blockId);
    auto opt_val = adapter.get(concord::kvbc::categorization::kRequestsRecord, key, blockId);
    if (!opt_val) {
      std::stringstream out;
      out << "block [" << blockId << "] does not contain external client requests\n";
      return out.str();
    }
    auto imm_val = std::get<concord::kvbc::categorization::ImmutableValue>(*opt_val);
    concord::messages::execution_data::RequestsRecord record;
    std::vector<uint8_t> v{imm_val.data.begin(), imm_val.data.end()};
    concord::messages::execution_data::deserialize(v, record);

    auto opt_keys_val = adapter.get(concord::kvbc::categorization::kConcordInternalCategoryId,
                                    std::string(1, concord::kvbc::kClientsPublicKeys),
                                    record.keys_version);
    if (!opt_keys_val) {
      std::stringstream out;
      out << "No keys were found at block " << record.keys_version;
      throw std::invalid_argument{out.str()};
    }

    auto keys_val = std::get<kvbc::categorization::VersionedValue>(*opt_keys_val);
    auto keys = keys_val.data;

    concord::messages::keys_and_signatures::ClientsPublicKeys client_keys;
    std::vector<uint8_t> v_keys{keys.begin(), keys.end()};
    concord::messages::keys_and_signatures::deserialize(v_keys, client_keys);

    std::stringstream out;
    out << "block [" << blockId << "] contains [" << record.requests.size() << "] requests\n";
    out << "Corresponding client keys are published at block [" << record.keys_version << "]\n";
    out << "{\n";
    out << "\"requests\": [\n";
    for (const auto &req : record.requests) {
      HexEncoder encoder;
      std::string hex_digest;
      encoder.Attach(new StringSink(hex_digest));
      encoder.Put(reinterpret_cast<const CryptoPP::byte *>(req.signature.c_str()), req.signature.size());
      encoder.MessageEnd();
      out << "\t{\n";
      out << "\t\t\"client_id\": " << req.clientId << ",\n";
      out << "\t\t\"cid\": \"" << req.cid << "\",\n";
      out << "\t\t\"signature_digest\": \"" << hex_digest << "\",\n";
      out << "\t\t\"persistency_type\": \"" << persistencyType(req.requestPersistencyType) << "\",\n";
      std::string verification_result;
      auto verifier = std::make_unique<concord::util::crypto::RSAVerifier>(
          client_keys.ids_to_keys[req.clientId].key,
          (concord::util::crypto::KeyFormat)client_keys.ids_to_keys[req.clientId].format);

      if (req.requestPersistencyType == concord::messages::execution_data::EPersistecyType::RAW_ON_CHAIN) {
        auto result = verifier->verify(req.request, req.signature);
        verification_result = result ? "ok" : "failed";
      } else {
        verification_result = "Raw request is not available for validation";
      }

      out << "\t\t\"verification_result\": \"" << verification_result << "\",\n";
      out << "\t},\n";
    }
    out << "]\n}\n";
    return out.str();
  }
};

inline std::map<std::string, std::string> getKVStr(
    const std::variant<BlockMerkleInput, VersionedInput, ImmutableInput> &val) {
  std::map<std::string, std::string> kvout;
  std::visit(
      [&kvout](auto &&arg) {
        for (auto const &c : arg.kv) {
          using T = std::decay_t<decltype(arg)>;
          if constexpr (std::is_same_v<T, BlockMerkleInput>) {
            kvout[concordUtils::bufferToHex(c.first.data(), c.first.size())] =
                concordUtils::bufferToHex(c.second.data(), c.second.size());
          } else if constexpr (std::is_same_v<T, VersionedInput>) {
            kvout[concordUtils::bufferToHex(c.first.data(), c.first.size())] =
                concordUtils::bufferToHex(c.second.data.data(), c.second.data.size());
          } else if constexpr (std::is_same_v<T, ImmutableInput>) {
            kvout[concordUtils::bufferToHex(c.first.data(), c.first.size())] =
                concordUtils::bufferToHex(c.second.data.data(), c.second.data.size());
          }
        }
      },
      val);

  return kvout;
}

struct GetBlockKeyValues {
  const bool read_only = true;
  std::string description() const {
    return "getBlockKeyValues BLOCK-ID [CATEGORY]\n"
           "  Returns the block's key-values. If a CATEGORY is passed only those keys/values are listed.";
  }

  std::string execute(const KeyValueBlockchain &adapter, const CommandArguments &args) const {
    if (args.values.empty()) {
      throw std::invalid_argument{"Missing BLOCK-ID argument"};
    }

    auto blockId = toBlockId(args.values.front());
    const auto updates = adapter.getBlockUpdates(blockId);
    if (!updates) {
      throw NotFoundException{"Couldn't find a block by ID = " + std::to_string(blockId)};
    }

    std::stringstream out;
    out << "{" << std::endl;

    if (args.values.size() >= 2) {
      auto requested_category = args.values[1];
      auto kvs = updates->categoryUpdates(requested_category);
      if (!kvs) {
        throw NotFoundException{"Couldn't find category = " + requested_category};
      }
      out << "\"" << requested_category << "\": " << toJson(getKVStr(kvs->get())) << std::endl;
    } else {
      auto catUpdates = updates->categoryUpdates();

      for (auto it = catUpdates.kv.begin(); it != catUpdates.kv.end(); ++it) {
        out << "\"" << it->first << "\": " << toJson(getKVStr(it->second));
        if (std::next(it) != catUpdates.kv.end()) out << ",";
        out << std::endl;
      }
    }

    out << "}";

    return out.str();
  }
};

struct GetCategories {
  const bool read_only = true;
  std::string description() const {
    return "getCategories [BLOCK-VERSION]\n"
           "  Returns all categories. If a BLOCK-VERSION is passed only categories in that block are listed.";
  }

  std::string execute(const KeyValueBlockchain &adapter, const CommandArguments &args) const {
    std::map<std::string, std::string> out;

    if (args.values.size() >= 1) {
      auto requested_block_version = toBlockId(args.values[0]);
      const auto updates = adapter.getBlockUpdates(requested_block_version);
      if (!updates) {
        throw NotFoundException{"Couldn't find a block by ID = " + std::to_string(requested_block_version)};
      }
      auto catUpdates = updates->categoryUpdates();
      for (auto const &c : catUpdates.kv) {
        out[c.first] = getCategoryType(c.second);
      }
    } else {
      auto categories = adapter.blockchainCategories();
      for (auto const &c : categories) {
        out[c.first] = cat_type_str.at(c.second);
      }
    }

    return toJson(out);
  }
};

struct GetEarliestCategoryUpdates {
  const bool read_only = true;
  std::string description() const {
    return "getEarliestCategoryUpdates CATEGORY-ID [BLOCK-VERSION-TO]\n"
           "  Returns the first blockID and a category updates that contains the given category in the \n"
           "  [genesisBlockID, BLOCK-VERSION-TO] range.\n"
           "  If BLOCK-VERSION-TO is not set, the search range is [genesisBlockID, lastReachableBlockID].\n"
           "  Note that this method performs linear search which may take time on big blockchains.";
  }

  std::string execute(const KeyValueBlockchain &adapter, const CommandArguments &args) const {
    if (args.values.empty()) {
      throw NotFoundException{"No Category ID was given"};
    }
    auto latestBlockID = adapter.getLastReachableBlockId();
    if (args.values.size() >= 2) {
      latestBlockID = toBlockId(args.values[1]);
    }
    auto cat = args.values.front();
    std::map<std::string, std::string> cat_updates_map;
    BlockId relevantBlockId = adapter.getGenesisBlockId();
    for (auto block = adapter.getGenesisBlockId(); block <= latestBlockID; block++) {
      auto updates = adapter.getBlockUpdates(block);
      if (updates->categoryUpdates().kv.count(cat)) {
        relevantBlockId = block;
        cat_updates_map = getKVStr(updates->categoryUpdates().kv.at(cat));
        break;
      }
    }
    if (relevantBlockId == adapter.getGenesisBlockId()) {
      if (!adapter.getBlockUpdates(relevantBlockId)->categoryUpdates().kv.count(cat)) {
        throw NotFoundException{"Couldn't find category id in any block in the given range"};
      }
    }
    std::map<std::string, std::string> out{
        {"blockID", std::to_string(relevantBlockId)}, {"category", cat}, {"updates", toJson(cat_updates_map)}};
    return toJson(out);
  }
};

inline std::string getStaleKeysStr(const std::vector<std::string> &stale_keys) {
  if (stale_keys.empty()) return std::string();
  std::string strKeys;
  strKeys += "[";
  for (auto &k : stale_keys) {
    strKeys += "\"" + concordUtils::bufferToHex(k.data(), k.size()) + "\"" + ",";
  }
  strKeys.erase(strKeys.size() - 1);
  strKeys += "]";
  return strKeys;
}

struct GetCategoryEarliestStale {
  const bool read_only = true;
  std::string description() const {
    return "getCategoryEarliestStale CATEGORY-ID [BLOCK-VERSION-TO]\n"
           "  Returns the first blockID and a list of stale keys for this blockID a given category has in \n"
           "  the [genesisBlockID, BLOCK-VERSION-TO] range.\n"
           "  If BLOCK-VERSION-TO is not set, the search range is [genesisBlockID, lastReachableBlockID].\n"
           "  Note that this method performs linear search which may take time on big blockchains.";
  }

  std::string execute(KeyValueBlockchain &adapter, const CommandArguments &args) const {
    if (args.values.empty()) {
      throw NotFoundException{"No Category ID was given"};
    }
    auto latestBlockID = adapter.getLastReachableBlockId();
    if (args.values.size() >= 2) {
      latestBlockID = toBlockId(args.values[1]);
    }
    auto cat = args.values.front();
    BlockId relevantBlockId = adapter.getGenesisBlockId();
    std::map<std::string, std::vector<std::string>> stale_keys;
    std::string keys_as_string;
    for (auto block = adapter.getGenesisBlockId(); block <= latestBlockID; block++) {
      stale_keys = adapter.getBlockStaleKeys(block);
      if (stale_keys.count(cat) && !stale_keys[cat].empty()) {
        relevantBlockId = block;
        keys_as_string = getStaleKeysStr(stale_keys[cat]);
        break;
      }
    }
    if (keys_as_string.empty()) {
      throw NotFoundException{"Couldn't find stale keys for category id in any block in the given range"};
    }
    std::map<std::string, std::string> out{
        {"blockID", std::to_string(relevantBlockId)}, {"category", cat}, {"stale_keys", keys_as_string}};
    return toJson(out);
  }
};

struct GetStaleKeysSummary {
  const bool read_only = true;
  std::string description() const {
    return "getStaleKeysSummary [BLOCK-VERSION-FROM] [BLOCK-VERSION-TO]\n"
           "Return the number of stale keys per category type in the current blockchain.\n"
           "If BLOCK-VERSION-FROM and BLOCK-VERSION-TO has been given, the method will sum the stale keys from "
           "BLOCK-VERSION-FROM to "
           "BLOCK-VERSION-TO.\n"
           "If only one argument has been given, it will be retreated as BLOCK-VERSION-TO and BLOCK-VERSION-FROM will "
           "be set to the current genesis.\n"
           "Note that this operation is doing a linear search, hence in may take a while to be completed.";
  }

  std::string execute(KeyValueBlockchain &adapter, const CommandArguments &args) const {
    auto latestBlockID = adapter.getLastReachableBlockId();
    auto firstBlockID = adapter.getGenesisBlockId();
    if (args.values.size() == 2) {
      auto block_version_from = toBlockId(args.values[0]);
      auto block_version_to = toBlockId(args.values[1]);
      if (block_version_from < firstBlockID || block_version_from > latestBlockID) {
        throw std::invalid_argument(
            "BLOCK-VERSION-FROM is incorrect: current genesis: " + std::to_string(firstBlockID) +
            ", latest reachable block: " + std::to_string(latestBlockID) +
            ", BLOCK-VERSION-FROM: " + std::to_string(block_version_from));
      }
      if (block_version_to < firstBlockID || block_version_to > latestBlockID) {
        throw std::invalid_argument("BLOCK-VERSION-TO is incorrect: current genesis: " + std::to_string(firstBlockID) +
                                    ", latest reachable block: " + std::to_string(latestBlockID) +
                                    ", BLOCK-VERSION-TO: " + std::to_string(block_version_to));
      }
      firstBlockID = block_version_from;
      latestBlockID = block_version_to;
    }
    if (args.values.size() == 1) {
      auto block_version_to = toBlockId(args.values[0]);
      if (block_version_to < firstBlockID || block_version_to > latestBlockID) {
        throw std::invalid_argument("BLOCK-VERSION-TO is incorrect: current genesis: " + std::to_string(firstBlockID) +
                                    ", latest reachable block: " + std::to_string(latestBlockID) +
                                    ", BLOCK-VERSION-TO: " + std::to_string(block_version_to));
      }
      latestBlockID = block_version_to;
    }
    const auto &categories = adapter.blockchainCategories();
    std::map<CATEGORY_TYPE, uint64_t> stale_keys_per_category_type_;
    for (const auto &[cat_id, cat_type] : categories) {
      (void)cat_id;
      stale_keys_per_category_type_.emplace(cat_type, 0);
    }
    for (auto block = firstBlockID; block <= latestBlockID; block++) {
      auto stale_keys = adapter.getBlockStaleKeys(block);
      for (const auto &[cat_id, cat_type] : categories) {
        stale_keys_per_category_type_[cat_type] += stale_keys[cat_id].size();
      }
    }
    std::map<std::string, std::string> out;
    for (auto const &[cat_type, num_of_stale] : stale_keys_per_category_type_) {
      out[cat_type_str.at(cat_type)] = std::to_string(num_of_stale);
    }
    return toJson(out);
  }
};

struct GetValue {
  const bool read_only = true;
  std::string description() const {
    return "getValue CATEGORY HEX-KEY [BLOCK-VERSION]\n"
           "  Gets a value by category, a hex-encoded key and (optionally) a block version.\n"
           "  If no BLOCK-VERSION is passed, the value for the latest one will be returned\n"
           "  (if existing). If the key doesn't exist at BLOCK-VERSION, it will not be returned.";
  }

  std::string execute(const KeyValueBlockchain &adapter, const CommandArguments &args) const {
    if (args.values.size() < 2) {
      throw std::invalid_argument{"Missing CATEGORY and HEX-KEY arguments"};
    }
    const auto &category = args.values[0];
    const auto key = concordUtils::hexToSliver(args.values[1]).toString();
    std::optional<categorization::Value> val;
    if (args.values.size() >= 3) {
      auto requested_block_version = toBlockId(args.values[2]);
      val = adapter.get(category, key, requested_block_version);
    } else {
      val = adapter.getLatest(category, key);
    }
    if (!val) throw NotFoundException{"Couldn't find a value"};

    auto strval = std::visit([](auto &&arg) { return arg.data; }, *val);
    return toJson("value", concordUtils::bufferToHex(strval.data(), strval.size()));
  }
};

struct CompareTo {
  const bool read_only = true;
  std::string description() const {
    return "compareTo PATH-TO-OTHER-DB\n"
           "  Compares the passed DB at PATH-TO-DB to the one in PATH-TO-OTHER-DB.\n"
           "  Returns the ID of the first mismatching block, if such exists, in the overlapping \n"
           "  range found in the two databases. If there is no overlapping range in the \n"
           "  databases, no comparison is made.";
  }

  std::string execute(const KeyValueBlockchain &main_adapter, const CommandArguments &args) const {
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
                                              const KeyValueBlockchain &adapter1,
                                              const KeyValueBlockchain &adapter2) {
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
  const bool read_only = false;
  std::string description() const {
    return "removeMetadata\n"
           "  Removes metadata and state transfer data from RocksDB.";
  }

  std::string execute(KeyValueBlockchain &adapter, const CommandArguments &) const {
    using storage::v2MerkleTree::detail::EDBKeyType;

    static_assert(static_cast<uint8_t>(EDBKeyType::BFT) + 1 == static_cast<uint8_t>(EDBKeyType::Key),
                  "Key has to be after BFT, if not please review this functionality");

    const concordUtils::Sliver begin{std::string{static_cast<char>(EDBKeyType::BFT)}};
    const concordUtils::Sliver end{std::string{static_cast<char>(EDBKeyType::Key)}};

    const auto status = adapter.db()->asIDBClient()->rangeDel(begin, end);
    if (!status.isOK()) {
      throw std::runtime_error{"Failed to delete metadata and state transfer data: " + status.toString()};
    }
    // Once we managed to remove the metadata, we must start a new epoch (which means to add an epoch block)
    uint64_t epoch{0};
    {
      auto value = adapter.getLatest(concord::kvbc::categorization::kConcordInternalCategoryId,
                                     std::string{kvbc::keyTypes::reconfiguration_epoch_key});
      if (value) {
        const auto &data = std::get<categorization::VersionedValue>(*value).data;
        ConcordAssertEQ(data.size(), sizeof(uint64_t));
        epoch = concordUtils::fromBigEndianBuffer<uint64_t>(data.data());
      }
    }
    uint64_t last_executed_sn{0};
    {
      auto value = adapter.getLatest(concord::kvbc::categorization::kConcordInternalCategoryId,
                                     std::string{kvbc::keyTypes::bft_seq_num_key});
      if (value) {
        const auto &data = std::get<categorization::VersionedValue>(*value).data;
        ConcordAssertEQ(data.size(), sizeof(uint64_t));
        last_executed_sn = concordUtils::fromBigEndianBuffer<uint64_t>(data.data());
      }
    }
    epoch += 1;
    std::string epoch_str = concordUtils::toBigEndianStringBuffer(epoch);
    concord::kvbc::categorization::VersionedUpdates ver_updates;
    ver_updates.addUpdate(std::string{kvbc::keyTypes::reconfiguration_epoch_key}, std::move(epoch_str));
    std::string sn_str = concordUtils::toBigEndianStringBuffer(last_executed_sn);
    ver_updates.addUpdate(std::string{kvbc::keyTypes::bft_seq_num_key}, std::move(sn_str));
    concord::kvbc::categorization::Updates updates;
    updates.add(concord::kvbc::categorization::kConcordInternalCategoryId, std::move(ver_updates));
    adapter.addBlock(std::move(updates));
    std::vector<std::pair<std::string, std::string>> out;
    out.push_back({std::string{"result"}, std::string{"true"}});
    out.push_back({std::string{"epoch"}, std::to_string(epoch)});
    return toJson(out);
  }
};

struct GetSTMetadata {
  const bool read_only = true;
  std::string description() const {
    return "getSTMetadata\n"
           "  Shows State Transfer metadata ";
  }

  std::string toJson(const bftEngine::bcst::impl::DataStore::CheckpointDesc &chckpDesc,
                     const bftEngine::bcst::impl::DataStore::ResPagesDescriptor *rpDesc) const {
    std::ostringstream oss;
    oss << "{\"checkpointNum\": " << chckpDesc.checkpointNum << ", \"lastBlock\": " << chckpDesc.lastBlock
        << ", \"digestOfLastBlock\": \"" << chckpDesc.digestOfLastBlock.toString()
        << "\", \"digestOfResPagesDescriptor\": \"" << chckpDesc.digestOfResPagesDescriptor.toString() << "\"";
    if (rpDesc) {
      oss << ", \"reserved_pages\": [";
      for (uint32_t i = 0; i < rpDesc->numOfPages; ++i)
        if (rpDesc->d[i].relevantCheckpoint > 0) {
          oss << "{\"pageId\": " << rpDesc->d[i].pageId << ","
              << "\"relevantCheckpoint\": " << rpDesc->d[i].relevantCheckpoint << ","
              << "\"pageDigest\": \"" << rpDesc->d[i].pageDigest.toString() << "\"}";
          if (i < rpDesc->numOfPages - 1) oss << ", ";
        }
      oss << "]";
    }
    oss << "}";
    return oss.str();
  }

  std::string execute(const KeyValueBlockchain &adapter, const CommandArguments &) const {
    using bftEngine::bcst::impl::DataStore;
    using bftEngine::bcst::impl::DBDataStore;
    using storage::v2MerkleTree::STKeyManipulator;
    std::unique_ptr<DataStore> ds = std::make_unique<DBDataStore>(
        adapter.db()->asIDBClient(), 1024 * 4, std::make_shared<STKeyManipulator>(), true);
    std::map<std::string, std::string> result;
    result["Initialized"] = std::to_string(ds->initialized());
    std::ostringstream oss;
    auto replicas = ds->getReplicas();
    std::copy(replicas.cbegin(), replicas.cend(), std::ostream_iterator<std::uint16_t>(oss, ","));
    result["replicas"] = "[" + oss.str() + std::string("]");
    result["MyReplicaId"] = std::to_string(ds->getMyReplicaId());
    result["fVal"] = std::to_string(ds->getFVal());
    result["MaxNumOfStoredCheckpoints"] = std::to_string(ds->getMaxNumOfStoredCheckpoints());
    result["NumberOfReservedPages"] = std::to_string(ds->getNumberOfReservedPages());
    result["LastStoredCheckpoint"] = std::to_string(ds->getLastStoredCheckpoint());
    result["FirstStoredCheckpoint"] = std::to_string(ds->getFirstStoredCheckpoint());
    for (uint64_t chckp = ds->getFirstStoredCheckpoint(); chckp > 0 && chckp <= ds->getLastStoredCheckpoint(); chckp++)
      if (ds->hasCheckpointDesc(chckp)) {
        result["checkpoint_" + std::to_string(chckp)] =
            GetSTMetadata::toJson(ds->getCheckpointDesc(chckp), ds->getResPagesDescriptor(chckp));
      }
    result["numOfAllPendingResPage"] = std::to_string(ds->numOfAllPendingResPage());
    result["IsFetchingState"] = std::to_string(ds->getIsFetchingState());
    if (ds->hasCheckpointBeingFetched())
      result["CheckpointBeingFetched"] = GetSTMetadata::toJson(ds->getCheckpointBeingFetched(), nullptr);
    result["FirstRequiredBlock"] = std::to_string(ds->getFirstRequiredBlock());
    result["LastRequiredBlock"] = std::to_string(ds->getLastRequiredBlock());

    return concordUtils::toJson(result);
  }
};

struct ResetMetadata {
  const bool read_only = false;
  std::string description() const {
    return "resetMetadata REPLICA_ID\n"
           "  resets BFT and State Transfer metadata for live replica restore"
           "  REPLICA_ID - of the target replica";
  }

  std::string execute(const KeyValueBlockchain &adapter, const CommandArguments &args) const {
    if (args.values.empty()) throw std::invalid_argument{"Missing REPLICA_ID argument"};
    std::uint16_t repId = concord::util::to<std::uint16_t>(args.values.front());

    std::map<std::string, std::string> result;
    // Update/reset ST metadata
    using namespace concord::storage;
    using namespace bftEngine::bcst::impl;
    using storage::v2MerkleTree::STKeyManipulator;
    using storage::v2MerkleTree::MetadataKeyManipulator;
    using bftEngine::MetadataStorage;
    std::unique_ptr<DataStore> ds = std::make_unique<DBDataStore>(
        adapter.db()->asIDBClient(), 1024 * 4, std::make_shared<STKeyManipulator>(), true);

    if (ds->initialized()) {
      DataStoreTransaction::Guard g(ds->beginTransaction());
      g.txn()->setMyReplicaId(repId);
      g.txn()->setFirstRequiredBlock(0);
      g.txn()->setLastRequiredBlock(0);
      g.txn()->setIsFetchingState(false);
      g.txn()->deleteAllPendingPages();
      result["st"] = "replicaId " + std::to_string(repId);
    } else {
      result["st"] = "ST metadata is not initialized - nothing to do.";
    }
    // Update BFT metadata
    // Update sender id to the one of a destination replica in the CheckpointMsg for the last stable sequence number
    std::unique_ptr<MetadataStorage> mdtStorage(
        new DBMetadataStorage(adapter.db()->asIDBClient().get(), std::make_unique<MetadataKeyManipulator>()));
    // in this case n, f and c have no use
    shared_ptr<bftEngine::impl::PersistentStorage> p(new bftEngine::impl::PersistentStorageImp(4, 1, 0));
    uint16_t numOfObjects = 0;
    auto objectDescriptors = ((PersistentStorageImp *)p.get())->getDefaultMetadataObjectDescriptors(numOfObjects);
    bool isNewStorage = mdtStorage->initMaxSizeOfObjects(objectDescriptors.get(), numOfObjects);
    ((PersistentStorageImp *)p.get())->init(move(mdtStorage));
    SeqNum stableSeqNum = p->getLastStableSeqNum();
    CheckpointMsg *cpm = p->getAndAllocateCheckpointMsgInCheckWindow(stableSeqNum);
    result["new bft mdt"] = std::to_string(isNewStorage);
    auto lastExecutedSn = p->getLastExecutedSeqNum();
    p->beginWriteTran();
    if (cpm && cpm->senderId() != repId) {
      cpm->setSenderId(repId);
      p->setCheckpointMsgInCheckWindow(stableSeqNum, cpm);
      result["stable seq num"] = std::to_string(stableSeqNum);
    }
    p->setPrimaryLastUsedSeqNum(lastExecutedSn);
    p->endWriteTran();
    return toJson(result);
  }
};

using Command = std::variant<GetGenesisBlockID,
                             GetLastReachableBlockID,
                             GetLastStateTransferBlockID,
                             GetLastBlockID,
                             GetRawBlock,
                             GetRawBlockRange,
                             GetBlockInfo,
                             GetBlockKeyValues,
                             GetCategories,
                             GetEarliestCategoryUpdates,
                             GetCategoryEarliestStale,
                             GetStaleKeysSummary,
                             GetValue,
                             CompareTo,
                             RemoveMetadata,
                             GetSTMetadata,
                             ResetMetadata,
                             GetBlockRequests,
                             VerifyBlockRequests>;

inline const auto commands_map = std::map<std::string, Command>{
    std::make_pair("getGenesisBlockID", GetGenesisBlockID{}),
    std::make_pair("getLastReachableBlockID", GetLastReachableBlockID{}),
    std::make_pair("getLastStateTransferBlockID", GetLastStateTransferBlockID{}),
    std::make_pair("getLastBlockID", GetLastBlockID{}),
    std::make_pair("getRawBlock", GetRawBlock{}),
    std::make_pair("getRawBlockRange", GetRawBlockRange{}),
    std::make_pair("getBlockInfo", GetBlockInfo{}),
    std::make_pair("getBlockKeyValues", GetBlockKeyValues{}),
    std::make_pair("getCategories", GetCategories{}),
    std::make_pair("getEarliestCategoryUpdates", GetEarliestCategoryUpdates{}),
    std::make_pair("getCategoryEarliestStale", GetCategoryEarliestStale{}),
    std::make_pair("getStaleKeysSummary", GetStaleKeysSummary{}),
    std::make_pair("getValue", GetValue{}),
    std::make_pair("compareTo", CompareTo{}),
    std::make_pair("removeMetadata", RemoveMetadata{}),
    std::make_pair("getSTMetadata", GetSTMetadata{}),
    std::make_pair("resetMetadata", ResetMetadata{}),
    std::make_pair("getBlockRequests", GetBlockRequests{}),
    std::make_pair("verifyBlockRequests", VerifyBlockRequests{}),
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
  ret += "  " + kToolName + " /rocksdb-path getValue merkle 0x0a0b0c\n";
  ret += "  " + kToolName + " /rocksdb-path getValue versioned 0x0a0b0c 42\n";

  return ret;
}

inline int run(const CommandLineArguments &cmd_line_args, std::ostream &out, std::ostream &err) {
#ifdef USE_LOG4CPP
  // Make sure the output is clean
  logging::Logger::getRoot().setLogLevel(log4cplus::WARN_LOG_LEVEL);
#endif
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
    auto read_only = std::visit([](const auto &command) { return command.read_only; }, cmd_it->second);
    auto adapter = getAdapter(cmd_line_args.values[1], read_only);
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

}  // namespace concord::kvbc::tools::db_editor
