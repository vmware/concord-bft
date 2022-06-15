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

#include <boost/algorithm/string/predicate.hpp>
#include <rocksdb/table_properties.h>

#include "kvbc_key_types.hpp"
#include "db_editor_common.hpp"
#include "categorization/kv_blockchain.h"
#include "execution_data.cmf.hpp"
#include "keys_and_signatures.cmf.hpp"
#include "concord.cmf.hpp"
#include "db_interfaces.h"
#include "kvbc_key_types.h"
#include "categorization/db_categories.h"
#include "storage/merkle_tree_key_manipulator.h"
#include "bcstatetransfer/DBDataStore.hpp"
#include "bcstatetransfer/SimpleBCStateTransfer.hpp"
#include "bftengine/PersistentStorageImp.hpp"
#include "bftengine/DbMetadataStorage.hpp"
#include "crypto_utils.hpp"
#include "json_output.hpp"
#include "bftengine/ReplicaSpecificInfoManager.hpp"
#include "kvbc_adapter/replica_adapter.hpp"

#include <unordered_map>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#include <cryptopp/dll.h>
#pragma GCC diagnostic pop

namespace concord::kvbc::tools::db_editor {

using namespace categorization;

inline const auto kToolName = "kv_blockchain_db_editor"s;

inline const std::map<CATEGORY_TYPE, std::string> cat_type_str = {
    std::make_pair(CATEGORY_TYPE::block_merkle, "block_merkle"),
    std::make_pair(CATEGORY_TYPE::immutable, "immutable"),
    std::make_pair(CATEGORY_TYPE::versioned_kv, "versioned_kv")};

inline concord::kvbc::adapter::ReplicaBlockchain getAdapter(const std::string &path, const bool read_only = false) {
  auto db = getDBClient(path, read_only);
  auto native = concord::storage::rocksdb::NativeClient::fromIDBClient(db);
  std::optional<std::string> opt_val;
  if (native->hasColumnFamily(v4blockchain::detail::MISC_CF)) {
    opt_val = native->get(v4blockchain::detail::MISC_CF, kvbc::keyTypes::blockchain_version);
  }
  // Make sure we don't link the temporary ST chain as we don't want to change the DB in any way.
  const auto link_temp_st_chain = false;
  if (opt_val && *opt_val == kvbc::V4Version()) {
    bftEngine::ReplicaConfig::instance().kvBlockchainVersion = 4;
  } else {
    bftEngine::ReplicaConfig::instance().kvBlockchainVersion = 1;
  }

  return concord::kvbc::adapter::ReplicaBlockchain(concord::storage::rocksdb::NativeClient::fromIDBClient(db),
                                                   link_temp_st_chain);
}

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
  std::string execute(const concord::kvbc::adapter::ReplicaBlockchain &adapter, const CommandArguments &) const {
    return toJson("genesisBlockID", adapter.getGenesisBlockId());
  }
};

struct GetLastReachableBlockID {
  const bool read_only = true;
  std::string description() const {
    return "getLastReachableBlockID\n"
           "  Returns the last reachable block ID.";
  }

  std::string execute(const concord::kvbc::adapter::ReplicaBlockchain &adapter, const CommandArguments &) const {
    return toJson("lastReachableBlockID", adapter.getLastBlockId());
  }
};

struct GetLastStateTransferBlockID {
  const bool read_only = true;
  std::string description() const {
    return "getLastStateTransferBlockID\n"
           " Returns the last state transfer block ID. If there is no state transfer returns n/a.";
  }

  std::string execute(const concord::kvbc::adapter::ReplicaBlockchain &adapter, const CommandArguments &) const {
    auto last_blockId = adapter.getLastBlockNum();
    auto last_reachable = adapter.getLastBlockId();
    // last_blockId is from the ST chain if it's bigger than the last reachable
    return toJson("lastStateTransferBlockID", last_blockId > last_reachable ? std::to_string(last_blockId) : "n/a"s);
  }
};

struct GetLastBlockID {
  const bool read_only = true;
  std::string description() const {
    return "getLastBlockID\n"
           " Returns the last block ID. Either reachable or state transfer.";
  }

  std::string execute(const concord::kvbc::adapter::ReplicaBlockchain &adapter, const CommandArguments &) const {
    return toJson("lastBlockID", adapter.getLastBlockNum());
  }
};

struct GetRawBlock {
  const bool read_only = true;
  std::string description() const {
    return "getRawBlock BLOCK-ID\n"
           "  Returns a serialized raw block (encoded in hex).";
  }

  std::string execute(const concord::kvbc::adapter::ReplicaBlockchain &adapter, const CommandArguments &args) const {
    if (args.values.empty()) {
      throw std::invalid_argument{"Missing BLOCK-ID argument"};
    }
    const auto &blockId = args.values.front();
    uint32_t size_mb = 30 * 1024 * 1024;  // 30mb
    uint32_t real_size = 0;
    auto buffer = std::string(size_mb, 0);
    adapter.getBlock(toBlockId(blockId), buffer.data(), size_mb, &real_size);
    return toJson("rawBlock",
                  concordUtils::bufferToHex(reinterpret_cast<const std::uint8_t *>(buffer.c_str()), real_size));
  }
};

struct GetRawBlockRange {
  const bool read_only = true;
  std::string description() const {
    return "getRawBlockRange BLOCK-ID-START BLOCK-ID-END\n"
           "  Returns a list of serialized raw blocks (encoded in hex) in the [BLOCK-ID-START, BLOCK-ID-END) range.";
  }

  std::string execute(const concord::kvbc::adapter::ReplicaBlockchain &adapter, const CommandArguments &args) const {
    if (args.values.size() < 2) {
      throw std::invalid_argument{"Missing or invalid block range"};
    }
    const auto end = toBlockId(args.values[1]);
    if (end == 0) {
      throw std::invalid_argument{"Invalid BLOCK-ID-END value"};
    }
    const auto first = toBlockId(args.values[0]);
    const auto last = std::min(end - 1, adapter.getLastBlockNum());
    if (first > last) {
      throw std::invalid_argument{"Invalid block range"};
    }
    auto raw_blocks = std::vector<std::pair<std::string, std::string>>{};
    uint32_t size_mb = 30 * 1024 * 1024;
    uint32_t real_size = 0;
    auto buffer = std::string(size_mb, 0);
    for (auto i = first; i <= last; ++i) {
      adapter.getBlock(i, buffer.data(), size_mb, &real_size);
      raw_blocks.emplace_back(
          "rawBlock" + std::to_string(i),
          concordUtils::bufferToHex(reinterpret_cast<const std::uint8_t *>(buffer.c_str()), real_size));
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

  std::string execute(const concord::kvbc::adapter::ReplicaBlockchain &adapter, const CommandArguments &args) const {
    if (args.values.empty()) {
      throw std::invalid_argument{"Missing BLOCK-ID argument"};
    }
    auto blockId = toBlockId(args.values.front());
    bftEngine::bcst::StateTransferDigest parent_digest;
    auto has_digest = adapter.getPrevDigestFromBlock(blockId, &parent_digest);
    const auto updates = adapter.getBlockUpdates(blockId);
    if (!has_digest) {
      throw NotFoundException{"Couldn't find a block by ID = " + std::to_string(blockId)};
    }
    auto catUpdates = updates->categoryUpdates();
    size_t keyValueTotalCount = 0;
    std::stringstream out;
    out << "{" << std::endl;
    out << "  \"parentBlockDigest\": \"" << concordUtils::bufferToHex(parent_digest.content, DIGEST_SIZE) << "\","
        << std::endl;
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

  std::string execute(const concord::kvbc::adapter::ReplicaBlockchain &adapter, const CommandArguments &args) const {
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

  std::string execute(const concord::kvbc::adapter::ReplicaBlockchain &adapter, const CommandArguments &args) const {
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

  std::string execute(const concord::kvbc::adapter::ReplicaBlockchain &adapter, const CommandArguments &args) const {
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

  std::string execute(const concord::kvbc::adapter::ReplicaBlockchain &adapter, const CommandArguments &args) const {
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
      // E.L
      auto categories = adapter.blockchainCategories();
      for (auto const &c : categories) {
        out[c.first] = cat_type_str.at(c.second);
      }
    }

    return concordUtils::kContainerToJson(out);
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

  std::string execute(const concord::kvbc::adapter::ReplicaBlockchain &adapter, const CommandArguments &args) const {
    if (args.values.empty()) {
      throw NotFoundException{"No Category ID was given"};
    }
    auto latestBlockID = adapter.getLastBlockId();
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
    return concordUtils::kContainerToJson(out);
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
           "  Note that this method performs linear search which may take time on big blockchains."
           "  Note that this method does not take into account stale active keys";
  }

  std::string execute(concord::kvbc::adapter::ReplicaBlockchain &adapter, const CommandArguments &args) const {
    if (args.values.empty()) {
      throw NotFoundException{"No Category ID was given"};
    }
    auto latestBlockID = adapter.getLastBlockId();
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

  std::string execute(concord::kvbc::adapter::ReplicaBlockchain &adapter, const CommandArguments &args) const {
    if (adapter.blockchainVersion() == BLOCKCHAIN_VERSION::V4_BLOCKCHAIN) {
      throw std::invalid_argument{"Operation not valid for v4 blockchain"};
    }
    auto latestBlockID = adapter.getLastBlockId();
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
    std::map<CATEGORY_TYPE, std::set<std::string>> stale_active_keys_per_category_type_;
    for (const auto &[cat_id, cat_type] : categories) {
      (void)cat_id;
      stale_keys_per_category_type_.emplace(cat_type, 0);
    }
    for (auto block = firstBlockID; block <= latestBlockID; block++) {
      auto stale_keys = adapter.getReadOnlyCategorizedBlockchain()->getBlockStaleKeys(block);
      auto stale_active_keys = adapter.getReadOnlyCategorizedBlockchain()->getStaleActiveKeys(block);
      for (const auto &[cat_id, cat_type] : categories) {
        stale_keys_per_category_type_[cat_type] += stale_keys[cat_id].size();
        stale_active_keys_per_category_type_[cat_type].insert(stale_active_keys[cat_id].begin(),
                                                              stale_active_keys[cat_id].end());
      }
    }
    stale_keys_per_category_type_[CATEGORY_TYPE::block_merkle] +=
        stale_active_keys_per_category_type_[CATEGORY_TYPE::block_merkle].size();
    stale_keys_per_category_type_[CATEGORY_TYPE::versioned_kv] +=
        stale_active_keys_per_category_type_[CATEGORY_TYPE::versioned_kv].size();
    std::map<std::string, std::string> out;
    out[cat_type_str.at(CATEGORY_TYPE::block_merkle)] =
        std::to_string(stale_keys_per_category_type_[CATEGORY_TYPE::block_merkle]);
    out[cat_type_str.at(CATEGORY_TYPE::versioned_kv)] =
        std::to_string(stale_keys_per_category_type_[CATEGORY_TYPE::versioned_kv]);
    out[cat_type_str.at(CATEGORY_TYPE::immutable)] =
        std::to_string(stale_keys_per_category_type_[CATEGORY_TYPE::immutable]);
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

  std::string execute(const concord::kvbc::adapter::ReplicaBlockchain &adapter, const CommandArguments &args) const {
    if (args.values.size() < 2) {
      throw std::invalid_argument{"Missing CATEGORY and HEX-KEY arguments"};
    }
    const auto &category = args.values[0];
    const auto key = concordUtils::hexToSliver(args.values[1]).toString();
    std::optional<categorization::Value> val;

    std::unordered_map<std::string, std::string> outputMap;
    if (args.values.size() >= 3) {
      auto requested_block_version = toBlockId(args.values[2]);
      val = adapter.get(category, key, requested_block_version);
    } else {
      val = adapter.getLatest(category, key);
      const auto taggedVersion = adapter.getLatestVersion(category, key);
      if (taggedVersion) {
        auto uint64Version = taggedVersion.value().version;
        outputMap.insert(std::make_pair(std::string("version"), std::to_string(uint64Version)));
      }
    }
    if (!val) throw NotFoundException{"Couldn't find a value"};

    auto strval = std::visit([](auto &&arg) { return arg.data; }, *val);

    outputMap.insert(std::make_pair("value", concordUtils::bufferToHex(strval.data(), strval.size())));

    return concordUtils::kContainerToJson(outputMap);
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

  std::string execute(const concord::kvbc::adapter::ReplicaBlockchain &main_adapter,
                      const CommandArguments &args) const {
    if (args.values.empty()) {
      throw std::invalid_argument{"Missing PATH-TO-OTHER-DB argument"};
    }
    const auto other_adapter = getAdapter(args.values.front(), read_only);

    const auto main_genesis = main_adapter.getGenesisBlockId();
    const auto other_genesis = other_adapter.getGenesisBlockId();
    const auto compared_range_first_block_id = std::max(main_genesis, other_genesis);

    const auto main_last_reachable = main_adapter.getLastBlockId();
    const auto other_last_reachable = other_adapter.getLastBlockId();
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
                                              const concord::kvbc::adapter::ReplicaBlockchain &adapter1,
                                              const concord::kvbc::adapter::ReplicaBlockchain &adapter2) {
    ConcordAssertGT(left, 0);
    ConcordAssertGE(right, left);
    auto mismatch = std::optional<BlockId>{};
    // Exploit the blockchain property - if a block is different, try to search for earlier differences to the left.
    // Otherwise, go right.
    uint32_t size_mb = 30 * 1024 * 1024;  // 30mb
    while (left <= right) {
      auto current = (right - left) / 2 + left;
      uint32_t real_size1 = 0;
      auto buffer1 = std::string(size_mb, 0);
      adapter1.getBlock(current, buffer1.data(), size_mb, &real_size1);
      buffer1.resize(real_size1);
      uint32_t real_size2 = 0;
      auto buffer2 = std::string(size_mb, 0);
      adapter2.getBlock(current, buffer2.data(), size_mb, &real_size2);
      buffer2.resize(real_size2);

      if (buffer1 != buffer2) {
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

  std::string execute(concord::kvbc::adapter::ReplicaBlockchain &adapter, const CommandArguments &) const {
    using storage::v2MerkleTree::detail::EDBKeyType;

    static_assert(static_cast<uint8_t>(EDBKeyType::BFT) + 1 == static_cast<uint8_t>(EDBKeyType::Key),
                  "Key has to be after BFT, if not please review this functionality");

    const concordUtils::Sliver begin{std::string{static_cast<char>(EDBKeyType::BFT)}};
    const concordUtils::Sliver end{std::string{static_cast<char>(EDBKeyType::Key)}};
    // E.L return db client
    const auto status = adapter.asIDBClient()->rangeDel(begin, end);
    if (!status.isOK()) {
      throw std::runtime_error{"Failed to delete metadata and state transfer data: " + status.toString()};
    }
    // For backward compatibility, we add new epoch only if we have the reconfiguration category exist
    auto categories = adapter.blockchainCategories();
    if (categories.find(kConcordReconfigurationCategoryId) == categories.end()) {
      std::vector<std::pair<std::string, std::string>> out;
      out.push_back({std::string{"result"}, std::string{"true"}});
      return toJson(out);
    }
    // Once we managed to remove the metadata, we must start a new epoch (which means to add an epoch block)
    uint64_t epoch{0};
    {
      auto value = adapter.getLatest(concord::kvbc::categorization::kConcordReconfigurationCategoryId,
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
    concord::kvbc::categorization::Updates updates;
    updates.add(concord::kvbc::categorization::kConcordReconfigurationCategoryId, std::move(ver_updates));
    concord::kvbc::categorization::VersionedUpdates sn_updates;
    std::string sn_str = concordUtils::toBigEndianStringBuffer(last_executed_sn);
    sn_updates.addUpdate(std::string{kvbc::keyTypes::bft_seq_num_key}, std::move(sn_str));
    updates.add(concord::kvbc::categorization::kConcordInternalCategoryId, std::move(sn_updates));
    adapter.add(std::move(updates));
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
    oss << "{\"checkpointNum\": " << chckpDesc.checkpointNum << ", \"maxBlockId\": " << chckpDesc.maxBlockId
        << ", \"digestOfMaxBlockId\": \"" << chckpDesc.digestOfMaxBlockId.toString()
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

  std::string execute(concord::kvbc::adapter::ReplicaBlockchain &adapter, const CommandArguments &) const {
    using bftEngine::bcst::impl::DataStore;
    using bftEngine::bcst::impl::DBDataStore;
    using storage::v2MerkleTree::STKeyManipulator;
    std::unique_ptr<DataStore> ds =
        std::make_unique<DBDataStore>(adapter.asIDBClient(), 1024 * 4, std::make_shared<STKeyManipulator>(), true);
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
           "  resets BFT and State Transfer metadata for live replica restore\n"
           "  REPLICA_ID - of the target replica\n"
           "  F - the value of f in the original blockchain\n"
           "  C - the value of c in the original blockchain\n"
           "  principles - the number of principles in the original blockchain.\n"
           "  MAX_CLIENT_BATCH_SIZE - the maximal client's batch size in the original blockchain\n"
           "  Note that if latest 4 parameters is not provided, RSIs won't be removed from the metadata";
  }

  std::string execute(concord::kvbc::adapter::ReplicaBlockchain &adapter, const CommandArguments &args) const {
    if (args.values.empty()) throw std::invalid_argument{"Missing REPLICA_ID argument"};
    std::uint16_t repId = concord::util::to<std::uint16_t>(args.values.front());
    bool removeRsis = args.values.size() == 5;
    uint32_t fVal = removeRsis ? concord::util::to<std::uint32_t>(args.values[1]) : 1;
    uint32_t cVal = removeRsis ? concord::util::to<std::uint32_t>(args.values[2]) : 0;
    uint32_t principles = removeRsis ? concord::util::to<std::uint32_t>(args.values[3]) : 0;
    uint32_t maxClientBatchSize = removeRsis ? concord::util::to<std::uint32_t>(args.values[4]) : 0;
    std::map<std::string, std::string> result;
    // Update/reset ST metadata
    using namespace concord::storage;
    using namespace bftEngine::bcst::impl;
    using storage::v2MerkleTree::STKeyManipulator;
    using storage::v2MerkleTree::MetadataKeyManipulator;
    using bftEngine::MetadataStorage;
    std::unique_ptr<DataStore> ds =
        std::make_unique<DBDataStore>(adapter.asIDBClient(), 1024 * 4, std::make_shared<STKeyManipulator>(), true);

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
        new DBMetadataStorage(adapter.asIDBClient().get(), std::make_unique<MetadataKeyManipulator>()));
    // in this case n, f and c have no use
    uint32_t nVal = removeRsis ? 3 * fVal + 2 * cVal + 1 : 4;
    shared_ptr<bftEngine::impl::PersistentStorage> p(new bftEngine::impl::PersistentStorageImp(
        nVal, fVal, cVal, principles, maxClientBatchSize));  // TODO: add here rsi reset
    uint16_t numOfObjects = 0;
    auto objectDescriptors = ((PersistentStorageImp *)p.get())->getDefaultMetadataObjectDescriptors(numOfObjects);
    bool isNewStorage = mdtStorage->initMaxSizeOfObjects(objectDescriptors, numOfObjects);
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
    if (removeRsis) {
      for (uint32_t principle = 0; principle < nVal + principles; principle++) {
        uint32_t baseIndex = principle * maxClientBatchSize;
        for (uint32_t index = 0; index < maxClientBatchSize; index++) {
          bftEngine::impl::RsiItem rsi_item(0, 0, std::string());
          p->setReplicaSpecificInfo(baseIndex + index, rsi_item.serialize());
        }
      }
    }
    p->endWriteTran();
    return toJson(result);
  }
};

struct ListColumnFamilies {
  const bool read_only = true;
  std::string description() const {
    return "listColumnFamilies\n"
           " List the names of all column families in RocksDB";
  }

  std::string toJson(const std::unordered_set<std::string> &result) const {
    std::ostringstream oss;
    oss << "{\n  \"column_families\":[";
    if (!result.empty()) {
      auto it = result.begin();
      oss << "\"" << *it << "\"";
      while (++it != result.end()) {
        oss << ",\n\"" << *it << "\"";
      }
    }
    oss << "]\n}";
    return oss.str();
  }

  std::string execute(const concord::kvbc::adapter::ReplicaBlockchain &adapter, const CommandArguments &args) const {
    auto result = adapter.columnFamilies();
    return toJson(result);
  }
};

struct GetColumnFamilyStats {
  const bool read_only = true;
  const std::string *properties[3] = {&rocksdb::DB::Properties::kTotalSstFilesSize,
                                      &rocksdb::DB::Properties::kEstimateNumKeys,
                                      &rocksdb::DB::Properties::kNumEntriesActiveMemTable};
  std::string description() const {
    std::ostringstream oss;
    oss << "GetColumnStats optional:COLUMN_FAMILY_NAME\n"
        << " gets following column family stats:\n";
    for (const auto p : properties) {
      oss << *p << ",\n";
    }
    return oss.str();
  }

  std::string execute(const concord::kvbc::adapter::ReplicaBlockchain &adapter, const CommandArguments &args) const {
    std::unordered_set<std::string> columnFamilies;
    if (args.values.size() > 0) {
      for (const auto &s : args.values) {
        columnFamilies.insert(s);
      }
    } else {
      columnFamilies = adapter.columnFamilies();
    }

    std::ostringstream oss;

    oss << "{";

    for (auto columnFamily : columnFamilies) {
      oss << "\n\t\"" << columnFamily << "\" : {";
      uint64_t v;
      for (auto property : properties) {
        adapter.getCFProperty(columnFamily, *property, &v);
        oss << "\n\t\t\"" << *property << "\" : " << v << ",";
      }

      oss << "\n\t},";
    }
    oss << "\n}";
    return oss.str();
  }
};

struct VerifyDbCheckpoint {
  using CheckPointMsgStatus = std::vector<std::pair<const CheckpointMsg &, bool>>;
  using CheckpointDesc = bftEngine::bcst::impl::DataStore::CheckpointDesc;
  using BlockHashData = std::tuple<uint64_t, BlockDigest, BlockDigest>;  //<blockId, parentHash, blockHash>
  using IVerifier = concord::util::crypto::IVerifier;
  using RSAVerifier = concord::util::crypto::RSAVerifier;
  using KeyFormat = concord::util::crypto::KeyFormat;
  using ReplicaId = uint16_t;
  const bool read_only = true;
  std::string description() const {
    std::ostringstream oss;
    oss << "verifyDbCheckpoint\n"
        << " verifies block digest added on lastStable checkpoint against the digest "
        << " recorded with checkpoint descriptor and the digest from (2f+1) checkpoint messages \n"
        << " optionally it verifies N number of blocks from the block added"
        << " on last stable checkpoint in reverse order"
        << " and optionally verifies signature of bft-checkpoint messages\n"
        << " N - Number of additional blocks to verify from the block added on last stable checkpoint\n"
        << " Usage: verifyDbCheckpoint [N] [true/false]\n";
    return oss.str();
  }
  std::string toString(const CheckPointMsgStatus &statusList) const {
    std::ostringstream os;
    os << "{"
       << "\n";
    for (const auto &s : statusList) {
      auto &cp = s.first;
      os << "  \"ReplicaId\": " << cp.idOfGeneratedReplica() << ", \"seqNum\": " << cp.seqNumber()
         << " \"blockId\": " << cp.state() << " \"blockDigest\": " << cp.stateDigest()
         << ", \"verified\": " << std::boolalpha << s.second << "\n";
    }
    os << "  }";
    return os.str();
  }

  std::map<ReplicaId, std::unique_ptr<IVerifier>> getVerifiers(
      std::set<ReplicaId> replicas, const concord::kvbc::adapter::ReplicaBlockchain &adapter) const {
    auto category_id = concord::kvbc::categorization::kConcordReconfigurationCategoryId;
    auto key_prefix = std::string{kvbc::keyTypes::reconfiguration_rep_main_key};
    std::map<ReplicaId, unique_ptr<IVerifier>> replica_keys;
    for (auto repId : replicas) {
      auto key = key_prefix + std::to_string(repId);
      auto val = adapter.getLatest(category_id, key);
      if (val.has_value()) {
        std::visit(
            [&](auto &&arg) {
              auto strval = arg.data;
              std::vector<uint8_t> data_buf(strval.begin(), strval.end());
              concord::messages::ReplicaMainKeyUpdate cmd;
              concord::messages::deserialize(data_buf, cmd);
              auto format = cmd.format;
              transform(format.begin(), format.end(), format.begin(), ::tolower);
              auto key_format = ((format == "hex") ? KeyFormat::HexaDecimalStrippedFormat : KeyFormat::PemFormat);
              replica_keys.emplace(repId, std::make_unique<RSAVerifier>(cmd.key, key_format));
            },
            *val);
      }
    }
    return replica_keys;
  }

  bool verifySig(const char *data, uint32_t data_len, const char *sig, uint32_t sig_len, IVerifier *verifier) const {
    if (verifier) {
      auto signature_len = verifier->signatureLength();
      if (signature_len == sig_len) {
        std::string _data(data, data_len);
        std::string _sig(sig, sig_len);
        return verifier->verify(_data, _sig);
      }
    }
    return false;
  }

  bool verify(const CheckpointMsg &msg,
              const CheckpointDesc &desc,
              bool verifySignature,
              const std::map<ReplicaId, std::unique_ptr<IVerifier>> &verifiers) const {
    auto is_digest_valid = (!desc.digestOfMaxBlockId.isZero() && (msg.stateDigest() == desc.digestOfMaxBlockId) &&
                            (msg.state() == desc.maxBlockId));
    auto is_check_point_signature_valid{true};
    if (verifySignature) {
      is_check_point_signature_valid = false;
      if (auto it = verifiers.find(msg.idOfGeneratedReplica()); it != verifiers.end()) {
        auto verifier = (it->second).get();
        if (verifier) {
          is_check_point_signature_valid = verifySig(msg.body(),
                                                     msg.getHeaderLen(),
                                                     msg.body() + msg.getHeaderLen() + msg.spanContextSize(),
                                                     msg.size() - msg.getHeaderLen() - msg.spanContextSize(),
                                                     verifier);
        }
      }
    }
    return is_digest_valid && is_check_point_signature_valid;
  }

  BlockDigest getBlockDigest(const concord::kvbc::adapter::ReplicaBlockchain &adapter, const uint64_t &blockId) const {
    using bftEngine::bcst::computeBlockDigest;
    uint32_t size_mb = 30 * 1024 * 1024;  // 30mb
    uint32_t real_size = 0;
    auto buffer = std::string(size_mb, 0);
    adapter.getBlock(blockId, buffer.data(), size_mb, &real_size);
    buffer.resize(real_size);

    return computeBlockDigest(blockId, buffer.c_str(), buffer.size());
  }

  bool verifyBlockChain(const concord::kvbc::adapter::ReplicaBlockchain &adapter,
                        const uint64_t startBlockId,
                        const uint64_t lastBlockId) const {
    using namespace bftEngine::bcst::impl;
    ConcordAssert(lastBlockId <= adapter.getLastBlockId());
    auto const &numOfThreads = thread::hardware_concurrency();
    auto blockHashData = std::vector<std::future<BlockHashData>>{};
    blockHashData.reserve(numOfThreads);
    auto currBlockId = lastBlockId;
    auto parentHash = getBlockDigest(adapter, lastBlockId);
    while (currBlockId > startBlockId) {
      auto count = std::min(static_cast<uint64_t>(numOfThreads), (currBlockId - startBlockId));
      auto blockId = currBlockId;
      for (auto i = 0u; i < count; blockId--, i++) {
        blockHashData.push_back(std::async([&, blockId]() -> BlockHashData {
          const auto &blockDigest = getBlockDigest(adapter, blockId);
          bftEngine::bcst::StateTransferDigest parent_digest;
          auto has_digest = adapter.getPrevDigestFromBlock(blockId, &parent_digest);
          ConcordAssert(has_digest);
          BlockDigest parentBlockDigest;
          std::copy(parent_digest.content, parent_digest.content + DIGEST_SIZE, parentBlockDigest.begin());
          return std::make_tuple(blockId, parentBlockDigest, blockDigest);
        }));
      }
      for (auto it = blockHashData.begin(); it != blockHashData.end(); it++) {
        const auto &futureObj = it->get();
        const auto &computedHash = get<2>(futureObj);
        if (parentHash != computedHash) return false;
        parentHash = get<1>(futureObj);
      }
      currBlockId -= count;
      blockHashData.clear();
    }
    return true;
  }

  std::string execute(const concord::kvbc::adapter::ReplicaBlockchain &adapter, const CommandArguments &args) const {
    std::map<std::string, std::string> result;
    using namespace concord::storage;
    using namespace bftEngine::bcst::impl;
    using storage::v2MerkleTree::STKeyManipulator;
    using storage::v2MerkleTree::MetadataKeyManipulator;
    using bftEngine::MetadataStorage;
    uint64_t numOfBlocksToVerify{0};
    auto verifyCheckpointMsgSignature{false};
    if (!args.values.empty()) {
      numOfBlocksToVerify = toBlockId(args.values.front());
      if (args.values.size() == 2) {
        auto verify_sig = args.values.back();
        transform(verify_sig.begin(), verify_sig.end(), verify_sig.begin(), ::tolower);
        verifyCheckpointMsgSignature = (verify_sig == "true");
      }
    }
    std::unique_ptr<DataStore> ds =
        std::make_unique<DBDataStore>(adapter.asIDBClient(), 1024 * 4, std::make_shared<STKeyManipulator>(), true);
    result["MyReplicaId"] = std::to_string(ds->getMyReplicaId());
    auto replicas = ds->getReplicas();
    auto verifiers = getVerifiers(replicas, adapter);
    const auto &f = ds->getFVal();
    result["LastStoredCheckpoint"] = std::to_string(ds->getLastStoredCheckpoint());
    auto chckp = ds->getLastStoredCheckpoint();
    CheckpointDesc checkPtDesc;
    if (ds->hasCheckpointDesc(chckp)) {
      checkPtDesc = ds->getCheckpointDesc(chckp);
      result["LastStoredCheckpointBlockId"] = std::to_string(checkPtDesc.maxBlockId);
      auto computedDigest = getBlockDigest(adapter, checkPtDesc.maxBlockId);
      result["calculatedBlockHash"] = concordUtils::bufferToHex(computedDigest.data(), computedDigest.size());
      if (computedDigest.size() != sizeof(checkPtDesc.digestOfMaxBlockId) ||
          (std::memcmp(computedDigest.data(), checkPtDesc.digestOfMaxBlockId.get(), computedDigest.size()))) {
        result["lastBlockVerification"] = "Fail";
        return concordUtils::toJson(result);
      }
    } else {
      result["lastBlockVerification"] = "Fail";
      return concordUtils::toJson(result);
    }
    result["lastBlockVerification"] = "Ok";
    std::unique_ptr<MetadataStorage> mdtStorage(
        new DBMetadataStorage(adapter.asIDBClient().get(), std::make_unique<MetadataKeyManipulator>()));
    shared_ptr<bftEngine::impl::PersistentStorage> p(new bftEngine::impl::PersistentStorageImp(4, 1, 0, 0, 0));
    uint16_t numOfObjects = 0;
    auto objectDescriptors = ((PersistentStorageImp *)p.get())->getDefaultMetadataObjectDescriptors(numOfObjects);
    mdtStorage->initMaxSizeOfObjects(objectDescriptors, numOfObjects);
    ((PersistentStorageImp *)p.get())->init(move(mdtStorage));
    const auto &desc = p->getDescriptorOfLastStableCheckpoint();
    CheckPointMsgStatus status;
    for (const auto &cp : desc.checkpointMsgs) {
      if (cp) status.push_back({*cp, verify(*cp, checkPtDesc, verifyCheckpointMsgSignature, verifiers)});
    }
    result["LastStableCheckpointMsgs"] = toString(status);
    const auto &numOfValidCheckPtMsgs =
        count_if(status.begin(), status.end(), [](const auto &item) { return (item.second == true); });
    if (numOfValidCheckPtMsgs < (f + 1)) {
      result["CheckpointMsgsVerification"] = "Fail";
      return concordUtils::toJson(result);
    }
    result["fVal"] = std::to_string(f);
    result["ValidCheckpointMsgsCount"] = std::to_string(numOfValidCheckPtMsgs);
    if (numOfBlocksToVerify) {
      const auto &gensisBlockId = adapter.getGenesisBlockId();
      numOfBlocksToVerify = std::min(numOfBlocksToVerify, (checkPtDesc.maxBlockId - gensisBlockId));
      result["BlockChainVerificationStatus"] =
          verifyBlockChain(adapter, (checkPtDesc.maxBlockId - numOfBlocksToVerify - 1), checkPtDesc.maxBlockId)
              ? "Ok"
              : "Fail";
    }
    result["NumOfBlocksVerifiedFromLastStableCheckPtBlock"] = std::to_string(numOfBlocksToVerify);
    return concordUtils::toJson(result);
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
                             VerifyBlockRequests,
                             ListColumnFamilies,
                             GetColumnFamilyStats,
                             VerifyDbCheckpoint>;

inline const auto commands_map =
    std::map<std::string, Command>{std::make_pair("getGenesisBlockID", GetGenesisBlockID{}),
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
                                   std::make_pair("listColumnFamilies", ListColumnFamilies{}),
                                   std::make_pair("getColumnFamilyStats", GetColumnFamilyStats{}),
                                   std::make_pair("verifyDbCheckpoint", VerifyDbCheckpoint{})};

inline std::string usage() {
  std::string ret;
  try {
    ret = "Usage: " + kToolName + " PATH-TO-DB COMMAND [ARGUMENTS]...\n\n";
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

  } catch (const std::bad_variant_access &e) {
    ret = "Bad variant access exception occurred: " + std::string(e.what()) + '\n';
  } catch (const std::exception &e) {
    ret = "Exception occurred: " + std::string(e.what()) + '\n';
  }
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