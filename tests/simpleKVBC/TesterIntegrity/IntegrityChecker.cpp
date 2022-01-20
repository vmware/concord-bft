// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "IntegrityChecker.hpp"
#include <getopt.h>
#include "string.hpp"
#include "ReplicaConfig.hpp"
#include "ReplicasInfo.hpp"
#include "config/config_file_parser.hpp"
#include "KeyfileIOUtils.hpp"
#include "SigManager.hpp"
#include "direct_kv_storage_factory.h"
#include "CheckpointInfo.hpp"
#include "bcstatetransfer/BCStateTran.hpp"

namespace concord::tests::integrity {

std::string hex2string(const std::string& s) {
  std::string result;
  result.reserve(s.length() / 2);
  for (size_t i = 0; i < s.length(); i += 2) result.push_back(std::stoi(s.substr(i, 2).c_str(), NULL, 16));
  return result;
}
using concordUtils::Status;
using bftEngine::bcst::BLOCK_DIGEST_SIZE;
using bftEngine::bcst::impl::BCStateTran;
using concord::kvbc::BlockDigest;
using concord::kvbc::v1DirectKeyValue::S3StorageFactory;

IntegrityChecker::IntegrityChecker(int argc, char** argv) { setupParams(argc, argv); }

void IntegrityChecker::setupParams(int argc, char** argv) {
  struct option longOptions[] = {{"keys-file", required_argument, 0, 'k'},
                                 {"s3-config-file", required_argument, 0, '3'},
                                 {"validate-key", required_argument, 0, 'v'},
                                 {"validate-all", no_argument, 0, 'a'},
                                 {"help", no_argument, 0, 'h'},
                                 {0, 0, 0, 0}};
  auto usage = [argv, longOptions]() {
    size_t arrSize = sizeof(longOptions) / sizeof(longOptions[0]) - 1;
    std::cerr << "\nUsage: " << argv[0] << " MANDATORY_OPTIONS OPTION\n"
              << "Checks integrity of a blockchain.\n"
              << "First two arguments are MANDATORY.\n\n";

    for (size_t i = 0; i < arrSize; ++i) {
      option o = longOptions[i];
      std::cerr << "\t-" << (char)o.val << ", --" << std::setw(15) << std::left << o.name << std::setw(10) << std::right
                << ((o.has_arg) ? "argument\n" : "\n");
    }
    std::cerr << std::endl;
  };
  int o = 0;
  int optionIndex = 0;
  LOG_INFO(GL, "Command line options:");
  while ((o = getopt_long(argc, argv, "k:3:v:ah", longOptions, &optionIndex)) != -1) {
    switch (o) {
      case 'k': {
        params_.keys_file_present = true;
        auto& config = bftEngine::ReplicaConfig::instance();
        auto sys = inputReplicaKeyfileMultisig(optarg, config);
        (void)sys;  // currently for ro replica cryptosys is null
        params_.repsInfo = new ReplicasInfo(config, true, false);

        bftEngine::impl::SigManager::init(config.replicaId,
                                          config.replicaPrivateKey,
                                          config.publicKeysOfReplicas,
                                          concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat,
                                          nullptr /*publicKeysOfClients*/,
                                          concord::util::crypto::KeyFormat::PemFormat,
                                          *params_.repsInfo);
      } break;
      case '3': {
        params_.s3_config_present = true;
        auto s3_config = config::S3ConfigFileParser(optarg).parse();
        dbset_ = std::make_unique<S3StorageFactory>(std::string("not used"), s3_config)->newDatabaseSet();
        LOG_INFO(GL, " s3 configuration: " << s3_config);
      } break;
      case 'v': {
        params_.validate_key_present = true;
        params_.key_to_validate = hex2string(std::string(optarg));  // tmp we pass hex key instead of original
      } break;
      case 'a': {
        params_.validate_all_present = true;
      } break;
      case 'h': {
        usage();
        std::exit(1);
      }
      case '?': {
        usage();
        throw std::runtime_error("invalid arguments");
      } break;

      default:
        break;
    }
  }
  if (!params_.complete()) {
    usage();
    throw std::runtime_error("missing required arguments");
  }
}

std::pair<BlockId, STDigest> IntegrityChecker::getLatestsCheckpointDescriptor() const {
  BlockId lastBlock = dbset_.dbAdapter->getLatestBlockId();
  LOG_INFO(GL, "last block: " << lastBlock);
  auto it =
      dynamic_cast<s3::Client*>(dbset_.metadataDBClient.get())->getIterator<s3::Client::SortByModifiedDescIterator>();
  std::string checkpoints_prefix("concord/metadata/checkpoints/");
  auto [key, val] = it->seekAtLeast(Sliver::copy(checkpoints_prefix.data(), checkpoints_prefix.length()));
  if (it->isEnd()) throw std::runtime_error("no checkpoints information in S3 storage");

  std::string suff = key.toString().substr(checkpoints_prefix.length());
  BlockId block = concord::util::to<BlockId>(suff.substr(0, suff.find_last_of('/')));
  LOG_INFO(GL, key.toString() << ": " << val.toString() << " blockId: " << block);
  delete it;
  auto desc = getCheckpointDescriptor(key);
  STDigest digest(desc.checkpointMsgs[0]->digestOfState().content());
  return std::make_pair(block, digest);
}

DescriptorOfLastStableCheckpoint IntegrityChecker::getCheckpointDescriptor(const Sliver& key) const {
  LOG_DEBUG(GL, "key: " << key.toString());
  Sliver checkpoint_descriptor;
  if (Status s = dbset_.metadataDBClient->get(key, checkpoint_descriptor); !s.isOK()) {
    LOG_FATAL(GL, "failed to get checkpoint descriptor for key: " << key.toString() << " status: " << s.toString());
    std::exit(1);
  }
  auto& config = bftEngine::ReplicaConfig::instance();
  DescriptorOfLastStableCheckpoint desc(config.getnumReplicas(), {});
  uint32_t dbDescSize = DescriptorOfLastStableCheckpoint::maxSize(config.getnumReplicas());
  size_t actualSize = 0;
  char* buff = new char[dbDescSize];
  memcpy(buff, checkpoint_descriptor.data(), checkpoint_descriptor.length());
  desc.deserialize(buff, dbDescSize, actualSize);
  validateCheckpointDescriptor(desc);
  return desc;
}

std::pair<BlockId, STDigest> IntegrityChecker::getCheckpointDescriptor(const BlockId& block_id) const {
  auto it =
      dynamic_cast<s3::Client*>(dbset_.metadataDBClient.get())->getIterator<s3::Client::SortByModifiedDescIterator>();
  std::string checkpoints_prefix("concord/metadata/checkpoints/");
  auto key_val = it->seekAtLeast(Sliver::copy(checkpoints_prefix.data(), checkpoints_prefix.length()));
  if (it->isEnd()) throw std::runtime_error("no checkpoints information in S3 storage");

  BlockId first_good_block_descriptor = 0;
  Sliver first_good_block_descriptor_key;
  while (!it->isEnd()) {
    std::string suff = key_val.first.toString().substr(checkpoints_prefix.length());
    BlockId block = concord::util::to<BlockId>(suff.substr(0, suff.find_last_of('/')));
    first_good_block_descriptor_key = key_val.first.clone();
    LOG_DEBUG(GL, "descriptor key: " << key_val.first.toString() << " blockId: " << block);
    if (block < block_id) break;

    first_good_block_descriptor = block;
    key_val = it->next();
  }
  delete it;
  auto desc = getCheckpointDescriptor(first_good_block_descriptor_key);
  STDigest digest(desc.checkpointMsgs[0]->digestOfState().content());
  return std::make_pair(first_good_block_descriptor, digest);
}

void IntegrityChecker::validateCheckpointDescriptor(const DescriptorOfLastStableCheckpoint& desc) const {
  // no self certificate; static to avoid destruction of CheckpointMsg's during destruction of CheckpointInfo
  static std::map<SeqNum, bftEngine::impl::CheckpointInfo<false>> checkpointsInfo;
  checkpointsInfo.clear();
  for (auto m : desc.checkpointMsgs) {
    m->validate(*params_.repsInfo);
    LOG_INFO(GL,
             KVLOG(m->seqNumber(),
                   m->epochNumber(),
                   m->state(),
                   m->digestOfState(),
                   m->otherDigest(),
                   m->idOfGeneratedReplica()));
    checkpointsInfo[m->seqNumber()].addCheckpointMsg(m, m->idOfGeneratedReplica());
    if (checkpointsInfo[m->seqNumber()].isCheckpointCertificateComplete()) {
      LOG_INFO(GL, "Checkpoint descriptor is valid for block " << m->state());
      return;
    }
  }
  throw std::runtime_error("Checkpoint descriptor is not valid for block " +
                           std::to_string(desc.checkpointMsgs[0]->state()));
}

STDigest IntegrityChecker::checkBlock(const BlockId& block_id, const STDigest& expected_digest) const {
  const auto rawBlock = getBlock(block_id, expected_digest);
  STDigest parentBlockDigest;
  static_assert(rawBlock.data.parent_digest.size() == BLOCK_DIGEST_SIZE);
  static_assert(sizeof(Digest) == BLOCK_DIGEST_SIZE);
  memcpy(const_cast<char*>(parentBlockDigest.get()), rawBlock.data.parent_digest.data(), BLOCK_DIGEST_SIZE);
  LOG_DEBUG(GL, "parent block digest: " << parentBlockDigest.toString());
  return parentBlockDigest;
}

concord::kvbc::categorization::RawBlock IntegrityChecker::getBlock(const BlockId& block_id) const {
  // get and parse the block
  auto rawBlockSer = dbset_.dbAdapter->getRawBlock(block_id);
  return concord::kvbc::categorization::RawBlock::deserialize(rawBlockSer);
}

concord::kvbc::categorization::RawBlock IntegrityChecker::getBlock(const BlockId& block_id,
                                                                   const STDigest& expected_digest) const {
  // get and parse the block
  auto rawBlockSer = dbset_.dbAdapter->getRawBlock(block_id);
  STDigest calcDigest;
  BCStateTran::computeDigestOfBlock(
      block_id, reinterpret_cast<const char*>(rawBlockSer.data()), rawBlockSer.size(), &calcDigest);
  if (expected_digest == calcDigest) {
    LOG_INFO(GL, "block: " << block_id << " digest match: " << expected_digest.toString());
  } else
    throw std::runtime_error("block:" + std::to_string(block_id) + std::string(" expected digest: ") +
                             expected_digest.toString() + std::string(" doesn't match calculated digest: ") +
                             calcDigest.toString());
  return concord::kvbc::categorization::RawBlock::deserialize(rawBlockSer);
}

void IntegrityChecker::validateAll() const {
  auto [block_id, digest] = getLatestsCheckpointDescriptor();
  for (auto block = block_id; block > 0; --block) digest = checkBlock(block, digest);
}

void IntegrityChecker::validateKey(const std::string& key) const {
  // Retrieve the latest block number for specified key
  Sliver sKey = Sliver::copy(key.data(), key.length());
  auto [containing_block, _] = dbset_.dbAdapter->getValue(sKey, 0);
  (void)_;
  auto containing_block_id = concord::util::to<BlockId>(containing_block.data());
  LOG_INFO(GL, "containing_block_id: " << containing_block_id);

  auto [block_id, digest] = getCheckpointDescriptor(containing_block_id);
  for (auto block = block_id; block >= containing_block_id; --block) digest = checkBlock(block, digest);

  // retrieve block and get value
  auto raw_block = getBlock(containing_block_id);
  for (auto& [category, value] : raw_block.data.updates.kv) {
    auto cat = category;
    std::visit(
        [cat, key](auto&& arg) {
          using T = std::decay_t<decltype(arg)>;
          if constexpr (std::is_same_v<T, concord::kvbc::categorization::BlockMerkleInput>) {
            auto search = arg.kv.find(key);
            if (search == arg.kv.end())
              LOG_DEBUG(GL, "key [" << key << "] not found in category: " << cat);
            else
              LOG_INFO(GL, "found key [ " << key << "] value [" << search->second << "] in category " << cat);
          } else if constexpr (std::is_same_v<T, concord::kvbc::categorization::VersionedInput>) {
            auto search = arg.kv.find(key);
            if (search == arg.kv.end())
              LOG_DEBUG(GL, "key [" << key << "] not found in category: " << cat);
            else
              LOG_INFO(GL, "found key [ " << key << "] value [" << search->second.data << "] in category " << cat);
          } else if constexpr (std::is_same_v<T, concord::kvbc::categorization::ImmutableInput>) {
            auto search = arg.kv.find(key);
            if (search == arg.kv.end())
              LOG_DEBUG(GL, "key [" << key << "] not found in category: " << cat);
            else
              LOG_INFO(GL, "found key [ " << key << "] value [" << search->second.data << "] in category " << cat);
          }
        },
        value);
  }
  // printBlockContent(containing_block_id, rawBlock);
}

void IntegrityChecker::printBlockContent(const BlockId& block_id,
                                         const concord::kvbc::categorization::RawBlock& raw_block) const {
  LOG_INFO(GL, "======================= BLOCK " << block_id << " =======================");
  for (auto& [cat, value] : raw_block.data.updates.kv) {
    LOG_INFO(GL, "category: " << cat);
    std::visit(
        [](auto&& arg) {
          using T = std::decay_t<decltype(arg)>;
          if constexpr (std::is_same_v<T, concord::kvbc::categorization::BlockMerkleInput>) {
            for (auto& [k, v] : arg.kv) LOG_INFO(GL, "kv: " << k << " " << v);
            for (auto& v : arg.deletes) LOG_INFO(GL, "deletes: " << v);
          } else if constexpr (std::is_same_v<T, concord::kvbc::categorization::VersionedInput>) {
            for (auto& [k, v] : arg.kv) LOG_INFO(GL, "kv: " << k << " " << v.data);
            for (auto& v : arg.deletes) LOG_INFO(GL, "deletes: " << v);
          } else if constexpr (std::is_same_v<T, concord::kvbc::categorization::ImmutableInput>) {
            for (auto& [k, v] : arg.kv) {
              LOG_INFO(GL, "kv: " << k << " " << v.data);
              for (auto& t : v.tags) LOG_INFO(GL, "tags: " << t);
            }
          }
        },
        value);
  }
  LOG_INFO(GL, "==========================================================");
}

}  // namespace concord::tests::integrity
