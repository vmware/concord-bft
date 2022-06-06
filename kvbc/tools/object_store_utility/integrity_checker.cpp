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

#include "integrity_checker.hpp"
#include "string.hpp"
#include "s3/config_parser.hpp"
#include "bftengine/ReplicaConfig.hpp"
#include "bftengine/ReplicasInfo.hpp"
#include "bftengine/SigManager.hpp"
#include "bftengine/CheckpointInfo.hpp"
#include "bcstatetransfer/BCStateTran.hpp"
#include "direct_kv_storage_factory.h"

namespace concord::kvbc::tools {
using namespace std::placeholders;
using concordUtils::Status;
using bftEngine::bcst::impl::BCStateTran;
using kvbc::v1DirectKeyValue::S3StorageFactory;

void IntegrityChecker::initKeysConfig(const fs::path& keys_file) {
  LOG_DEBUG(logger_, keys_file);
  auto& config = bftEngine::ReplicaConfig::instance();
  concord::util::ConfigFileParser parser(logger_, keys_file.c_str());
  parser.parse();

  config.numReplicas = parser.get_value<std::uint16_t>("num_replicas");
  config.fVal = parser.get_value<std::uint16_t>("f_val");
  config.cVal = parser.get_value<std::uint16_t>("c_val");
  config.publicKeysOfReplicas.clear();
  auto rsaPublicKeys = parser.get_values<std::string>("rsa_public_keys");

  if (rsaPublicKeys.size() < config.numReplicas)
    throw std::runtime_error("number of replicas and number of replicas don't match: " + keys_file.string());

  for (size_t i = 0; i < config.numReplicas; ++i)
    config.publicKeysOfReplicas.insert(std::pair<uint16_t, std::string>(i, rsaPublicKeys[i]));

  config.replicaId = config.numReplicas;  // "my" replica id shouldn't match one of the regular replicas

  repsInfo_ = new ReplicasInfo(config, true, false);

  bftEngine::impl::SigManager::init(config.replicaId,
                                    "", /*private key*/
                                    config.publicKeysOfReplicas,
                                    util::crypto::KeyFormat::HexaDecimalStrippedFormat,
                                    nullptr /*publicKeysOfClients*/,
                                    util::crypto::KeyFormat::PemFormat,
                                    *repsInfo_);
}

void IntegrityChecker::initS3Config(const fs::path& s3_file) {
  const std::string checkpoints_suffix("metadata/checkpoints/");
  auto s3_config = storage::s3::ConfigFileParser(s3_file).parse();
  LOG_DEBUG(logger_, "s3 configuration: " << s3_config);
  s3_dbset_ = std::make_unique<S3StorageFactory>(std::string("not used"), s3_config)->newDatabaseSet();
  checkpoints_prefix_ =
      s3_config.pathPrefix.empty() ? checkpoints_suffix : s3_config.pathPrefix + "/" + checkpoints_suffix;
}

std::pair<BlockId, Digest> IntegrityChecker::getLatestsCheckpointDescriptor() const {
  BlockId lastBlock = s3_dbset_.dbAdapter->getLatestBlockId();
  LOG_INFO(logger_, "Last block: " << lastBlock);
  auto it = dynamic_cast<s3::Client*>(s3_dbset_.metadataDBClient.get())
                ->getIterator<s3::Client::SortByModifiedDescIterator>();
  auto [key, val] = it->seekAtLeast(Sliver::copy(checkpoints_prefix_.data(), checkpoints_prefix_.length()));
  (void)val;
  if (it->isEnd()) throw std::runtime_error("no checkpoints information in S3 storage");

  std::string suff = key.toString().substr(checkpoints_prefix_.length());
  BlockId block = util::to<BlockId>(suff.substr(0, suff.find_last_of('/')));
  LOG_INFO(logger_, "Latest checkpoint descriptor: " << key.toString() << " for block: " << block);
  delete it;
  auto desc = getCheckpointDescriptor(key);
  Digest digest(desc.checkpointMsgs[0]->stateDigest().content());
  return std::make_pair(block, digest);
}

DescriptorOfLastStableCheckpoint IntegrityChecker::getCheckpointDescriptor(const Sliver& key) const {
  LOG_DEBUG(logger_, "key: " << key.toString());
  Sliver checkpoint_descriptor;
  if (Status s = s3_dbset_.metadataDBClient->get(key, checkpoint_descriptor); !s.isOK()) {
    LOG_FATAL(logger_,
              "failed to get checkpoint descriptor for key: " << key.toString() << " status: " << s.toString());
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

std::pair<BlockId, Digest> IntegrityChecker::getCheckpointDescriptor(const BlockId& block_id) const {
  auto it = dynamic_cast<s3::Client*>(s3_dbset_.metadataDBClient.get())
                ->getIterator<s3::Client::SortByModifiedDescIterator>();
  auto key_val = it->seekAtLeast(Sliver::copy(checkpoints_prefix_.data(), checkpoints_prefix_.length()));
  if (it->isEnd()) throw std::runtime_error("no checkpoints information in S3 storage");

  BlockId first_good_block_descriptor = 0;
  Sliver first_good_block_descriptor_key;
  while (!it->isEnd()) {
    std::string suff = key_val.first.toString().substr(checkpoints_prefix_.length());
    BlockId block = util::to<BlockId>(suff.substr(0, suff.find_last_of('/')));
    first_good_block_descriptor_key = key_val.first.clone();
    LOG_DEBUG(logger_, "descriptor key: " << key_val.first.toString() << " blockId: " << block);
    if (block < block_id) break;

    first_good_block_descriptor = block;
    key_val = it->next();
  }
  delete it;
  if (first_good_block_descriptor < block_id)
    throw std::runtime_error("no checkpoints information for block " + std::to_string(block_id));

  auto desc = getCheckpointDescriptor(first_good_block_descriptor_key);
  Digest digest(desc.checkpointMsgs[0]->stateDigest().content());
  return std::make_pair(first_good_block_descriptor, digest);
}

void IntegrityChecker::validateCheckpointDescriptor(const DescriptorOfLastStableCheckpoint& desc) const {
  // no self certificate; static to avoid destruction of CheckpointMsg's during destruction of CheckpointInfo
  static std::map<SeqNum, bftEngine::impl::CheckpointInfo<false>> checkpointsInfo;
  checkpointsInfo.clear();
  for (auto m : desc.checkpointMsgs) {
    m->validate(*repsInfo_);
    LOG_INFO(logger_,
             "Checkpoint message from replica: " << m->idOfGeneratedReplica() << " block:" << m->state()
                                                 << " digest: " << m->stateDigest());
    LOG_DEBUG(logger_,
              KVLOG(m->seqNumber(),
                    m->epochNumber(),
                    m->state(),
                    m->stateDigest(),
                    m->reservedPagesDigest(),
                    m->rvbDataDigest(),
                    m->idOfGeneratedReplica()));
    checkpointsInfo[m->seqNumber()].addCheckpointMsg(m, m->idOfGeneratedReplica());
    if (checkpointsInfo[m->seqNumber()].isCheckpointCertificateComplete()) {
      LOG_INFO(logger_, "Checkpoint descriptor is valid for block " << m->state());
      return;
    }
  }
  throw std::runtime_error("Checkpoint descriptor is not valid for block " +
                           std::to_string(desc.checkpointMsgs[0]->state()));
}

Digest IntegrityChecker::checkBlock(const BlockId& block_id, const Digest& expected_digest) const {
  const auto rawBlock = getBlock(block_id, expected_digest);
  Digest parentBlockDigest;
  static_assert(rawBlock.data.parent_digest.size() == DIGEST_SIZE);
  static_assert(sizeof(Digest) == DIGEST_SIZE);
  memcpy(const_cast<char*>(parentBlockDigest.get()), rawBlock.data.parent_digest.data(), DIGEST_SIZE);
  LOG_DEBUG(logger_, "parent block digest: " << parentBlockDigest.toString());
  return parentBlockDigest;
}
std::pair<Digest, concord::kvbc::categorization::RawBlock> IntegrityChecker::getBlock(const BlockId& block_id) const {
  // get and parse the block
  auto rawBlockSer = s3_dbset_.dbAdapter->getRawBlock(block_id);
  return std::make_pair(computeBlockDigest(block_id, rawBlockSer.string_view()),
                        concord::kvbc::categorization::RawBlock::deserialize(rawBlockSer));
}

concord::kvbc::categorization::RawBlock IntegrityChecker::getBlock(const BlockId& block_id,
                                                                   const Digest& expected_digest) const {
  // get and parse the block
  auto rawBlockSer = s3_dbset_.dbAdapter->getRawBlock(block_id);
  auto calcDigest = computeBlockDigest(block_id, rawBlockSer.string_view());
  if (expected_digest == calcDigest) {
    LOG_INFO(logger_, "block: " << block_id << " digest match: " << expected_digest.toString());
  } else
    throw std::runtime_error("block:" + std::to_string(block_id) + std::string(" expected digest: ") +
                             expected_digest.toString() + std::string(" doesn't match calculated digest: ") +
                             calcDigest.toString());
  return kvbc::categorization::RawBlock::deserialize(rawBlockSer);
}

Digest IntegrityChecker::computeBlockDigest(const BlockId& block_id, const std::string_view block) const {
  Digest calcDigest;
  BCStateTran::computeDigestOfBlock(block_id, block.data(), block.size(), &calcDigest);

  return calcDigest;
}

void IntegrityChecker::validateRange(const BlockId& until_block) const {
  auto [block_id, digest] = getLatestsCheckpointDescriptor();
  for (auto block = block_id; block > until_block; --block) digest = checkBlock(block, digest);
  LOG_INFO(logger_,
           "Successfully validated " << (block_id - until_block) << " blocks: " << block_id << " => " << until_block);
}

void IntegrityChecker::validateKey(const std::string& key) const {
  // Retrieve the latest block number for specified key
  Sliver sKey = Sliver::copy(key.data(), key.length());
  auto [containing_block, _] = s3_dbset_.dbAdapter->getValue(sKey, 0);
  (void)_;
  auto containing_block_id = util::to<BlockId>(containing_block.data());
  LOG_INFO(logger_, "containing_block_id: " << containing_block_id);

  auto [block_id, digest] = getCheckpointDescriptor(containing_block_id);
  for (auto block = block_id; block >= containing_block_id; --block) digest = checkBlock(block, digest);

  // retrieve block and get value
  auto [block_digest, raw_block] = getBlock(containing_block_id);
  (void)block_digest;
  for (auto& [category, value] : raw_block.data.updates.kv) {
    auto cat = category;
    std::visit(
        [cat, key, this](auto&& arg) {
          using T = std::decay_t<decltype(arg)>;
          if constexpr (std::is_same_v<T, kvbc::categorization::BlockMerkleInput>) {
            auto search = arg.kv.find(key);
            if (search == arg.kv.end())
              LOG_DEBUG(logger_, "key [" << key << "] not found in category: " << cat);
            else
              LOG_INFO(logger_, "found key [ " << key << "] value [" << search->second << "] in category " << cat);
          } else if constexpr (std::is_same_v<T, kvbc::categorization::VersionedInput>) {
            auto search = arg.kv.find(key);
            if (search == arg.kv.end())
              LOG_DEBUG(logger_, "key [" << key << "] not found in category: " << cat);
            else
              LOG_INFO(logger_, "found key [ " << key << "] value [" << search->second.data << "] in category " << cat);
          } else if constexpr (std::is_same_v<T, kvbc::categorization::ImmutableInput>) {
            auto search = arg.kv.find(key);
            if (search == arg.kv.end())
              LOG_DEBUG(logger_, "key [" << key << "] not found in category: " << cat);
            else
              LOG_INFO(logger_, "found key [ " << key << "] value [" << search->second.data << "] in category " << cat);
          }
        },
        value);
  }
}

void IntegrityChecker::printBlockContent(const BlockId& block_id,
                                         const kvbc::categorization::RawBlock& raw_block) const {
  LOG_INFO(logger_, "======================= BLOCK " << block_id << " =======================");
  for (auto& [cat, value] : raw_block.data.updates.kv) {
    LOG_INFO(logger_, "category: " << cat);
    std::visit(
        [this](auto&& arg) {
          using T = std::decay_t<decltype(arg)>;
          if constexpr (std::is_same_v<T, kvbc::categorization::BlockMerkleInput>) {
            for (auto& [k, v] : arg.kv) LOG_INFO(logger_, "kv: " << k << " " << v);
            for (auto& v : arg.deletes) LOG_INFO(logger_, "deletes: " << v);
          } else if constexpr (std::is_same_v<T, kvbc::categorization::VersionedInput>) {
            for (auto& [k, v] : arg.kv) LOG_INFO(logger_, "kv: " << k << " " << v.data);
            for (auto& v : arg.deletes) LOG_INFO(logger_, "deletes: " << v);
          } else if constexpr (std::is_same_v<T, kvbc::categorization::ImmutableInput>) {
            for (auto& [k, v] : arg.kv) {
              LOG_INFO(logger_, "kv: " << k << " " << v.data);
              for (auto& t : v.tags) LOG_INFO(logger_, "tags: " << t);
            }
          }
        },
        value);
  }
  LOG_INFO(logger_, "==========================================================");
}

}  // namespace concord::kvbc::tools
