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
#pragma once

#include <string_view>
#include <optional>

#include "util/filesystem.hpp"
#include "kv_types.hpp"
#include "Logger.hpp"
#include "s3/client.hpp"
#include "bftengine/PersistentStorage.hpp"
#include "storage_factory_interface.h"
#include "categorization/blocks.h"

namespace bftEngine::impl {
class ReplicasInfo;
}
namespace concord::kvbc {
class IStorageFactory;
}

namespace concord::kvbc::tools {
using namespace concord::storage;
using concordUtils::Sliver;
using bftEngine::impl::DescriptorOfLastStableCheckpoint;
using concord::kvbc::BlockId;

/** Class for checking blockchain integrity
 *
 */
class IntegrityChecker {
 public:
  IntegrityChecker(logging::Logger logger) : logger_(logger) {}

  /** Validate the whole blockchain backwards */
  void validateAll() const { validateRange(0); }

  /** Validate a range of blocks backwards from the last available checkpoint descriptor up to until_block,
   *  i.e. tail => until_block
   */
  void validateRange(const BlockId& until_block) const;

  /** Validate key and get its value */
  void validateKey(const std::string& key) const;

  /** Get latest checkpoint descriptor.
   *  @return BlockId of latest checkpoint descriptor
   *  @return Digest of block in a latest checkpoint descriptor
   */
  std::pair<BlockId, Digest> getLatestsCheckpointDescriptor() const;

  /** Get first checkpoint descriptor after a given block
   *  @return BlockId of checkpoint descriptor
   *  @return Digest of block in a  checkpoint descriptor
   */
  std::pair<BlockId, Digest> getCheckpointDescriptor(const BlockId& block_id) const;

  /** Get checkpoint descriptor for given checkpoint descriptor key */
  DescriptorOfLastStableCheckpoint getCheckpointDescriptor(const Sliver& key) const;

  /** Validate checkpoint descriptor messages signatures */
  void validateCheckpointDescriptor(const DescriptorOfLastStableCheckpoint& desc) const;

  /** 1. Retrieve block;
   *  2. Calculate its digest;
   *  3. Compare with expected digest;
   *  4. Deserialize block, retrieve parent digest.
   *  @return parent digest
   */
  Digest checkBlock(const BlockId& block_id, const Digest& expected_digest) const;

  /** Get block for block id
   * @return block digest, de-serialized RawBlock
   */
  std::pair<Digest, concord::kvbc::categorization::RawBlock> getBlock(const BlockId&) const;

  /** Get block for block id and validade it against expected digest
   * @return deserialized RawBlock
   */
  concord::kvbc::categorization::RawBlock getBlock(const BlockId&, const Digest& expected_digest) const;

  /** Calculate block digest
   * @return block digest
   */
  Digest computeBlockDigest(const BlockId&, const std::string_view block) const;
  /** Print block content */
  void printBlockContent(const BlockId&, const concord::kvbc::categorization::RawBlock&) const;
  void initKeysConfig(const fs::path&);
  void initS3Config(const fs::path&);

 protected:
  bftEngine::impl::ReplicasInfo* repsInfo_ = nullptr;
  concord::kvbc::IStorageFactory::DatabaseSet s3_dbset_;
  std::string checkpoints_prefix_;
  logging::Logger logger_;
};

}  // namespace concord::kvbc::tools
