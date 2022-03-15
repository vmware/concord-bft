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

#include <optional>
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

namespace concord::storage::s3 {

using namespace concord::storage;
using concordUtils::Sliver;
using bftEngine::impl::DescriptorOfLastStableCheckpoint;
using concord::kvbc::BlockId;

/** Class for checking blockchain integrity
 *
 */
class IntegrityChecker {
 public:
  IntegrityChecker(int argc, char** argv);

  /** Check integrity with respect to provided options */
  void check() const {
    if (params_.validate_all_present.has_value())
      validateAll();
    else if (params_.validate_key_present.has_value())
      validateKey(params_.key_to_validate);
  }

 protected:
  /** Validate the whole blockchain */
  void validateAll() const;

  /** Validate key and get its value */
  void validateKey(const std::string& key) const;

  /** Parse CLI params */
  void setupParams(int argc, char** argv);

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
   * @return deserialized RawBlock
   */
  concord::kvbc::categorization::RawBlock getBlock(const BlockId&) const;

  /** Get block for block id and validade it against expected digest
   * @return deserialized RawBlock
   */

  concord::kvbc::categorization::RawBlock getBlock(const BlockId&, const Digest& expected_digest) const;

  /** Print block content */
  void printBlockContent(const BlockId&, const concord::kvbc::categorization::RawBlock&) const;

 protected:
  struct Params {
    bool keys_file_present = false;
    bool s3_config_present = false;
    std::optional<bool> validate_all_present;
    std::optional<bool> validate_key_present;

    std::string key_to_validate;
    bftEngine::impl::ReplicasInfo* repsInfo = nullptr;

    bool complete() const {
      return keys_file_present and s3_config_present and
             (validate_all_present.has_value() or validate_key_present.has_value());
    }
  } params_;

  concord::kvbc::IStorageFactory::DatabaseSet dbset_;
  logging::Logger logger_ = logging::getLogger("concord.storage.s3.integrity");
};

}  // namespace concord::storage::s3
