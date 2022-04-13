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
#include <boost/program_options.hpp>

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
namespace po = boost::program_options;
using namespace concord::storage;
using concordUtils::Sliver;
using bftEngine::impl::DescriptorOfLastStableCheckpoint;
using concord::kvbc::BlockId;

/** Class for checking blockchain integrity
 *
 */
class IntegrityChecker {
 public:
  IntegrityChecker();

  /** Parse CLI params */
  void parseCLIArgs(int argc, char** argv);

  /** Check integrity with respect to provided options */
  void check() const;

  po::options_description& getOptions() { return cli_mandatory_options_; }

 protected:
  /** Validate the whole blockchain */
  void validateAll() const;

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
   * @return deserialized RawBlock
   */
  concord::kvbc::categorization::RawBlock getBlock(const BlockId&) const;

  /** Get block for block id and validade it against expected digest
   * @return deserialized RawBlock
   */

  concord::kvbc::categorization::RawBlock getBlock(const BlockId&, const Digest& expected_digest) const;

  /** Print block content */
  void printBlockContent(const BlockId&, const concord::kvbc::categorization::RawBlock&) const;

  void initKeysConfig(const fs::path&);
  void initS3Config(const fs::path&);

 protected:
  bftEngine::impl::ReplicasInfo* repsInfo_ = nullptr;
  concord::kvbc::IStorageFactory::DatabaseSet dbset_;
  std::string checkpoints_prefix_;
  logging::Logger logger_ = logging::getLogger("concord.kvbc.tools.integrity");
  po::variables_map var_map_;
  po::options_description cli_options_{("\nIntegrity check options")};
  po::options_description cli_mandatory_options_{"\nIntegrity check mandatory options"};
  po::options_description cli_actions_{"\nIntegrity check options"};
};

}  // namespace concord::kvbc::tools
