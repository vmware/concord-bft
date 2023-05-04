// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "kvbc_adapter/categorization/blocks_deleter_adapter.hpp"
#include "util/assertUtils.hpp"

namespace concord::kvbc::adapter::categorization {

BlocksDeleterAdapter::BlocksDeleterAdapter(std::shared_ptr<concord::kvbc::categorization::KeyValueBlockchain> &kvbc,
                                           const std::optional<aux::AdapterAuxTypes> &aux_types)
    : kvbc_{kvbc.get()} {
  ConcordAssertNE(kvbc_, nullptr);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// IBlocksDeleter implementation
void BlocksDeleterAdapter::deleteGenesisBlock() {
  const auto genesisBlock = kvbc_->getGenesisBlockId();
  if (genesisBlock == 0) {
    throw std::logic_error{"Cannot delete the genesis block from an empty blockchain"};
  }
  kvbc_->deleteBlock(genesisBlock);
}

BlockId BlocksDeleterAdapter::deleteBlocksUntil(BlockId until, bool delete_files_in_range) {
  const auto genesisBlock = kvbc_->getGenesisBlockId();
  if (genesisBlock == 0) {
    throw std::logic_error{"Cannot delete a block range from an empty blockchain"};
  } else if (until <= genesisBlock) {
    throw std::invalid_argument{"Invalid 'until' value passed to deleteBlocksUntil()"};
  }

  const auto lastReachableBlock = kvbc_->getLastReachableBlockId();
  const auto lastDeletedBlock = std::min(lastReachableBlock, until - 1);
  const auto start = std::chrono::steady_clock::now();
  for (auto i = genesisBlock; i <= lastDeletedBlock; ++i) {
    ConcordAssert(kvbc_->deleteBlock(i));
  }
  auto jobDuration =
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count();
  histograms_.delete_batch_blocks_duration->record(jobDuration);
  return lastDeletedBlock;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

}  // namespace concord::kvbc::adapter::categorization
