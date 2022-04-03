// Concord
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "categorization/updates.h"
#include "rocksdb/native_client.h"
#include "v4blockchain/detail/st_chain.h"
#include "v4blockchain/detail/latest_keys.h"
#include "v4blockchain/detail/blockchain.h"
#include <memory>
#include <string>

namespace concord::kvbc::v4blockchain {
/*
This class is the entrypoint to storage.
It dispatches all calls to the relevant targets (blockchain,latest keys,state transfer) and glues the flows.
*/
class KeyValueBlockchain {
 public:
  KeyValueBlockchain(const std::shared_ptr<concord::storage::rocksdb::NativeClient>&,
                     bool link_st_chain,
                     const std::optional<std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE>>&
                         category_types = std::nullopt);

  /////////////////////// Add Block ///////////////////////
  BlockId add(categorization::Updates&&);

  //////////////////Garbage collection for Keys that use TimeStamp API///////////////////////////
  // Using the RocksDB timestamp API, means that older user versions are not being deleted
  // On compaction unless they are mark as safe to delete.
  // If we get a new sequnce number it means that the previous sequence nunber was committed and it's
  // safe to trim up to the last block that was added during that sn.
  // An exception is when db checkpoint is being taken where no trimming is allowed.
  uint64_t markHistoryForGarbageCollectionIfNeeded(const categorization::Updates& updates);
  void checkpointInProcess(bool flag) { checkpointInProcess_ = flag; }
  uint64_t getBlockSequenceNumber(const categorization::Updates& updates) const;
  std::optional<uint64_t> getLastBlockSequenceNumber() { return last_block_sn_; }
  void setLastBlockSequenceNumber(uint64_t sn) { last_block_sn_ = sn; }
  // Stats for testing
  uint64_t gc_counter{};

  // In v4 storage in contrast to the categorized storage, pruning does not impact the state i.e. the digest
  // Of the blocks, in order to restrict deviation in the tail we add the genesis at the time the block is added,
  // as part of the block.
  // On state transfer completion this value can be used for pruning.
  void addGenesisBlockKey(categorization::Updates& updates) const;

  const v4blockchain::detail::Blockchain& getBlockchain() const { return block_chain_; };
  const v4blockchain::detail::LatestKeys& getLatestKeys() const { return latest_keys_; };

 private:
  std::shared_ptr<concord::storage::rocksdb::NativeClient> native_client_;
  v4blockchain::detail::Blockchain block_chain_;
  v4blockchain::detail::StChain state_transfer_chain_;
  v4blockchain::detail::LatestKeys latest_keys_;
  // flag to mark whether a checkpoint is being taken.
  std::atomic_bool checkpointInProcess_{false};
  std::optional<uint64_t> last_block_sn_;
};

}  // namespace concord::kvbc::v4blockchain
