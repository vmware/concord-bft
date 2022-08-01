// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
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

// #include "blocks.h"
#include <unordered_set>
#include <unordered_map>
#include <atomic>
#include <limits>
#include "rocksdb/native_client.h"
#include <memory>
#include "kv_types.hpp"
#include "v4blockchain/detail/blocks.h"
#include "categorization/updates.h"
#include "endianness.hpp"
#include "kv_types.hpp"
#include "thread_pool.hpp"
#include "v4blockchain/detail/column_families.h"

namespace concord::kvbc::v4blockchain::detail {
/*
  This class composes the blockchain out of detail::Block.
  It knows to :
  - add block to storage.
  - read block from storage.
  - maintain the state of the genesis and last reachable blocks.
*/
class Blockchain {
 public:
  static constexpr auto MAX_BLOCK_ID = std::numeric_limits<BlockId>::max();
  static constexpr auto INVALID_BLOCK_ID = BlockId{0};

  // creates the blockchain column family if it does not exists and loads the last and genesis block ids.
  Blockchain(const std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client);
  ///////////////////ADD////////////////////////////////////////
  // construct a new block from the input updates and links it to the previous block by storing the last block digest.
  BlockId addBlock(const concord::kvbc::categorization::Updates&, storage::rocksdb::NativeWriteBatch&);
  BlockId addBlock(v4blockchain::detail::Block& block, storage::rocksdb::NativeWriteBatch&);
  //////////////////DELETE//////////////////////////////////////
  // Delete up to until not including until if until is within last reachable block,
  // else delete up to last reachable block and not including last reachable block.
  // Do nothing of last reachable block is same as the genesis block.
  BlockId deleteBlocksUntil(BlockId until);
  void deleteGenesisBlock();
  void deleteLastReachableBlock(storage::rocksdb::NativeWriteBatch&);
  ///////////////////State Transfer/////////////////////////////////
  bool hasBlock(BlockId) const;
  ///////////////////////////////////////////////////////////////
  // Loads from storage the last and first block ids respectivly.
  std::optional<BlockId> loadLastReachableBlockId();
  std::optional<BlockId> loadGenesisBlockId();
  void setLastReachable(BlockId id) { last_reachable_block_id_ = id; }
  void setBlockId(BlockId id);
  BlockId getLastReachable() const { return last_reachable_block_id_; }
  BlockId getGenesisBlockId() const { return genesis_block_id_; }
  void setGenesisBlockId(BlockId id) {
    genesis_block_id_ = id;
    global_genesis_block_id = id;
  }

  // Returns the buffer that represents the block
  std::optional<std::string> getBlockData(concord::kvbc::BlockId id) const;
  std::optional<categorization::Updates> getBlockUpdates(BlockId id) const;

  concord::util::digest::BlockDigest getBlockParentDigest(concord::kvbc::BlockId id) const;

  // Returns the actual values from blockchain DB for each of the block ids.
  // This function expects unique blocks in block_ids.
  // values is a result argument which will contain all the values for each block
  // Order of blocks in block_ids is different from the order of blocks in the values
  // block_ids.size() >= values.size() after the execution of this function.
  void multiGetBlockData(const std::vector<BlockId>& block_ids,
                         std::unordered_map<BlockId, std::optional<std::string>>& values) const;

  // Returns the actual values from blockchain DB in the form of update operations
  // for each of the block ids.
  // This block id in block_ids can be non-unique.
  // values is a result argument which will contain all the values for each block
  // Order of blocks in block_ids is different from the order of blocks in the values
  // block_ids.size() >= values.size() after the execution of this function.
  void multiGetBlockUpdates(std::vector<BlockId> block_ids,
                            std::unordered_map<BlockId, std::optional<categorization::Updates>>& values) const;

  concord::util::digest::BlockDigest calculateBlockDigest(concord::kvbc::BlockId id) const;

  // Generates a key (big endian string representation) from the block id.
  static std::string generateKey(BlockId id) { return concordUtils::toBigEndianStringBuffer(id); }

  // Non copyable and moveable
  Blockchain(const Blockchain&) = delete;
  Blockchain(Blockchain&&) = delete;
  Blockchain& operator=(const Blockchain&) = delete;
  Blockchain& operator=(Blockchain&&) = delete;

  // stats for tests
  uint64_t from_future{};
  uint64_t from_storage{};
  static std::atomic<BlockId> global_genesis_block_id;
  void deleteBlock(BlockId id, storage::rocksdb::NativeWriteBatch& wb) {
    ConcordAssertLE(id, last_reachable_block_id_);
    ConcordAssertGE(id, genesis_block_id_);
    wb.del(v4blockchain::detail::BLOCKS_CF, generateKey(id));
  }

 private:
  std::atomic<BlockId> last_reachable_block_id_{INVALID_BLOCK_ID};
  std::atomic<BlockId> genesis_block_id_{INVALID_BLOCK_ID};
  std::shared_ptr<concord::storage::rocksdb::NativeClient> native_client_;
  util::ThreadPool thread_pool_{1};
  std::optional<std::future<BlockDigest>> future_digest_;
};

}  // namespace concord::kvbc::v4blockchain::detail
