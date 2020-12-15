// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
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

#include "blocks.h"

namespace concord::kvbc::categorization::detail {

// This class exposes an API for interaction with the blockchain and the state transfer blockchain.
class Blockchain {
 public:
  static constexpr auto MAX_BLOCK_ID = std::numeric_limits<BlockId>::max();
  static constexpr auto INITIAL_GENESIS_BLOCK_ID = BlockId{1};

  Blockchain(const std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client);

  /////////////////////// Last/genesis block IDs operations ///////////////////////
  // Last reachable
  std::optional<BlockId> loadLastReachableBlockId();
  void setLastReachableBlockId(const BlockId id) { last_reachable_block_id_ = id; }
  BlockId getLastReachableBlockId() const { return last_reachable_block_id_; }

  // Genesis
  std::optional<BlockId> loadGenesisBlockId();
  void setGenesisBlockId(const BlockId id) { genesis_block_id_ = id; }
  BlockId getGenesisBlockId() const { return genesis_block_id_; }

  // both
  void setAddedBlockId(const BlockId last) {
    setLastReachableBlockId(last);
    // We don't allow deletion of the genesis block if it is the only one left in the system. We do allow deleting it as
    // last reachable, though, to support replica state sync. Therefore, if we couldn't load the genesis block ID on
    // startup, it means there are no blocks in storage and we are now adding the first one. Make sure we set the
    // genesis block ID cache to reflect that.
    if (genesis_block_id_ == 0) {
      genesis_block_id_ = INITIAL_GENESIS_BLOCK_ID;
    }
  }

  void addBlock(const Block& new_block, storage::rocksdb::NativeWriteBatch& wb) {
    wb.put(detail::BLOCKS_CF, Block::generateKey(new_block.id()), Block::serialize(new_block));
  }

  void deleteBlock(const BlockId id, storage::rocksdb::NativeWriteBatch& wb) {
    wb.del(detail::BLOCKS_CF, Block::generateKey(id));
  }

  std::optional<Block> getBlock(const BlockId block_id) {
    auto block_ser = native_client_->get(detail::BLOCKS_CF, Block::generateKey(block_id));
    if (!block_ser) {
      return std::optional<Block>{};
    }
    return Block::deserialize(*block_ser);
  }

  /////////////////////// State transfer Block chain ///////////////////////
  class StateTransfer {
   public:
    StateTransfer(const std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client)
        : native_client_{native_client} {
      loadLastBlockId();
    }

    void deleteBlock(const BlockId id, storage::rocksdb::NativeWriteBatch& wb) {
      auto db_key = Block::generateKey(id);
      wb.del(detail::ST_CHAIN_CF, db_key);
      // If we deleted the latest block reset it
      if (*latestSTTempBlockId_ == id) {
        // E.L log
        latestSTTempBlockId_.reset();
      }
      return;
    }

    void loadLastBlockId() {
      auto itr = native_client_->getIterator(detail::ST_CHAIN_CF);
      auto max_db_key = Block::generateKey(MAX_BLOCK_ID);
      itr.seekAtMost(max_db_key);
      if (!itr) {
        latestSTTempBlockId_.reset();
        return;
      }
      BlockKey key{};
      detail::deserialize(itr.keyView(), key);
      latestSTTempBlockId_ = key.block_id;
    }

    std::optional<BlockId> getLastBlockId() const { return latestSTTempBlockId_; }

   private:
    std::optional<BlockId> latestSTTempBlockId_;
    std::shared_ptr<concord::storage::rocksdb::NativeClient> native_client_;
  };

  BlockId getLatestBlockId(const Blockchain::StateTransfer& st_chain) const {
    if (st_chain.getLastBlockId().has_value()) {
      return st_chain.getLastBlockId().value();
    }
    return getLastReachableBlockId();
  }

 private:
  BlockId last_reachable_block_id_{0};
  BlockId genesis_block_id_{0};
  std::shared_ptr<concord::storage::rocksdb::NativeClient> native_client_;
};

}  // namespace concord::kvbc::categorization::detail
