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
  static constexpr auto INVALID_BLOCK_ID = BlockId{0};

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

  std::optional<Block> getBlock(const BlockId block_id) const {
    auto block_ser = native_client_->get(detail::BLOCKS_CF, Block::generateKey(block_id));
    if (!block_ser) {
      return std::optional<Block>{};
    }
    return Block::deserialize(block_ser.value());
  }

  std::optional<RawBlock> getRawBlock(const BlockId block_id, const CategoriesMap& categorires) const {
    auto block = getBlock(block_id);
    if (!block) {
      return std::optional<RawBlock>{};
    }
    return RawBlock(block.value(), native_client_, categorires);
  }

  std::optional<Hash> parentDigest(BlockId block_id) const {
    const auto block_ser = native_client_->getSlice(detail::BLOCKS_CF, Block::generateKey(block_id));
    if (!block_ser) {
      return std::nullopt;
    }
    auto digest = ParentDigest{};
    deserialize(*block_ser, digest);
    return digest.value;
  }

  bool hasBlock(BlockId block_id) const {
    return native_client_->getSlice(detail::BLOCKS_CF, Block::generateKey(block_id)).has_value();
  }

  /////////////////////// State transfer Block chain ///////////////////////
  class StateTransfer {
   public:
    StateTransfer(const std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client)
        : native_client_{native_client} {
      if (detail::createColumnFamilyIfNotExisting(detail::ST_CHAIN_CF, *native_client_.get())) {
        LOG_INFO(CAT_BLOCK_LOG,
                 "Created [" << detail::ST_CHAIN_CF << "] column family for the state transfer blockchain");
      }
      loadLastBlockId();
      if (last_block_id_ > 0) {
        LOG_INFO(CAT_BLOCK_LOG, "State transfer last block id: " << last_block_id_);
      }
    }

    void deleteBlock(const BlockId id, storage::rocksdb::NativeWriteBatch& wb) {
      wb.del(detail::ST_CHAIN_CF, Block::generateKey(id));
    }

    void updateLastIdAfterDeletion(const BlockId id) {
      if (last_block_id_ == 0 || last_block_id_ != id) {
        return;
      }
      last_block_id_ = 0;
      loadLastBlockId();
      return;
    }

    void loadLastBlockId() {
      auto itr = native_client_->getIterator(detail::ST_CHAIN_CF);
      auto max_db_key = Block::generateKey(MAX_BLOCK_ID);
      itr.seekAtMost(max_db_key);
      if (!itr) {
        last_block_id_ = 0;
        return;
      }
      BlockKey key{};
      detail::deserialize(itr.keyView(), key);
      last_block_id_ = key.block_id;
    }

    BlockId getLastBlockId() const { return last_block_id_; }

    void addBlock(const BlockId id, const RawBlock& block, storage::rocksdb::NativeWriteBatch& wb) const {
      wb.put(detail::ST_CHAIN_CF, Block::generateKey(id), RawBlock::serialize(block));
    }

    std::optional<RawBlock> getRawBlock(const BlockId block_id) const {
      auto raw_block_ser = native_client_->get(detail::ST_CHAIN_CF, Block::generateKey(block_id));
      if (!raw_block_ser.has_value()) {
        return std::optional<RawBlock>{};
      }
      return RawBlock::deserialize(raw_block_ser.value());
    }

    std::optional<Hash> parentDigest(BlockId block_id) const {
      const auto raw_block_ser = native_client_->getSlice(detail::ST_CHAIN_CF, Block::generateKey(block_id));
      if (!raw_block_ser) {
        return std::nullopt;
      }
      auto digest = ParentDigest{};
      deserialize(*raw_block_ser, digest);
      return digest.value;
    }

    bool hasBlock(BlockId block_id) const {
      return native_client_->getSlice(detail::ST_CHAIN_CF, Block::generateKey(block_id)).has_value();
    }

    void updateLastId(const BlockId id) {
      std::lock_guard<std::mutex> l(update_last_block_id_mutex_);
      if (last_block_id_ >= id) {
        return;
      }
      last_block_id_ = id;
    }

    void resetChain() { last_block_id_ = 0; }

   private:
    // if last_block_id_ is 0 it means no ST chain
    std::atomic<BlockId> last_block_id_;
    std::mutex update_last_block_id_mutex_;  // To support multithreaded putBlock during State Transfer
    std::shared_ptr<concord::storage::rocksdb::NativeClient> native_client_;
  };

  BlockId getLatestBlockId(const Blockchain::StateTransfer& st_chain) const {
    if (st_chain.getLastBlockId() > 0) {
      return st_chain.getLastBlockId();
    }
    return getLastReachableBlockId();
  }

 private:
  std::atomic<BlockId> last_reachable_block_id_{0};
  std::atomic<BlockId> genesis_block_id_{0};
  std::shared_ptr<concord::storage::rocksdb::NativeClient> native_client_;
};

}  // namespace concord::kvbc::categorization::detail
