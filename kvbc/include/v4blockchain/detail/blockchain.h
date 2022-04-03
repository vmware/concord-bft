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
  // construct a new block from the input updates and links it to the previous block by storing the last block digest.
  BlockId addBlock(const concord::kvbc::categorization::Updates&, storage::rocksdb::NativeWriteBatch&);

  // Loads from storage the last and first block ids respectivly.
  std::optional<BlockId> loadLastReachableBlockId();
  std::optional<BlockId> loadGenesisBlockId();

  void setLastReachable(BlockId id) { last_reachable_block_id_ = id; }
  BlockId getLastReachable() const { return last_reachable_block_id_; }
  BlockId getGenesisBlockId() const { return genesis_block_id_; }

  // Returns the buffer that represents the block
  std::optional<std::string> getBlockData(concord::kvbc::BlockId id) const;

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

 private:
  std::atomic<BlockId> last_reachable_block_id_{INVALID_BLOCK_ID};
  std::atomic<BlockId> genesis_block_id_{INVALID_BLOCK_ID};
  std::shared_ptr<concord::storage::rocksdb::NativeClient> native_client_;
  util::ThreadPool thread_pool_{1};
  std::optional<std::future<BlockDigest>> future_digest_;
};

}  // namespace concord::kvbc::v4blockchain::detail
