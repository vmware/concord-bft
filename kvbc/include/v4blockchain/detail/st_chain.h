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

#include <memory>
#include "rocksdb/native_client.h"
#include "kv_types.hpp"
#include "v4blockchain/detail/blocks.h"
#include "v4blockchain/detail/blockchain.h"

namespace concord::kvbc::v4blockchain::detail {

class StChain {
 public:
  StChain(const std::shared_ptr<concord::storage::rocksdb::NativeClient>&);
  ////// Blocks operations/////////////////////////////////
  bool hasBlock(kvbc::BlockId) const;
  void addBlock(const kvbc::BlockId, const char* block, const uint32_t blockSize);
  void deleteBlock(const kvbc::BlockId id, storage::rocksdb::NativeWriteBatch& wb);
  std::optional<v4blockchain::detail::Block> getBlock(kvbc::BlockId) const;
  // Returns the buffer that represents the block
  std::optional<std::string> getBlockData(concord::kvbc::BlockId) const;
  concord::util::digest::BlockDigest getBlockParentDigest(concord::kvbc::BlockId id) const;
  ///////// ST last block ID
  void resetChain() { last_block_id_ = 0; }
  void updateLastIdAfterDeletion(const kvbc::BlockId);
  // reads the last block id from storage.
  void loadLastBlockId();
  kvbc::BlockId getLastBlockId() const { return last_block_id_; }
  // If last block id was deleted, laod from storage the new last.
  void updateLastIdIfBigger(const kvbc::BlockId);

 private:
  // if last_block_id_ is 0 it means no ST chain
  std::atomic<kvbc::BlockId> last_block_id_;
  std::shared_ptr<concord::storage::rocksdb::NativeClient> native_client_;
};

}  // namespace concord::kvbc::v4blockchain::detail
