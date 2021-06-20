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

#include "kv_types.hpp"
#include "Logger.hpp"
#include "sliver.hpp"
#include "sparse_merkle/base_types.h"
#include "sparse_merkle/keys.h"
#include "storage/db_types.h"

namespace concord::kvbc::v2MerkleTree::detail {

// DBKeyManipulator is an internal implementation detail. Exposed for testing purposes only.
class DBKeyManipulator {
 public:
  static Key genBlockDbKey(BlockId version);
  static Key genNonProvableDbKey(BlockId block_id, const Key &key);
  static Key genDataDbKey(const sparse_merkle::LeafKey &key);
  static Key genDataDbKey(const Key &key, const sparse_merkle::Version &version);
  static Key genInternalDbKey(const sparse_merkle::InternalNodeKey &key);
  static Key genStaleDbKey(const sparse_merkle::InternalNodeKey &key, const sparse_merkle::Version &staleSinceVersion);
  static Key genStaleDbKey(const sparse_merkle::LeafKey &key, const sparse_merkle::Version &staleSinceVersion);
  static Key genNonProvableStaleDbKey(const Key &key, BlockId staleSinceBlock);
  // Version-only stale keys do not exist in the DB. They are used as a placeholder for searching through the stale
  // index. Rationale is that they are a lower bound of all stale keys for a specific version due to lexicographical
  // ordering, the fact that the version comes first and that real stale keys are longer and always follow version-only
  // ones.
  static Key genStaleDbKey(const sparse_merkle::Version &staleSinceVersion);

  static Key generateSTTempBlockKey(BlockId blockId);

  // Extract the block ID from a EDBKeyType::Key and EKeySubtype::NonProvable key.
  static BlockId extractBlockIdFromNonProvableKey(const Key &key);

  // Extract the key from a EDBKeyType::Key and EKeySubtype::NonProvable key.
  static Key extractKeyFromNonProvableKey(const Key &key);

  // Extract the block ID from a EDBKeyType::Block key or from a EKeySubtype::Leaf key.
  static BlockId extractBlockIdFromKey(const Key &key);

  // Extract the hash from a leaf key.
  static sparse_merkle::Hash extractHashFromLeafKey(const Key &key);

  // Extract the stale since version from a stale node index key.
  static sparse_merkle::Version extractVersionFromProvableStaleKey(const Key &key);

  // Extract the stale since block id from a stale node index key.
  static BlockId extractBlockIdFromNonProvableStaleKey(const Key &key);

  // Extract the actual key from the stale node index key.
  static Key extractKeyFromProvableStaleKey(const Key &key);

  // Extract the actual key from the non-provable stale key.
  static Key extractKeyFromNonProvableStaleKey(const Key &key);

  // Extract the version of an internal key.
  static sparse_merkle::Version extractVersionFromInternalKey(const Key &key);

  // Undefined behavior if an incorrect type is read from the buffer. Exposed for testing purposes.
  static storage::v2MerkleTree::detail::EDBKeyType getDBKeyType(const concordUtils::Sliver &);
  static storage::v2MerkleTree::detail::EKeySubtype getKeySubtype(const concordUtils::Sliver &);
  static storage::v2MerkleTree::detail::EBFTSubtype getBftSubtype(const concordUtils::Sliver &s);

 protected:
  static logging::Logger &logger() {
    static auto logger_ = logging::getLogger("concord.kvbc.v2MerkleTree.DBKeyManipulator");
    return logger_;
  }
};

}  // namespace concord::kvbc::v2MerkleTree::detail
