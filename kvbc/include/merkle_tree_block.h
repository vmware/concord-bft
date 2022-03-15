// Copyright (c) 2020 VMware, Inc. All Rights Reserved.

#pragma once

#include "kv_types.hpp"
#include "sparse_merkle/base_types.h"
#include "Digest.hpp"

#include <cstdint>
#include <iterator>
#include <unordered_map>

using concord::util::digest::BlockDigest;

namespace concord::kvbc::v2MerkleTree::block::detail {

// Creates a block that adds a set of key/values.
RawBlock create(const SetOfKeyValuePairs &updates,
                const BlockDigest &parentDigest,
                const sparse_merkle::Hash &stateHash);

// Creates a block from a set of key/value pairs and a set of keys to delete.
RawBlock create(const SetOfKeyValuePairs &updates,
                const OrderedKeysSet &deletes,
                const BlockDigest &parentDigest,
                const sparse_merkle::Hash &stateHash);

// Returns the block data in the form of a set of key/value pairs.
SetOfKeyValuePairs getData(const RawBlock &block);

// Returns a set of deleted keys in the passed raw block.
OrderedKeysSet getDeletedKeys(const RawBlock &block);

// Returns the parent digest of the passed block.
BlockDigest getParentDigest(const RawBlock &block);

// Returns the state hash of the passed block.
sparse_merkle::Hash getStateHash(const RawBlock &block);

// Key lengths are serialized as a KeyLengthType in network byte order.
using KeyLengthType = std::uint32_t;

// KeyData is serialized as a single byte. Therefore, 7 more flags can be added.
struct KeyData {
  bool deleted{false};
};

inline bool operator==(const KeyData &lhs, const KeyData &rhs) { return lhs.deleted == rhs.deleted; }

using Keys = std::unordered_map<Key, const KeyData>;

// Represents a block node. The parentDigest pointer must point to a buffer that is at least DIGEST_SIZE bytes
// long.
struct Node {
  using BlockIdType = BlockId;

  static constexpr auto PARENT_DIGEST_SIZE = DIGEST_SIZE;

  static constexpr auto STATE_HASH_SIZE = sparse_merkle::Hash::SIZE_IN_BYTES;

  static constexpr auto STATE_ROOT_VERSION_SIZE = sparse_merkle::Version::SIZE_IN_BYTES;

  static constexpr auto MIN_KEY_SIZE = 1 + sizeof(KeyLengthType);  // Add a byte of key data.

  static constexpr auto MIN_SIZE = sizeof(BlockIdType) + PARENT_DIGEST_SIZE + STATE_HASH_SIZE + STATE_ROOT_VERSION_SIZE;

  Node() = default;

  Node(BlockIdType pBlockId,
       const BlockDigest &pParentDigest,
       const sparse_merkle::Hash &pStateHash,
       const sparse_merkle::Version &pStateRootVersion,
       const Keys &pKeys = Keys{})
      : blockId{pBlockId},
        parentDigest{pParentDigest},
        stateHash{pStateHash},
        stateRootVersion{pStateRootVersion},
        keys{pKeys} {}

  void setParentDigest(const void *pParentDigest) {
    const auto parentDigestPtr = static_cast<const std::uint8_t *>(pParentDigest);
    std::copy(parentDigestPtr, parentDigestPtr + DIGEST_SIZE, std::begin(parentDigest));
  }

  BlockIdType blockId{0};
  BlockDigest parentDigest;
  sparse_merkle::Hash stateHash;
  sparse_merkle::Version stateRootVersion;
  Keys keys;
};

inline bool operator==(const Node &lhs, const Node &rhs) {
  return lhs.blockId == rhs.blockId && lhs.parentDigest == rhs.parentDigest && lhs.stateHash == rhs.stateHash &&
         lhs.stateRootVersion == rhs.stateRootVersion && lhs.keys == rhs.keys;
}

// Merkle-specific data added to raw blocks.
struct RawBlockMerkleData {
  static constexpr auto MIN_SIZE = sparse_merkle::Hash::SIZE_IN_BYTES;
  static constexpr auto STATE_HASH_SIZE = sparse_merkle::Hash::SIZE_IN_BYTES;
  static constexpr auto MIN_KEY_SIZE = sizeof(KeyLengthType);

  RawBlockMerkleData() = default;

  RawBlockMerkleData(const sparse_merkle::Hash &pStateHash, const OrderedKeysSet &pDeletedKeys = OrderedKeysSet{})
      : stateHash{pStateHash}, deletedKeys{pDeletedKeys} {}

  sparse_merkle::Hash stateHash;
  // Keep keys ordered so that serialization outputs deterministic raw blocks.
  OrderedKeysSet deletedKeys;
};

inline bool operator==(const RawBlockMerkleData &lhs, const RawBlockMerkleData &rhs) {
  return lhs.stateHash == rhs.stateHash && lhs.deletedKeys == rhs.deletedKeys;
}

// Creates a block node that is saved to the DB. It only includes the keys in the block, excluding the values as they
// are part of the state tree.
// Precondition: Keys cannot be longer than std::numeric_limits<KeyLengthType>::max() .
concordUtils::Sliver createNode(const Node &node);

// Parses a block node from a raw buffer.
Node parseNode(const concordUtils::Sliver &buffer);

}  // namespace concord::kvbc::v2MerkleTree::block::detail
