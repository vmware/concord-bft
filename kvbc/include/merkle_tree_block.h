// Copyright (c) 2020 VMware, Inc. All Rights Reserved.

#pragma once

#include "bcstatetransfer/SimpleBCStateTransfer.hpp"
#include "kv_types.hpp"
#include "sparse_merkle/base_types.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <iterator>
#include <unordered_map>

namespace concord {
namespace storage {
namespace blockchain {
namespace v2MerkleTree {
namespace block {

inline constexpr auto BLOCK_DIGEST_SIZE = bftEngine::SimpleBlockchainStateTransfer::BLOCK_DIGEST_SIZE;

// Creates a block that includes a set of key/values. The passed parentDigest buffer must be of size BLOCK_DIGEST_SIZE
// bytes.

concordUtils::Sliver create(const concord::kvbc::SetOfKeyValuePairs &updates,
                            const void *parentDigest,
                            const sparse_merkle::Hash &stateHash);

// Returns the block data in the form of a set of key/value pairs.
concord::kvbc::SetOfKeyValuePairs getData(const concordUtils::Sliver &block);

// Returns the parent digest of size BLOCK_DIGEST_SIZE bytes.
const void *getParentDigest(const concordUtils::Sliver &block);

// Returns the state hash of the passed block.
sparse_merkle::Hash getStateHash(const concordUtils::Sliver &block);

namespace detail {

// Key lengths are serialized as a KeyLengthType in network byte order.
using KeyLengthType = std::uint32_t;

// KeyData is serialized as a single byte. Therefore, 7 more flags can be added.
struct KeyData {
  bool deleted{false};
};

inline bool operator==(const KeyData &lhs, const KeyData &rhs) { return lhs.deleted == rhs.deleted; }

using Keys = std::unordered_map<concord::kvbc::Key, const KeyData>;

// Represents a block node. The parentDigest pointer must point to a buffer that is at least BLOCK_DIGEST_SIZE bytes
// long.
struct Node {
  using BlockIdType = concord::kvbc::BlockId;

  static constexpr auto PARENT_DIGEST_SIZE = BLOCK_DIGEST_SIZE;

  static constexpr auto STATE_HASH_SIZE = sparse_merkle::Hash::SIZE_IN_BYTES;

  static constexpr auto STATE_ROOT_VERSION_SIZE = sparse_merkle::Version::SIZE_IN_BYTES;

  static constexpr auto MIN_KEY_SIZE = 1 + sizeof(KeyLengthType);  // Add a byte of key data.

  static constexpr auto MIN_SIZE = sizeof(BlockIdType) + PARENT_DIGEST_SIZE + STATE_HASH_SIZE + STATE_ROOT_VERSION_SIZE;

  Node() = default;

  Node(BlockIdType pBlockId,
       const void *pParentDigest,
       const sparse_merkle::Hash &pStateHash,
       const sparse_merkle::Version &pStateRootVersion,
       Keys pKeys = Keys{})
      : blockId{pBlockId}, stateHash{pStateHash}, stateRootVersion{pStateRootVersion}, keys{pKeys} {
    setParentDigest(pParentDigest);
  }

  void setParentDigest(const void *pParentDigest) {
    const auto parentDigestPtr = static_cast<const std::uint8_t *>(pParentDigest);
    std::copy(parentDigestPtr, parentDigestPtr + BLOCK_DIGEST_SIZE, std::begin(parentDigest));
  }

  BlockIdType blockId{0};
  std::array<std::uint8_t, BLOCK_DIGEST_SIZE> parentDigest;
  sparse_merkle::Hash stateHash;
  sparse_merkle::Version stateRootVersion;
  Keys keys;
};

inline bool operator==(const Node &lhs, const Node &rhs) {
  return lhs.blockId == rhs.blockId && lhs.parentDigest == rhs.parentDigest && lhs.stateHash == rhs.stateHash &&
         lhs.stateRootVersion == rhs.stateRootVersion && lhs.keys == rhs.keys;
}

// Creates a block node that is saved to the DB. It only includes the keys in the block, excluding the values as they
// are part of the state tree.
// Precondition: Keys cannot be longer than std::numeric_limits<KeyLengthType>::max() .
concordUtils::Sliver createNode(const Node &node);

// Parses a block node from a raw buffer.
Node parseNode(const concordUtils::Sliver &buffer);

}  // namespace detail

}  // namespace block
}  // namespace v2MerkleTree
}  // namespace blockchain
}  // namespace storage
}  // namespace concord
