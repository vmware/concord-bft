#include "merkle_tree_block.h"

#include "assertUtils.hpp"
#include "direct_kv_block.h"
#include "kv_types.hpp"
#include "merkle_tree_serialization.h"

#include <cstdint>

namespace concord::kvbc::v2MerkleTree::block::detail {

using sparse_merkle::Hash;
using ::concordUtils::Sliver;

// Use the v1DirectKeyValue implementation and just add the merkle-specific data at the back. We can do that, because
// users are not expected to interpret the returned buffer themselves.

RawBlock create(const SetOfKeyValuePairs &updates, const BlockDigest &parentDigest, const Hash &stateHash) {
  const auto merkleData = v2MerkleTree::detail::serialize(RawBlockMerkleData{stateHash});
  auto out = SetOfKeyValuePairs{};
  return v1DirectKeyValue::block::detail::create(updates, out, parentDigest, merkleData.data(), merkleData.size());
}

RawBlock create(const SetOfKeyValuePairs &updates,
                const OrderedKeysSet &deletes,
                const BlockDigest &parentDigest,
                const Hash &stateHash) {
  const auto merkleData = v2MerkleTree::detail::serialize(RawBlockMerkleData{stateHash, deletes});
  auto out = SetOfKeyValuePairs{};
  return v1DirectKeyValue::block::detail::create(updates, out, parentDigest, merkleData.data(), merkleData.size());
}

SetOfKeyValuePairs getData(const RawBlock &block) { return v1DirectKeyValue::block::detail::getData(block); }

OrderedKeysSet getDeletedKeys(const RawBlock &block) {
  return v2MerkleTree::detail::deserialize<RawBlockMerkleData>(v1DirectKeyValue::block::detail::getUserData(block))
      .deletedKeys;
}

BlockDigest getParentDigest(const RawBlock &block) { return v1DirectKeyValue::block::detail::getParentDigest(block); }

Hash getStateHash(const RawBlock &block) {
  return v2MerkleTree::detail::deserialize<RawBlockMerkleData>(v1DirectKeyValue::block::detail::getUserData(block))
      .stateHash;
}

Sliver createNode(const Node &node) { return v2MerkleTree::detail::serialize(node); }

Node parseNode(const Sliver &buffer) { return v2MerkleTree::detail::deserialize<Node>(buffer); }

}  // namespace concord::kvbc::v2MerkleTree::block::detail
