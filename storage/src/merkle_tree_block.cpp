#include "blockchain/merkle_tree_block.h"

#include "assertUtils.hpp"
#include "blockchain/direct_kv_block.h"
#include "blockchain/merkle_tree_serialization.h"
#include "endianness.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits>
#include <string>

namespace concord {
namespace storage {
namespace blockchain {
namespace v2MerkleTree {

namespace block {

using sparse_merkle::Hash;

using ::concordUtils::BlockId;
using ::concordUtils::Sliver;
using ::concordUtils::SetOfKeyValuePairs;
using ::concordUtils::toBigEndianArrayBuffer;

// Use the v1DirectKeyValue implementation and just add the the block ID and the state hash at the back. We want that so
// that they are included in the block digest. We can do that, because users are not expected to interpret the returned
// buffer themselves.
Sliver create(BlockId blockId, const SetOfKeyValuePairs &updates, const void *parentDigest, const Hash &stateHash) {
  std::array<std::uint8_t, sizeof(blockId) + Hash::SIZE_IN_BYTES> userData;
  const auto blockIdBuf = toBigEndianArrayBuffer(blockId);
  std::copy(std::cbegin(blockIdBuf), std::cend(blockIdBuf), std::begin(userData));
  std::copy(stateHash.data(), stateHash.data() + Hash::SIZE_IN_BYTES, std::begin(userData) + blockIdBuf.size());

  SetOfKeyValuePairs out;
  return v1DirectKeyValue::block::create(updates, out, parentDigest, userData.data(), userData.size());
}

SetOfKeyValuePairs getData(const Sliver &block) { return v1DirectKeyValue::block::getData(block); }

const void *getParentDigest(const Sliver &block) { return v1DirectKeyValue::block::getParentDigest(block); }

Hash getStateHash(const Sliver &block) {
  Assert(block.length() >= Hash::SIZE_IN_BYTES);
  const auto data = reinterpret_cast<const std::uint8_t *>(block.data());
  return Hash{data + (block.length() - Hash::SIZE_IN_BYTES)};
}

namespace detail {

Sliver createNode(const Node &node) { return v2MerkleTree::detail::serialize(node); }

Node parseNode(const Sliver &buffer) { return v2MerkleTree::detail::deserialize<Node>(buffer); }
}  // namespace detail

}  // namespace block
}  // namespace v2MerkleTree
}  // namespace blockchain
}  // namespace storage
}  // namespace concord
