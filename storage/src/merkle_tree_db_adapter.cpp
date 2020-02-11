// Copyright 2020 VMware, all rights reserved

#include <assertUtils.hpp>
#include "blockchain/db_types.h"
#include "blockchain/merkle_tree_db_adapter.h"
#include "Logger.hpp"
#include "sparse_merkle/base_types.h"
#include "sliver.hpp"
#include "status.hpp"
#include "endianness.hpp"

#include <exception>
#include <iterator>
#include <limits>
#include <string>
#include <type_traits>
#include <vector>
#include <utility>

using concordUtils::Sliver;
using concordUtils::Status;

using concord::storage::blockchain::v2MerkleTree::detail::EDBKeyType;
using concord::storage::blockchain::v2MerkleTree::detail::EKeySubtype;
using concord::storage::blockchain::v2MerkleTree::detail::EBFTSubtype;

using concord::storage::sparse_merkle::BatchedInternalNode;
using concord::storage::sparse_merkle::Hash;
using concord::storage::sparse_merkle::Hasher;
using concord::storage::sparse_merkle::IDBReader;
using concord::storage::sparse_merkle::InternalChild;
using concord::storage::sparse_merkle::InternalNodeKey;
using concord::storage::sparse_merkle::LeafNode;
using concord::storage::sparse_merkle::LeafKey;
using concord::storage::sparse_merkle::LeafChild;
using concord::storage::sparse_merkle::NibblePath;
using concord::storage::sparse_merkle::Tree;

namespace concord {
namespace storage {
namespace blockchain {

namespace v2MerkleTree {

namespace {

// Specifies the key type. Used when serializing the stale node index.
enum class StaleKeyType : std::uint8_t {
  Internal,
  Leaf,
};

// Specifies the child type. Used when serializing BatchedInternalNode objects.
enum class BatchedInternalNodeChildType : std::uint8_t { Internal, Leaf };

template <typename E>
constexpr auto toChar(E e) {
  static_assert(std::is_enum_v<E>);
  return static_cast<char>(e);
}

// Serialize integral types in big-endian (network) byte order so that lexicographical comparison works and we can get
// away without a custom key comparator. Do not allow bools.
template <typename T>
std::string serializeImp(T v) {
  static_assert(std::is_integral_v<T> && !std::is_same_v<T, bool>);

  v = concordUtils::hostToNet(v);

  const auto data = reinterpret_cast<const char *>(&v);
  return std::string{data, sizeof(v)};
}

std::string serializeImp(EDBKeyType type) { return std::string{toChar(type)}; }

std::string serializeImp(EKeySubtype type) { return std::string{toChar(EDBKeyType::Key), toChar(type)}; }

std::string serializeImp(EBFTSubtype type) { return std::string{toChar(EDBKeyType::BFT), toChar(type)}; }

std::string serializeImp(StaleKeyType type) { return std::string{toChar(type)}; }

std::string serializeImp(const std::vector<std::uint8_t> &v) { return std::string{std::cbegin(v), std::cend(v)}; }

std::string serializeImp(const NibblePath &path) {
  return serializeImp(static_cast<std::uint8_t>(path.length())) + serializeImp(path.data());
}

std::string serializeImp(const Hash &hash) {
  return std::string{reinterpret_cast<const char *>(hash.data()), hash.size()};
}

std::string serializeImp(const InternalNodeKey &key) {
  return serializeImp(key.version().value()) + serializeImp(key.path());
}

std::string serializeImp(const LeafKey &key) { return serializeImp(key.hash()) + serializeImp(key.version().value()); }

std::string serializeImp(const LeafChild &child) { return serializeImp(child.hash) + serializeImp(child.key); }

std::string serializeImp(const InternalChild &child) {
  return serializeImp(child.hash) + serializeImp(child.version.value());
}

std::string serializeImp(const BatchedInternalNode &intNode) {
  static_assert(BatchedInternalNode::MAX_CHILDREN < 32);

  struct Visitor {
    Visitor(std::string &buf) : buf_{buf} {}

    void operator()(const LeafChild &leaf) { buf_ += toChar(BatchedInternalNodeChildType::Leaf) + serializeImp(leaf); }

    void operator()(const InternalChild &internal) {
      buf_ += toChar(BatchedInternalNodeChildType::Internal) + serializeImp(internal);
    }

    std::string &buf_;
  };

  const auto &children = intNode.children();
  std::string serializedChildren;
  auto mask = std::uint32_t{0};
  for (auto i = 0u; i < children.size(); ++i) {
    const auto &child = children[i];
    if (child) {
      mask |= (1 << i);
      std::visit(Visitor{serializedChildren}, *child);
    }
  }

  // Serialize by putting a 32 bit bitmask specifying which indexes are set in the children array. Then, put a type
  // before each child and then the child itself.
  return serializeImp(mask) + serializedChildren;
}

std::string serializeImp(const LeafNode &leafNode) { return leafNode.value.toString(); }

std::string serialize() { return std::string{}; }

template <typename T1, typename... T>
std::string serialize(const T1 &v1, const T &... v) {
  return serializeImp(v1) + serialize(v...);
}

template <typename T>
T deserialize(const Sliver &buf);

template <>
BatchedInternalNode deserialize<BatchedInternalNode>(const Sliver &buf) {
  BatchedInternalNode::ChildrenContainer children;
  // TODO: deserialize
  return BatchedInternalNode{children};
}

// Converts the updates as returned by the merkle tree to key/value pairs suitable for the DB.
SetOfKeyValuePairs batchToDBUpdates(const Tree::UpdateBatch &batch) {
  SetOfKeyValuePairs updates;
  const Sliver emptySliver;

  // Internal stale node indexes. Use the index only and set the key to an empty sliver.
  for (const auto &intKey : batch.stale.internal_keys) {
    const auto key = DBKeyManipulator::genStaleDbKey(intKey, batch.stale.stale_since_version.value());
    updates[key] = emptySliver;
  }

  // Leaf stale node indexes. Use the index only and set the key to an empty sliver.
  for (const auto &leafKey : batch.stale.leaf_keys) {
    const auto key = DBKeyManipulator::genStaleDbKey(leafKey, batch.stale.stale_since_version.value());
    updates[key] = emptySliver;
  }

  // Internal nodes.
  for (const auto &[intKey, intNode] : batch.internal_nodes) {
    const auto key = DBKeyManipulator::genInternalDbKey(intKey);
    updates[key] = serializeImp(intNode);
  }

  // Leaf nodes.
  for (const auto &[leafKey, leafNode] : batch.leaf_nodes) {
    const auto key = DBKeyManipulator::genDataDbKey(leafKey);
    updates[key] = serializeImp(leafNode);
  }

  return updates;
}

EDBKeyType getDBKeyType(const Sliver &s) {
  Assert(!s.empty());

  switch (s[0]) {
    case toChar(EDBKeyType::Block):
      return EDBKeyType::Block;
    case toChar(EDBKeyType::Key):
      return EDBKeyType::Key;
    case toChar(EDBKeyType::BFT):
      return EDBKeyType::BFT;
  }

  Assert(false);
}

EKeySubtype getKeySubtype(const Sliver &s) {
  Assert(s.length() > 1);

  switch (s[1]) {
    case toChar(EKeySubtype::Internal):
      return EKeySubtype::Internal;
    case toChar(EKeySubtype::Stale):
      return EKeySubtype::Stale;
    case toChar(EKeySubtype::Leaf):
      return EKeySubtype::Leaf;
  }

  Assert(false);
}

}  // namespace

Sliver DBKeyManipulator::genBlockDbKey(BlockId version) { return serialize(EDBKeyType::Block, version); }

Sliver DBKeyManipulator::genDataDbKey(const LeafKey &key) { return serialize(EKeySubtype::Leaf, key); }

Sliver DBKeyManipulator::genDataDbKey(const Sliver &key, BlockId version) {
  auto hasher = Hasher{};
  return genDataDbKey(LeafKey{hasher.hash(key.data(), key.length()), version});
}

Sliver DBKeyManipulator::genInternalDbKey(const InternalNodeKey &key) { return serialize(EKeySubtype::Internal, key); }

Sliver DBKeyManipulator::genStaleDbKey(const InternalNodeKey &key, BlockId staleSinceVersion) {
  // Use a serialization type to discriminate between internal and leaf keys.
  return serialize(EKeySubtype::Stale, staleSinceVersion, StaleKeyType::Internal, key);
}

Sliver DBKeyManipulator::genStaleDbKey(const LeafKey &key, BlockId staleSinceVersion) {
  // Use a serialization type to discriminate between internal and leaf keys.
  return serialize(EKeySubtype::Stale, staleSinceVersion, StaleKeyType::Leaf, key);
}

Sliver DBKeyManipulator::generateMetadataKey(ObjectId objectId) { return serialize(EBFTSubtype::Metadata, objectId); }

Sliver DBKeyManipulator::generateStateTransferKey(ObjectId objectId) { return serialize(EBFTSubtype::ST, objectId); }

Sliver DBKeyManipulator::generateSTPendingPageKey(uint32_t pageId) {
  return serialize(EBFTSubtype::STPendingPage, pageId);
}

Sliver DBKeyManipulator::generateSTCheckpointDescriptorKey(uint64_t chkpt) {
  return serialize(EBFTSubtype::STCheckpointDescriptor, chkpt);
}

Sliver DBKeyManipulator::generateSTReservedPageStaticKey(uint32_t pageId, uint64_t chkpt) {
  return serialize(EBFTSubtype::STReservedPageStatic, pageId, chkpt);
}

Sliver DBKeyManipulator::generateSTReservedPageDynamicKey(uint32_t pageId, uint64_t chkpt) {
  return serialize(EBFTSubtype::STReservedPageDynamic, pageId, chkpt);
}

BlockId DBKeyManipulator::extractBlockIdFromKey(const concordUtils::Key &key) {
  Assert(key.length() > sizeof(BlockId));

  const auto offset = key.length() - sizeof(BlockId);
  const auto id = concordUtils::netToHost(*reinterpret_cast<const BlockId *>(key.data() + offset));

  LOG_TRACE(
      logger(),
      "Got block ID " << id << " from key " << (HexPrintBuffer{key.data(), key.length()}) << ", offset " << offset);
  return id;
}

DBAdapter::DBAdapter(const std::shared_ptr<IDBClient> &db, bool readOnly)
    : DBAdapterBase{db, readOnly}, smTree_{std::make_shared<Reader>(*this)} {}

Status DBAdapter::getKeyByReadVersion(BlockId version,
                                      const concordUtils::Sliver &key,
                                      concordUtils::Sliver &outValue,
                                      BlockId &actualVersion) const {
  outValue = Sliver{};
  actualVersion = 0;

  auto iter = db_->getIteratorGuard();
  const auto dbKey = DBKeyManipulator::genDataDbKey(key, version);
  // Seek for a key that is greater than or equal to the requested one.
  const auto &[foundKey, foundValue] = iter->seekAtLeast(dbKey);
  if (iter->isEnd()) {
    const auto &[prevKey, prevValue] = iter->previous();
    // Make sure we get Leaf keys only if there wasn't an exact match.
    if (!prevKey.empty() && getDBKeyType(prevKey) == EDBKeyType::Key && getKeySubtype(prevKey) == EKeySubtype::Leaf) {
      outValue = prevValue;
      actualVersion = DBKeyManipulator::extractBlockIdFromKey(prevKey);
    }
  } else if (foundKey == dbKey) {
    outValue = foundValue;
    actualVersion = DBKeyManipulator::extractBlockIdFromKey(foundKey);
  }

  return Status::OK();
}

BlockId DBAdapter::getLatestBlock() const {
  // Generate maximal key for type 'BlockId'.
  const auto key = DBAdapterBase::getLatestBlock(DBKeyManipulator::genBlockDbKey(std::numeric_limits<BlockId>::max()));
  if (key.empty()) {  // no blocks
    return 0;
  }

  const auto blockId = DBKeyManipulator::extractBlockIdFromKey(key);
  LOG_TRACE(logger_, "Latest block ID " << blockId);
  return blockId;
}

BatchedInternalNode DBAdapter::Reader::get_latest_root() const {
  const auto latestVersion = adapter_.getLatestBlock();
  if (latestVersion == 0) {
    return BatchedInternalNode{};
  }
  return get_internal(InternalNodeKey::root(latestVersion));
}

BatchedInternalNode DBAdapter::Reader::get_internal(const InternalNodeKey &key) const {
  Sliver res;
  const auto status = adapter_.getDb()->get(DBKeyManipulator::genInternalDbKey(key), res);
  if (!status.isOK()) {
    throw std::runtime_error{"Failed to get the requested merkle tree internal node"};
  }

  return deserialize<BatchedInternalNode>(res);
}

LeafNode DBAdapter::Reader::get_leaf(const LeafKey &key) const {
  LeafNode leaf;
  const auto status = adapter_.getDb()->get(DBKeyManipulator::genDataDbKey(key), leaf.value);
  if (status.isNotFound()) {
    throw std::out_of_range{"Could not find the requested merkle tree leaf"};
  } else if (!status.isOK()) {
    throw std::runtime_error{"Failed to get the requested merkle tree leaf"};
  }
  return leaf;
}

Status DBAdapter::addBlock(const SetOfKeyValuePairs &updates, BlockId blockId) {
  const auto updateBatch = smTree_.update(updates);
  auto dbUpdates = batchToDBUpdates(updateBatch);

  // TODO: add a block with (at least) the following data:
  //  - a list of keys (without values)
  //  - parent block hash
  //  - merkle tree root after applying the updates

  return db_->multiPut(dbUpdates);
}

}  // namespace v2MerkleTree
}  // namespace blockchain
}  // namespace storage
}  // namespace concord
