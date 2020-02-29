// Copyright 2020 VMware, all rights reserved

#include <assertUtils.hpp>
#include "bcstatetransfer/SimpleBCStateTransfer.hpp"
#include "blockchain/db_types.h"
#include "blockchain/merkle_tree_block.h"
#include "blockchain/merkle_tree_db_adapter.h"
#include "blockchain/merkle_tree_serialization.h"
#include "endianness.hpp"
#include "Logger.hpp"
#include "sparse_merkle/base_types.h"
#include "sparse_merkle/keys.h"
#include "sliver.hpp"
#include "status.hpp"

#include <exception>
#include <limits>
#include <memory>
#include <utility>

namespace concord {
namespace storage {
namespace blockchain {

namespace v2MerkleTree {

namespace {

using BlockNode = block::detail::Node;
using BlockKeyData = block::detail::KeyData;

using ::concordUtils::Key;
using ::concordUtils::netToHost;
using ::concordUtils::Sliver;
using ::concordUtils::Status;

using ::bftEngine::SimpleBlockchainStateTransfer::computeBlockDigest;
using ::bftEngine::SimpleBlockchainStateTransfer::StateTransferDigest;

using sparse_merkle::BatchedInternalNode;
using sparse_merkle::Hash;
using sparse_merkle::Hasher;
using sparse_merkle::InternalNodeKey;
using sparse_merkle::LeafNode;
using sparse_merkle::LeafKey;
using sparse_merkle::Tree;
using sparse_merkle::Version;

using namespace detail;

// Converts the updates as returned by the merkle tree to key/value pairs suitable for the DB.
SetOfKeyValuePairs batchToDbUpdates(const Tree::UpdateBatch &batch) {
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
    updates[key] = serialize(intNode);
  }

  // Leaf nodes.
  for (const auto &[leafKey, leafNode] : batch.leaf_nodes) {
    const auto key = DBKeyManipulator::genDataDbKey(leafKey);
    updates[key] = leafNode.value;
  }

  return updates;
}

// Undefined behavior if an incorrect type is read from the buffer.
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

  // Dummy return to silence the compiler.
  return EDBKeyType::Block;
}

// Undefined behavior if an incorrect type is read from the buffer.
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

  // Dummy return to silence the compiler.
  return EKeySubtype::Internal;
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

BlockId DBKeyManipulator::extractBlockIdFromKey(const Key &key) {
  Assert(key.length() > sizeof(BlockId));

  const auto offset = key.length() - sizeof(BlockId);
  const auto id = netToHost(*reinterpret_cast<const BlockId *>(key.data() + offset));

  LOG_TRACE(
      logger(),
      "Got block ID " << id << " from key " << (HexPrintBuffer{key.data(), key.length()}) << ", offset " << offset);
  return id;
}

Hash DBKeyManipulator::extractHashFromLeafKey(const Key &key) {
  constexpr auto keyTypeOffset = sizeof(EDBKeyType) + sizeof(EKeySubtype);
  Assert(key.length() > keyTypeOffset + Hash::SIZE_IN_BYTES);
  Assert(getDBKeyType(key) == EDBKeyType::Key);
  Assert(getKeySubtype(key) == EKeySubtype::Leaf);
  return Hash{reinterpret_cast<const uint8_t *>(key.data() + keyTypeOffset)};
}

DBAdapter::DBAdapter(const std::shared_ptr<IDBClient> &db, bool readOnly)
    : DBAdapterBase{db, readOnly}, smTree_{std::make_shared<Reader>(*this)} {}

Status DBAdapter::getKeyByReadVersion(BlockId version,
                                      const Sliver &key,
                                      Sliver &outValue,
                                      BlockId &actualVersion) const {
  outValue = Sliver{};
  actualVersion = 0;

  auto iter = db_->getIteratorGuard();
  const auto dbKey = DBKeyManipulator::genDataDbKey(key, version);
  const auto handleKey = [&outValue, &actualVersion, &key](const auto &foundKey, const auto &foundValue) {
    auto hasher = Hasher{};
    const auto keyHash = hasher.hash(key.data(), key.length());
    // Make sure we process leaf keys only that have the same hash as the hash of the passed key (i.e. are the same key
    // the user has requested).
    if (!foundKey.empty() && getDBKeyType(foundKey) == EDBKeyType::Key &&
        getKeySubtype(foundKey) == EKeySubtype::Leaf && DBKeyManipulator::extractHashFromLeafKey(foundKey) == keyHash) {
      outValue = foundValue;
      actualVersion = DBKeyManipulator::extractBlockIdFromKey(foundKey);
    }
  };

  // Seek for a key that is greater than or equal to the requested one.
  const auto &[foundKey, foundValue] = iter->seekAtLeast(dbKey);
  if (iter->isEnd()) {
    // If no keys are greater than or equal to the requested one, try with the last element.
    const auto &[lastKey, lastValue] = iter->last();
    handleKey(lastKey, lastValue);
  } else if (foundKey == dbKey) {
    // We have an exact match.
    outValue = foundValue;
    actualVersion = DBKeyManipulator::extractBlockIdFromKey(foundKey);
  } else {
    // We have found a key that is bigger than the requested one. Since leaf keys are ordered lexicographically, first
    // by hash and then by version, then if there is a key having the same hash and a lower version, it should directly
    // precede the found one. Therefore, try the previous element.
    const auto &[prevKey, prevValue] = iter->previous();
    handleKey(prevKey, prevValue);
  }

  return Status::OK();
}

BlockId DBAdapter::getLatestBlock() const {
  // Generate maximal key for type 'BlockId'.
  const auto maxBlockKey = DBKeyManipulator::genBlockDbKey(std::numeric_limits<BlockId>::max());
  auto iter = db_->getIteratorGuard();
  auto foundKey = iter->seekAtLeast(maxBlockKey).first;
  if (iter->isEnd()) {
    // If there are no keys bigger than or equal to the maximal block key, use the last key in the whole range as it may
    // be the actual latest block key. That will allow us to find the latest block even if there are no other bigger
    // keys in the system. Please see db_types.h for key ordering.
    foundKey = iter->last().first;
  } else if (foundKey != maxBlockKey) {
    // We have found a key that is greater - go back.
    foundKey = iter->previous().first;
  }

  // Consider block keys only.
  if (!foundKey.empty() && getDBKeyType(foundKey) == EDBKeyType::Block) {
    const auto blockId = DBKeyManipulator::extractBlockIdFromKey(foundKey);
    LOG_TRACE(logger_, "Latest block ID " << blockId);
    return blockId;
  }

  // No blocks in the system.
  return 0;
}

Sliver DBAdapter::createBlockNode(const SetOfKeyValuePairs &updates, BlockId blockId) const {
  // Make sure the digest is zero-initialized by using {} initialization.
  auto parentBlockDigest = StateTransferDigest{};
  if (blockId > 1) {
    auto found = false;
    Sliver parentBlock;
    const auto status = getBlockById(blockId - 1, parentBlock, found);
    if (!status.isOK() || !found || parentBlock.empty()) {
      LOG_FATAL(logger_, "createBlockNode: no block or block data for parent block ID " << blockId - 1);
      std::exit(1);
    }
    computeBlockDigest(blockId - 1, parentBlock.data(), parentBlock.length(), &parentBlockDigest);
  }

  auto node = BlockNode{blockId, parentBlockDigest.content, smTree_.get_root_hash()};
  for (const auto &[k, v] : updates) {
    // Treat empty values as deleted keys.
    node.keys.emplace(k, BlockKeyData{v.empty()});
  }
  return block::detail::createNode(node);
}

Status DBAdapter::getBlockById(BlockId blockId, Sliver &block, bool &found) const {
  const auto blockKey = DBKeyManipulator::genBlockDbKey(blockId);

  Sliver blockNodeSliver;
  if (const auto status = db_->get(blockKey, blockNodeSliver); status.isNotFound()) {
    found = false;
    return Status::OK();
  } else if (!status.isOK()) {
    return status;
  }

  const auto blockNode = block::detail::parseNode(blockNodeSliver);
  Assert(blockId == blockNode.blockId);

  SetOfKeyValuePairs keyValues;
  for (const auto &[key, keyData] : blockNode.keys) {
    if (!keyData.deleted) {
      Sliver value;
      if (const auto status = db_->get(DBKeyManipulator::genDataDbKey(key, blockId), value); !status.isOK()) {
        // If the key is not found, treat as corrupted storage and abort.
        Assert(!status.isNotFound());
        return status;
      }
      keyValues[key] = value;
    }
  }

  block = block::create(blockId, keyValues, blockNode.parentDigest.data(), blockNode.stateHash);
  found = true;
  return Status::OK();
}

// Reference: Key ordering is defined in db_types.h .
BatchedInternalNode DBAdapter::Reader::get_latest_root() const {
  // Generate a root key with the maximal possible version.
  const auto maxRootKey = DBKeyManipulator::genInternalDbKey(InternalNodeKey::root(Version::max()));
  auto iter = adapter_.getDb()->getIteratorGuard();

  // Look for a key that is greater or equal to the maximal internal root key.
  //
  // Note: code below relies on the fact that root keys have empty nibble paths and, therefore, are always preceding
  // other internal keys (as root keys will be shorter). Additionally, root keys will be orderd by their version.
  auto foundKey = iter->seekAtLeast(maxRootKey).first;
  if (iter->isEnd()) {
    // If there are no keys bigger than or equal to the maximal internal root key, use the last key in the whole range
    // as it may be the actual maximal internal root key. That will allow us to find the latest root even if there are
    // no other bigger keys in the system. Please see db_types.h for key ordering.
    foundKey = iter->last().first;
  } else if (foundKey != maxRootKey) {
    // We have found a key that is greater - go back.
    foundKey = iter->previous().first;
  }

  // Consider internal keys only. If it is an internal key, it must be a root one as roots precede non-root ones.
  if (!foundKey.empty() && getDBKeyType(foundKey) == EDBKeyType::Key &&
      getKeySubtype(foundKey) == EKeySubtype::Internal) {
    constexpr auto keyOffset = sizeof(EDBKeyType) + sizeof(EKeySubtype);
    return get_internal(deserialize<InternalNodeKey>(Sliver{foundKey, keyOffset, foundKey.length() - keyOffset}));
  }

  return BatchedInternalNode{};
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

  // Key updates.
  auto dbUpdates = batchToDbUpdates(updateBatch);

  // Block key.
  dbUpdates[DBKeyManipulator::genBlockDbKey(blockId)] = createBlockNode(updates, blockId);

  return db_->multiPut(dbUpdates);
}

}  // namespace v2MerkleTree
}  // namespace blockchain
}  // namespace storage
}  // namespace concord
