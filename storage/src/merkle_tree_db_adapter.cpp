// Copyright 2020 VMware, all rights reserved

#include <assertUtils.hpp>
#include "bcstatetransfer/SimpleBCStateTransfer.hpp"
#include "blockchain/db_types.h"
#include "blockchain/merkle_tree_block.h"
#include "blockchain/merkle_tree_db_adapter.h"
#include "blockchain/merkle_tree_serialization.h"
#include "endianness.hpp"
#include "Logger.hpp"
#include "sparse_merkle/update_batch.h"
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

using ::concordUtils::fromBigEndianBuffer;
using ::concordUtils::Key;
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

constexpr auto MAX_BLOCK_ID = std::numeric_limits<BlockId>::max();

// Converts the updates as returned by the merkle tree to key/value pairs suitable for the DB.
SetOfKeyValuePairs batchToDbUpdates(const sparse_merkle::UpdateBatch &batch) {
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

// Undefined behavior if an incorrect type is read from the buffer.
EBFTSubtype getBftSubtype(const Sliver &s) {
  Assert(s.length() > 1);

  switch (s[1]) {
    case toChar(EBFTSubtype::Metadata):
      return EBFTSubtype::Metadata;
    case toChar(EBFTSubtype::ST):
      return EBFTSubtype::ST;
    case toChar(EBFTSubtype::STPendingPage):
      return EBFTSubtype::STPendingPage;
    case toChar(EBFTSubtype::STReservedPageStatic):
      return EBFTSubtype::STReservedPageStatic;
    case toChar(EBFTSubtype::STReservedPageDynamic):
      return EBFTSubtype::STReservedPageDynamic;
    case toChar(EBFTSubtype::STCheckpointDescriptor):
      return EBFTSubtype::STCheckpointDescriptor;
    case toChar(EBFTSubtype::STTempBlock):
      return EBFTSubtype::STTempBlock;
  }
  Assert(false);

  // Dummy return to silence the compiler.
  return EBFTSubtype::Metadata;
}

auto hash(const Sliver &buf) {
  auto hasher = Hasher{};
  return hasher.hash(buf.data(), buf.length());
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

Sliver DBKeyManipulator::generateSTTempBlockKey(BlockId blockId) {
  return serialize(EBFTSubtype::STTempBlock, blockId);
}

BlockId DBKeyManipulator::extractBlockIdFromKey(const Key &key) {
  Assert(key.length() > sizeof(BlockId));

  const auto offset = key.length() - sizeof(BlockId);
  const auto id = fromBigEndianBuffer<BlockId>(key.data() + offset);

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

DBAdapter::DBAdapter(const std::shared_ptr<IDBClient> &db)
    : DBAdapterBase{db, false}, smTree_{std::make_shared<Reader>(*this)} {
  // Make sure that if linkSTChainFrom() has been interrupted (e.g. a crash or an abnormal shutdown), all DBAdapter
  // methods will return the correct values. For example, if state transfer had completed and linkSTChainFrom() was
  // interrupted, getLatestBlock() should be equal to getLastReachableBlock() on the next startup. Another example is
  // getKeyByReadVersion() that returns keys from the blockchain only and ignores keys in the temporary state
  // transfer chain.
  if (!linkSTChainFrom(getLatestBlock() + 1).isOK()) {
    throw std::runtime_error{"Failed to link chains on DBAdapter construction"};
  }
}

Status DBAdapter::getKeyByReadVersion(BlockId version, const Key &key, Sliver &outValue, BlockId &actualVersion) const {
  outValue = Sliver{};
  actualVersion = 0;

  auto iter = db_->getIteratorGuard();
  const auto dbKey = DBKeyManipulator::genDataDbKey(key, version);

  // Seek for a key that is less than or equal to the requested one.
  //
  // Since leaf keys are ordered lexicographically, first by hash and then by version, then if there is a key having
  // the same hash and a lower version, it should directly precede the found one.
  const auto &[foundKey, foundValue] = iter->seekAtMost(dbKey);
  if (foundKey == dbKey) {
    // We have an exact match.
    outValue = foundValue;
    actualVersion = version;
  }
  // Make sure we process leaf keys only that have the same hash as the hash of the passed key (i.e. is the same key
  // the user has requested).
  else if (!foundKey.empty() && getDBKeyType(foundKey) == EDBKeyType::Key &&
           getKeySubtype(foundKey) == EKeySubtype::Leaf &&
           DBKeyManipulator::extractHashFromLeafKey(foundKey) == hash(key)) {
    outValue = foundValue;
    actualVersion = DBKeyManipulator::extractBlockIdFromKey(foundKey);
  }
  return Status::OK();
}

BlockId DBAdapter::getLastReachableBlock() const {
  // Generate maximal key for type 'BlockId'.
  const auto maxBlockKey = DBKeyManipulator::genBlockDbKey(MAX_BLOCK_ID);
  auto iter = db_->getIteratorGuard();
  // As blocks are ordered by block ID, seek for a block key with an ID that is less than or equal to the maximal
  // allowed block ID.
  const auto foundKey = iter->seekAtMost(maxBlockKey).first;
  // Consider block keys only.
  if (!foundKey.empty() && getDBKeyType(foundKey) == EDBKeyType::Block) {
    const auto blockId = DBKeyManipulator::extractBlockIdFromKey(foundKey);
    LOG_TRACE(logger_, "Latest reachable block ID " << blockId);
    return blockId;
  }
  // No blocks in the system.
  return 0;
}

BlockId DBAdapter::getLatestBlock() const {
  const auto latestBlockKey = DBKeyManipulator::generateSTTempBlockKey(MAX_BLOCK_ID);
  auto iter = db_->getIteratorGuard();
  const auto foundKey = iter->seekAtMost(latestBlockKey).first;
  if (!foundKey.empty() && getDBKeyType(foundKey) == EDBKeyType::BFT &&
      getBftSubtype(foundKey) == EBFTSubtype::STTempBlock) {
    const auto blockId = DBKeyManipulator::extractBlockIdFromKey(foundKey);
    LOG_TRACE(logger_, "Latest block ID " << blockId);
    return blockId;
  }
  // No state transfer blocks in the system. Fallback to getLastReachableBlock() .
  return getLastReachableBlock();
}

Sliver DBAdapter::createBlockNode(const SetOfKeyValuePairs &updates,
                                  BlockId blockId,
                                  const Version &stateRootVersion) const {
  // Make sure the digest is zero-initialized by using {} initialization.
  auto parentBlockDigest = StateTransferDigest{};
  if (blockId > 1) {
    Sliver parentBlock;
    const auto status = getBlockById(blockId - 1, parentBlock);
    if (!status.isOK() || parentBlock.empty()) {
      LOG_FATAL(logger_, "createBlockNode: no block or block data for parent block ID " << blockId - 1);
      std::exit(1);
    }
    computeBlockDigest(blockId - 1, parentBlock.data(), parentBlock.length(), &parentBlockDigest);
  }

  auto node = BlockNode{blockId, parentBlockDigest.content, smTree_.get_root_hash(), stateRootVersion};
  for (const auto &[k, v] : updates) {
    // Treat empty values as deleted keys.
    node.keys.emplace(k, BlockKeyData{v.empty()});
  }
  return block::detail::createNode(node);
}

Status DBAdapter::getBlockById(BlockId blockId, Sliver &block) const {
  const auto blockKey = DBKeyManipulator::genBlockDbKey(blockId);
  Sliver blockNodeSliver;
  if (auto status = db_->get(blockKey, blockNodeSliver); status.isNotFound()) {
    // If a block node is not found, look for a state transfer block.
    return db_->get(DBKeyManipulator::generateSTTempBlockKey(blockId), block);
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
  return Status::OK();
}

concordUtils::SetOfKeyValuePairs DBAdapter::lastReachableBlockkDbUpdates(const SetOfKeyValuePairs &updates,
                                                                         BlockId blockId) {
  const auto updateBatch = smTree_.update(updates);

  // Key updates.
  auto dbUpdates = batchToDbUpdates(updateBatch);

  // Block key.
  dbUpdates[DBKeyManipulator::genBlockDbKey(blockId)] = createBlockNode(updates, blockId, smTree_.get_version());

  return dbUpdates;
}

BatchedInternalNode DBAdapter::Reader::get_latest_root() const {
  const auto lastBlock = adapter_.getLastReachableBlock();
  if (lastBlock == 0) {
    return BatchedInternalNode{};
  }

  auto blockNodeSliver = Sliver{};
  Assert(adapter_.getDb()->get(DBKeyManipulator::genBlockDbKey(lastBlock), blockNodeSliver).isOK());
  const auto blockNode = block::detail::parseNode(blockNodeSliver);
  return get_internal(InternalNodeKey::root(blockNode.stateRootVersion));
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

Status DBAdapter::addLastReachableBlock(const SetOfKeyValuePairs &updates) {
  if (updates.empty()) {
    return Status::IllegalOperation("Adding empty blocks is not allowed");
  }

  const auto blockId = getLastReachableBlock() + 1;
  return db_->multiPut(lastReachableBlockkDbUpdates(updates, blockId));
}

Status DBAdapter::linkSTChainFrom(BlockId blockId) {
  for (auto i = blockId; i <= getLatestBlock(); ++i) {
    auto block = Sliver{};
    const auto sTBlockKey = DBKeyManipulator::generateSTTempBlockKey(i);
    const auto status = db_->get(sTBlockKey, block);
    if (status.isNotFound()) {
      // We don't have a chain from blockId to getLatestBlock() at that stage. Return success and wait for the
      // missing blocks - they will be added on subsequent calls to addBlock().
      return Status::OK();
    } else if (!status.isOK()) {
      LOG_ERROR(logger_, "Failed to get next block data on state transfer, block ID " << i);
      return status;
    }

    const auto txnStatus = writeSTLinkTransaction(sTBlockKey, block, i);
    if (!status.isOK()) {
      return status;
    }
  }
  return Status::OK();
}

Status DBAdapter::writeSTLinkTransaction(const Key &sTBlockKey, const Sliver &block, BlockId blockId) {
  // Deleting the ST block and adding the block to the blockchain via the merkle tree should be done atomically. We
  // implement that by using a transaction.
  auto txn = std::unique_ptr<ITransaction>{db_->beginTransaction()};

  // Delete the ST block key in the transaction.
  txn->del(sTBlockKey);

  // Put the block DB updates in the transaction.
  const auto addDbUpdates = lastReachableBlockkDbUpdates(block::getData(block), blockId);
  for (const auto &[key, value] : addDbUpdates) {
    txn->put(key, value);
  }

  try {
    txn->commit();
  } catch (const std::exception &e) {
    const auto msg = std::string{"Failed to commit an addBlock() DB transaction, reason: "} + e.what();
    LOG_ERROR(logger_, msg);
    return Status::GeneralError(msg);
  } catch (...) {
    const auto msg = "Failed to commit an addBlock() DB transaction";
    LOG_ERROR(logger_, msg);
    return Status::GeneralError(msg);
  }

  return Status::OK();
}

Status DBAdapter::addBlock(const Sliver &block, BlockId blockId) {
  const auto lastReachableBlock = getLastReachableBlock();
  if (blockId <= lastReachableBlock) {
    const auto msg = "Cannot add an existing block ID " + std::to_string(blockId);
    LOG_ERROR(logger_, msg);
    return Status::IllegalOperation(msg);
  } else if (lastReachableBlock + 1 == blockId) {
    // If adding the next block, append to the blockchain via the merkle tree and try to link with the ST temporary
    // chain.
    const auto status = addLastReachableBlock(block::getData(block));
    if (!status.isOK()) {
      LOG_ERROR(logger_, "Failed to add a block to the end of the blockchain on state transfer, block ID " << blockId);
      return status;
    }
    return linkSTChainFrom(blockId + 1);
  }

  // If not adding the next block, treat as a temporary state transfer block.
  const auto status = db_->put(DBKeyManipulator::generateSTTempBlockKey(blockId), block);
  if (!status.isOK()) {
    LOG_ERROR(logger_, "Failed to add temporary block on state transfer, block ID " << blockId);
    return status;
  }
  return Status::OK();
}

}  // namespace v2MerkleTree
}  // namespace blockchain
}  // namespace storage
}  // namespace concord
