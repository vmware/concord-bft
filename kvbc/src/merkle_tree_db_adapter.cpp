// Copyright 2020 VMware, all rights reserved

#include <assertUtils.hpp>
#include "bcstatetransfer/SimpleBCStateTransfer.hpp"
#include "storage/db_types.h"
#include "merkle_tree_block.h"
#include "merkle_tree_db_adapter.h"
#include "merkle_tree_serialization.h"
#include "endianness.hpp"
#include "Logger.hpp"
#include "sparse_merkle/update_batch.h"
#include "sparse_merkle/base_types.h"
#include "sparse_merkle/keys.h"
#include "sliver.hpp"
#include "status.hpp"
#include "hex_tools.h"

#include <exception>
#include <limits>
#include <memory>
#include <utility>

namespace concord::kvbc::v2MerkleTree {

namespace {

using namespace ::std::string_literals;

using BlockNode = block::detail::Node;
using BlockKeyData = block::detail::KeyData;

using ::concordUtils::fromBigEndianBuffer;
using ::concordUtils::Sliver;
using ::concordUtils::Status;
using ::concordUtils::HexPrintBuffer;

using ::concord::storage::IDBClient;
using ::concord::storage::ObjectId;

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
SetOfKeyValuePairs batchToDbUpdates(const sparse_merkle::UpdateBatch &batch, BlockId blockId) {
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
    updates[key] = detail::serialize(detail::DatabaseLeafValue{blockId, leafNode});
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

Key DBKeyManipulator::genBlockDbKey(BlockId version) { return serialize(EDBKeyType::Block, version); }

Key DBKeyManipulator::genDataDbKey(const LeafKey &key) { return serialize(EKeySubtype::Leaf, key); }

Key DBKeyManipulator::genDataDbKey(const Key &key, BlockId version) {
  auto hasher = Hasher{};
  return genDataDbKey(LeafKey{hasher.hash(key.data(), key.length()), version});
}

Key DBKeyManipulator::genInternalDbKey(const InternalNodeKey &key) { return serialize(EKeySubtype::Internal, key); }

Key DBKeyManipulator::genStaleDbKey(const InternalNodeKey &key, BlockId staleSinceVersion) {
  // Use a serialization type to discriminate between internal and leaf keys.
  return serialize(EKeySubtype::Stale, staleSinceVersion, StaleKeyType::Internal, key);
}

Key DBKeyManipulator::genStaleDbKey(const LeafKey &key, BlockId staleSinceVersion) {
  // Use a serialization type to discriminate between internal and leaf keys.
  return serialize(EKeySubtype::Stale, staleSinceVersion, StaleKeyType::Leaf, key);
}

Key DBKeyManipulator::generateMetadataKey(ObjectId objectId) { return serialize(EBFTSubtype::Metadata, objectId); }

Key DBKeyManipulator::generateStateTransferKey(ObjectId objectId) { return serialize(EBFTSubtype::ST, objectId); }

Key DBKeyManipulator::generateSTPendingPageKey(uint32_t pageId) {
  return serialize(EBFTSubtype::STPendingPage, pageId);
}

Key DBKeyManipulator::generateSTCheckpointDescriptorKey(uint64_t chkpt) {
  return serialize(EBFTSubtype::STCheckpointDescriptor, chkpt);
}

Key DBKeyManipulator::generateSTReservedPageStaticKey(uint32_t pageId, uint64_t chkpt) {
  return serialize(EBFTSubtype::STReservedPageStatic, pageId, chkpt);
}

Key DBKeyManipulator::generateSTReservedPageDynamicKey(uint32_t pageId, uint64_t chkpt) {
  return serialize(EBFTSubtype::STReservedPageDynamic, pageId, chkpt);
}

Key DBKeyManipulator::generateSTTempBlockKey(BlockId blockId) { return serialize(EBFTSubtype::STTempBlock, blockId); }

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
    : logger_{concordlogger::Log::getLogger("concord.kvbc.v2MerkleTree.DBAdapter")},
      // The smTree_ member needs an initialized DB. Therefore, do that in the initializer list before constructing
      // smTree_ .
      db_{[&db]() {
        db->init(false);
        return db;
      }()},
      smTree_{std::make_shared<Reader>(*this)} {
  // Make sure that if linkSTChainFrom() has been interrupted (e.g. a crash or an abnormal shutdown), all DBAdapter
  // methods will return the correct values. For example, if state transfer had completed and linkSTChainFrom() was
  // interrupted, getLatestBlockId() should be equal to getLastReachableBlockId() on the next startup. Another example
  // is getKeyByReadVersion() that returns keys from the blockchain only and ignores keys in the temporary state
  // transfer chain.
  linkSTChainFrom(getLastReachableBlockId() + 1);
}

std::pair<Value, BlockId> DBAdapter::getValue(const Key &key, const BlockId &blockVersion) const {
  auto stateRootVersion = sparse_merkle::Version{};
  // Find a block with an ID that is less than or equal to the requested block version and extract the state root
  // version from it.
  {
    const auto blockKey = DBKeyManipulator::genBlockDbKey(blockVersion);
    auto iter = db_->getIteratorGuard();
    const auto [foundBlockKey, foundBlockValue] = iter->seekAtMost(blockKey);
    if (!foundBlockKey.empty() && getDBKeyType(foundBlockKey) == EDBKeyType::Block) {
      stateRootVersion = detail::deserializeStateRootVersion(foundBlockValue);
    } else {
      throw NotFoundException{"Couldn't find a value by key and block version = " + std::to_string(blockVersion)};
    }
  }

  auto iter = db_->getIteratorGuard();
  const auto leafKey = DBKeyManipulator::genDataDbKey(key, stateRootVersion.value());

  // Seek for a leaf key with a version that is less than or equal to the state root version from the found block.
  // Since leaf keys are ordered lexicographically, first by hash and then by state root version, then if there is a key
  // having the same hash and a lower version, it should directly precede the found one.
  const auto [foundKey, foundValue] = iter->seekAtMost(leafKey);
  // Make sure we only process leaf keys that have the same hash as the hash of the key the user has passed. The state
  // root version can be less than the one we seek for.
  const auto isLeafKey =
      !foundKey.empty() && getDBKeyType(foundKey) == EDBKeyType::Key && getKeySubtype(foundKey) == EKeySubtype::Leaf;
  if (isLeafKey && DBKeyManipulator::extractHashFromLeafKey(foundKey) == hash(key)) {
    const auto dbLeafVal = detail::deserialize<detail::DatabaseLeafValue>(foundValue);
    return std::make_pair(dbLeafVal.leafNode.value, dbLeafVal.blockId);
  }

  throw NotFoundException{"Couldn't find a value by key and block version = " + std::to_string(blockVersion)};
}

BlockId DBAdapter::getLastReachableBlockId() const {
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

BlockId DBAdapter::getLatestBlockId() const {
  const auto latestBlockKey = DBKeyManipulator::generateSTTempBlockKey(MAX_BLOCK_ID);
  auto iter = db_->getIteratorGuard();
  const auto foundKey = iter->seekAtMost(latestBlockKey).first;
  if (!foundKey.empty() && getDBKeyType(foundKey) == EDBKeyType::BFT &&
      getBftSubtype(foundKey) == EBFTSubtype::STTempBlock) {
    const auto blockId = DBKeyManipulator::extractBlockIdFromKey(foundKey);
    LOG_TRACE(logger_, "Latest block ID " << blockId);
    return blockId;
  }
  // No state transfer blocks in the system. Fallback to getLastReachableBlockId() .
  return getLastReachableBlockId();
}

Sliver DBAdapter::createBlockNode(const SetOfKeyValuePairs &updates, BlockId blockId) const {
  // Make sure the digest is zero-initialized by using {} initialization.
  auto parentBlockDigest = StateTransferDigest{};
  if (blockId > 1) {
    const auto parentBlock = getRawBlock(blockId - 1);
    computeBlockDigest(blockId - 1, parentBlock.data(), parentBlock.length(), &parentBlockDigest);
  }

  auto node = BlockNode{blockId, parentBlockDigest.content, smTree_.get_root_hash(), smTree_.get_version()};
  for (const auto &[k, v] : updates) {
    // Treat empty values as deleted keys.
    node.keys.emplace(k, BlockKeyData{v.empty()});
  }
  return block::detail::createNode(node);
}

RawBlock DBAdapter::getRawBlock(const BlockId &blockId) const {
  const auto blockKey = DBKeyManipulator::genBlockDbKey(blockId);
  Sliver blockNodeSliver;
  if (auto statusNode = db_->get(blockKey, blockNodeSliver); statusNode.isNotFound()) {
    // If a block node is not found, look for a state transfer block.
    Sliver block;
    if (auto statusSt = db_->get(DBKeyManipulator::generateSTTempBlockKey(blockId), block); statusSt.isNotFound()) {
      throw NotFoundException{"Couldn't find a block by ID = " + std::to_string(blockId)};
    } else if (!statusSt.isOK()) {
      throw std::runtime_error{"Failed to get State Transfer block ID = " + std::to_string(blockId) +
                               " from DB, reason: " + statusSt.toString()};
    }
    return block;
  } else if (!statusNode.isOK()) {
    throw std::runtime_error{"Failed to get block node ID = " + std::to_string(blockId) +
                             " from DB, reason: " + statusNode.toString()};
  }

  const auto blockNode = block::detail::parseNode(blockNodeSliver);
  Assert(blockId == blockNode.blockId);

  SetOfKeyValuePairs keyValues;
  for (const auto &[key, keyData] : blockNode.keys) {
    if (!keyData.deleted) {
      Sliver value;
      if (const auto status = db_->get(DBKeyManipulator::genDataDbKey(key, blockNode.stateRootVersion.value()), value);
          !status.isOK()) {
        // If the key is not found, treat as corrupted storage and abort.
        Assert(!status.isNotFound());
        throw std::runtime_error{"Failed to get value by key from DB, block node ID = " + std::to_string(blockId)};
      }
      const auto dbLeafVal = detail::deserialize<detail::DatabaseLeafValue>(value);
      Assert(dbLeafVal.blockId == blockId);
      keyValues[key] = dbLeafVal.leafNode.value;
    }
  }

  return block::create(keyValues, blockNode.parentDigest.data(), blockNode.stateHash);
}

SetOfKeyValuePairs DBAdapter::lastReachableBlockDbUpdates(const SetOfKeyValuePairs &updates, BlockId blockId) {
  auto dbUpdates = SetOfKeyValuePairs{};
  // Create a block with the same state root as the previous one if there are no updates.
  if (!updates.empty()) {
    // Key updates.
    const auto updateBatch = smTree_.update(updates);
    dbUpdates = batchToDbUpdates(updateBatch, blockId);
  }

  // Block key.
  dbUpdates[DBKeyManipulator::genBlockDbKey(blockId)] = createBlockNode(updates, blockId);

  return dbUpdates;
}

BatchedInternalNode DBAdapter::Reader::get_latest_root() const {
  const auto lastBlock = adapter_.getLastReachableBlockId();
  if (lastBlock == 0) {
    return BatchedInternalNode{};
  }

  auto blockNodeSliver = Sliver{};
  Assert(adapter_.getDb()->get(DBKeyManipulator::genBlockDbKey(lastBlock), blockNodeSliver).isOK());
  const auto stateRootVersion = detail::deserializeStateRootVersion(blockNodeSliver);
  if (stateRootVersion == 0) {
    // A version of 0 means that the tree is empty and we return an empty BatchedInternalNode in that case.
    return BatchedInternalNode{};
  }

  return get_internal(InternalNodeKey::root(stateRootVersion));
}

BatchedInternalNode DBAdapter::Reader::get_internal(const InternalNodeKey &key) const {
  Sliver res;
  const auto status = adapter_.getDb()->get(DBKeyManipulator::genInternalDbKey(key), res);
  if (!status.isOK()) {
    throw std::runtime_error{"Failed to get the requested merkle tree internal node"};
  }
  return deserialize<BatchedInternalNode>(res);
}

BlockId DBAdapter::addBlock(const SetOfKeyValuePairs &updates) {
  for (const auto &kv : updates) {
    if (kv.second.empty()) {
      const auto msg = "Adding empty values in a block is not supported";
      LOG_ERROR(logger_, msg);
      throw std::invalid_argument{msg};
    }
  }

  const auto blockId = getLastReachableBlockId() + 1;
  const auto status = db_->multiPut(lastReachableBlockDbUpdates(updates, blockId));
  if (!status.isOK()) {
    throw std::runtime_error{"Failed to add block, reason: " + status.toString()};
  }
  return blockId;
}

void DBAdapter::linkSTChainFrom(BlockId blockId) {
  for (auto i = blockId; i <= getLatestBlockId(); ++i) {
    auto block = Sliver{};
    const auto sTBlockKey = DBKeyManipulator::generateSTTempBlockKey(i);
    const auto status = db_->get(sTBlockKey, block);
    if (status.isNotFound()) {
      // We don't have a chain from blockId to getLatestBlockId() at that stage. Return success and wait for the
      // missing blocks - they will be added on subsequent calls to addRawBlock().
      return;
    } else if (!status.isOK()) {
      const auto msg = "Failed to get next block data on state transfer, block ID = " + std::to_string(i) +
                       ", reason: " + status.toString();
      LOG_ERROR(logger_, msg);
      throw std::runtime_error{msg};
    }

    writeSTLinkTransaction(sTBlockKey, block, i);
  }
}

void DBAdapter::writeSTLinkTransaction(const Key &sTBlockKey, const Sliver &block, BlockId blockId) {
  // Deleting the ST block and adding the block to the blockchain via the merkle tree should be done atomically. We
  // implement that by using a transaction.
  auto txn = std::unique_ptr<concord::storage::ITransaction>{db_->beginTransaction()};

  // Delete the ST block key in the transaction.
  txn->del(sTBlockKey);

  // Put the block DB updates in the transaction.
  const auto addDbUpdates = lastReachableBlockDbUpdates(block::getData(block), blockId);
  for (const auto &[key, value] : addDbUpdates) {
    txn->put(key, value);
  }

  // Commit the transaction.
  txn->commit();
}

void DBAdapter::addRawBlock(const RawBlock &block, const BlockId &blockId) {
  const auto lastReachableBlock = getLastReachableBlockId();
  if (blockId <= lastReachableBlock) {
    const auto msg = "Cannot add an existing block ID " + std::to_string(blockId);
    LOG_ERROR(logger_, msg);
    throw std::invalid_argument{msg};
  } else if (lastReachableBlock + 1 == blockId) {
    // If adding the next block, append to the blockchain via the merkle tree and try to link with the ST temporary
    // chain.
    addBlock(block::getData(block));

    try {
      linkSTChainFrom(blockId + 1);
    } catch (const std::exception &e) {
      LOG_FATAL(logger_, "Aborting due to failure to link chains after block has been added, reason: "s + e.what());
      std::exit(1);
    } catch (...) {
      LOG_FATAL(logger_, "Aborting due to failure to link chains after block has been added");
      std::exit(1);
    }

    return;
  }

  // If not adding the next block, treat as a temporary state transfer block.
  const auto status = db_->put(DBKeyManipulator::generateSTTempBlockKey(blockId), block);
  if (!status.isOK()) {
    const auto msg = "Failed to add temporary block on state transfer, block ID = " + std::to_string(blockId) +
                     ", reason: " + status.toString();
    LOG_ERROR(logger_, msg);
    throw std::runtime_error{msg};
  }
}

void DBAdapter::deleteBlock(const BlockId &blockId) { throw std::runtime_error{"Not implemented"}; }

}  // namespace concord::kvbc::v2MerkleTree
