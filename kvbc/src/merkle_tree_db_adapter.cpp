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

#include "bcstatetransfer/SimpleBCStateTransfer.hpp"
#include "merkle_tree_db_adapter.h"
#include "merkle_tree_key_manipulator.h"
#include "merkle_tree_serialization.h"
#include "Logger.hpp"
#include "sliver.hpp"
#include "sparse_merkle/histograms.h"
#include "status.hpp"
#include "string.hpp"

#include <exception>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>

namespace concord::kvbc::v2MerkleTree {

namespace {

using namespace ::std::string_literals;

using ::concordUtils::Sliver;

using ::concord::storage::IDBClient;
using ::concord::storage::v2MerkleTree::detail::EDBKeyType;
using ::concord::storage::v2MerkleTree::detail::EKeySubtype;
using ::concord::storage::v2MerkleTree::detail::EBFTSubtype;

using ::bftEngine::bcst::computeBlockDigest;

using sparse_merkle::BatchedInternalNode;
using sparse_merkle::Hasher;
using sparse_merkle::InternalNodeKey;
using sparse_merkle::Version;
using sparse_merkle::detail::histograms;

using namespace concord::diagnostics;
using namespace detail;

constexpr auto MAX_BLOCK_ID = std::numeric_limits<BlockId>::max();

// Converts the updates as returned by the merkle tree to key/value pairs suitable for the DB.
SetOfKeyValuePairs batchToDbUpdates(const sparse_merkle::UpdateBatch &batch, BlockId blockId) {
  TimeRecorder scoped_timer(*histograms.dba_batch_to_db_updates);
  SetOfKeyValuePairs updates;
  const Sliver emptySliver;

  // Internal stale node indexes. Use the index only and set the key to an empty sliver.
  for (const auto &intKey : batch.stale.internal_keys) {
    const auto key = DBKeyManipulator::genStaleDbKey(intKey, batch.stale.stale_since_version);
    updates[key] = emptySliver;
  }

  // Leaf stale node indexes. Use the index only and set the key to an empty sliver.
  for (const auto &leafKey : batch.stale.leaf_keys) {
    const auto key = DBKeyManipulator::genStaleDbKey(leafKey, batch.stale.stale_since_version);
    updates[key] = emptySliver;
  }

  // Internal nodes.
  for (const auto &[intKey, intNode] : batch.internal_nodes) {
    const auto key = DBKeyManipulator::genInternalDbKey(intKey);
    TimeRecorder scoped(*histograms.dba_serialize_internal);
    updates[key] = serialize(intNode);
  }

  // Leaf nodes.
  for (const auto &[leafKey, leafNode] : batch.leaf_nodes) {
    const auto key = DBKeyManipulator::genDataDbKey(leafKey);
    TimeRecorder scoped(*histograms.dba_serialize_leaf);
    updates[key] = detail::serialize(detail::DatabaseLeafValue{blockId, leafNode});
  }

  return updates;
}

auto hash(const Sliver &buf) {
  auto hasher = Hasher{};
  return hasher.hash(buf.data(), buf.length());
}

template <typename VersionT, typename VersionExtractorT>
KeysVector keysForVersion(const std::shared_ptr<IDBClient> &db,
                          const Key &firstKey,
                          const VersionT &version,
                          EKeySubtype keySubtype,
                          const VersionExtractorT &extractVersion) {
  TimeRecorder scoped(*histograms.dba_keys_for_version);
  auto keys = KeysVector{};
  auto iter = db->getIteratorGuard();
  // Loop until a different key type or a key with the next version is encountered.
  auto currentKey = iter->seekAtLeast(firstKey).first;
  while (!currentKey.empty() && DBKeyManipulator::getDBKeyType(currentKey) == EDBKeyType::Key &&
         DBKeyManipulator::getKeySubtype(currentKey) == keySubtype && extractVersion(currentKey) == version) {
    keys.push_back(currentKey);
    currentKey = iter->next().first;
  }
  return keys;
}

void add(KeysVector &to, const KeysVector &src) { to.insert(std::end(to), std::cbegin(src), std::cend(src)); }

template <typename ContainerT>
std::pair<ContainerT, ContainerT> splitToProvableAndNonProvable(const ContainerT &updates,
                                                                const DBAdapter::NonProvableKeySet &nonProvableKeySet) {
  ContainerT provableKvPairs = updates;
  ContainerT nonProvableKvPairs;
  for (const auto &k : nonProvableKeySet) {
    auto iter = provableKvPairs.find(k);
    if (iter != std::cend(provableKvPairs)) {
      nonProvableKvPairs.emplace(iter->first, iter->second);
      provableKvPairs.erase(iter);
    }
  }
  return {provableKvPairs, nonProvableKvPairs};
}
}  // namespace

DBAdapter::DBAdapter(const std::shared_ptr<IDBClient> &db,
                     bool linkTempSTChain,
                     const NonProvableKeySet &nonProvableKeySet,
                     const std::shared_ptr<concord::performance::PerformanceManager> &pm)
    : logger_{logging::getLogger("concord.kvbc.v2MerkleTree.DBAdapter")},
      // The smTree_ member needs an initialized DB. Therefore, do that in the initializer list before constructing
      // smTree_ .
      db_{db},
      genesisBlockId_{loadGenesisBlockId()},
      lastReachableBlockId_{loadLastReachableBlockId()},
      latestSTTempBlockId_{loadLatestTempSTBlockId()},
      smTree_{std::make_shared<Reader>(*this)},
      nonProvableKeySet_{nonProvableKeySet},
      pm_{pm} {
  if (!nonProvableKeySet_.empty()) {
    const auto length = nonProvableKeySet_.begin()->length();
    for (const auto &k : nonProvableKeySet_) {
      if (length != k.length()) {
        throw std::runtime_error{"Non-provable keys must have the same length"};
      }
    }
  }
  if (linkTempSTChain) {
    // Make sure that if linkSTChainFrom() has been interrupted (e.g. a crash or an abnormal shutdown), all DBAdapter
    // methods will return the correct values. For example, if state transfer had completed and linkSTChainFrom() was
    // interrupted, getLatestBlockId() should be equal to getLastReachableBlockId() on the next startup. Another example
    // is getValue() that returns keys from the blockchain only and ignores keys in the temporary state
    // transfer chain.
    linkSTChainFrom(getLastReachableBlockId() + 1);
  }
}

std::optional<std::pair<Value, BlockId>> DBAdapter::getValueForNonProvableKey(const Key &key,
                                                                              const BlockId &blockVersion) const {
  const auto &blockKey = DBKeyManipulator::genNonProvableDbKey(blockVersion, key);
  auto iter = db_->getIteratorGuard();
  const auto [foundBlockKey, foundBlockValue] = iter->seekAtMost(blockKey);
  if (!foundBlockKey.empty() && DBKeyManipulator::getDBKeyType(foundBlockKey) == EDBKeyType::Key &&
      DBKeyManipulator::getKeySubtype(foundBlockKey) == EKeySubtype::NonProvable &&
      DBKeyManipulator::extractKeyFromNonProvableKey(foundBlockKey) == key) {
    return {{foundBlockValue, DBKeyManipulator::extractBlockIdFromNonProvableKey(foundBlockKey)}};
  }
  return std::nullopt;
}

std::pair<Value, BlockId> DBAdapter::getValue(const Key &key, const BlockId &blockVersion) const {
  TimeRecorder scoped_timer(*histograms.dba_get_value);
  auto stateRootVersion = Version{};
  if (nonProvableKeySet_.find(key) != std::cend(nonProvableKeySet_)) {
    auto ret = getValueForNonProvableKey(key, blockVersion);
    if (ret) {
      return std::move(ret.value());
    }
  }
  // Find a block with an ID that is less than or equal to the requested block version and extract the state root
  // version from it.
  {
    const auto blockKey = DBKeyManipulator::genBlockDbKey(blockVersion);
    auto iter = db_->getIteratorGuard();
    const auto [foundBlockKey, foundBlockValue] = iter->seekAtMost(blockKey);
    if (!foundBlockKey.empty() && DBKeyManipulator::getDBKeyType(foundBlockKey) == EDBKeyType::Block) {
      TimeRecorder scoped(*histograms.dba_deserialize_block);
      stateRootVersion = detail::deserializeStateRootVersion(foundBlockValue);
    } else {
      throw NotFoundException{"Couldn't find a value by key and block version = " + std::to_string(blockVersion)};
    }
  }

  // Seek for a leaf key with a version that is less than or equal to the state root version from the found block.
  // Since leaf keys are ordered lexicographically, first by hash and then by state root version, then if there is a key
  // having the same hash and a lower version, it should directly precede the found one.
  const auto foundKv = getLeafKeyValAtMostVersion(key, stateRootVersion);
  if (foundKv) {
    TimeRecorder scoped(*histograms.dba_deserialize_leaf);
    const auto dbLeafVal = detail::deserialize<detail::DatabaseLeafValue>(foundKv.value().second);
    if (dbLeafVal.deletedInBlockId.has_value() && dbLeafVal.deletedInBlockId.value() <= blockVersion) {
      throw NotFoundException{"Key already deleted at block version = " +
                              std::to_string(dbLeafVal.deletedInBlockId.value())};
    }
    // Return the value at the block version it was added in, even if the block itself has been deleted.
    return std::make_pair(dbLeafVal.leafNode.value, dbLeafVal.addedInBlockId);
  }

  throw NotFoundException{"Couldn't find a value by key and block version = " + std::to_string(blockVersion)};
}

BlockId DBAdapter::getGenesisBlockId() const { return genesisBlockId_; }

BlockId DBAdapter::getLastReachableBlockId() const { return lastReachableBlockId_; }

BlockId DBAdapter::getLatestBlockId() const {
  if (latestSTTempBlockId_.has_value()) {
    return *latestSTTempBlockId_;
  }
  return getLastReachableBlockId();
}

BlockId DBAdapter::loadGenesisBlockId() const {
  auto iter = db_->getIteratorGuard();
  const auto key = iter->seekAtLeast(DBKeyManipulator::genBlockDbKey(INITIAL_GENESIS_BLOCK_ID)).first;
  if (!key.empty() && DBKeyManipulator::getDBKeyType(key) == EDBKeyType::Block) {
    return DBKeyManipulator::extractBlockIdFromKey(key);
  }
  return 0;
}

BlockId DBAdapter::loadLastReachableBlockId() const {
  // Generate maximal key for type 'BlockId'.
  const auto maxBlockKey = DBKeyManipulator::genBlockDbKey(MAX_BLOCK_ID);
  auto iter = db_->getIteratorGuard();
  // As blocks are ordered by block ID, seek for a block key with an ID that is less than or equal to the maximal
  // allowed block ID.
  const auto foundKey = iter->seekAtMost(maxBlockKey).first;
  // Consider block keys only.
  if (!foundKey.empty() && DBKeyManipulator::getDBKeyType(foundKey) == EDBKeyType::Block) {
    const auto blockId = DBKeyManipulator::extractBlockIdFromKey(foundKey);
    LOG_TRACE(logger_, "Latest reachable block ID " << blockId);
    return blockId;
  }
  // No blocks in the system.
  return 0;
}

std::optional<BlockId> DBAdapter::loadLatestTempSTBlockId() const {
  const auto latestBlockKey = DBKeyManipulator::generateSTTempBlockKey(MAX_BLOCK_ID);
  auto iter = db_->getIteratorGuard();
  const auto foundKey = iter->seekAtMost(latestBlockKey).first;
  if (!foundKey.empty() && DBKeyManipulator::getDBKeyType(foundKey) == EDBKeyType::BFT &&
      DBKeyManipulator::getBftSubtype(foundKey) == EBFTSubtype::STTempBlock) {
    const auto blockId = DBKeyManipulator::extractBlockIdFromKey(foundKey);
    LOG_TRACE(logger_, "Latest block ID " << blockId);
    return blockId;
  }
  // No state transfer blocks in the system.
  return std::nullopt;
}

Sliver DBAdapter::createBlockNode(const SetOfKeyValuePairs &updates,
                                  const OrderedKeysSet &deletes,
                                  BlockId blockId,
                                  const BlockDigest &parentBlockDigest) const {
  TimeRecorder scoped_timer(*histograms.dba_create_block_node);
  auto node = block::detail::Node{blockId, parentBlockDigest, smTree_.get_root_hash(), smTree_.get_version()};
  for (const auto &kv : updates) {
    node.keys.emplace(kv.first, block::detail::KeyData{false});
  }

  for (const auto &k : deletes) {
    node.keys.emplace(k, block::detail::KeyData{true});
  }

  return block::detail::createNode(node);
}

RawBlock DBAdapter::getRawBlock(const BlockId &blockId) const {
  TimeRecorder scoped_timer(*histograms.dba_get_raw_block);
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
  ConcordAssert(blockId == blockNode.blockId);

  auto keyValues = SetOfKeyValuePairs{};
  auto deletedKeys = OrderedKeysSet{};
  for (const auto &[key, keyData] : blockNode.keys) {
    if (nonProvableKeySet_.find(key) != std::cend(nonProvableKeySet_)) {
      Sliver value;
      if (db_->get(DBKeyManipulator::genNonProvableDbKey(blockId, key), value).isOK()) {
        keyValues[key] = value;
        // Key found, no need to look in the Tree Keys
        continue;
      }
    }
    // Look for the Tree keys or Non-Provable keys from old DBs
    if (!keyData.deleted) {
      Sliver value;
      if (const auto status = db_->get(DBKeyManipulator::genDataDbKey(key, blockNode.stateRootVersion), value);
          !status.isOK()) {
        // If the key is not found, treat as corrupted storage and abort.
        ConcordAssert(!status.isNotFound());
        throw std::runtime_error{"Failed to get value by key from DB, block node ID = " + std::to_string(blockId)};
      }
      TimeRecorder scoped(*histograms.dba_deserialize_leaf);
      const auto dbLeafVal = detail::deserialize<detail::DatabaseLeafValue>(value);
      ConcordAssert(dbLeafVal.addedInBlockId == blockId);
      keyValues[key] = dbLeafVal.leafNode.value;
    } else {
      deletedKeys.insert(key);
    }
  }

  return block::detail::create(keyValues, deletedKeys, blockNode.parentDigest, blockNode.stateHash);
}

std::future<BlockDigest> DBAdapter::computeParentBlockDigest(BlockId blockId) const {
  // Get the parent block in the same thread as not all IDBClient implementations may support concurrent operations.
  auto parentBlock = std::optional<RawBlock>{std::nullopt};
  if (blockId > INITIAL_GENESIS_BLOCK_ID) {
    parentBlock = getRawBlock(blockId - 1);
  }
  return std::async(std::launch::async, [blockId, parentBlock] {
    // Make sure the digest is zero-initialized by using {} initialization.
    auto parentBlockDigest = BlockDigest{};
    if (parentBlock) {
      histograms.dba_hashed_parent_block_size->recordAtomic(parentBlock->length());
      static constexpr bool is_atomic = true;
      TimeRecorder<is_atomic> scoped(*histograms.dba_hash_parent_block);
      parentBlockDigest = computeBlockDigest(blockId - 1, parentBlock->data(), parentBlock->length());
    }
    return parentBlockDigest;
  });
}

SetOfKeyValuePairs DBAdapter::lastReachableBlockDbUpdates(const SetOfKeyValuePairs &updates,
                                                          const OrderedKeysSet &deletes,
                                                          BlockId blockId) {
  TimeRecorder scoped_timer(*histograms.dba_last_reachable_block_db_updates);
  // Compute the parent block digest in parallel with the tree update.
  auto parentBlockDigestFuture = computeParentBlockDigest(blockId);

  // Find keys that are deleted only and not updated. We need that list, because updates take precedence over deletes
  // and we should only try to update deleted keys.
  // Make sure the deletes do not contain non-provable keys.
  auto deletesOnly = KeysVector{};
  for (const auto &key : deletes) {
    if (updates.find(key) == std::cend(updates)) {
      deletesOnly.push_back(key);
    }
    if (nonProvableKeySet_.find(key) != std::cend(nonProvableKeySet_)) {
      throw std::runtime_error{"The deletes key set contains a non-provable key: " + key.toString()};
    }
  }

  auto dbUpdates = SetOfKeyValuePairs{};

  // Keep a list of actually deleted keys so that we can add these in the block node. We don't add non-existent keys in
  // the block node.
  auto actuallyDeleted = OrderedKeysSet{};

  // If a key is deleted only, try to find its previous version and set the 'deletedInBlockId' value for it. Assumption
  // here is that the tree will not output leaf nodes for deleted only keys.
  for (const auto &key : deletesOnly) {
    const auto foundKv = getLeafKeyValAtMostVersion(key, smTree_.get_version());
    if (foundKv) {
      DatabaseLeafValue dbLeafValue;
      {
        TimeRecorder scoped(*histograms.dba_deserialize_leaf);
        dbLeafValue = deserialize<DatabaseLeafValue>(foundKv.value().second);
      }
      dbLeafValue.deletedInBlockId = blockId;
      {
        TimeRecorder scoped(*histograms.dba_serialize_leaf);
        dbUpdates[foundKv.value().first] = serialize(dbLeafValue);
      }
      actuallyDeleted.insert(key);
    }
  }

  const auto [provableKvPairs, nonProvableKvPairs] = splitToProvableAndNonProvable(updates, nonProvableKeySet_);

  // If there are no updates and no actual deletes, do not update the tree and do create an empty block.
  if (!provableKvPairs.empty() || !actuallyDeleted.empty()) {
    // Key updates.
    const auto updateBatch =
        smTree_.update(provableKvPairs, KeysVector{std::cbegin(actuallyDeleted), std::cend(actuallyDeleted)});
    const auto batchDbUpdates = batchToDbUpdates(updateBatch, blockId);
    dbUpdates.insert(std::cbegin(batchDbUpdates), std::cend(batchDbUpdates));
  }

  // Block node with updates and actually deleted keys.
  dbUpdates[DBKeyManipulator::genBlockDbKey(blockId)] =
      createBlockNode(updates, actuallyDeleted, blockId, parentBlockDigestFuture.get());

  for (const auto &[k, v] : nonProvableKvPairs) {
    dbUpdates[DBKeyManipulator::genNonProvableDbKey(blockId, k)] = v;
    if (blockId > INITIAL_GENESIS_BLOCK_ID) {
      const auto &staleKv = getValueForNonProvableKey(k, blockId - 1);
      // Marking non-provable keys as stale
      if (staleKv) {
        const auto &nonProvableKey = DBKeyManipulator::genNonProvableDbKey(staleKv.value().second, k);
        const auto &nonProvableStaleKey = DBKeyManipulator::genNonProvableStaleDbKey(nonProvableKey, blockId);
        dbUpdates[nonProvableStaleKey] = Value{};
      }
    }
  }

  // update metrics
  uint64_t sizeOfUpdatesInBytes = 0;
  for (auto &kv : dbUpdates) {
    sizeOfUpdatesInBytes += kv.first.length() + kv.second.length();
  }
  histograms.dba_size_of_updates->record(sizeOfUpdatesInBytes);
  return dbUpdates;
}

std::pair<sparse_merkle::UpdateBatch, sparse_merkle::detail::UpdateCache> DBAdapter::updateTree(
    const SetOfKeyValuePairs &updates, const OrderedKeysSet &deletes) {
  return smTree_.update_with_cache(updates, KeysVector{std::cbegin(deletes), std::cend(deletes)});
}

BatchedInternalNode DBAdapter::Reader::get_latest_root() const {
  const auto lastBlock = adapter_.getLastReachableBlockId();
  if (lastBlock == 0) {
    return BatchedInternalNode{};
  }

  auto blockNodeSliver = Sliver{};
  const auto getStatus = adapter_.getDb()->get(DBKeyManipulator::genBlockDbKey(lastBlock), blockNodeSliver);
  (void)getStatus;
  ConcordAssert(getStatus.isOK());
  const auto stateRootVersion = detail::deserializeStateRootVersion(blockNodeSliver);
  if (stateRootVersion == 0) {
    // A version of 0 means that the tree is empty and we return an empty BatchedInternalNode in that case.
    return BatchedInternalNode{};
  }

  return get_internal(InternalNodeKey::root(stateRootVersion));
}

BatchedInternalNode DBAdapter::Reader::get_internal(const InternalNodeKey &key) const {
  Sliver res;
  auto status = concordUtils::Status::OK();
  {
    TimeRecorder scoped_timer(*histograms.dba_get_internal);
    status = adapter_.getDb()->get(DBKeyManipulator::genInternalDbKey(key), res);
  }
  if (!status.isOK()) {
    throw std::runtime_error{"Failed to get the requested merkle tree internal node"};
  }
  {
    TimeRecorder scoped_timer(*histograms.dba_deserialize_internal);
    return deserialize<BatchedInternalNode>(res);
  }
}

BlockId DBAdapter::addBlock(const SetOfKeyValuePairs &updates, const OrderedKeysSet &deletes) {
  const auto addedBlockId = getLastReachableBlockId() + 1;
  auto dbUpdates = lastReachableBlockDbUpdates(updates, deletes, addedBlockId);
  pm_->Delay<concord::performance::SlowdownPhase::StorageBeforeDbWrite>(dbUpdates);
  const auto status = db_->multiPut(dbUpdates);
  if (!status.isOK()) {
    throw std::runtime_error{"Failed to add block, reason: " + status.toString()};
  }

  // We've successfully added a block - increment the last reachable block ID.
  lastReachableBlockId_ = addedBlockId;

  // We don't allow deletion of the genesis block if it is the only one left in the system. We do allow deleting it as
  // last reachable, though, to support replica state sync. Therefore, if we couldn't load the genesis block ID on
  // startup, it means there are no blocks in storage and we are now adding the first one. Make sure we set the genesis
  // block ID cache to reflect that.
  if (genesisBlockId_ == 0) {
    genesisBlockId_ = INITIAL_GENESIS_BLOCK_ID;
  }

  return addedBlockId;
}

void DBAdapter::linkUntilBlockId(BlockId until_block_id) { throw std::runtime_error{"Not implemented!"}; }

BlockId DBAdapter::addBlock(const SetOfKeyValuePairs &updates) { return addBlock(updates, OrderedKeysSet{}); }

BlockId DBAdapter::addBlock(const OrderedKeysSet &deletes) { return addBlock(SetOfKeyValuePairs{}, deletes); }

void DBAdapter::linkSTChainFrom(BlockId blockId) {
  TimeRecorder scoped_timer(*histograms.dba_link_st_chain);
  const auto latest_block_id = getLatestBlockId();
  histograms.dba_num_blocks_for_st_link->record(latest_block_id - blockId);
  for (auto i = blockId; i <= latest_block_id; ++i) {
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

  // Linking has fully completed and we should not have any more ST temporary blocks left. Therefore, make sure we don't
  // have any value for the latest ST temporary block ID cache.
  latestSTTempBlockId_.reset();
}

void DBAdapter::writeSTLinkTransaction(const Key &sTBlockKey, const Sliver &block, BlockId blockId) {
  // Deleting the ST block and adding the block to the blockchain via the merkle tree should be done atomically. We
  // implement that by using a transaction.
  auto txn = std::unique_ptr<concord::storage::ITransaction>{db_->beginTransaction()};

  // Delete the ST block key in the transaction.
  txn->del(sTBlockKey);

  // Put the block DB updates in the transaction.
  const auto addDbUpdates =
      lastReachableBlockDbUpdates(getBlockData(block), block::detail::getDeletedKeys(block), blockId);
  for (const auto &[key, value] : addDbUpdates) {
    txn->put(key, value);
  }

  // Commit the transaction.
  txn->commit();

  // Update the last reachable block ID cache after the transaction commits as that is equivalent to adding a block.
  lastReachableBlockId_ = blockId;
}

void DBAdapter::addRawBlock(const RawBlock &block, const BlockId &blockId, bool lastBlock) {
  TimeRecorder scoped_timer(*histograms.dba_add_raw_block);
  const auto lastReachableBlock = getLastReachableBlockId();
  if (blockId <= lastReachableBlock) {
    const auto msg = "Cannot add an existing block ID " + std::to_string(blockId);
    LOG_ERROR(logger_, msg);
    throw std::invalid_argument{msg};
  } else if (lastReachableBlock + 1 == blockId) {
    // If adding the next block, append to the blockchain via the merkle tree and try to link with the ST temporary
    // chain.
    addBlock(getBlockData(block), block::detail::getDeletedKeys(block));

    try {
      linkSTChainFrom(blockId + 1);
    } catch (const std::exception &e) {
      LOG_FATAL(logger_, "Aborting due to failure to link chains after block has been added, reason: "s + e.what());
      std::terminate();
    } catch (...) {
      LOG_FATAL(logger_, "Aborting due to failure to link chains after block has been added");
      std::terminate();
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

  // Update the cached latest ST temporary block ID if we have received and persisted such a block.
  if (latestSTTempBlockId_.has_value()) {
    if (blockId > *latestSTTempBlockId_) {
      latestSTTempBlockId_ = blockId;
    }
  } else {
    latestSTTempBlockId_ = blockId;
  }
}

void DBAdapter::deleteBlock(const BlockId &blockId) {
  TimeRecorder scoped_timer(*histograms.dba_delete_block);

  // Deleting blocks that don't exist is not an error.
  if (blockId < INITIAL_GENESIS_BLOCK_ID) {
    return;
  }

  const auto latestBlockId = getLatestBlockId();
  if (latestBlockId == 0 || blockId > latestBlockId) {
    return;
  }

  const auto lastReachableBlockId = getLastReachableBlockId();
  if (blockId > lastReachableBlockId) {
    const auto status = db_->del(DBKeyManipulator::generateSTTempBlockKey(blockId));
    if (!status.isOK() && !status.isNotFound()) {
      const auto msg = "Failed to delete a temporary state transfer block with ID = " + std::to_string(blockId) +
                       ", reason: " + status.toString();
      LOG_ERROR(logger_, msg);
      throw std::runtime_error{msg};
    }
    // Since we support receiving state transfer blocks in arbitrary order, we don't have a way of knowing which is the
    // next latest block after deleting one of them. Therefore, we load the latest one from DB.
    latestSTTempBlockId_ = loadLatestTempSTBlockId();
    return;
  }

  const auto genesisBlockId = getGenesisBlockId();
  if (blockId == lastReachableBlockId && blockId == genesisBlockId) {
    throw std::logic_error{"Deleting the only block in the system is not supported"};
  } else if (blockId == lastReachableBlockId) {
    deleteLastReachableBlock();
  } else if (blockId == genesisBlockId) {
    deleteGenesisBlock();
  } else {
    throw std::invalid_argument{"Cannot delete blocks in the middle of the blockchain"};
  }
}

void DBAdapter::deleteLastReachableBlock() {
  if (lastReachableBlockId_ == 0) {
    return;
  }

  deleteKeysForBlock(lastReachableBlockKeyDeletes(lastReachableBlockId_), lastReachableBlockId_);

  // Since we allow deletion of the only block left as last reachable (due to replica state sync), set both genesis and
  // last reachable cache variables to 0. Otherise, only decrement the last reachable block ID cache.
  if (lastReachableBlockId_ == genesisBlockId_) {
    genesisBlockId_ = 0;
    lastReachableBlockId_ = 0;
  } else {
    // Decrement the last reachable block ID cache.
    --lastReachableBlockId_;
  }
}

void DBAdapter::deleteGenesisBlock() {
  // We assume there are blocks in the system.
  ConcordAssertGE(genesisBlockId_, INITIAL_GENESIS_BLOCK_ID);
  // And we assume this is not the only block in the blockchain. That excludes ST temporary blocks as they are not yet
  // part of the blockchain.
  ConcordAssertNE(genesisBlockId_, lastReachableBlockId_);

  deleteKeysForBlock(genesisBlockKeyDeletes(genesisBlockId_), genesisBlockId_);

  // Increment the genesis block ID cache.
  ++genesisBlockId_;
}

block::detail::Node DBAdapter::getBlockNode(BlockId blockId) const {
  const auto blockNodeKey = DBKeyManipulator::genBlockDbKey(blockId);
  auto blockNodeSliver = Sliver{};
  const auto status = db_->get(blockNodeKey, blockNodeSliver);
  if (!status.isOK()) {
    const auto msg =
        "Failed to get block node for block ID = " + std::to_string(blockId) + ", reason: " + status.toString();
    LOG_ERROR(logger_, msg);
    throw std::runtime_error{msg};
  }
  return block::detail::parseNode(blockNodeSliver);
}

KeysVector DBAdapter::staleIndexNonProvableKeysForBlock(BlockId blockId) const {
  return keysForVersion(db_,
                        DBKeyManipulator::genNonProvableStaleDbKey(Key{}, blockId),
                        blockId,
                        EKeySubtype::NonProvableStale,
                        [](const Key &key) { return DBKeyManipulator::extractBlockIdFromNonProvableStaleKey(key); });
}

KeysVector DBAdapter::staleIndexProvableKeysForVersion(const Version &version) const {
  // Rely on the fact that stale keys are ordered lexicographically by version and keys with a version only precede any
  // real ones (as they are longer). Note that version-only keys don't exist in the DB and we just use them as a
  // placeholder for the search. See stale key generation code.
  return keysForVersion(
      db_, DBKeyManipulator::genStaleDbKey(version), version, EKeySubtype::ProvableStale, [](const Key &key) {
        return DBKeyManipulator::extractVersionFromProvableStaleKey(key);
      });
}

KeysVector DBAdapter::internalProvableKeysForVersion(const Version &version) const {
  // Rely on the fact that root internal keys always precede non-root ones - due to lexicographical ordering and root
  // internal keys having empty nibble paths. See InternalNodeKey serialization code.
  return keysForVersion(db_,
                        DBKeyManipulator::genInternalDbKey(InternalNodeKey::root(version)),
                        version,
                        EKeySubtype::Internal,
                        [](const Key &key) { return DBKeyManipulator::extractVersionFromInternalKey(key); });
}

KeysVector DBAdapter::lastReachableBlockKeyDeletes(BlockId blockId) const {
  const auto blockNode = getBlockNode(blockId);
  auto keysToDelete = KeysVector{};

  const auto [provableKeys, nonProvableKeys] = splitToProvableAndNonProvable(blockNode.keys, nonProvableKeySet_);
  // Delete leaf keys at the last version.
  for (const auto &key : provableKeys) {
    keysToDelete.push_back(DBKeyManipulator::genDataDbKey(key.first, blockNode.stateRootVersion));
  }

  // Delete non-provable keys.
  for (const auto &key : nonProvableKeys) {
    keysToDelete.push_back(DBKeyManipulator::genNonProvableDbKey(blockId, key.first));
  }

  // Delete internal keys at the last version.
  add(keysToDelete, internalProvableKeysForVersion(blockNode.stateRootVersion));

  // Delete the block node key.
  keysToDelete.push_back(DBKeyManipulator::genBlockDbKey(blockId));

  // Clear the tree stale index for the last version.
  add(keysToDelete, staleIndexProvableKeysForVersion(blockNode.stateRootVersion));

  // Clear the non-provable stale index for the last block
  add(keysToDelete, staleIndexNonProvableKeysForBlock(blockId));

  return keysToDelete;
}

KeysVector DBAdapter::genesisBlockKeyDeletes(BlockId blockId) const {
  const auto blockNode = getBlockNode(blockId);
  auto keysToDelete = KeysVector{};

  // Delete the block node key.
  keysToDelete.push_back(DBKeyManipulator::genBlockDbKey(blockId));

  // Delete stale keys.
  const auto staleKeys = staleIndexProvableKeysForVersion(blockNode.stateRootVersion);
  for (const auto &staleKey : staleKeys) {
    keysToDelete.push_back(DBKeyManipulator::extractKeyFromProvableStaleKey(staleKey));
  }

  // Clear the stale index for the last version.
  add(keysToDelete, staleKeys);

  // Clear the non-provable stale index for the last version.
  const auto nonProvableStaleKeys = staleIndexNonProvableKeysForBlock(blockId);
  add(keysToDelete, nonProvableStaleKeys);

  // Delete stale keys.
  for (const auto &key : nonProvableStaleKeys) {
    keysToDelete.push_back(DBKeyManipulator::extractKeyFromNonProvableStaleKey(key));
  }

  return keysToDelete;
}

std::optional<std::pair<Key, Value>> DBAdapter::getLeafKeyValAtMostVersion(
    const Key &key, const sparse_merkle::Version &version) const {
  TimeRecorder scoped_timer(*histograms.dba_get_leaf_key_val_at_most_version);
  auto iter = db_->getIteratorGuard();
  const auto [foundKey, foundValue] = iter->seekAtMost(DBKeyManipulator::genDataDbKey(key, version));
  if (!foundKey.empty() && DBKeyManipulator::getDBKeyType(foundKey) == EDBKeyType::Key &&
      DBKeyManipulator::getKeySubtype(foundKey) == EKeySubtype::Leaf &&
      DBKeyManipulator::extractHashFromLeafKey(foundKey) == hash(key)) {
    return std::make_pair(foundKey, foundValue);
  }
  return std::nullopt;
}

void DBAdapter::deleteKeysForBlock(const KeysVector &keys, BlockId blockId) const {
  TimeRecorder scoped_timer(*histograms.dba_delete_keys_for_block);
  const auto status = db_->multiDel(keys);
  if (!status.isOK()) {
    const auto msg =
        "Failed to delete blockchain block with ID = " + std::to_string(blockId) + ", reason: " + status.toString();
    LOG_ERROR(logger_, msg);
    throw std::runtime_error{msg};
  }
}

SetOfKeyValuePairs DBAdapter::getBlockData(const RawBlock &rawBlock) const { return block::detail::getData(rawBlock); }

BlockDigest DBAdapter::getParentDigest(const RawBlock &rawBlock) const {
  return block::detail::getParentDigest(rawBlock);
}

bool DBAdapter::hasBlock(const BlockId &blockId) const {
  TimeRecorder scoped_timer(*histograms.dba_has_block);
  const auto statusNode = db_->has(DBKeyManipulator::genBlockDbKey(blockId));
  if (statusNode.isNotFound()) {
    const auto statusSt = db_->has(DBKeyManipulator::generateSTTempBlockKey(blockId));
    if (statusSt.isNotFound()) {
      return false;
    } else if (!statusSt.isOK()) {
      throw std::runtime_error{"Failed to check for existence of temporary ST block ID = " + std::to_string(blockId)};
    }
  } else if (!statusNode.isOK()) {
    throw std::runtime_error{"Failed to check for existence of block node for block ID = " + std::to_string(blockId)};
  }
  return true;
}

}  // namespace concord::kvbc::v2MerkleTree
