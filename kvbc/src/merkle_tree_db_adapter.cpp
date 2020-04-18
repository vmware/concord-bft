// Copyright 2020 VMware, all rights reserved

#include <assertUtils.hpp>
#include "bcstatetransfer/SimpleBCStateTransfer.hpp"
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
#include <iterator>
#include <limits>
#include <memory>
#include <utility>

namespace concord::kvbc::v2MerkleTree {

namespace {

using namespace ::std::string_literals;

using ::concordUtils::fromBigEndianBuffer;
using ::concordUtils::Sliver;
using ::concordUtils::Status;
using ::concordUtils::HexPrintBuffer;

using ::concord::storage::IDBClient;
using ::concord::storage::ObjectId;

using ::bftEngine::SimpleBlockchainStateTransfer::computeBlockDigest;

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
    updates[key] = serialize(intNode);
  }

  // Leaf nodes.
  for (const auto &[leafKey, leafNode] : batch.leaf_nodes) {
    const auto key = DBKeyManipulator::genDataDbKey(leafKey);
    updates[key] = detail::serialize(detail::DatabaseLeafValue{blockId, leafNode});
  }

  return updates;
}

auto hash(const Sliver &buf) {
  auto hasher = Hasher{};
  return hasher.hash(buf.data(), buf.length());
}

template <typename VersionExtractor>
KeysVector keysForVersion(const std::shared_ptr<IDBClient> &db,
                          const Key &firstKey,
                          const Version &version,
                          EKeySubtype keySubtype,
                          const VersionExtractor &extractVersion) {
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

}  // namespace

Key DBKeyManipulator::genBlockDbKey(BlockId version) { return serialize(EDBKeyType::Block, version); }

Key DBKeyManipulator::genDataDbKey(const LeafKey &key) { return serialize(EKeySubtype::Leaf, key); }

Key DBKeyManipulator::genDataDbKey(const Key &key, const Version &version) {
  auto hasher = Hasher{};
  return genDataDbKey(LeafKey{hasher.hash(key.data(), key.length()), version});
}

Key DBKeyManipulator::genInternalDbKey(const InternalNodeKey &key) { return serialize(EKeySubtype::Internal, key); }

Key DBKeyManipulator::genStaleDbKey(const InternalNodeKey &key, const Version &staleSinceVersion) {
  return serialize(EKeySubtype::Stale, staleSinceVersion.value(), EKeySubtype::Internal, key);
}

Key DBKeyManipulator::genStaleDbKey(const LeafKey &key, const Version &staleSinceVersion) {
  return serialize(EKeySubtype::Stale, staleSinceVersion.value(), EKeySubtype::Leaf, key);
}

Key DBKeyManipulator::genStaleDbKey(const Version &staleSinceVersion) {
  return serialize(EKeySubtype::Stale, staleSinceVersion.value());
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
  Assert(DBKeyManipulator::getDBKeyType(key) == EDBKeyType::Key);
  Assert(DBKeyManipulator::getKeySubtype(key) == EKeySubtype::Leaf);
  return Hash{reinterpret_cast<const uint8_t *>(key.data() + keyTypeOffset)};
}

Version DBKeyManipulator::extractVersionFromStaleKey(const Key &key) {
  constexpr auto keyTypeOffset = sizeof(EDBKeyType) + sizeof(EKeySubtype);
  Assert(key.length() >= keyTypeOffset + Version::SIZE_IN_BYTES);
  Assert(DBKeyManipulator::getDBKeyType(key) == EDBKeyType::Key);
  Assert(DBKeyManipulator::getKeySubtype(key) == EKeySubtype::Stale);
  return fromBigEndianBuffer<Version::Type>(key.data() + keyTypeOffset);
}

Key DBKeyManipulator::extractKeyFromStaleKey(const Key &key) {
  constexpr auto keyOffset = sizeof(EDBKeyType) + sizeof(EKeySubtype) + Version::SIZE_IN_BYTES;
  Assert(key.length() > keyOffset);
  Assert(DBKeyManipulator::getDBKeyType(key) == EDBKeyType::Key);
  Assert(DBKeyManipulator::getKeySubtype(key) == EKeySubtype::Stale);
  return Key{key, keyOffset, key.length() - keyOffset};
}

Version DBKeyManipulator::extractVersionFromInternalKey(const Key &key) {
  constexpr auto keyOffset = sizeof(EDBKeyType) + sizeof(EKeySubtype);
  Assert(key.length() > keyOffset);
  return deserialize<InternalNodeKey>(Sliver{key, keyOffset, key.length() - keyOffset}).version();
}

// Undefined behavior if an incorrect type is read from the buffer.
EDBKeyType DBKeyManipulator::getDBKeyType(const Sliver &s) {
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
EKeySubtype DBKeyManipulator::getKeySubtype(const Sliver &s) {
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
EBFTSubtype DBKeyManipulator::getBftSubtype(const Sliver &s) {
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

DBAdapter::DBAdapter(const std::shared_ptr<IDBClient> &db)
    : logger_{concordlogger::Log::getLogger("concord.kvbc.v2MerkleTree.DBAdapter")},
      // The smTree_ member needs an initialized DB. Therefore, do that in the initializer list before constructing
      // smTree_ .
      db_{db},
      smTree_{std::make_shared<Reader>(*this)} {
  // Make sure that if linkSTChainFrom() has been interrupted (e.g. a crash or an abnormal shutdown), all DBAdapter
  // methods will return the correct values. For example, if state transfer had completed and linkSTChainFrom() was
  // interrupted, getLatestBlockId() should be equal to getLastReachableBlockId() on the next startup. Another example
  // is getKeyByReadVersion() that returns keys from the blockchain only and ignores keys in the temporary state
  // transfer chain.
  linkSTChainFrom(getLastReachableBlockId() + 1);
}

std::pair<Value, BlockId> DBAdapter::getValue(const Key &key, const BlockId &blockVersion) const {
  auto stateRootVersion = Version{};
  // Find a block with an ID that is less than or equal to the requested block version and extract the state root
  // version from it.
  {
    const auto blockKey = DBKeyManipulator::genBlockDbKey(blockVersion);
    auto iter = db_->getIteratorGuard();
    const auto [foundBlockKey, foundBlockValue] = iter->seekAtMost(blockKey);
    if (!foundBlockKey.empty() && DBKeyManipulator::getDBKeyType(foundBlockKey) == EDBKeyType::Block) {
      stateRootVersion = detail::deserializeStateRootVersion(foundBlockValue);
    } else {
      throw NotFoundException{"Couldn't find a value by key and block version = " + std::to_string(blockVersion)};
    }
  }

  auto iter = db_->getIteratorGuard();
  const auto leafKey = DBKeyManipulator::genDataDbKey(key, stateRootVersion);

  // Seek for a leaf key with a version that is less than or equal to the state root version from the found block.
  // Since leaf keys are ordered lexicographically, first by hash and then by state root version, then if there is a key
  // having the same hash and a lower version, it should directly precede the found one.
  const auto [foundKey, foundValue] = iter->seekAtMost(leafKey);
  // Make sure we only process leaf keys that have the same hash as the hash of the key the user has passed. The state
  // root version can be less than the one we seek for.
  const auto isLeafKey = !foundKey.empty() && DBKeyManipulator::getDBKeyType(foundKey) == EDBKeyType::Key &&
                         DBKeyManipulator::getKeySubtype(foundKey) == EKeySubtype::Leaf;
  if (isLeafKey && DBKeyManipulator::extractHashFromLeafKey(foundKey) == hash(key)) {
    const auto dbLeafVal = detail::deserialize<detail::DatabaseLeafValue>(foundValue);
    // Return the value at the block version it was written at, even if the block itself has been deleted.
    return std::make_pair(dbLeafVal.leafNode.value, dbLeafVal.blockId);
  }

  throw NotFoundException{"Couldn't find a value by key and block version = " + std::to_string(blockVersion)};
}

BlockId DBAdapter::getGenesisBlockId() const {
  auto iter = db_->getIteratorGuard();
  const auto key = iter->seekAtLeast(DBKeyManipulator::genBlockDbKey(INITIAL_GENESIS_BLOCK_ID)).first;
  if (!key.empty() && DBKeyManipulator::getDBKeyType(key) == EDBKeyType::Block) {
    return DBKeyManipulator::extractBlockIdFromKey(key);
  }
  return 0;
}

BlockId DBAdapter::getLastReachableBlockId() const {
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

BlockId DBAdapter::getLatestBlockId() const {
  const auto latestBlockKey = DBKeyManipulator::generateSTTempBlockKey(MAX_BLOCK_ID);
  auto iter = db_->getIteratorGuard();
  const auto foundKey = iter->seekAtMost(latestBlockKey).first;
  if (!foundKey.empty() && DBKeyManipulator::getDBKeyType(foundKey) == EDBKeyType::BFT &&
      DBKeyManipulator::getBftSubtype(foundKey) == EBFTSubtype::STTempBlock) {
    const auto blockId = DBKeyManipulator::extractBlockIdFromKey(foundKey);
    LOG_TRACE(logger_, "Latest block ID " << blockId);
    return blockId;
  }
  // No state transfer blocks in the system. Fallback to getLastReachableBlockId() .
  return getLastReachableBlockId();
}

Sliver DBAdapter::createBlockNode(const SetOfKeyValuePairs &updates, BlockId blockId) const {
  // Make sure the digest is zero-initialized by using {} initialization.
  auto parentBlockDigest = BlockDigest{};
  if (blockId > INITIAL_GENESIS_BLOCK_ID) {
    const auto parentBlock = getRawBlock(blockId - 1);
    parentBlockDigest = computeBlockDigest(blockId - 1, parentBlock.data(), parentBlock.length());
  }

  auto node = block::detail::Node{blockId, parentBlockDigest, smTree_.get_root_hash(), smTree_.get_version()};
  for (const auto &[k, v] : updates) {
    // Treat empty values as deleted keys.
    node.keys.emplace(k, block::detail::KeyData{v.empty()});
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
      if (const auto status = db_->get(DBKeyManipulator::genDataDbKey(key, blockNode.stateRootVersion), value);
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

  return block::detail::create(keyValues, blockNode.parentDigest, blockNode.stateHash);
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
  const auto addDbUpdates = lastReachableBlockDbUpdates(getBlockData(block), blockId);
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
    addBlock(getBlockData(block));

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

void DBAdapter::deleteBlock(const BlockId &blockId) {
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
    return;
  }

  auto keysToDelete = KeysVector{};
  const auto genesisBlockId = getGenesisBlockId();
  if (blockId == lastReachableBlockId && blockId == genesisBlockId) {
    throw std::logic_error{"Deleting the only block in the system is not supported"};
  } else if (blockId == lastReachableBlockId) {
    keysToDelete = lastReachableBlockKeyDeletes(blockId);
  } else if (blockId == genesisBlockId) {
    keysToDelete = genesisBlockKeyDeletes(blockId);
  } else {
    throw std::invalid_argument{"Cannot delete blocks in the middle of the blockchain"};
  }

  const auto status = db_->multiDel(keysToDelete);
  if (!status.isOK()) {
    const auto msg =
        "Failed to delete blockchain block with ID = " + std::to_string(blockId) + ", reason: " + status.toString();
    LOG_ERROR(logger_, msg);
    throw std::runtime_error{msg};
  }
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

KeysVector DBAdapter::staleIndexKeysForVersion(const Version &version) const {
  // Rely on the fact that stale keys are ordered lexicographically by version and keys with a version only precede any
  // real ones (as they are longer). Note that version-only keys don't exist in the DB and we just use them as a
  // placeholder for the search. See stale key generation code.
  return keysForVersion(db_, DBKeyManipulator::genStaleDbKey(version), version, EKeySubtype::Stale, [](const Key &key) {
    return DBKeyManipulator::extractVersionFromStaleKey(key);
  });
}

KeysVector DBAdapter::internalKeysForVersion(const Version &version) const {
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

  // Delete leaf keys at the last version.
  for (const auto &key : blockNode.keys) {
    keysToDelete.push_back(DBKeyManipulator::genDataDbKey(key.first, blockNode.stateRootVersion));
  }

  // Delete internal keys at the last version.
  add(keysToDelete, internalKeysForVersion(blockNode.stateRootVersion));

  // Delete the block node key.
  keysToDelete.push_back(DBKeyManipulator::genBlockDbKey(blockId));

  // Clear the stale index for the last version.
  add(keysToDelete, staleIndexKeysForVersion(blockNode.stateRootVersion));

  return keysToDelete;
}

KeysVector DBAdapter::genesisBlockKeyDeletes(BlockId blockId) const {
  const auto blockNode = getBlockNode(blockId);
  auto keysToDelete = KeysVector{};

  // Delete the block node key.
  keysToDelete.push_back(DBKeyManipulator::genBlockDbKey(blockId));

  // Delete stale keys.
  const auto staleKeys = staleIndexKeysForVersion(blockNode.stateRootVersion);
  for (const auto &staleKey : staleKeys) {
    keysToDelete.push_back(DBKeyManipulator::extractKeyFromStaleKey(staleKey));
  }

  // Clear the stale index for the last version.
  add(keysToDelete, staleKeys);

  return keysToDelete;
}

SetOfKeyValuePairs DBAdapter::getBlockData(const RawBlock &rawBlock) const { return block::detail::getData(rawBlock); }

BlockDigest DBAdapter::getParentDigest(const RawBlock &rawBlock) const {
  return block::detail::getParentDigest(rawBlock);
}

bool DBAdapter::hasBlock(const BlockId &blockId) const {
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
