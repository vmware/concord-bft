// Copyright 2018 VMware, all rights reserved

// @file direct_kv_db_adapter.cpp
//
// Data is stored in the form of key value pairs. However, the key used is a
// composite database key. Its composition is : Key Type | Key | Block Id
//
// Block Id is functionally equivalent to the version of the block.
//
// For the purposes of iteration, the keys appear in the following order:
//    Ascending order of Key Type
//    -> Ascending order of Key
//       -> Descending order of Block Id

#include "Logger.hpp"
#include "sliver.hpp"
#include "status.hpp"
#include "kv_types.hpp"
#include "direct_kv_block.h"
#include "direct_kv_db_adapter.h"
#include "bcstatetransfer/SimpleBCStateTransfer.hpp"
#include "hex_tools.h"
#include "sha_hash.hpp"
#include "string.hpp"
#include "assertUtils.hpp"
#include "categorization/kv_blockchain.h"
#include "v4blockchain/v4_blockchain.h"

#include <string.h>

#include <cstring>
#include <exception>
#include <limits>
#include <string>
#include <utility>
#include <sstream>
#include <iomanip>
#include <mutex>

using logging::Logger;
using concordUtils::Status;
using concordUtils::Sliver;
using concord::storage::ObjectId;
using concordUtils::HexPrintBuffer;

namespace concord::kvbc::v1DirectKeyValue {

using storage::v1DirectKeyValue::detail::EDBKeyType;

/**
 * @brief Helper function that generates a composite database Key of type Block.
 *
 * For such Database keys, the "key" component is an empty sliver.
 *
 * @param _blockid BlockId object of the block id that needs to be
 *                 incorporated into the composite database key.
 * @return Sliver object of the generated composite database key.
 */
Key RocksKeyGenerator::blockKey(const BlockId &blockId) const {
  return genDbKey(EDBKeyType::E_DB_KEY_TYPE_BLOCK, Sliver(), blockId);
}

/**
 * @brief Helper function that generates a composite database Key of type Key.
 *
 * @param _key Sliver object of the "key" component.
 * @param _blockid BlockId object of the block id that needs to be incorporated
 *                 into the composite database Key.
 * @return Sliver object of the generated composite database key.
 */
Key RocksKeyGenerator::dataKey(const Key &key, const BlockId &blockId) const {
  return genDbKey(EDBKeyType::E_DB_KEY_TYPE_KEY, key, blockId);
}

Key S3KeyGenerator::blockKey(const BlockId &blockId) const {
  LOG_DEBUG(logger(), prefix_ + std::string("blocks/") + std::to_string(blockId));
  return prefix_ + std::string("blocks/") + std::to_string(blockId);
}

Key S3KeyGenerator::dataKey(const Key &key, const BlockId &blockId) const {
  auto digest = concord::util::SHA3_256().digest(key.data(), key.length());
  auto digest_str = concord::util::SHA3_256::toHexString(digest);
  LOG_DEBUG(logger(), prefix_ + std::string("keys/") + digest_str);
  return prefix_ + std::string("keys/") + digest_str;
}

Key S3KeyGenerator::mdtKey(const Key &key) const {
  LOG_DEBUG(logger(), prefix_ + std::string("metadata/") + key.toString());
  return prefix_ + std::string("metadata/") + key.toString();
}

std::string S3KeyGenerator::string2hex(const std::string &s) {
  std::ostringstream oss;
  for (size_t i = 0; i < s.length(); i++) oss << std::hex << std::setw(2) << std::setfill('0') << (uint)s[i];
  return oss.str();
}
std::string S3KeyGenerator::hex2string(const std::string &s) {
  std::string result;
  result.reserve(s.length() / 2);
  for (size_t i = 0; i < s.length(); i += 2) result.push_back(std::stoi(s.substr(i, 2).c_str(), NULL, 16));
  return result;
}

/*
 * If key a is smaller than key b, return a negative number; if larger, return a
 * positive number; if equal, return zero.
 *
 * Comparison is done by decomposed parts. Types are compared first, followed by
 * type specific comparison. */
int DBKeyComparator::composedKeyComparison(const char *_a_data,
                                           size_t _a_length,
                                           const char *_b_data,
                                           size_t _b_length) {
  EDBKeyType aType = DBKeyManipulator::extractTypeFromKey(_a_data);
  EDBKeyType bType = DBKeyManipulator::extractTypeFromKey(_b_data);
  if (aType != bType) return (int)aType - (int)bType;

  switch (aType) {
    case EDBKeyType::E_DB_KEY_TYPE_BFT_ST_KEY:
    case EDBKeyType::E_DB_KEY_TYPE_BFT_ST_PENDING_PAGE_KEY:
    case EDBKeyType::E_DB_KEY_TYPE_BFT_METADATA_KEY: {
      // Compare object IDs.
      ObjectId aObjId = DBKeyManipulator::extractObjectIdFromKey(_a_data, _a_length);
      ObjectId bObjId = DBKeyManipulator::extractObjectIdFromKey(_b_data, _b_length);
      return (aObjId > bObjId) ? 1 : (bObjId > aObjId) ? -1 : 0;
    }
    case EDBKeyType::E_DB_KEY_TYPE_BFT_ST_CHECKPOINT_DESCRIPTOR_KEY: {
      uint64_t aChkpt, bChkpt;
      aChkpt = storage::v1DirectKeyValue::STKeyManipulator::extractCheckPointFromKey(_a_data, _a_length);
      bChkpt = storage::v1DirectKeyValue::STKeyManipulator::extractCheckPointFromKey(_b_data, _b_length);
      return (aChkpt > bChkpt) ? 1 : (bChkpt > aChkpt) ? -1 : 0;
    }
    case EDBKeyType::E_DB_KEY_TYPE_BFT_ST_RESERVED_PAGE_DYNAMIC_KEY: {
      // Pages are sorted in ascending order, checkpoints in ascending order
      uint32_t aPageId, bPageId;
      uint64_t aChkpt, bChkpt;
      std::tie(aPageId, aChkpt) =
          storage::v1DirectKeyValue::STKeyManipulator::extractPageIdAndCheckpointFromKey(_a_data, _a_length);
      std::tie(bPageId, bChkpt) =
          storage::v1DirectKeyValue::STKeyManipulator::extractPageIdAndCheckpointFromKey(_b_data, _b_length);
      if (aPageId != bPageId) return (aPageId > bPageId) ? 1 : (bPageId > aPageId) ? -1 : 0;
      return (aChkpt < bChkpt) ? -1 : (aChkpt > bChkpt) ? 1 : 0;
    }
    case EDBKeyType::E_DB_KEY_TYPE_KEY: {
      int keyComp = DBKeyManipulator::compareKeyPartOfComposedKey(_a_data, _a_length, _b_data, _b_length);
      if (keyComp != 0) return keyComp;
      // Extract the block ids to compare so that endianness of environment does not matter.
      BlockId aId = DBKeyManipulator::extractBlockIdFromKey(_a_data, _a_length);
      BlockId bId = DBKeyManipulator::extractBlockIdFromKey(_b_data, _b_length);
      // Block ids are sorted in reverse order when part of a composed key (key + blockId)
      return (bId > aId) ? 1 : (aId > bId) ? -1 : 0;
    }
    case EDBKeyType::E_DB_KEY_TYPE_BLOCK: {
      // Extract the block ids to compare so that endianness of environment does not matter.
      BlockId aId = DBKeyManipulator::extractBlockIdFromKey(_a_data, _a_length);
      BlockId bId = DBKeyManipulator::extractBlockIdFromKey(_b_data, _b_length);
      // Block ids are sorted in ascending order when part of a block key
      return (aId > bId) ? 1 : (bId > aId) ? -1 : 0;
    }
    default:
      LOG_ERROR(logger(), "invalid key type: " << (char)aType);
      ConcordAssert(false);
      throw std::runtime_error("DBKeyComparator::composedKeyComparison: invalid key type");
  }  // switch
}

/**
 * @brief Generates a Composite Database Key from a Sliver object.
 *
 * Merges the key type, data of the key and the block id to generate a composite
 * database key.
 *
 * Format : Key Type | Key | Block Id

 * @param _type Composite Database key type (Either of First, Key, Block or
 *              Last).
 * @param _key Sliver object of the key.
 * @param _blockId BlockId object.
 * @return Sliver object of the generated composite database key.
 */
Sliver RocksKeyGenerator::genDbKey(EDBKeyType _type, const Key &_key, BlockId _blockId) {
  size_t sz = sizeof(EDBKeyType) + sizeof(BlockId) + _key.length();
  char *out = new char[sz];
  size_t offset = 0;
  copyToAndAdvance(out, &offset, sz, (char *)&_type, sizeof(EDBKeyType));
  copyToAndAdvance(out, &offset, sz, (char *)_key.data(), _key.length());
  copyToAndAdvance(out, &offset, sz, (char *)&_blockId, sizeof(BlockId));
  return Sliver(out, sz);
}

/**
 * @brief Extracts the Block Id of a composite database key.
 *
 * Returns the block id part of the Sliver object passed as a parameter.
 *
 * @param _key The Sliver object of the composite database key whose block id
 *             gets returned.
 * @return The block id of the composite database key.
 */
BlockId DBKeyManipulator::extractBlockIdFromKey(const Key &_key) {
  return extractBlockIdFromKey(_key.data(), _key.length());
}

/**
 * @brief Extracts the Block Id of a composite database key.
 *
 * Returns the block id part of the buffer passed as a parameter.
 *
 * @param _key_data The buffer containing the composite database key whose block id
 *                  gets returned.
 * @param _key_length The number of bytes in the _key_data buffer.
 * @return The block id of the composite database key.
 */
BlockId DBKeyManipulator::extractBlockIdFromKey(const char *_key_data, size_t _key_length) {
  ConcordAssert(_key_length >= sizeof(BlockId));
  const auto offset = _key_length - sizeof(BlockId);
  const auto id = *reinterpret_cast<const BlockId *>(_key_data + offset);

  LOG_TRACE(logger(),
            "Got block ID " << id << " from key " << (HexPrintBuffer{_key_data, _key_length}) << ", offset " << offset);
  return id;
}

/**
 * @brief Extracts the type of a composite database key.
 *
 * Returns the data part of the Sliver object passed as a parameter.
 *
 * @param _key The Sliver object of the composite database key whose type gets
 *             returned.
 * @return The type of the composite database key.
 */
EDBKeyType DBKeyManipulator::extractTypeFromKey(const Key &_key) {
  ConcordAssert(!_key.empty());
  return extractTypeFromKey(_key.data());
}

/**
 * @brief Extracts the type of a composite database key.
 *
 * Returns the data part of the buffer passed as a parameter.
 *
 * @param _key_data The buffer containing the composite database key whose
 *                  type gets returned.
 * @return The type of the composite database key.
 */
EDBKeyType DBKeyManipulator::extractTypeFromKey(const char *_key_data) {
  static_assert(sizeof(EDBKeyType) == 1, "Let's avoid byte-order problems.");
  ConcordAssert((_key_data[0] < (char)EDBKeyType::E_DB_KEY_TYPE_LAST) &&
                (_key_data[0] >= (char)EDBKeyType::E_DB_KEY_TYPE_FIRST));
  return (EDBKeyType)_key_data[0];
}

/**
 * @brief Extracts the Metadata Object Id of a composite database key.
 *
 * Returns the object id part of the Sliver object passed as a parameter.
 *
 * @param _key The Sliver object of the composite database key whose object id
 *             gets returned.
 * @return The object id of the composite database key.
 */
ObjectId DBKeyManipulator::extractObjectIdFromKey(const Key &_key) {
  return extractObjectIdFromKey(_key.data(), _key.length());
}

/**
 * @brief Extracts the Metadata Object Id of a composite database key.
 *
 * Returns the object id part of the buffer passed as a parameter.
 *
 * @param _key_data The buffer containing the composite database key whose object id
 *                  gets returned.
 * @param _key_length The number of bytes in the _key_data buffer.
 * @return The object id of the composite database key.
 */
ObjectId DBKeyManipulator::extractObjectIdFromKey(const char *_key_data, size_t _key_length) {
  ConcordAssert(_key_length >= sizeof(ObjectId));
  size_t offset = _key_length - sizeof(ObjectId);
  ObjectId id = *(ObjectId *)(_key_data + offset);

  LOG_TRACE(
      logger(),
      "Got object ID " << id << " from key " << (HexPrintBuffer{_key_data, _key_length}) << ", offset " << offset);
  return id;
}

/**
 * @brief Extracts the key from a composite database key.
 *
 * @param _composedKey Sliver object of the composite database key.
 * @return Sliver object of the key extracted from the composite database key.
 */
Sliver DBKeyManipulator::extractKeyFromKeyComposedWithBlockId(const Key &_composedKey) {
  size_t sz = _composedKey.length() - sizeof(BlockId) - sizeof(EDBKeyType);
  Sliver out = Sliver(_composedKey, sizeof(EDBKeyType), sz);
  LOG_TRACE(logger(), "Got key " << out << " from composed key " << _composedKey);
  return out;
}

/**
 * @brief Compare the part of the composed key that would have been returned
 * from extractKeyFromKeyComposedWithBlockId.
 *
 * This is an optimization, to avoid creating sub-Slivers that will be destroyed
 * immediately in the RocksKeyComparator.
 *
 * @param a_data Pointer to buffer containing a composed key.
 * @param a_length Number of bytes in a_data.
 * @param b_data Pointer to buffer containing a composed key.
 * @param b_length Number of bytes in b_data.
 */
int DBKeyManipulator::compareKeyPartOfComposedKey(const char *a_data,
                                                  size_t a_length,
                                                  const char *b_data,
                                                  size_t b_length) {
  ConcordAssert(a_length >= sizeof(BlockId) + sizeof(EDBKeyType));
  ConcordAssert(b_length >= sizeof(BlockId) + sizeof(EDBKeyType));

  const char *a_key = a_data + sizeof(EDBKeyType);
  const size_t a_key_length = a_length - sizeof(BlockId) - sizeof(EDBKeyType);
  const char *b_key = b_data + sizeof(EDBKeyType);
  const size_t b_key_length = b_length - sizeof(BlockId) - sizeof(EDBKeyType);

  int result = memcmp(a_key, b_key, std::min(a_key_length, b_key_length));
  if (a_key_length == b_key_length) {
    return result;
  }

  if (result == 0) {
    if (a_key_length < b_key_length) {
      return -1;
    } else {
      return 1;
    }
  }

  return result;
}

Key DBKeyManipulator::extractKeyFromMetadataKey(const Key &_composedKey) {
  size_t sz = _composedKey.length() - sizeof(EDBKeyType);
  Sliver out = Sliver(_composedKey, sizeof(EDBKeyType), sz);
  LOG_TRACE(logger(), "Got metadata key " << out << " from composed key " << _composedKey);
  return out;
}

bool DBKeyManipulator::isKeyContainBlockId(const Key &_composedKey) {
  return (_composedKey.length() > sizeof(BlockId) + sizeof(EDBKeyType));
}

/**
 * @brief Converts a key value pair consisting using a composite database key
 * into a key value pair using a "simple" key - one without the key type
 * included.
 *
 * @param _p Key value pair consisting of a composite database key
 * @return Key value pair consisting of a simple key.
 */
KeyValuePair DBKeyManipulator::composedToSimple(KeyValuePair _p) {
  if (_p.first.length() == 0) {
    return _p;
  }
  Key key;
  if (isKeyContainBlockId(_p.first))
    key = extractKeyFromKeyComposedWithBlockId(_p.first);
  else
    key = extractKeyFromMetadataKey(_p.first);

  return KeyValuePair(key, _p.second);
}

DBAdapter::DBAdapter(std::shared_ptr<storage::IDBClient> db,
                     std::unique_ptr<IDataKeyGenerator> keyGen,
                     bool use_mdt,
                     bool save_kv_pairs_separately,
                     const std::shared_ptr<concord::performance::PerformanceManager> &pm)
    : logger_{logging::getLogger("concord.kvbc.v1DirectKeyValue.DBAdapter")},
      db_(db),
      keyGen_{std::move(keyGen)},
      mdt_{use_mdt},
      lastBlockId_{fetchLatestBlockId()},
      lastReachableBlockId_{fetchLastReachableBlockId()},
      saveKvPairsSeparately_{save_kv_pairs_separately},
      pm_{pm} {}

BlockId DBAdapter::addBlock(const SetOfKeyValuePairs &kv) {
  BlockId blockId = getLastReachableBlockId() + 1;
  // Make sure the digest is zero-initialized by using {} initialization.
  auto blockDigest = BlockDigest{};
  if (blockId > INITIAL_GENESIS_BLOCK_ID) {
    const auto parentBlockData = getRawBlock(blockId - 1);
    blockDigest = bftEngine::bcst::computeBlockDigest(
        blockId - 1, reinterpret_cast<const char *>(parentBlockData.data()), parentBlockData.length());
  }

  SetOfKeyValuePairs outKv;
  const auto block = block::detail::create(kv, outKv, blockDigest);
  pm_->Delay<concord::performance::SlowdownPhase::StorageBeforeDbWrite>(outKv);
  if (Status s = addBlockAndUpdateMultiKey(outKv, blockId, block); !s.isOK())
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": failed: ") + s.toString());

  setLatestBlock(blockId);
  setLastReachableBlockNum(blockId);

  return blockId;
}

void DBAdapter::addRawBlock(const RawBlock &block, const BlockId &blockId, bool lastBlock) {
  SetOfKeyValuePairs keys;
  if (saveKvPairsSeparately_ && block.length() > 0) {
    std::optional<concord::kvbc::categorization::CategoryInput> categoryInput;
    std::string strBlockId = std::to_string(blockId);
    concordUtils::Sliver slivBlockId = concordUtils::Sliver::copy(strBlockId.data(), strBlockId.length());
    if (concord::kvbc::BlockVersion::getBlockVersion(block) == concord::kvbc::block_version::V1) {
      auto parsedBlock = concord::kvbc::v4blockchain::detail::Block(block.string_view());
      categoryInput.emplace(parsedBlock.getUpdates().categoryUpdates());
    } else {
      auto parsedBlock = concord::kvbc::categorization::RawBlock::deserialize(block);
      categoryInput.emplace(parsedBlock.data.updates);
    }
    if (categoryInput.has_value()) {
      for (auto &[key, value] : categoryInput->kv) {
        LOG_DEBUG(logger_, KVLOG(key, blockId));
        std::visit(
            [this, &keys, slivBlockId](auto &&arg) {
              for (auto &[k, v] : arg.kv) {
                LOG_DEBUG(logger_, KVLOG(k));
                concordUtils::Sliver sKey = Sliver::copy(k.data(), k.length());
                keys[sKey] = slivBlockId;
                (void)v;
              }
            },
            value);
      }
    }
  }

  if (Status s = addBlockAndUpdateMultiKey(keys, blockId, block); !s.isOK()) {
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": failed: blockId: ") + std::to_string(blockId) +
                             std::string(" reason: ") + s.toString());
  }

  // when ST runs, blocks arrive in batches in reverse order. we need to keep
  // track on the "Gap" and close it. There might be temporary multiple gaps due to the way blocks are committed.
  // As long as we keep track of LatestBlockId and LastReachableBlockId, we will be able to survive any crash.
  // When all gaps are closed, the last reachable block becomes the same as the last block.
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto latestBlockId = getLatestBlockId();
    if (blockId > latestBlockId) {
      setLatestBlock(blockId);
      latestBlockId = blockId;
    }
    auto i = blockId;
    if (i == (getLastReachableBlockId() + 1)) {
      do {
        setLastReachableBlockNum(i);
        ++i;
      } while ((i <= latestBlockId) && hasBlock(i));
      LOG_TRACE(logger_, "Connected blocks [" << blockId << " -> " << i << "]");
    }
  }
}

Status DBAdapter::addBlockAndUpdateMultiKey(const SetOfKeyValuePairs &kvMap,
                                            const BlockId &blockId,
                                            const Sliver &rawBlock) {
  SetOfKeyValuePairs updatedKVMap;
  if (saveKvPairsSeparately_)
    for (auto &[key, value] : kvMap) updatedKVMap[keyGen_->dataKey(key, 0)] = value;

  updatedKVMap[keyGen_->blockKey(blockId)] = rawBlock;
  return db_->multiPut(updatedKVMap);
}

/**
 * @brief Deletes a block from the database.
 *
 * Deletes:
 *    - the key value pair corresponding to the composite database key of type "Block" generated from the block id
 *    - all keys composing this block
 *
 * @param _blockId The ID of the block to be deleted.
 */
void DBAdapter::deleteBlock(const BlockId &blockId) {
  try {
    RawBlock blockRaw = getRawBlock(blockId);
    KeysVector keysVec;
    const auto numOfElements = ((block::detail::Header *)blockRaw.data())->numberOfElements;
    auto *entries = (block::detail::Entry *)(blockRaw.data() + sizeof(block::detail::Header));
    if (saveKvPairsSeparately_) {
      for (size_t i = 0u; i < numOfElements; i++)
        keysVec.push_back(keyGen_->dataKey(Key(blockRaw, entries[i].keyOffset, entries[i].keySize), blockId));
    }
    keysVec.push_back(keyGen_->blockKey(blockId));

    if (Status s = db_->multiDel(keysVec); !s.isOK())
      throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": failed: blockId: ") + std::to_string(blockId) +
                               std::string(" reason: ") + s.toString());
    if (blockId > 1) ConcordAssert(hasBlock(blockId - 1));
    setLatestBlock(blockId - 1);
    setLastReachableBlockNum(blockId - 1);
  } catch (const NotFoundException &e) {
  }
}

/**
 * @brief Deletes the last reachable block.
 */
void DBAdapter::deleteLastReachableBlock() {
  const auto lastReachableBlockId = getLastReachableBlockId();
  if (lastReachableBlockId == 0) {
    return;
  }
  deleteBlock(lastReachableBlockId);
}

/**
 * @brief Searches for record in the database by the read version.
 *
 * Read version is used as the block id for generating a composite database key
 * which is then used to lookup in the database.
 *
 * @param readVersion BlockId object signifying the read version with which a
 *                    lookup needs to be done.
 * @param key Sliver object of the key.
 * @return outValue Sliver object where the value of the lookup result is stored.
 * @return  outBlock BlockId object where the read version of the result is stored.
 */
std::pair<Value, BlockId> DBAdapter::getValue(const Key &key, const BlockId &blockVersion) const {
  LOG_TRACE(logger_, "Getting value of key [" << key.toString() << "] for read version " << blockVersion);
  Key searchKey = keyGen_->dataKey(key, blockVersion);
  LOG_TRACE(logger_, "searchKey: " << searchKey.toString());
  Sliver value;
  if (Status s = db_->get(searchKey, value); s.isOK()) return std::make_pair(value, 0);

  throw NotFoundException{__PRETTY_FUNCTION__ + std::string(": key: ") + searchKey.toString() +
                          std::string(" blockVersion: ") + std::to_string(blockVersion)};
}

/**
 * @brief Looks up data by block id.
 *
 * Constructs a composite database key using the block id to fire a get request.
 *
 * @param _blockId BlockId object used for looking up data.
 * @param _blockRaw Sliver object where the result of the lookup is stored.
 * @param _found true if lookup successful, else false.
 * @return Status of the operation.
 */
RawBlock DBAdapter::getRawBlock(const BlockId &blockId) const {
  RawBlock blockRaw;
  if (Status s = db_->get(keyGen_->blockKey(blockId), blockRaw); !s.isOK()) {
    if (s.isNotFound())
      throw NotFoundException(__PRETTY_FUNCTION__ + std::string(": blockId: ") + std::to_string(blockId));
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": failed for blockId: ") + std::to_string(blockId) +
                             std::string(" reason: ") + s.toString());
  }
  return blockRaw;
}

bool DBAdapter::hasBlock(const BlockId &blockId) const {
  if (Status s = db_->has(keyGen_->blockKey(blockId)); s.isNotFound()) return false;
  return true;
}
// TODO(SG): Add status checks with getStatus() on iterator.
// TODO(JGC): unserstand difference between .second and .data()

/**
 * @brief Used to retrieve the latest block.
 *
 * Searches for the key with the largest block id component.
 *
 * @return Block ID of the latest block.
 */
BlockId DBAdapter::fetchLatestBlockId() const {
  if (mdt_) return mdtGetLatestBlockId();
  // Note: RocksDB stores keys in a sorted fashion as per the logic provided in
  // a custom comparator (for our case, refer to the `composedKeyComparison`
  // method above). In short, keys of type 'block' are stored first followed by
  // keys of type 'key'. All keys of type 'block' are sorted in ascending order
  // of block ids.

  // Generate maximal key for type 'block'.
  const auto maxKey = keyGen_->blockKey(std::numeric_limits<BlockId>::max());
  auto iter = db_->getIteratorGuard();
  // Since we use the maximal key, SeekAtLeast will take the iterator
  // to one position beyond the key corresponding to the largest block id.
  iter->seekAtLeast(maxKey);

  // Read the previous key.
  const auto key = iter->previous().first;
  if (key.empty()) {  // no blocks
    return 0;
  }

  const auto blockId = DBKeyManipulator::extractBlockIdFromKey(key);
  LOG_TRACE(logger_, "Latest block ID " << blockId);
  return blockId;
}

/**
 * @brief Used to retrieve the last reachable block
 *
 * Searches for the last reachable block.
 * From ST perspective, this is maximal block number N
 * such that all blocks INITIAL_GENESIS_BLOCK_ID <= i <= N exist.
 * In the normal state, it should be equal to last block ID
 *
 * @return Block ID of the last reachable block.
 */
BlockId DBAdapter::fetchLastReachableBlockId() const {
  if (mdt_) return mdtGetLastReachableBlockId();

  BlockId lastReachableId = 0;
  storage::IDBClient::IDBClientIterator *iter = db_->getIterator();

  Sliver blockKey = keyGen_->blockKey(1);
  KeyValuePair kvp = iter->seekAtLeast(blockKey);
  if (kvp.first.length() == 0) {
    db_->freeIterator(iter);
    return 0;
  }

  while (!iter->isEnd() && (DBKeyManipulator::extractTypeFromKey(kvp.first) == EDBKeyType::E_DB_KEY_TYPE_BLOCK)) {
    BlockId id = DBKeyManipulator::extractBlockIdFromKey(kvp.first);
    if (id == lastReachableId + 1) {
      lastReachableId++;
      kvp = iter->next();
    } else {
      break;
    }
  }

  db_->freeIterator(iter);
  return lastReachableId;
}

BlockId DBAdapter::mdtGetLatestBlockId() const {
  static Key lastBlockIdKey = keyGen_->mdtKey(std::string("last-block-id"));
  Sliver val;
  if (Status s = mdtGet(lastBlockIdKey, val); s.isOK())
    return concord::util::to<BlockId>(val.toString());
  else
    return 0;
}

BlockId DBAdapter::mdtGetLastReachableBlockId() const {
  static Key lastReachableBlockIdKey = keyGen_->mdtKey(std::string("last-reachable-block-id"));
  Sliver val;
  if (Status s = mdtGet(lastReachableBlockIdKey, val); s.isOK())
    return concord::util::to<BlockId>(val.toString());
  else
    return 0;
}

void DBAdapter::setLastReachableBlockNum(const BlockId &blockId) {
  lastReachableBlockId_ = blockId;
  if (mdt_) {
    static Key lastReachableBlockIdKey = keyGen_->mdtKey(std::string("last-reachable-block-id"));
    mdtPut(lastReachableBlockIdKey, std::to_string(blockId));
  }
}

void DBAdapter::setLatestBlock(const BlockId &blockId) {
  lastBlockId_ = blockId;
  if (mdt_) {
    static Key lastBlockIdKey = keyGen_->mdtKey(std::string("last-block-id"));
    mdtPut(lastBlockIdKey, std::to_string(blockId));
  }
}
Status DBAdapter::mdtPut(const concordUtils::Sliver &key, const concordUtils::Sliver &val) {
  if (Status s = db_->put(key, val); s.isOK())
    return s;
  else
    throw std::runtime_error("failed to put key: " + key.toString() + std::string(" reason: ") + s.toString());
}

Status DBAdapter::mdtGet(const concordUtils::Sliver &key, concordUtils::Sliver &val) const {
  if (Status s = db_->get(key, val); s.isOK() || s.isNotFound())
    return s;
  else
    throw std::runtime_error("failed to get key: " + key.toString() + std::string(" reason: ") + s.toString());
}

SetOfKeyValuePairs DBAdapter::getBlockData(const RawBlock &rawBlock) const { return block::detail::getData(rawBlock); }

BlockDigest DBAdapter::getParentDigest(const RawBlock &rawBlock) const {
  return block::detail::getParentDigest(rawBlock);
}
void DBAdapter::setLastKnownReconfigurationCmdBlock(std::string &blockData) {
  if (mdt_) {
    try {
      static Key lastKnownReconfigCmdBlockIdKey = keyGen_->mdtKey(std::string("last-reconfig-cmd-block-id"));
      mdtPut(lastKnownReconfigCmdBlockIdKey, std::move(blockData));
    } catch (std::exception &e) {
      LOG_ERROR(logger_, "Error in put reconfig block" << e.what());
    }
  }
}
void DBAdapter::getLastKnownReconfigurationCmdBlock(std::string &outBlockData) const {
  if (mdt_) {
    try {
      static Key lastKnownReconfigCmdBlockIdKey = keyGen_->mdtKey(std::string("last-reconfig-cmd-block-id"));
      Sliver val;
      if (Status s = mdtGet(lastKnownReconfigCmdBlockIdKey, val); s.isOK())
        outBlockData = val.toString();
      else
        LOG_WARN(logger_, "Unable to get the last known reconfig block");
    } catch (std::exception &e) {
      LOG_WARN(logger_, "Unable to get the last known reconfig block: " << e.what());
    }
  }
}

}  // namespace concord::kvbc::v1DirectKeyValue
