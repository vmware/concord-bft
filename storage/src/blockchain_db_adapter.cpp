// Copyright 2018 VMware, all rights reserved

// @file blockchain_db_adapter.cpp
//
// @brief Contains helper functions for working with composite database keys and
// using these keys to perform basic database operations.
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
#include <chrono>
#include <limits>
#include "hash_defs.h"
#include "hex_tools.h"
#include "sliver.hpp"
#include "status.hpp"
#include <string.h>
#include "blockchain/db_interfaces.h"
#include "blockchain/db_adapter.h"

using concordlogger::Logger;
using concordUtils::Status;
using concordUtils::hexPrint;

namespace concord {
namespace storage {
namespace blockchain {

struct HexPrintBuffer {
  const uint8_t* bytes;
  const size_t size;
};

// Print a uint8_t* of bytes as its 0x<hex> representation.
std::ostream& operator<<(std::ostream& s, const HexPrintBuffer p) {
  hexPrint(s, p.bytes, p.size);
  return s;
}

DBAdapter::DBAdapter(IDBClient* db, bool readOnly):
    logger_(concordlogger::Log::getLogger("concord.storage.BlockchainDBAdapter")),
    db_(db),
    key_manipulator_(new KeyManipulator()),
    m_isEnd(false)
    {

  db_->init(readOnly);
}

/*
 * If key a is smaller than key b, return a negative number; if larger, return a
 * positive number; if equal, return zero.
 *
 * Comparison is done by decomposed parts. Types are compared first, followed by
 * type specific comparison. */
int KeyManipulator::composedKeyComparison(const uint8_t* _a_data, size_t _a_length,
                                          const uint8_t* _b_data, size_t _b_length) {
  EDBKeyType aType = KeyManipulator::extractTypeFromKey(_a_data);
  EDBKeyType bType = KeyManipulator::extractTypeFromKey(_b_data);
  if (aType != bType)
    return (int)aType - (int)bType;

  switch(aType){
    case EDBKeyType::E_DB_KEY_TYPE_BFT_ST_KEY:
    case EDBKeyType::E_DB_KEY_TYPE_BFT_ST_PENDING_PAGE_KEY:
    case EDBKeyType::E_DB_KEY_TYPE_BFT_METADATA_KEY: {
      // Compare object IDs.
      ObjectId aObjId = KeyManipulator::extractObjectIdFromKey(_a_data, _a_length);
      ObjectId bObjId = KeyManipulator::extractObjectIdFromKey(_b_data, _b_length);
      return (aObjId > bObjId) ? 1 : (bObjId > aObjId) ? -1 : 0;
    }
    case EDBKeyType::E_DB_KEY_TYPE_BFT_ST_CHECKPOINT_DESCRIPTOR_KEY:{
      uint64_t aChkpt, bChkpt;
      aChkpt = extractCheckPointFromKey(_a_data, _a_length);
      bChkpt = extractCheckPointFromKey(_b_data, _b_length);
      return (aChkpt > bChkpt) ? 1 : (bChkpt > aChkpt) ? -1 : 0;
    }
    case EDBKeyType::E_DB_KEY_TYPE_BFT_ST_RESERVED_PAGE_STATIC_KEY:
    case EDBKeyType::E_DB_KEY_TYPE_BFT_ST_RESERVED_PAGE_DYNAMIC_KEY:{
      // Pages are sorted in ascending order, checkpoints in descending order
       uint32_t aPageId, bPageId;
       uint64_t aChkpt, bChkpt;
       std::tie(aPageId, aChkpt) = extractPageIdAndCheckpointFromKey(_a_data, _a_length);
       std::tie(bPageId, bChkpt) = extractPageIdAndCheckpointFromKey(_b_data, _b_length);
       if (aPageId != bPageId) return (aPageId > bPageId)? 1: (bPageId > aPageId)? -1:0;
       return (aChkpt < bChkpt)? 1 : (aChkpt > bChkpt) ? -1 : 0;
    }
    case EDBKeyType::E_DB_KEY_TYPE_KEY: {
      int keyComp = KeyManipulator::compareKeyPartOfComposedKey(_a_data, _a_length, _b_data, _b_length);
      if (keyComp != 0) return keyComp;
      // Extract the block ids to compare so that endianness of environment does not matter.
      BlockId aId = KeyManipulator::extractBlockIdFromKey(_a_data, _a_length);
      BlockId bId = KeyManipulator::extractBlockIdFromKey(_b_data, _b_length);
      // Block ids are sorted in reverse order when part of a composed key (key + blockId)
      return (bId > aId) ? 1 : (aId > bId) ? -1 : 0;
    }
    case EDBKeyType::E_DB_KEY_TYPE_BLOCK: {
      // Extract the block ids to compare so that endianness of environment does not matter.
      BlockId aId = KeyManipulator::extractBlockIdFromKey(_a_data, _a_length);
      BlockId bId = KeyManipulator::extractBlockIdFromKey(_b_data, _b_length);
      // Block ids are sorted in ascending order when part of a block key
      return (aId > bId) ? 1 : (bId > aId) ? -1 : 0;
    }
    default:
      LOG_ERROR(logger(), "invalid key type: " << (char) aType);
      assert(false);

  } //switch
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
Sliver KeyManipulator::genDbKey(EDBKeyType _type, const Key& _key, BlockId _blockId) {
  size_t sz = sizeof(EDBKeyType) + sizeof(BlockId) + _key.length();
  uint8_t *out = new uint8_t[sz];
  size_t offset = 0;
  copyToAndAdvance(out, &offset, sz, (uint8_t *)&_type, sizeof(EDBKeyType));
  copyToAndAdvance(out, &offset, sz, (uint8_t *)_key.data(), _key.length());
  copyToAndAdvance(out, &offset, sz, (uint8_t *)&_blockId, sizeof(BlockId));
  return Sliver(out, sz);
}

/**
 * @brief Helper function that generates a composite database Key of type Block.
 *
 * For such Database keys, the "key" component is an empty sliver.
 *
 * @param _blockid BlockId object of the block id that needs to be
 *                 incorporated into the composite database key.
 * @return Sliver object of the generated composite database key.
 */
Sliver KeyManipulator::genBlockDbKey(BlockId _blockId) {
  return genDbKey(EDBKeyType::E_DB_KEY_TYPE_BLOCK, Sliver(), _blockId);
}

/**
 * @brief Helper function that generates a composite database Key of type Key.
 *
 * @param _key Sliver object of the "key" component.
 * @param _blockid BlockId object of the block id that needs to be incorporated
 *                 into the composite database Key.
 * @return Sliver object of the generated composite database key.
 */
Sliver KeyManipulator::genDataDbKey(const Key& _key, BlockId _blockId) {
  return genDbKey(EDBKeyType::E_DB_KEY_TYPE_KEY, _key, _blockId);
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
EDBKeyType KeyManipulator::extractTypeFromKey(const Key& _key) {
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
EDBKeyType KeyManipulator::extractTypeFromKey(const uint8_t* _key_data) {
  static_assert(sizeof(EDBKeyType) == 1, "Let's avoid byte-order problems.");
  assert((_key_data[0] < (uint8_t)EDBKeyType::E_DB_KEY_TYPE_LAST) && (_key_data[0] >= (uint8_t)EDBKeyType::E_DB_KEY_TYPE_FIRST));
  return (EDBKeyType)_key_data[0];
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
BlockId KeyManipulator::extractBlockIdFromKey(const Key& _key) {
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
BlockId KeyManipulator::extractBlockIdFromKey(const uint8_t* _key_data, size_t _key_length) {
  size_t offset = _key_length - sizeof(BlockId);
  BlockId id = *(BlockId *)(_key_data + offset);

  LOG_TRACE(logger(), "Got block ID " << id << " from key " << (HexPrintBuffer{_key_data, _key_length})  << ", offset " << offset);
  return id;
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
ObjectId KeyManipulator::extractObjectIdFromKey(const Key& _key) {
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
ObjectId KeyManipulator::extractObjectIdFromKey(const uint8_t* _key_data, size_t _key_length) {
  assert(_key_length >= sizeof(ObjectId));
  size_t offset = _key_length - sizeof(ObjectId);
  ObjectId id = *(ObjectId *)(_key_data + offset);

  LOG_TRACE(logger(), "Got object ID " << id << " from key " << (HexPrintBuffer{_key_data, _key_length}) << ", offset " << offset);
  return id;
}

/**
 * @brief Extracts the key from a composite database key.
 *
 * @param _composedKey Sliver object of the composite database key.
 * @return Sliver object of the key extracted from the composite database key.
 */
Sliver KeyManipulator::extractKeyFromKeyComposedWithBlockId(const Key& _composedKey) {
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
int KeyManipulator::compareKeyPartOfComposedKey(const uint8_t *a_data,
                                                size_t a_length,
                                                const uint8_t *b_data,
                                                size_t b_length) {
  assert(a_length >= sizeof(BlockId) + sizeof(EDBKeyType));
  assert(b_length >= sizeof(BlockId) + sizeof(EDBKeyType));

  const uint8_t *a_key = a_data + sizeof(EDBKeyType);
  const size_t a_key_length = a_length - sizeof(BlockId) - sizeof(EDBKeyType);
  const uint8_t *b_key = b_data + sizeof(EDBKeyType);
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

Sliver KeyManipulator::extractKeyFromMetadataKey(const Key& _composedKey) {
  size_t sz = _composedKey.length() - sizeof(EDBKeyType);
  Sliver out = Sliver(_composedKey, sizeof(EDBKeyType), sz);
  LOG_TRACE(logger(), "Got metadata key " << out << " from composed key "
                                              << _composedKey);
  return out;
}

bool KeyManipulator::isKeyContainBlockId(const Key& _composedKey) {
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
KeyValuePair KeyManipulator::composedToSimple(KeyValuePair _p) {
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

/**
 * @brief Generates a Composite Database Key from objectId.
 *
 * Merges the key type and the block id to generate a composite
 * database key for E_DB_KEY_TYPE_METADATA_KEY.
 *
 * Format : Key Type | Object Id
 *
 * @return Sliver object of the generated composite database key.
 */

Sliver KeyManipulator::generateMetadataKey(ObjectId objectId) {
  size_t keySize = sizeof(EDBKeyType) + sizeof(objectId);
  auto keyBuf = new uint8_t[keySize];
  size_t offset = 0;
  EDBKeyType keyType = EDBKeyType::E_DB_KEY_TYPE_BFT_METADATA_KEY;
  copyToAndAdvance(keyBuf, &offset, keySize, (uint8_t *)&keyType,
                   sizeof(EDBKeyType));
  copyToAndAdvance(keyBuf, &offset, keySize, (uint8_t *)&objectId,
                   sizeof(objectId));
  return Sliver(keyBuf, keySize);
}
/*
 * Format : Key Type | Object Id
 */
Sliver KeyManipulator::generateStateTransferKey(ObjectId objectId){
  size_t keySize = sizeof(EDBKeyType) + sizeof(objectId);
  auto keyBuf = new uint8_t[keySize];
  size_t offset = 0;
  EDBKeyType keyType = EDBKeyType::E_DB_KEY_TYPE_BFT_ST_KEY;
  copyToAndAdvance(keyBuf, &offset, keySize, (uint8_t *)&keyType, sizeof(EDBKeyType));
  copyToAndAdvance(keyBuf, &offset, keySize, (uint8_t *)&objectId,sizeof(objectId));
  return Sliver(keyBuf, keySize);
}
/**
 * Format : Key Type | Page Id
 */
Sliver KeyManipulator::generateSTPendingPageKey(uint32_t pageid){
  size_t keySize = sizeof(EDBKeyType) + sizeof(pageid);
  auto keyBuf = new uint8_t[keySize];
  size_t offset = 0;
  auto keyType = EDBKeyType::E_DB_KEY_TYPE_BFT_ST_PENDING_PAGE_KEY;
  copyToAndAdvance(keyBuf, &offset, keySize, (uint8_t *)&keyType, sizeof(EDBKeyType));
  copyToAndAdvance(keyBuf, &offset, keySize, (uint8_t *)&pageid,  sizeof(pageid));
  return Sliver(keyBuf, keySize);
}
/**
 * Format : Key Type | Checkpoint
 */
Sliver KeyManipulator::generateSTCheckpointDescriptorKey(uint64_t chkpt){
  size_t keySize = sizeof(EDBKeyType) + sizeof(chkpt);
  auto keyBuf = new uint8_t[keySize];
  size_t offset = 0;
  auto keyType = EDBKeyType::E_DB_KEY_TYPE_BFT_ST_CHECKPOINT_DESCRIPTOR_KEY;
  copyToAndAdvance(keyBuf, &offset, keySize, (uint8_t *)&keyType, sizeof(EDBKeyType));
  copyToAndAdvance(keyBuf, &offset, keySize, (uint8_t *)&chkpt,   sizeof(chkpt));
  return Sliver(keyBuf, keySize);
}
/**
 * Format : Key Type | Page Id | Checkpoint
 */
Sliver KeyManipulator::generateSTReservedPageStaticKey(uint32_t pageid, uint64_t chkpt) {
  return generateReservedPageKey(EDBKeyType::E_DB_KEY_TYPE_BFT_ST_RESERVED_PAGE_STATIC_KEY, pageid, chkpt);
}
/**
 * Format : Key Type | Page Id | Checkpoint
 */
Sliver KeyManipulator::generateSTReservedPageDynamicKey(uint32_t pageid, uint64_t chkpt) {
  return generateReservedPageKey(EDBKeyType::E_DB_KEY_TYPE_BFT_ST_RESERVED_PAGE_DYNAMIC_KEY, pageid, chkpt);
}
/**
 * Format : Key Type | Page Id | Checkpoint
 */
Sliver KeyManipulator::generateReservedPageKey(EDBKeyType keyType, uint32_t pageid, uint64_t chkpt){
  size_t keySize = sizeof(EDBKeyType) + sizeof(pageid) + sizeof(chkpt);
  auto keyBuf = new uint8_t[keySize];
  size_t offset = 0;
  copyToAndAdvance(keyBuf, &offset, keySize, (uint8_t *)&keyType, sizeof(EDBKeyType));
  copyToAndAdvance(keyBuf, &offset, keySize, (uint8_t *)&pageid,  sizeof(pageid));
  copyToAndAdvance(keyBuf, &offset, keySize, (uint8_t *)&chkpt,   sizeof(chkpt));
  return Sliver(keyBuf, keySize);
}

/**
 * @brief Adds a block to the database.
 *
 * Generates a new composite database key of type Block and adds a block to the
 * database using it.
 *
 * @param _blockId The block id to be used for generating the composite database
 *                 key.
 * @param _blockRaw The value that needs to be added to the database along with
 *                  the composite database key.
 * @return Status of the put operation.
 */
Status DBAdapter::addBlock(BlockId _blockId, Sliver _blockRaw) {
  Sliver dbKey = key_manipulator_->genBlockDbKey(_blockId);
  Status s = db_->put(dbKey, _blockRaw);
  return s;
}

bool KeyManipulator::copyToAndAdvance(uint8_t *_buf, size_t *_offset, size_t _maxOffset, uint8_t *_src, size_t _srcSize) {
  if (!_buf && !_offset && !_src)
    assert(false);

  if (*_offset >= _maxOffset && _srcSize > 0)
    assert(false);

  memcpy(_buf + *_offset, _src, _srcSize);
  *_offset += _srcSize;

  return true;
}

uint64_t KeyManipulator::extractCheckPointFromKey(const uint8_t* _key_data, size_t _key_length) {
  assert(_key_length >= sizeof(uint64_t));
  uint64_t chkp = *(uint64_t *)(_key_data + 1);

  LOG_TRACE(logger(), "checkpoint " << chkp << " from key " << (HexPrintBuffer{_key_data, _key_length}));
  return chkp;
}

std::pair<uint32_t, uint64_t>
KeyManipulator::extractPageIdAndCheckpointFromKey(const uint8_t* _key_data, size_t _key_length){
  assert(_key_length >= sizeof(uint32_t) + sizeof(uint64_t));

  uint32_t pageId = *(uint32_t *)(_key_data + 1);
  uint64_t chkp = *(uint64_t *)(_key_data + sizeof(uint32_t) + 1);
  LOG_TRACE(logger(), "pageId " << pageId << " checkpoint " << chkp << " from key " << (HexPrintBuffer{_key_data, _key_length}));
  return std::make_pair(pageId, chkp);
}

/**
 * @brief Puts a key value pair to the database with a composite database key of
 * type Key.
 *
 * Generates a new composite database key of type Key and adds a block to the
 * database using it.
 *
 * @param _key The key used for generating the composite database key.
 * @param _block The block used for generating the composite database key.
 * @param _value The value that needs to be added to the database.
 * @return Status of the put operation.
 */
Status DBAdapter::updateKey(const Key& _key, BlockId _block, Value _value) {
  Sliver composedKey = key_manipulator_->genDataDbKey(_key, _block);

  LOG_TRACE(logger_, "Updating composed key " << composedKey
                                                   << " with value " << _value
                                                   << " in block " << _block);

  Status s = db_->put(composedKey, _value);
  return s;
}

Status DBAdapter::addBlockAndUpdateMultiKey(
    const SetOfKeyValuePairs &_kvMap, BlockId _block, Sliver _blockRaw) {
  SetOfKeyValuePairs updatedKVMap;
  for (auto &it : _kvMap) {
    Sliver composedKey = key_manipulator_->genDataDbKey(it.first, _block);
    LOG_TRACE(logger_, "Updating composed key "
                                << composedKey << " with value " << it.second
                                << " in block " << _block);
    updatedKVMap[composedKey] = it.second;
  }
  updatedKVMap[key_manipulator_->genBlockDbKey(_block)] = _blockRaw;
  return db_->multiPut(updatedKVMap);
}

/**
 * @brief Deletes a key value pair from the database.
 *
 * Deletes the key value pair corresponding to the composite database key of
 * type "Key" generated from the key and block id provided as parameters to this
 * function.
 *
 * @param _key The key whose composite version needs to be deleted.
 * @param _blockId The block id (version) of the key to delete.
 * @return Status of the operation.
 */
Status DBAdapter::delKey(const Sliver& _key, BlockId _blockId) {
  Sliver composedKey = key_manipulator_->genDataDbKey(_key, _blockId);
  LOG_TRACE(logger_, "Deleting key " << _key << " block id " << _blockId);
  Status s = db_->del(composedKey);
  return s;
}

/**
 * @brief Deletes a key value pair from the database.
 *
 * Deletes the key value pair corresponding to the composite database key of
 * type "Block" generated from the block id provided as a parameter to this
 * function.
 *
 * @param _blockId The ID of the block to be deleted.
 * @return Status of the operation.
 */
Status DBAdapter::delBlock(BlockId _blockId) {
  Sliver dbKey = key_manipulator_->genBlockDbKey(_blockId);
  Status s = db_->del(dbKey);
  return s;
}

void DBAdapter::deleteBlockAndItsKeys(BlockId blockId) {
  Sliver blockRaw;
  bool found = false;
  Status s = getBlockById(blockId, blockRaw, found);
  if (!s.isOK()) {
    LOG_FATAL(logger_, "Failed to read block id: " << blockId);
    exit(1);
  }
  KeysVector keysVec;
  if (found && blockRaw.length() > 0) {
    uint16_t numOfElements = ((BlockHeader *)blockRaw.data())->numberOfElements;
    auto *entries = (BlockEntry *)(blockRaw.data() + sizeof(BlockHeader));
    for (size_t i = 0; i < numOfElements; i++) {
      keysVec.push_back(
          Key(blockRaw, entries[i].keyOffset, entries[i].keySize));
    }
  }
  if (found) {
    keysVec.push_back(key_manipulator_->genBlockDbKey(blockId));
  }
  s = db_->multiDel(keysVec);
  if (!s.isOK()) {
    LOG_FATAL(logger_, "Failed to delete block id: " << blockId);
    exit(1);
  }
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
 * @param outValue Sliver object where the value of the lookup result is stored.
 * @param outBlock BlockId object where the read version of the result is
 *                         stored.
 * @return Status OK
 */
Status DBAdapter::getKeyByReadVersion(BlockId readVersion,
                                                const Sliver& key,
                                                Sliver &outValue,
                                                BlockId &outBlock) const {
  LOG_TRACE(logger_, "Getting value of key " << key << " for read version "  << readVersion);
  IDBClient::IDBClientIterator *iter = db_->getIterator();
  Sliver foundKey, foundValue;
  Sliver searchKey = key_manipulator_->genDataDbKey(key, readVersion);
  KeyValuePair p = iter->seekAtLeast(searchKey);
  foundKey = key_manipulator_->composedToSimple(p).first;
  foundValue = p.second;

  LOG_TRACE(logger_, "Found key " << foundKey << " and value " << foundValue);

  if (!iter->isEnd()) {
    BlockId currentReadVersion = key_manipulator_->extractBlockIdFromKey(p.first);

    // TODO(JGC): Ask about reason for version comparison logic
    if (currentReadVersion <= readVersion && foundKey == key) {
      outValue = foundValue;
      outBlock = currentReadVersion;
    } else {
      outValue = Sliver();
      outBlock = 0;
    }
  } else {
    outValue = Sliver();
    outBlock = 0;
  }

  db_->freeIterator(iter);

  // TODO(GG): maybe return status of the operation?
  return Status::OK();
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
Status DBAdapter::getBlockById(BlockId _blockId, Sliver &_blockRaw,
                                         bool &_found) const {
  Sliver key = key_manipulator_->genBlockDbKey(_blockId);
  Status s = db_->get(key, _blockRaw);
  if (s.isNotFound()) {
    _found = false;
    return Status::OK();
  }

  _found = true;
  return s;
}

// TODO(BWF): is this still needed?
/**
 * @brief Makes a copy of a Sliver object.
 *
 * @param _src Sliver object that needs to be copied.
 * @param _trg Sliver object that contains the result.
 */
inline void CopyKey(Sliver _src, Sliver &_trg) {
  uint8_t *c = new uint8_t[_src.length()];
  memcpy(c, _src.data(), _src.length());
  _trg = Sliver(c, _src.length());
}

// TODO(SG): Add status checks with getStatus() on iterator.
// TODO(JGC): unserstand difference between .second and .data()
/**
 * @brief Finds the first key with block version lesser than or equal to
 * readVersion.
 *
 * Iterates from the first key to find the key with block version lesser than or
 * equal to the readVersion.
 *
 * @param iter Iterator object.
 * @param readVersion The read version used for searching.
 * @param actualVersion The read version of the result.
 * @param isEnd True if end of the database is reached while iterating, else
 *              false.
 * @param _key The result's key.
 * @param _value The result's value.
 * @return Status NotFound if database is empty, OK otherwise.
 */
Status DBAdapter::first(IDBClient::IDBClientIterator *iter,
                                  BlockId readVersion,
                                  OUT BlockId &actualVersion, OUT bool &isEnd,
                                  OUT Sliver &_key, OUT Sliver &_value) {
  Key firstKey;
  KeyValuePair p = key_manipulator_->composedToSimple(iter->first());
  if (iter->isEnd()) {
    m_isEnd = true;
    isEnd = true;
    return Status::NotFound("No keys");
  } else {
    CopyKey(p.first, firstKey);
  }

  bool foundKey = false;
  Sliver value;
  BlockId actualBlock = 0;
  while (!iter->isEnd() && p.first == firstKey) {
    BlockId currentBlock = key_manipulator_->extractBlockIdFromKey(iter->getCurrent().first);
    if (currentBlock <= readVersion) {
      value = p.second;
      actualBlock = currentBlock;
      foundKey = true;
      p = key_manipulator_->composedToSimple(iter->next());
    } else {
      if (!foundKey) {
        // If not found a key with actual block version < readVersion, then we
        // consider the next key as the first key candidate.

        // Start by exhausting the current key with all the newer blocks
        // records:
        while (!iter->isEnd() && p.first == firstKey) {
          p = key_manipulator_->composedToSimple(iter->next());
        }

        if (iter->isEnd()) {
          break;
        }

        CopyKey(p.first, firstKey);
      } else {
        // If we already found a suitable first key, we break when we find
        // the maximal
        break;
      }
    }
  }

  // It is possible all keys have actualBlock > readVersion (Actually, this
  // sounds like data is corrupted in this case - unless we allow empty blocks)
  if (iter->isEnd() && !foundKey) {
    m_isEnd = true;
    isEnd = true;
    return Status::OK();
  }

  m_isEnd = false;
  isEnd = false;
  actualVersion = actualBlock;
  _key = firstKey;
  _value = value;
  m_current = KeyValuePair(_key, _value);
  return Status::OK();
}

/**
 * @brief Finds the first key greater than or equal to the search key with block
 * version lesser than or equal to readVersion.
 *
 * Iterates from the first key greater than or equal to the search key to find
 * the key with block version lesser than or equal to the readVersion.
 *
 * @param iter Iterator object.
 * @param _searchKey Sliver object of the search key.
 * @param _readVersion BlockId of the read version used for searching.
 * @param _actualVersion BlockId in which the version of the result is stored.
 * @param _key The result's key.
 * @param _value The result's value.
 * @param _isEnd True if end of the database is reached while iterating, else
 *               false.
 *  @return Status NotFound if search unsuccessful, OK otherwise.
 */
// Only for data fields, i.e. E_DB_KEY_TYPE_KEY. It makes more sense to put data
// second, and blocks first. Stupid optimization nevertheless
Status DBAdapter::seekAtLeast(IDBClient::IDBClientIterator *iter,
                                        const Sliver& _searchKey, BlockId _readVersion,
                                        OUT BlockId &_actualVersion,
                                        OUT Sliver &_key, OUT Sliver &_value,
                                        OUT bool &_isEnd) {
  Key searchKey = _searchKey;
  BlockId actualBlock = 0;
  Value value;
  bool foundKey = false;
  Sliver rocksKey = key_manipulator_->genDataDbKey(searchKey, _readVersion);
  KeyValuePair p =  key_manipulator_->composedToSimple(iter->seekAtLeast(rocksKey));

  if (!iter->isEnd()) {
    // p.first is src, searchKey is target
    CopyKey(p.first, searchKey);
  }

  LOG_TRACE(
      logger_, "Searching " << _searchKey << " and currently iterator returned "
                           << searchKey << " for rocks key " << rocksKey);

  while (!iter->isEnd() && p.first == searchKey) {
    BlockId currentBlockId = key_manipulator_->extractBlockIdFromKey(iter->getCurrent().first);

    LOG_TRACE(logger_, "Considering key " << p.first << " with block ID "  << currentBlockId);

    if (currentBlockId <= _readVersion) {
      LOG_TRACE(logger_, "Found with Block Id " << currentBlockId << " and value " << p.second);
      value = p.second;
      actualBlock = currentBlockId;
      foundKey = true;
      break;
    } else {
      LOG_TRACE(
          logger_, "Read version " << currentBlockId << " > " << _readVersion);
      if (!foundKey) {
        // If not found a key with actual block version < readVersion, then
        // we consider the next key as the key candidate.
        LOG_TRACE(logger_, "Find next key");

        // Start by exhausting the current key with all the newer blocks
        // records:
        while (!iter->isEnd() && p.first == searchKey) {
          p = key_manipulator_->composedToSimple(iter->next());
        }

        if (iter->isEnd()) {
          break;
        }

        CopyKey(p.first, searchKey);

        LOG_TRACE(logger_, "Found new search key " << searchKey);
      } else {
        // If we already found a suitable key, we break when we find the
        // maximal
        break;
      }
    }
  }

  if (iter->isEnd() && !foundKey) {
    LOG_TRACE(logger_,
                    "Reached end of map without finding lower bound "
                    "key with suitable read version");
    m_isEnd = true;
    _isEnd = true;
    return Status::NotFound("Did not find key with suitable read version");
  }

  m_isEnd = false;
  _isEnd = false;
  _actualVersion = actualBlock;
  _key = searchKey;
  _value = value;
  LOG_TRACE(logger_, "Returnign key " << _key <<
                     " value " << _value <<
                     " in actual block " << _actualVersion <<
                     ", read version " << _readVersion);
  m_current = KeyValuePair(_key, _value);
  return Status::OK();
}

/**
 * @brief Finds the next key with the most recently updated value.
 *
 * Finds the most updated value for the next key (with block version <
 * readVersion).
 *
 * @param iter Iterator.
 * @param _readVersion BlockId object of the read version used for searching.
 * @param _key Sliver object of the result's key. Contains only the reference.
 * @param _value Sliver object of the result's value.
 * @param _actualVersion BlockId object in which the version of the result is
 *                       stored.
 * @param _isEnd True if end of the database is reached while iterating, else
 *               false.
 * @return Status OK.
 */
Status DBAdapter::next(IDBClient::IDBClientIterator *iter,
                                 BlockId _readVersion, OUT Sliver &_key,
                                 OUT Sliver &_value,
                                 OUT BlockId &_actualVersion,
                                 OUT bool &_isEnd) {
  KeyValuePair p = key_manipulator_->composedToSimple(iter->getCurrent());
  Key currentKey = p.first;

  // Exhaust all entries for this key
  while (!iter->isEnd() && p.first == currentKey) {
    p = key_manipulator_->composedToSimple(iter->next());
  }

  if (iter->isEnd()) {
    m_isEnd = true;
    _isEnd = true;
    return Status::OK();
  }

  // Find most updated value for next key (with block version < readVersion)
  Value value;
  BlockId actualBlock = 0;
  Key nextKey;
  CopyKey(p.first, nextKey);
  bool foundKey = false;
  // Find max version
  while (!iter->isEnd() && p.first == nextKey) {
    BlockId currentBlockId = key_manipulator_->extractBlockIdFromKey(iter->getCurrent().first);
    if (currentBlockId <= _readVersion) {
      value = p.second;
      actualBlock = currentBlockId;
      p = key_manipulator_->composedToSimple(iter->next());
    } else {
      if (!foundKey) {
        // If not found a key with actual block version < readVersion, then
        // we consider the next key as the key candidate.

        // Start by exhausting the current key with all the newer blocks
        // records:
        while (!iter->isEnd() && p.first == nextKey) {
          p = key_manipulator_->composedToSimple(iter->next());
        }

        if (iter->isEnd()) {
          break;
        }

        CopyKey(p.first, nextKey);
      } else {
        // If we already found a suitable key, we break when we find the
        // maximal
        break;
      }
    }
  }

  m_isEnd = false;
  _isEnd = false;
  _actualVersion = actualBlock;
  _key = nextKey;
  _value = value;
  m_current = KeyValuePair(_key, _value);

  // TODO return appropriate status?
  return Status::OK();
}

/**
 * @brief Finds the key and value at the current position.
 *
 * Does not use the iterator for this.
 *
 * @param iter Iterator.
 * @param _key Sliver object where the key of the result is stored.
 * @param _value Sliver object where the value of the result is stored.
 * @return Status OK.
 */
Status DBAdapter::getCurrent(IDBClient::IDBClientIterator *iter,
                                       OUT Sliver &_key, OUT Sliver &_value) {
  // Not calling to underlying DB iterator, because it may have next()'d during
  // seekAtLeast
  _key = m_current.first;
  _value = m_current.second;

  return Status::OK();
}

/**
 * @brief Used to find if the iterator is at the end of the database.
 *
 * @param iter Iterator.
 * @param _isEnd True if iterator is at the end of the database, else false.
 * @return Status OK.
 */
Status DBAdapter::isEnd(IDBClient::IDBClientIterator *iter, OUT bool &_isEnd) {
  // Not calling to underlying DB iterator, because it may have next()'d during
  // seekAtLeast
  LOG_TRACE(logger_, "Called is end, returning " << m_isEnd);
  _isEnd = m_isEnd;
  return Status::OK();
}

/**
 * @brief Used to monitor the database.
 */
void DBAdapter::monitor() const { db_->monitor(); }

/**
 * @brief Used to retrieve the latest block.
 *
 * Searches for the key with the largest block id component.
 *
 * @return Block ID of the latest block.
 */
BlockId DBAdapter::getLatestBlock() {
  // Note: RocksDB stores keys in a sorted fashion as per the logic provided in
  // a custom comparator (for our case, refer to the `composedKeyComparison`
  // method above). In short, keys of type 'block' are stored first followed by
  // keys of type 'key'. All keys of type 'block' are sorted in ascending order
  // of block ids.

  // Generate maximal key for type 'block'
  Sliver maxKey = key_manipulator_->genDbKey(EDBKeyType::E_DB_KEY_TYPE_BLOCK,
                                             Sliver(),
                                             std::numeric_limits<uint64_t>::max());
  IDBClient::IDBClientIterator *iter = db_->getIterator();

  // Since we use the maximal key, SeekAtLeast will take the iterator
  // to one position beyond the key corresponding to the largest block id.
  KeyValuePair x = iter->seekAtLeast(maxKey);

  // Read the previous key
  x = iter->previous();

  db_->freeIterator(iter);

  if ((x.first).length() == 0) {  // no blocks
    return 0;
  }

  LOG_TRACE( logger_, "Latest block ID " << key_manipulator_->extractBlockIdFromKey(x.first));

  return key_manipulator_->extractBlockIdFromKey(x.first);
}

/**
 * @brief Used to retrieve the last reachable block
 *
 * Searches for the last reachable block.
 * From ST perspective, this is maximal block number N
 * such that all blocks 1 <= i <= N exist.
 * In the normal state, it should be equal to last block ID
 *
 * @return Block ID of the last reachable block.
 */
BlockId DBAdapter::getLastReachableBlock() {
  IDBClient::IDBClientIterator *iter = db_->getIterator();

  BlockId lastReachableId = 0;
  Sliver blockKey = key_manipulator_->genBlockDbKey(1);
  KeyValuePair kvp = iter->seekAtLeast(blockKey);
  if (kvp.first.length() == 0) {
    db_->freeIterator(iter);
    return 0;
  }

  while (!iter->isEnd() &&
        (key_manipulator_->extractTypeFromKey(kvp.first) == EDBKeyType::E_DB_KEY_TYPE_BLOCK)) {
    BlockId id = key_manipulator_->extractBlockIdFromKey(kvp.first);
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

}
}
}
