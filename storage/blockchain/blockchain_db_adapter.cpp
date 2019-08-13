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

#include "blockchain_db_adapter.h"
#include "Logger.hpp"
#include <chrono>
#include <limits>
#include "hash_defs.h"
#include "sliver.hpp"
#include "status.hpp"
#include "blockchain_db_helpers.h"
#include "blockchain_db_interfaces.h"

using concordlogger::Logger;
using concordUtils::Status;

namespace concord {
namespace storage {
namespace blockchain {

BlockchainDBAdapter::BlockchainDBAdapter(IDBClient *db, bool readOnly):
      logger(concordlogger::Log::getLogger("concord.storage.BlockchainDBAdapter")){
  m_isEnd = false;
  m_db = db;
  Status status = m_db->init(readOnly);
  if (!status.isOK()) {
    LOG_FATAL(logger, "Failure in Database Initialization, status: " << status);
    throw std::runtime_error("Failure in Database Initialization");
  }
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
Sliver KeyManipulator::genDbKey(EDBKeyType _type, Key _key, BlockId _blockId) {
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
Sliver KeyManipulator::genDataDbKey(Key _key, BlockId _blockId) {
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
char KeyManipulator::extractTypeFromKey(Key _key) {
  static_assert(sizeof(EDBKeyType) == 1, "Let's avoid byte-order problems.");
  return _key.data()[0];
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
BlockId KeyManipulator::extractBlockIdFromKey(const concordlogger::Logger& logger, Key _key) {
  size_t offset = _key.length() - sizeof(BlockId);
  BlockId id = *(BlockId *)(_key.data() + offset);

  LOG_DEBUG(logger, "Got block ID " << id << " from key " << _key  << ", offset " << offset);
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
ObjectId KeyManipulator::extractObjectIdFromKey(const Logger &logger,
                                                Key _key) {
  assert(_key.length() >= sizeof(ObjectId));
  size_t offset = _key.length() - sizeof(ObjectId);
  ObjectId id = *(ObjectId *)(_key.data() + offset);

  LOG_DEBUG(logger, "Got object ID " << id << " from key " << _key
                                           << ", offset " << offset);
  return id;
}

/**
 * @brief Extracts the key from a composite database key.
 *
 * @param _composedKey Sliver object of the composite database key.
 * @return Sliver object of the key extracted from the composite database key.
 */
Sliver KeyManipulator::extractKeyFromKeyComposedWithBlockId(
    const Logger &logger, Key _composedKey) {
  size_t sz = _composedKey.length() - sizeof(BlockId) - sizeof(EDBKeyType);
  Sliver out = Sliver(_composedKey, sizeof(EDBKeyType), sz);
  LOG_DEBUG(logger,
                  "Got key " << out << " from composed key " << _composedKey);
  return out;
}

Sliver KeyManipulator::extractKeyFromMetadataKey(const Logger &logger,
                                                 Key _composedKey) {
  size_t sz = _composedKey.length() - sizeof(EDBKeyType);
  Sliver out = Sliver(_composedKey, sizeof(EDBKeyType), sz);
  LOG_DEBUG(logger, "Got metadata key " << out << " from composed key "
                                              << _composedKey);
  return out;
}

bool KeyManipulator::isKeyContainBlockId(Key _composedKey) {
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
KeyValuePair KeyManipulator::composedToSimple(const Logger &logger,
                                              KeyValuePair _p) {
  if (_p.first.length() == 0) {
    return _p;
  }
  Key key;
  if (isKeyContainBlockId(_p.first))
    key = extractKeyFromKeyComposedWithBlockId(logger, _p.first);
  else
    key = extractKeyFromMetadataKey(logger, _p.first);

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
Status BlockchainDBAdapter::addBlock(BlockId _blockId, Sliver _blockRaw) {
  Sliver dbKey = KeyManipulator::genBlockDbKey(_blockId);
  Status s = m_db->put(dbKey, _blockRaw);
  return s;
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
Status BlockchainDBAdapter::updateKey(Key _key, BlockId _block, Value _value) {
  Sliver composedKey = KeyManipulator::genDataDbKey(_key, _block);

  LOG_DEBUG(logger, "Updating composed key " << composedKey
                                                   << " with value " << _value
                                                   << " in block " << _block);

  Status s = m_db->put(composedKey, _value);
  return s;
}

Status BlockchainDBAdapter::addBlockAndUpdateMultiKey(
    const SetOfKeyValuePairs &_kvMap, BlockId _block, Sliver _blockRaw) {
  SetOfKeyValuePairs updatedKVMap;
  for (auto &it : _kvMap) {
    Sliver composedKey = KeyManipulator::genDataDbKey(it.first, _block);
    LOG_DEBUG(logger, "Updating composed key "
                                << composedKey << " with value " << it.second
                                << " in block " << _block);
    updatedKVMap[composedKey] = it.second;
  }
  updatedKVMap[KeyManipulator::genBlockDbKey(_block)] = _blockRaw;
  return m_db->multiPut(updatedKVMap);
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
Status BlockchainDBAdapter::delKey(Sliver _key, BlockId _blockId) {
  Sliver composedKey = KeyManipulator::genDataDbKey(_key, _blockId);
  LOG_DEBUG(logger, "Deleting key " << _key << " block id " << _blockId);
  Status s = m_db->del(composedKey);
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
Status BlockchainDBAdapter::delBlock(BlockId _blockId) {
  Sliver dbKey = KeyManipulator::genBlockDbKey(_blockId);
  Status s = m_db->del(dbKey);
  return s;
}

void BlockchainDBAdapter::deleteBlockAndItsKeys(BlockId blockId) {
  Sliver blockRaw;
  bool found = false;
  Status s = getBlockById(blockId, blockRaw, found);
  if (!s.isOK()) {
    LOG_FATAL(logger, "Failed to read block id: " << blockId);
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
    keysVec.push_back(KeyManipulator::genBlockDbKey(blockId));
  }
  s = m_db->multiDel(keysVec);
  if (!s.isOK()) {
    LOG_FATAL(logger, "Failed to delete block id: " << blockId);
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
Status BlockchainDBAdapter::getKeyByReadVersion(BlockId readVersion,
                                                Sliver key,
                                                Sliver &outValue,
                                                BlockId &outBlock) const {
  LOG_DEBUG(logger, "Getting value of key " << key << " for read version "  << readVersion);
  IDBClient::IDBClientIterator *iter = m_db->getIterator();
  Sliver foundKey, foundValue;
  Sliver searchKey = KeyManipulator::genDataDbKey(key, readVersion);
  KeyValuePair p = iter->seekAtLeast(searchKey);
  foundKey = KeyManipulator::composedToSimple(logger, p).first;
  foundValue = p.second;

  LOG_DEBUG(logger, "Found key " << foundKey << " and value " << foundValue);

  if (!iter->isEnd()) {
    BlockId currentReadVersion =
        KeyManipulator::extractBlockIdFromKey(logger, p.first);

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

  m_db->freeIterator(iter);

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
Status BlockchainDBAdapter::getBlockById(BlockId _blockId, Sliver &_blockRaw,
                                         bool &_found) const {
  Sliver key = KeyManipulator::genBlockDbKey(_blockId);
  Status s = m_db->get(key, _blockRaw);
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
Status BlockchainDBAdapter::first(IDBClient::IDBClientIterator *iter,
                                  BlockId readVersion,
                                  OUT BlockId &actualVersion, OUT bool &isEnd,
                                  OUT Sliver &_key, OUT Sliver &_value) {
  Key firstKey;
  KeyValuePair p = KeyManipulator::composedToSimple(logger, iter->first());
  if (iter->isEnd()) {
    m_isEnd = true;
    isEnd = true;
    return Status::NotFound("No keys");
  } else {
    CopyKey(p.first, firstKey);
  }

  bool foundKey = false;
  Sliver value;
  BlockId actualBlock;
  while (!iter->isEnd() && p.first == firstKey) {
    BlockId currentBlock =
        KeyManipulator::extractBlockIdFromKey(logger, iter->getCurrent().first);
    if (currentBlock <= readVersion) {
      value = p.second;
      actualBlock = currentBlock;
      foundKey = true;
      p = KeyManipulator::composedToSimple(logger, iter->next());
    } else {
      if (!foundKey) {
        // If not found a key with actual block version < readVersion, then we
        // consider the next key as the first key candidate.

        // Start by exhausting the current key with all the newer blocks
        // records:
        while (!iter->isEnd() && p.first == firstKey) {
          p = KeyManipulator::composedToSimple(logger, iter->next());
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
Status BlockchainDBAdapter::seekAtLeast(IDBClient::IDBClientIterator *iter,
                                        Sliver _searchKey, BlockId _readVersion,
                                        OUT BlockId &_actualVersion,
                                        OUT Sliver &_key, OUT Sliver &_value,
                                        OUT bool &_isEnd) {
  Key searchKey = _searchKey;
  BlockId actualBlock;
  Value value;
  bool foundKey = false;
  Sliver rocksKey = KeyManipulator::genDataDbKey(searchKey, _readVersion);
  KeyValuePair p =
      KeyManipulator::composedToSimple(logger, iter->seekAtLeast(rocksKey));

  if (!iter->isEnd()) {
    // p.first is src, searchKey is target
    CopyKey(p.first, searchKey);
  }

  LOG_DEBUG(
      logger, "Searching " << _searchKey << " and currently iterator returned "
                           << searchKey << " for rocks key " << rocksKey);

  while (!iter->isEnd() && p.first == searchKey) {
    BlockId currentBlockId =
        KeyManipulator::extractBlockIdFromKey(logger, iter->getCurrent().first);

    LOG_DEBUG(logger, "Considering key " << p.first << " with block ID "
                                               << currentBlockId);

    if (currentBlockId <= _readVersion) {
      LOG_DEBUG(logger, "Found with Block Id " << currentBlockId
                                                     << " and value "
                                                     << p.second);
      value = p.second;
      actualBlock = currentBlockId;
      foundKey = true;
      break;
    } else {
      LOG_DEBUG(
          logger, "Read version " << currentBlockId << " > " << _readVersion);
      if (!foundKey) {
        // If not found a key with actual block version < readVersion, then
        // we consider the next key as the key candidate.
        LOG_DEBUG(logger, "Find next key");

        // Start by exhausting the current key with all the newer blocks
        // records:
        while (!iter->isEnd() && p.first == searchKey) {
          p = KeyManipulator::composedToSimple(logger, iter->next());
        }

        if (iter->isEnd()) {
          break;
        }

        CopyKey(p.first, searchKey);

        LOG_DEBUG(logger, "Found new search key " << searchKey);
      } else {
        // If we already found a suitable key, we break when we find the
        // maximal
        break;
      }
    }
  }

  if (iter->isEnd() && !foundKey) {
    LOG_DEBUG(logger,
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
  LOG_DEBUG(logger, "Returnign key "
                              << _key << " value " << _value
                              << " in actual block " << _actualVersion
                              << ", read version " << _readVersion);
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
Status BlockchainDBAdapter::next(IDBClient::IDBClientIterator *iter,
                                 BlockId _readVersion, OUT Sliver &_key,
                                 OUT Sliver &_value,
                                 OUT BlockId &_actualVersion,
                                 OUT bool &_isEnd) {
  KeyValuePair p = KeyManipulator::composedToSimple(logger, iter->getCurrent());
  Key currentKey = p.first;

  // Exhaust all entries for this key
  while (!iter->isEnd() && p.first == currentKey) {
    p = KeyManipulator::composedToSimple(logger, iter->next());
  }

  if (iter->isEnd()) {
    m_isEnd = true;
    _isEnd = true;
    return Status::OK();
  }

  // Find most updated value for next key (with block version < readVersion)
  Value value;
  BlockId actualBlock;
  Key nextKey;
  CopyKey(p.first, nextKey);
  bool foundKey = false;
  // Find max version
  while (!iter->isEnd() && p.first == nextKey) {
    BlockId currentBlockId =
        KeyManipulator::extractBlockIdFromKey(logger, iter->getCurrent().first);
    if (currentBlockId <= _readVersion) {
      value = p.second;
      actualBlock = currentBlockId;
      p = KeyManipulator::composedToSimple(logger, iter->next());
    } else {
      if (!foundKey) {
        // If not found a key with actual block version < readVersion, then
        // we consider the next key as the key candidate.

        // Start by exhausting the current key with all the newer blocks
        // records:
        while (!iter->isEnd() && p.first == nextKey) {
          p = KeyManipulator::composedToSimple(logger, iter->next());
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
Status BlockchainDBAdapter::getCurrent(IDBClient::IDBClientIterator *iter,
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
Status BlockchainDBAdapter::isEnd(IDBClient::IDBClientIterator *iter,
                                  OUT bool &_isEnd) {
  // Not calling to underlying DB iterator, because it may have next()'d during
  // seekAtLeast
  LOG_DEBUG(logger, "Called is end, returning " << m_isEnd);
  _isEnd = m_isEnd;
  return Status::OK();
}

/**
 * @brief Used to monitor the database.
 */
void BlockchainDBAdapter::monitor() const { m_db->monitor(); }

/**
 * @brief Used to retrieve the latest block.
 *
 * Searches for the key with the largest block id component.
 *
 * @return Block ID of the latest block.
 */
BlockId BlockchainDBAdapter::getLatestBlock() {
  // Note: RocksDB stores keys in a sorted fashion as per the logic
  // provided in a custom comparator (for our case, refer to
  // Comparators.cpp). In short, keys of type 'block' are stored
  // first followed by keys of type 'key'. All keys of type 'block'
  // are sorted in ascending order of block ids.

  // Generate maximal key for type 'block'
  Sliver maxKey =
      KeyManipulator::genDbKey(EDBKeyType::E_DB_KEY_TYPE_BLOCK, Sliver(),
                               std::numeric_limits<uint64_t>::max());
  IDBClient::IDBClientIterator *iter = m_db->getIterator();

  // Since we use the maximal key, SeekAtLeast will take the iterator
  // to one position beyond the key corresponding to the largest block id.
  KeyValuePair x = iter->seekAtLeast(maxKey);

  // Read the previous key
  x = iter->previous();

  m_db->freeIterator(iter);

  if ((x.first).length() == 0) {  // no blocks
    return 0;
  }

  LOG_DEBUG(
      logger, "Latest block ID "
                  << KeyManipulator::extractBlockIdFromKey(logger, x.first));

  return KeyManipulator::extractBlockIdFromKey(logger, x.first);
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
BlockId BlockchainDBAdapter::getLastReachableBlock() {
  IDBClient::IDBClientIterator *iter = m_db->getIterator();

  BlockId lastReachableId = 0;
  Sliver blockKey = KeyManipulator::genBlockDbKey(1);
  KeyValuePair kvp = iter->seekAtLeast(blockKey);
  if (kvp.first.length() == 0) {
    m_db->freeIterator(iter);
    return 0;
  }

  while (!iter->isEnd() && (KeyManipulator::extractTypeFromKey(kvp.first) ==
                            (char)EDBKeyType::E_DB_KEY_TYPE_BLOCK)) {
    BlockId id = KeyManipulator::extractBlockIdFromKey(logger, kvp.first);
    if (id == lastReachableId + 1) {
      lastReachableId++;
      kvp = iter->next();
    } else {
      break;
    }
  }

  m_db->freeIterator(iter);
  return lastReachableId;
}

}
}
}
