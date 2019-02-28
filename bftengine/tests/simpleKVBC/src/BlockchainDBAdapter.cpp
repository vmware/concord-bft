// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

/** @file BlockChainDBAdapter.cpp
 *  @brief Contains helper functions for working with composite database keys
 *  and using these keys to perform basic database operations.
 *
 *  Data is stored in the form of key value pairs. However, the key used is a
 * composite database key. Its composition is : Key Type | Key | Block Id
 *
 *  Block Id is functionally equivalent to the version of the block.
 *
 *  For the purposes of iteration, the keys appear in the following order :
 *  Ascending order of Key Type -> Ascending order of Key -> Descending order of
 * Block Id
 */

#include "BlockchainDBAdapter.h"
#include "KVBCInterfaces.h"
#include <chrono>

namespace SimpleKVBC {
/** @brief Generates a Composite Database Key from a Slice object.
 *  Merges the key type, data of the key and the block id to generate a
 * composite database key. Format : Key Type | Key | Block Id
 *  @param _type Composite Database key type (Either of First, Key, Block or
 * Last).
 *  @param _key Slice object of the key.
 *  @param _blockId BlockId object.
 *  @return Slice object of the generated composite database key.
 */
Slice genDbKey(EDBKeyType _type, Slice _key, BlockId _blockId) {
  size_t sz = sizeof(EDBKeyType) + sizeof(BlockId) + _key.size;
  char* out = new char[sz];
  size_t offset = 0;
  copyToAndAdvance(out, &offset, sz, (char*)&_type, sizeof(EDBKeyType));
  copyToAndAdvance(out, &offset, sz, (char*)_key.data, _key.size);
  copyToAndAdvance(out, &offset, sz, (char*)&_blockId, sizeof(BlockId));

  return Slice(out, sz);
}

/** @brief Helper function that generates a composite database Key of type
 * Block. For such Database keys, the "key" component is an empty slice.
 *  @param _blockid BlockId object of the block id that needs to be incorporated
 * into the composite database key.
 *  @return Slice object of the generated composite database key.
 */
Slice genBlockDbKey(BlockId _blockId) {
  return genDbKey(EDBKeyType::E_DB_KEY_TYPE_BLOCK, Slice(""), _blockId);
}

/** @brief Helper function that generates a composite database Key of type Key.
 *  @param _key Slice object of the "key" component.
 *  @param _blockid BlockId object of the block id that needs to be incorporated
 * into the composite database Key.
 *  @return Slice object of the generated composite database key.
 */
Slice genDataDbKey(Slice _key, BlockId _blockId) {
  return genDbKey(EDBKeyType::E_DB_KEY_TYPE_KEY, _key, _blockId);
}

/** @brief Extracts the type of a composite database key.
 *  Returns the data part of the Slice object passed as a parameter.
 *  @param _key The Slice object of the composite database key whose type gets
 * returned.
 *  @return The type of the composite database key.
 */
char extractTypeFromKey(Slice _key) { return _key.data[0]; }

/** @brief Extracts the Block Id of a composite database key.
 *  Returns the block id part of the Slice object passed as a parameter.
 *  @param _key The Slice object of the composite database key whose block id
 * gets returned.
 *  @return The block id of the composite database key.
 */
BlockId extractBlockIdFromKey(Slice _key) {
  size_t offset = _key.size - sizeof(BlockId);

  BlockId id = *(BlockId*)(_key.data + offset);

  return id;
}

/** @brief Extracts the key from a composite database key.
 *  @param _composedKey Slice object of the composite database key.
 *  @return Slice object of the key extracted from the composite database key.
 */
Slice extractKeyFromKeyComposedWithBlockId(Slice _composedKey) {
  size_t sz = _composedKey.size - sizeof(BlockId) - sizeof(EDBKeyType);

  Slice out = Slice(_composedKey.data + sizeof(EDBKeyType), sz);
  return out;
}

/** @brief Converts a key value pair consisting of a composite database key into
 *  a key value pair consisting of a simple key.
 *  @param _p Key value pair consisting of a composite database key
 *  @return Key value pair consisting of a simple key.
 */
KeyValuePair composedToSimple(KeyValuePair _p) {
  if (_p.first.size == 0) {
    return _p;
  }

  Key key = extractKeyFromKeyComposedWithBlockId(_p.first);
  return KeyValuePair(key, _p.second);
}

/** @brief Adds a block to the database.
 *  Generates a new composite database key of type Block and adds a block to the
 *  database using it. This composite database key is then deleted.
 *  @param _blockId BlockId object of the block id to be used for generating the
 *  composite database key.
 *  @param _blockRaw Slice object of the value that needs to be added to the
 *  database along with the composite database key.
 *  @return Status of the put operation.
 */
Status BlockchainDBAdapter::addBlock(BlockId _blockId, Slice _blockRaw) {
  Slice dbKey = genBlockDbKey(_blockId);
  Status s = m_db->put(dbKey, _blockRaw);
  delete[] dbKey.data;
  return s;
}

/** @brief Puts a key value pair to the database with a composite database key
 *  of type Key.
 *  Generates a new composite database key of type Key and adds a block to the
 *  database using it. This composite database key is then deleted.
 *  @param _key Key object of the key used for generating the composite database
 *  key.
 *  @param _block BlockId object of the block used for generating the composite
 *  database key.
 *  @param _value Value object of the value that needs to be added to the
 * database along with the composite database key.
 */
Status BlockchainDBAdapter::updateKey(Key _key, BlockId _block, Value _value) {
  Slice composedKey = genDataDbKey(_key, _block);

  Status s = m_db->put(composedKey, _value);
  delete[] composedKey.data;
  return s;
}

/** @brief Searches for record in the database by the read version.
 *  Read version is used as the block id for generating a composite database key
 *  which is then used to lookup in the database.
 *  @param readVersion BlockId object signifying the read version with which a
 * lookup needs to be done.
 *  @param key Slice object of the key.
 *  @param outValue Slice object where the value of the lookup result is stored.
 *  @param outBlock BlockId object where the read version of the result is
 * stored.
 *  @return Status OK
 */
Status BlockchainDBAdapter::getKeyByReadVersion(BlockId readVersion,
                                                Slice key,
                                                Slice& outValue,
                                                BlockId& outBlock) const {
  IDBClient::IDBClientIterator* iter = m_db->getIterator();
  Slice foundKey, foundValue;
  Slice searchKey = genDataDbKey(key, readVersion);
  KeyValuePair p = iter->seekAtLeast(searchKey);
  delete[] searchKey.data;
  foundKey = composedToSimple(p).first;
  foundValue = p.second;

  if (!iter->isEnd()) {
    BlockId currentReadVersion = extractBlockIdFromKey(p.first);

    if (currentReadVersion <= readVersion &&
        foundKey == key)  // TODO Ask about reason for version comparison logic
    {
      outValue = foundValue;
      outBlock = currentReadVersion;
    } else {
      outValue = Slice();
      outBlock = 0;
    }
  } else {
    outValue = Slice();
    outBlock = 0;
  }

  m_db->freeIterator(iter);
  return Status::OK();  // TODO maybe return status of the operation?
}

/** @brief Looks up data by block id.
 *  Constructs a composite database key using the block id to fire a get
 * request.
 *  @param _blockId BlockId object used for looking up data.
 *  @param _blockRaw Slice object where the result of the lookup is stored.
 *  @param _found true if lookup successful, else false.
 *  @return Status of the operation.
 */
Status BlockchainDBAdapter::getBlockById(BlockId _blockId,
                                         Slice& _blockRaw,
                                         bool& _found) const {
  Slice key = genBlockDbKey(_blockId);
  Status s = m_db->get(key, _blockRaw);
  delete[] key.data;
  if (s.IsNotFound()) {
    _found = false;
    return Status::OK();
  }

  _found = true;
  return s;
}

bool BlockchainDBAdapter::hasBlockId(
    BlockId _blockId) const  // patch (we don't handle IO errors here)
{
  Slice key = genBlockDbKey(_blockId);
  Status s = m_db->hasKey(key);

  return (s.ok());
}

/** @brief Frees up the value of a Slice object.
 *  @param _block Slice object that needs its value freed.
 *  @return Status OK.
 */
Status BlockchainDBAdapter::freeFetchedBlock(Slice& _block) const {
  m_db->freeValue(_block);
  return Status::OK();  // TODO mayble return the status of the operation?
}

// Used for handling iterator temporary values. Leaks all over!
/** @brief Makes a copy of a Slice object.
 *  @param _src Slice object that needs to be copied.
 *  @param _trg Slice object that contains the result.
 */
inline void CopyKey(Slice _src, Slice& _trg) {
  char* c = new char[_src.size];
  memcpy(c, _src.data, _src.size);
  _trg = Slice(c, _src.size);
}

// TODO(SG): Add status checks with getStatus() on iterator.
// TODO unserstand difference between .second and .data
/** @brief Finds the first key with block version lesser than or equal to
 * readVersion. Iterates from the first key to find the key with block version
 * lesser than or equal to the readVersion.
 *  @param iter Iterator object.
 *  @param readVersion BlockId object of the read version used for searching.
 *  @param actualVersion BlockId object in which the version of the result is
 * stored.
 *  @param isEnd True if end of the database is reached while iterating, else
 * false.
 *  @param _key Slice object of the result's key. Contains only the reference.
 *  @param _value Slice object of the result's value.
 *  @return Status "No keys" if database is empty
 *          Status OK otherwise.
 */
Status BlockchainDBAdapter::first(IDBClient::IDBClientIterator* iter,
                                  BlockId readVersion,
                                  OUT BlockId& actualVersion,
                                  OUT bool& isEnd,
                                  OUT Slice& _key,
                                  OUT Slice& _value) {
  Key firstKey;

  KeyValuePair p = composedToSimple(iter->first());
  if (iter->isEnd()) {
    m_isEnd = true;
    isEnd = true;
    return Status::NotFound("No keys");
  } else {
    CopyKey(p.first, firstKey);
  }

  bool foundKey = false;
  Slice value;
  BlockId actualBlock;
  while (!iter->isEnd() && p.first == firstKey) {
    BlockId currentBlock = extractBlockIdFromKey(iter->getCurrent().first);
    if (currentBlock <= readVersion) {
      value = p.second;
      actualBlock = currentBlock;
      foundKey = true;
      p = composedToSimple(iter->next());
    } else {
      if (!foundKey)  // If not found a key with actual block version <
                      // readVersion, then we consider the next key as the first
                      // key candidate.
      {
        // Start by exhausting the current key with all the newer blocks
        // records:
        // TODO(SG): Continue from here
        while (!iter->isEnd() &&
               p.first == firstKey)  // Exhaust all entries for this key
        {
          p = composedToSimple(iter->next());
        }

        if (iter->isEnd()) {
          break;
        }

        if (firstKey.size > 0) {
          delete[] firstKey.data;
        }
        CopyKey(p.first, firstKey);
      } else  // If we already found a suitable first key, we break when we find
              // the maximal
      {
        break;
      }
    }
  }

  if (iter->isEnd() &&
      !foundKey)  // It is possible all keys have actualBlock > readVersion
                  // (Actually, this sounds like data is corrupted in this case
                  // - unless we allow empty blocks)
  {
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

/** @brief Finds the first key greater than or equal to the search key with
 * block version lesser than or equal to readVersion. Iterates from the first
 * key greater than or equal to the search key to find the key with block
 * version lesser than or equal to the readVersion.
 *  @param iter Iterator object.
 *  @param _searchKey Slice object of the search key.
 *  @param _readVersion BlockId object of the read version used for searching.
 *  @param _actualVersion BlockId object in which the version of the result is
 * stored.
 *  @param _key Slice object of the result's key. Contains only the reference.
 *  @param _value Slice object of the result's value.
 *  @param _isEnd True if end of the database is reached while iterating, else
 * false.
 *  @return Status "Did not find key with suitable read version" if search
 * unsuccessful, Status OK otherwise.
 */
// Only for data fields, i.e. E_DB_KEY_TYPE_KEY. It makes more sense to put data
// second, and blocks first. Stupid optimization nevertheless
Status BlockchainDBAdapter::seekAtLeast(IDBClient::IDBClientIterator* iter,
                                        Slice _searchKey,
                                        BlockId _readVersion,
                                        OUT BlockId& _actualVersion,
                                        OUT Slice& _key,
                                        OUT Slice& _value,
                                        OUT bool& _isEnd) {
  Key searchKey = _searchKey;
  BlockId actualBlock;
  Value value;
  bool foundKey = false;
  Slice rocksKey = genDataDbKey(searchKey, _readVersion);
  KeyValuePair p = composedToSimple(iter->seekAtLeast(rocksKey));

  if (!iter->isEnd()) {
    CopyKey(p.first, searchKey);  // p.first is src, searchKey is target
    // searchKey = Slice(p.first.data, p.first.size);
  }

  while (!iter->isEnd() && p.first == searchKey) {
    BlockId currentBlockId = extractBlockIdFromKey(iter->getCurrent().first);

    if (currentBlockId <= _readVersion) {
      value = p.second;
      actualBlock = currentBlockId;
      foundKey = true;
      break;
    } else {
      if (!foundKey)  // If not found a key with actual block version <
                      // readVersion, then we consider the next key as the key
                      // candidate.
      {
        while (!iter->isEnd() &&
               p.first == searchKey)  // Exhaust all entries for this key
        {
          p = composedToSimple(iter->next());
        }
        if (iter->isEnd()) {
          break;
        }

        if (searchKey.size > 0) {
          delete[] searchKey.data;
        }
        CopyKey(p.first, searchKey);
      } else  // If we already found a suitable key, we break when we find the
              // maximal
      {
        break;
      }
    }
  }

  delete[] rocksKey.data;

  if (iter->isEnd() && !foundKey) {
    m_isEnd = true;
    _isEnd = true;
    return Status::NotFound("Did not find key with suitable read version");

    _isEnd = true;
    return Status::NotFound("Did not find key with suitable read version");
  }

  m_isEnd = false;
  _isEnd = false;
  _actualVersion = actualBlock;
  _key = searchKey;
  _value = value;
  return Status::OK();
}

/** @brief Finds the next key with the most recently updated value.
 *  Finds the most updated value for the next key (with block version <
 * readVersion).
 *  @param iter Iterator.
 *  @param _readVersion BlockId object of the read version used for searching.
 *  @param _key Slice object of the result's key. Contains only the reference.
 *  @param _value Slice object of the result's value.
 *  @param _actualVersion BlockId object in which the version of the result is
 * stored.
 *  @param _isEnd True if end of the database is reached while iterating, else
 * false.
 *  @return Status OK.
 */
Status BlockchainDBAdapter::next(IDBClient::IDBClientIterator* iter,
                                 BlockId _readVersion,
                                 OUT Slice& _key,
                                 OUT Slice& _value,
                                 OUT BlockId& _actualVersion,
                                 OUT bool& _isEnd) {
  KeyValuePair p = composedToSimple(iter->getCurrent());
  Key currentKey = p.first;
  while (!iter->isEnd() &&
         p.first == currentKey)  // Exhaust all entries for this key
  {
    p = composedToSimple(iter->next());
  }

  if (iter->isEnd()) {
    m_isEnd = true;
    _isEnd = true;
    return Status::OK();
  }

  // Now find most updated value for next key (with block version < readVersion)
  Value value;
  BlockId actualBlock;
  Key nextKey;
  CopyKey(p.first, nextKey);
  bool foundKey = false;
  while (!iter->isEnd() && p.first == nextKey)  // Find max version
  {
    BlockId currentBlockId = extractBlockIdFromKey(iter->getCurrent().first);
    if (currentBlockId <= _readVersion) {
      value = p.second;
      actualBlock = currentBlockId;
      p = composedToSimple(iter->next());
    } else {
      if (!foundKey)  // If not found a key with actual block version <
                      // readVersion, then we consider the next key as the key
                      // candidate.
      {
        // Start by exhausting the current key with all the newer blocks
        // records:
        while (!iter->isEnd() &&
               p.first == nextKey)  // Exhaust all entries for this key
        {
          p = composedToSimple(iter->next());
        }
        if (iter->isEnd()) {
          break;
        }

        if (nextKey.size > 0) {
          delete[] nextKey.data;
        }
        CopyKey(p.first, nextKey);
      } else  // If we already found a suitable key, we break when we find the
              // maximal
      {
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
  return Status::OK();  // TODO return appropriate status?
}

/** @brief Finds the key and value at the current position.
 *  Does not use the iterator for this.
 *  @param iter Iterator.
 *  @param _key Slice object where the key of the result is stored.
 *  @param _value Slice object where the value of the result is stored.
 *  @return Status OK.
 */
Status BlockchainDBAdapter::getCurrent(IDBClient::IDBClientIterator* iter,
                                       OUT Slice& _key,
                                       OUT Slice& _value) {
  // Not calling to underlying DB iterator, because it may have next()'d during
  // seekAtLeast
  _key = m_current.first;
  _value = m_current.second;

  return Status::OK();
}

/** @brief Used to find if the iterator is at the end of the database.
 *  @param iter Iterator.
 *  @param _isEnd True if iterator is at the end of the database, else false.
 *  @return Status OK.
 */
Status BlockchainDBAdapter::isEnd(IDBClient::IDBClientIterator* iter,
                                  OUT bool& _isEnd) {
  _isEnd = m_isEnd;
  return Status::OK();
}

static BlockId fetchBlockId(Slice key) {
  assert(extractTypeFromKey(key) == (char)EDBKeyType::E_DB_KEY_TYPE_BLOCK);
  assert(key.size > sizeof(BlockId));

  const char* p = key.data + (key.size - sizeof(BlockId));

  BlockId retVal = *((BlockId*)p);

  for (size_t i = 0; i < key.size; i++) printf("%02X", key.data[i]);
  printf("\n");

  return retVal;
}

BlockId BlockchainDBAdapter::getLastBlock()  // TODO(GG): change - this is a
                                             // naive implementation
{
  IDBClient::IDBClientIterator* iter = getIterator();

  Slice blockKey = genBlockDbKey(lastKnownBlock + 1);
  KeyValuePair kvp = iter->seekAtLeast(blockKey);

  while (!iter->isEnd() && extractTypeFromKey(kvp.first) ==
                               (char)EDBKeyType::E_DB_KEY_TYPE_BLOCK) {
    lastKnownBlock++;
    kvp = iter->next();
  }

  freeIterator(iter);

  return lastKnownBlock;
}

BlockId
BlockchainDBAdapter::getLastReachableBlock()  // TODO(GG): change - this is a
                                              // naive implementation
{
  IDBClient::IDBClientIterator* iter = getIterator();

  Slice blockKey = genBlockDbKey(lastKnownReachableBlock + 1);
  KeyValuePair kvp = iter->seekAtLeast(blockKey);

  while (!iter->isEnd() &&
         (extractTypeFromKey(kvp.first) ==
          (char)EDBKeyType::E_DB_KEY_TYPE_BLOCK) &&
         (fetchBlockId(kvp.first) == lastKnownReachableBlock + 1)) {
    lastKnownReachableBlock++;
    kvp = iter->next();
  }

  freeIterator(iter);

  return lastKnownReachableBlock;
}
}  // namespace SimpleKVBC
