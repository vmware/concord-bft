// Copyright 2018 VMware, all rights reserved

#include "memorydb/client.h"

#include <chrono>
#include <cstring>

#include "hash_defs.h"
#include "sliver.hpp"

using concordUtils::Sliver;
using concordUtils::Status;

namespace concord {
namespace storage {
namespace memorydb {

/**
 * @brief Does nothing.
 *
 * Does nothing.
 * @return Status OK.
 */
Status Client::init(bool readOnly) {
  // TODO Can be used for constructor calls, etc.
  return Status::OK();
}

/**
 * @brief Services a read request from the In Memory Database.
 *
 * Tries to get the value associated with a key.
 * @param _key Reference to the key being looked up.
 * @param _outValue Reference to where the value gets stored if the lookup is
 *                  successful.
 * @return Status NotFound if no mapping is found, else, Status OK.
 */
Status Client::get(Sliver _key, OUT Sliver &_outValue) const {
  try {
    _outValue = map_.at(_key);
  } catch (const std::out_of_range &oor) {
    return Status::NotFound(oor.what());
  }

  return Status::OK();
}

Status Client::get(Sliver _key, OUT char *&buf, uint32_t bufSize, OUT uint32_t &_size) const {
  auto copy = Sliver::copy(buf, bufSize);
  auto status =  get(_key, copy);
  memcpy(buf, copy.data(), bufSize);
  return status;
}

/**
 * @brief Returns reference to a new object of IDBClientIterator.
 *
 * @return A pointer to IDBClientIterator object.
 */
IDBClient::IDBClientIterator *Client::getIterator() const {
  return new ClientIterator((Client *)this);
}

/**
 * @brief Frees the IDBClientIterator.
 *
 * @param _iter Pointer to object of class IDBClientIterator that needs to be
 *              freed.
 * @return Status InvalidArgument if iterator is null pointer, else, Status OK.
 */
Status Client::freeIterator(IDBClientIterator *_iter) const {
  if (_iter == NULL) {
    return Status::InvalidArgument("Invalid iterator");
  }

  delete (ClientIterator *)_iter;
  return Status::OK();
}

/**
 * @brief Services a write request to the In Memory database by adding a key
 * value pair to the map.
 *
 * If the map already contains the key, it replaces the value with the data
 * referred to by _value.
 *
 * @param _key Key of the mapping.
 * @param _value Value of the mapping.
 * @return Status OK.
 */
Status Client::put(Sliver _key, Sliver _value) {
  // Copy the key and the value
  bool keyExists = false;
  if (map_.find(_key) != map_.end()) {
    keyExists = true;
  }

  Sliver key;
  if (!keyExists) {
    uint8_t *keyBytes = new uint8_t[_key.length()];
    memcpy(keyBytes, _key.data(), _key.length());
    key = Sliver(keyBytes, _key.length());
  } else {
    key = _key;
  }

  Sliver value;
  uint8_t *valueBytes = new uint8_t[_value.length()];
  memcpy(valueBytes, _value.data(), _value.length());
  value = Sliver(valueBytes, _value.length());

  map_[key] = value;

  return Status::OK();
}

/**
 * @brief Deletes mapping from map.
 *
 * If map contains _key, this function will delete the key value pair from it.
 *
 * @param _key Reference to the key of the mapping.
 * @return Status OK.
 */
Status Client::del(Sliver _key) {
  bool keyExists = false;
  if (map_.find(_key) != map_.end()) {
    keyExists = true;
  }

  if (keyExists) {
    Sliver value = map_[_key];
    map_.erase(_key);
  }
  // Else: Error to delete non-existing key?

  return Status::OK();
}

Status Client::multiGet(const KeysVector &_keysVec, OUT ValuesVector &_valuesVec) {
  Status status = Status::OK();
  Sliver sliver;
  for (auto const &it : _keysVec) {
    status = get(it, sliver);
    if (!status.isOK()) return status;
    _valuesVec.push_back(sliver);
  }
  return status;
}

Status Client::multiPut(const SetOfKeyValuePairs &_keyValueMap) {
  Status status = Status::OK();
  for (const auto &it : _keyValueMap) {
    status = put(it.first, it.second);
    if (!status.isOK()) return status;
  }
  return status;
}

Status Client::multiDel(const KeysVector &_keysVec) {
  Status status = Status::OK();
  for (auto const &it : _keysVec) {
    status = del(it);
    if (!status.isOK()) return status;
  }
  return status;
}

/**
 * @brief Moves the iterator to the start of the map.
 *
 * @return Moves the iterator to the start of the map and returns the first key
 * value pair of the map.
 */
KeyValuePair ClientIterator::first() {
  m_current = m_parentClient->getMap().begin();
  if (m_current == m_parentClient->getMap().end()) {
    return KeyValuePair();
  }

  return KeyValuePair(m_current->first, m_current->second);
}

/**
 * @brief Returns the key value pair of the key which is greater than or equal
 * to _searchKey.
 *
 *  Returns the first key value pair whose key is not considered to go before
 *  _searchKey. Also, moves the iterator to this position.
 *
 *  @param _searchKey Key to search for.
 *  @return Key value pair of the key which is greater than or equal to
 *  _searchKey.
 */
KeyValuePair ClientIterator::seekAtLeast(Sliver _searchKey) {
  m_current = m_parentClient->getMap().lower_bound(_searchKey);
  if (m_current == m_parentClient->getMap().end()) {
    LOG_WARN(logger, "Key " << _searchKey << " not found");
    return KeyValuePair();
  }

  return KeyValuePair(m_current->first, m_current->second);
}

/**
 * @brief Decrements the iterator.
 *
 * Decrements the iterator and returns the previous key value pair.
 *
 * @return The previous key value pair.
 */
KeyValuePair ClientIterator::previous() {
  if (m_current == m_parentClient->getMap().begin()) {
    LOG_WARN(logger, "Iterator already at first key");
    return KeyValuePair();
  }
  --m_current;
  return KeyValuePair(m_current->first, m_current->second);
}

/**
 * @brief Increments the iterator.
 *
 * Increments the iterator and returns the next key value pair.
 *
 * @return The next key value pair.
 */
KeyValuePair ClientIterator::next() {
  ++m_current;
  if (m_current == m_parentClient->getMap().end()) {
    return KeyValuePair();
  }

  return KeyValuePair(m_current->first, m_current->second);
}

/**
 * @brief Returns the key value pair at the current position of the iterator.
 *
 * @return Current key value pair.
 */
KeyValuePair ClientIterator::getCurrent() {
  if (m_current == m_parentClient->getMap().end()) {
    return KeyValuePair();
  }

  return KeyValuePair(m_current->first, m_current->second);
}

/**
 * @brief Tells whether iterator is at the end of the map.
 *
 * @return True if iterator is at the end of the map, else False.
 */
bool ClientIterator::isEnd() { return m_current == m_parentClient->getMap().end(); }

/**
 * @brief Does nothing.
 *
 * @return Status OK.
 */
Status ClientIterator::getStatus() {
  // TODO Should be used for sanity checks.
  return Status::OK();
}

}  // namespace memorydb
}  // namespace storage
}  // namespace concord
