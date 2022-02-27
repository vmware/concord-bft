// Copyright 2018 VMware, all rights reserved

#include "memorydb/client.h"
#include "memorydb/transaction.h"

#include <cstdint>
#include <chrono>
#include <cstring>
#include <iterator>

#include "assertUtils.hpp"
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
void Client::init(bool readOnly) {}

/**
 * @brief Services a read request from the In Memory Database.
 *
 * Tries to get the value associated with a key.
 * @param _key Reference to the key being looked up.
 * @param _outValue Reference to where the value gets stored if the lookup is
 *                  successful.
 * @return Status NotFound if no mapping is found, else, Status OK.
 */
Status Client::get(const Sliver &_key, Sliver &_outValue) const {
  try {
    _outValue = map_.at(_key);
  } catch (const std::out_of_range &oor) {
    return Status::NotFound(oor.what());
  }
  storage_metrics_.keys_reads_++;
  storage_metrics_.total_read_bytes_ += _outValue.length();
  return Status::OK();
}

Status Client::get(const Sliver &_key, char *&buf, uint32_t bufSize, uint32_t &_size) const {
  Sliver value;
  auto status = get(_key, value);
  if (!status.isOK()) return status;

  _size = static_cast<uint32_t>(value.length());
  if (bufSize < _size) {
    LOG_ERROR(logger, "Object value is bigger than specified buffer bufSize=" << bufSize << ", _realSize=" << _size);
    return Status::GeneralError("Object value is bigger than specified buffer");
  }
  memcpy(buf, value.data(), _size);
  return status;
}

Status Client::has(const Sliver &_key) const {
  Sliver dummy_out;
  return get(_key, dummy_out);
}

/**
 * @brief Returns reference to a new object of IDBClientIterator.
 *
 * @return A pointer to IDBClientIterator object.
 */
IDBClient::IDBClientIterator *Client::getIterator() const { return new ClientIterator((Client *)this); }

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
Status Client::put(const Sliver &_key, const Sliver &_value) {
  map_.insert_or_assign(_key, _value.clone());
  storage_metrics_.keys_writes_++;
  storage_metrics_.total_written_bytes_ += _key.length() + _value.length();
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
Status Client::del(const Sliver &_key) {
  map_.erase(_key);
  return Status::OK();
}

Status Client::multiGet(const KeysVector &_keysVec, ValuesVector &_valuesVec) {
  Status status = Status::OK();
  for (auto const &it : _keysVec) {
    Sliver sliver;
    status = get(it, sliver);
    if (!status.isOK()) return status;
    _valuesVec.push_back(std::move(sliver));
  }
  return status;
}

Status Client::multiPut(const SetOfKeyValuePairs &_keyValueMap, bool sync) {
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
 * @brief Deletes keys in the [_beginKey, _endKey) range.
 *
 * @param _begin Reference to the begin key in the range (included).
 * @param _end Reference to the end key in the range (excluded).
 * @return Status OK.
 */
Status Client::rangeDel(const Sliver &_beginKey, const Sliver &_endKey) {
  if (_beginKey == _endKey) {
    return Status::OK();
  }

  // Make sure that _beginKey comes before _endKey .
  ConcordAssert(comp_(_beginKey, _endKey));

  auto beginIt = map_.lower_bound(_beginKey);
  if (beginIt == std::end(map_)) {
    return Status::OK();
  }
  auto endIt = map_.lower_bound(_endKey);
  map_.erase(beginIt, endIt);
  return Status::OK();
}

ITransaction *Client::beginTransaction() {
  // The transaction ID counter is intentionally not thread-safe as memorydb only supports single-thread operations.
  static std::uint64_t current_transaction_id = 0;
  return new Transaction{*this, ++current_transaction_id};
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
  auto &metrics = m_parentClient->getStorageMetrics();
  metrics.keys_reads_++;
  metrics.total_read_bytes_ += m_current->second.length();
  return KeyValuePair(m_current->first, m_current->second);
}

/**
 * @brief Moves the iterator to the last element of the map.
 *
 * @return The last element of the map if it is not empty and an empty pair otherwise.
 */
KeyValuePair ClientIterator::last() {
  if (m_parentClient->getMap().empty()) {
    m_current = m_parentClient->getMap().end();
    return KeyValuePair();
  }

  m_current = --m_parentClient->getMap().end();
  auto &metrics = m_parentClient->getStorageMetrics();
  metrics.keys_reads_++;
  metrics.total_read_bytes_ += m_current->second.length();
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
KeyValuePair ClientIterator::seekAtLeast(const Sliver &_searchKey) {
  m_current = m_parentClient->getMap().lower_bound(_searchKey);
  if (m_current == m_parentClient->getMap().end()) {
    LOG_TRACE(logger, "Key " << _searchKey << " not found");
    return KeyValuePair();
  }
  auto &metrics = m_parentClient->getStorageMetrics();
  metrics.keys_reads_++;
  metrics.total_read_bytes_ += m_current->second.length();
  return KeyValuePair(m_current->first, m_current->second);
}

/**
 * @brief Returns the key value pair of the last key which is less than or equal
 * to _searchKey.
 *
 *  @param _searchKey Key to search for.
 *  @return Key value pair of the last key which is less or equal to
 *  _searchKey.
 */
KeyValuePair ClientIterator::seekAtMost(const Sliver &_searchKey) {
  const auto &map = m_parentClient->getMap();
  if (map.empty()) {
    m_current = map.end();
    LOG_TRACE(logger, "Key " << _searchKey << " not found");
    return KeyValuePair();
  }

  // Find keys that are greater than or equal to the search key.
  m_current = map.lower_bound(_searchKey);
  if (m_current == map.end()) {
    // If there are no keys that are greater than or equal to the search key, then it means the last one is less than
    // the search key. Therefore, go back from the end iterator.
    --m_current;
  } else if (_searchKey != m_current->first) {
    // We have found a key that is greater than the search key. If it is not the first element, it means that the
    // previous one will be less than the search key. If it is the first element, it means there are no keys that are
    // less than the search key and we return an empty key/value pair.
    if (m_current != map.begin()) {
      --m_current;
    } else {
      return KeyValuePair();
    }
  }
  auto &metrics = m_parentClient->getStorageMetrics();
  metrics.keys_reads_++;
  metrics.total_read_bytes_ += m_current->second.length();
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
  auto &metrics = m_parentClient->getStorageMetrics();
  metrics.keys_reads_++;
  metrics.total_read_bytes_ += m_current->second.length();
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
  auto &metrics = m_parentClient->getStorageMetrics();
  metrics.keys_reads_++;
  metrics.total_read_bytes_ += m_current->second.length();
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
  auto &metrics = m_parentClient->getStorageMetrics();
  metrics.keys_reads_++;
  metrics.total_read_bytes_ += m_current->second.length();
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
