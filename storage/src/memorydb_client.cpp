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

void Client::init(bool readOnly) {}

std::set<std::string> Client::partitions() const {
  auto ret = std::set<std::string>{};
  for (const auto &p : map_) {
    ret.insert(p.first);
  }
  return ret;
}

bool Client::hasPartition(const std::string &partition) const { return (map_.count(partition) == 1); }

Status Client::addPartition(const std::string &partition) {
  if (hasPartition(partition)) {
    return Status::InvalidArgument("Partition already exists");
  }
  map_[partition] = TKVStore{[this](const Sliver &a, const Sliver &b) { return comp_(a, b); }};
  return Status::OK();
}

Status Client::dropPartition(const std::string &partition) {
  if (partition == defaultPartition()) {
    return Status::IllegalOperation("Cannot delete the default partition");
  }
  if (!hasPartition(partition)) {
    return Status::InvalidArgument("Partition doesn't exist");
  }
  map_.erase(partition);
  return Status::OK();
}

Status Client::get(const std::string &partition, const Sliver &key, Sliver &outValue) const {
  auto partitionIt = map_.find(partition);
  if (partitionIt == std::cend(map_)) {
    return Status::InvalidArgument("Cannot get a key-value from an unknown partition");
  }
  try {
    outValue = partitionIt->second.at(key);
  } catch (const std::out_of_range &oor) {
    return Status::NotFound(oor.what());
  }
  storage_metrics_.keys_reads_.Get().Inc();
  storage_metrics_.total_read_bytes_.Get().Inc(outValue.length());
  storage_metrics_.tryToUpdateMetrics();
  return Status::OK();
}

Status Client::get(const Sliver &key, Sliver &outValue) const { return get(kDefaultPartition, key, outValue); }

Status Client::get(const Sliver &_key, OUT char *&buf, uint32_t bufSize, OUT uint32_t &_size) const {
  Sliver value;
  auto status = get(kDefaultPartition, _key, value);
  if (!status.isOK()) return status;

  _size = static_cast<uint32_t>(value.length());
  if (bufSize < _size) {
    LOG_ERROR(logger, "Object value is bigger than specified buffer bufSize=" << bufSize << ", _realSize=" << _size);
    return Status::GeneralError("Object value is bigger than specified buffer");
  }
  memcpy(buf, value.data(), _size);
  return status;
}

Status Client::has(const std::string &partition, const Sliver &key) const {
  Sliver dummy_out;
  return get(partition, key, dummy_out);
}

Status Client::has(const Sliver &key) const { return has(kDefaultPartition, key); }

std::unique_ptr<IDBClient::IDBClientIterator> Client::getIterator() const {
  return std::make_unique<ClientIterator>(this, map_.at(kDefaultPartition));
}

std::unique_ptr<IDBClient::IDBClientIterator> Client::getIterator(const std::string &partition) const {
  auto partitionIt = map_.find(partition);
  if (partitionIt == std::cend(map_)) {
    return nullptr;
  }
  return std::make_unique<ClientIterator>(this, partitionIt->second);
}

/**
 * @brief Services a write request to the In Memory database by adding a key
 * value pair to the map.
 *
 * If the map already contains the key, it replaces the value with the data
 * referred to by _value.
 *
 * @param partition DB partition to put the key value mapping.
 * @param key Key of the mapping.
 * @param value Value of the mapping.
 * @return Status OK on success or IllegalOperation when the passed partition doesn't exist.
 */
Status Client::put(const std::string &partition, const Sliver &key, const Sliver &value) {
  auto partitionIt = map_.find(partition);
  if (partitionIt == std::cend(map_)) {
    return Status::InvalidArgument("Cannot put key-values into an unknown partition");
  }
  partitionIt->second.insert_or_assign(key, value.clone());
  storage_metrics_.keys_writes_.Get().Inc();
  storage_metrics_.total_written_bytes_.Get().Inc(key.length() + value.length());
  storage_metrics_.tryToUpdateMetrics();
  return Status::OK();
}

Status Client::put(const Sliver &key, const Sliver &value) { return put(kDefaultPartition, key, value); }

/**
 * @brief Deletes mapping from map.
 *
 * If map contains _key, this function will delete the key value pair from it.
 *
 * @param partition DB partition to delete the key from.
 * @param key Reference to the key of the mapping.
 * @return Status OK on success or IllegalOperation when the passed partition doesn't exist.
 */
Status Client::del(const std::string &partition, const Sliver &key) {
  auto partitionIt = map_.find(partition);
  if (partitionIt == std::cend(map_)) {
    return Status::InvalidArgument("Cannot delete key-values from an unknown partition");
  }
  partitionIt->second.erase(key);
  return Status::OK();
}

Status Client::del(const Sliver &key) { return del(kDefaultPartition, key); }

Status Client::multiGet(const std::string &partition, const KeysVector &keys, ValuesVector &values) const {
  values.clear();
  if (!hasPartition(partition)) {
    return Status::InvalidArgument("Cannot get key-values from an unknown partition");
  }
  for (auto const &it : keys) {
    Sliver sliver;
    auto status = get(partition, it, sliver);
    if (!status.isOK()) return status;
    values.push_back(std::move(sliver));
  }
  return Status::OK();
}

Status Client::multiGet(const KeysVector &keys, ValuesVector &values) const {
  return multiGet(kDefaultPartition, keys, values);
}

Status Client::multiPut(const std::string &partition, const SetOfKeyValuePairs &keyValues) {
  if (!hasPartition(partition)) {
    return Status::InvalidArgument("Cannot multiPut key-values into an unknown partition");
  }
  for (const auto &it : keyValues) {
    [[maybe_unused]] const auto status = put(partition, it.first, it.second);
    // Failing individual puts will leave the DB in inconsistent state. If that happens, assert.
    ConcordAssert(status.isOK());
  }
  return Status::OK();
}

Status Client::multiPut(const SetOfKeyValuePairs &keyValues) { return multiPut(kDefaultPartition, keyValues); }

Status Client::multiDel(const std::string &partition, const KeysVector &keys) {
  if (!hasPartition(partition)) {
    return Status::InvalidArgument("Cannot delete key-values from an unknown partition");
  }
  for (auto &k : keys) {
    [[maybe_unused]] const auto status = del(partition, k);
    // Failing individual deletions will leave the DB in inconsistent state. If that happens, assert.
    ConcordAssert(status.isOK());
  }
  return Status::OK();
}

Status Client::multiDel(const KeysVector &keys) { return multiDel(kDefaultPartition, keys); }

Status Client::multiGet(const std::vector<PartitionedKey> &keys, ValuesVector &values) const {
  values.clear();
  for (auto &pk : keys) {
    auto value = Sliver{};
    auto status = get(pk.partition, pk.key, value);
    if (!status.isOK()) return status;
    values.emplace_back(std::move(value));
  }
  return Status::OK();
}

Status Client::multiPut(const std::vector<PartitionedKeyValue> &keyValues) {
  // Ensure individual puts will not fail due to a missing partition before changing the DB.
  for (auto &pkv : keyValues) {
    if (!hasPartition(pkv.partition)) {
      return Status::InvalidArgument("Cannot multiPut key-values into an unknown partition");
    }
  }
  for (auto &pkv : keyValues) {
    [[maybe_unused]] const auto status = put(pkv.partition, pkv.keyValue.first, pkv.keyValue.second);
    // Failing individual puts will leave the DB in inconsistent state. If that happens, assert.
    ConcordAssert(status.isOK());
  }
  return Status::OK();
}

Status Client::multiDel(const std::vector<PartitionedKey> &keys) {
  // Ensure individual deletions will not fail due to a missing partition before changing the DB.
  for (auto &pk : keys) {
    if (!hasPartition(pk.partition)) {
      return Status::InvalidArgument("Cannot multiDel key-values from an unknown partition");
    }
  }
  for (auto &pk : keys) {
    [[maybe_unused]] const auto status = del(pk.partition, pk.key);
    // Failing individual deletions will leave the DB in inconsistent state. If that happens, assert.
    ConcordAssert(status.isOK());
  }
  return Status::OK();
}

/**
 * @brief Deletes keys in the [_beginKey, _endKey) range.
 *
 * @param partition A DB partition to delete a range from.
 * @param begin Reference to the begin key in the range (included).
 * @param end Reference to the end key in the range (excluded).
 * @return Status OK on success or InvalidArgument when the passed partition doesn't exist.
 */
Status Client::rangeDel(const std::string &partition, const Sliver &beginKey, const Sliver &endKey) {
  auto partitionIt = map_.find(partition);
  if (partitionIt == std::cend(map_)) {
    return Status::InvalidArgument("Cannot delete a range from an uknown partition");
  }
  auto &kvMap = partitionIt->second;

  if (beginKey == endKey) {
    return Status::OK();
  }

  // Make sure that _beginKey comes before _endKey .
  ConcordAssert(comp_(beginKey, endKey));

  auto beginIt = kvMap.lower_bound(beginKey);
  if (beginIt == std::end(kvMap)) {
    return Status::OK();
  }
  auto endIt = kvMap.lower_bound(endKey);
  kvMap.erase(beginIt, endIt);
  return Status::OK();
}

Status Client::rangeDel(const Sliver &beginKey, const Sliver &endKey) {
  return rangeDel(kDefaultPartition, beginKey, endKey);
}

// The transaction ID counter is intentionally not thread-safe as memorydb only supports single-thread operations.
static std::uint64_t current_transaction_id = 0;

ITransaction *Client::beginTransaction() { return new Transaction{*this, ++current_transaction_id}; }

std::unique_ptr<ITransaction> Client::startTransaction() { return std::unique_ptr<ITransaction>{beginTransaction()}; }

std::unique_ptr<IPartitionedTransaction> Client::startPartitionedTransaction() {
  return std::make_unique<Transaction>(*this, ++current_transaction_id);
}

/**
 * @brief Moves the iterator to the start of the map.
 *
 * @return Moves the iterator to the start of the map and returns the first key
 * value pair of the map.
 */
KeyValuePair ClientIterator::first() {
  m_current = m_partitionStore.begin();
  if (m_current == m_partitionStore.end()) {
    m_valid = false;
    return KeyValuePair();
  }
  m_valid = true;
  auto &metrics = m_parentClient->getStorageMetrics();
  metrics.keys_reads_.Get().Inc();
  metrics.total_read_bytes_.Get().Inc(m_current->second.length());
  return KeyValuePair(m_current->first, m_current->second);
}

/**
 * @brief Moves the iterator to the last element of the map.
 *
 * @return The last element of the map if it is not empty and an empty pair otherwise.
 */
KeyValuePair ClientIterator::last() {
  if (m_partitionStore.empty()) {
    m_valid = false;
    m_current = m_partitionStore.end();
    return KeyValuePair();
  }
  m_valid = true;

  m_current = --m_partitionStore.end();
  auto &metrics = m_parentClient->getStorageMetrics();
  metrics.keys_reads_.Get().Inc();
  metrics.total_read_bytes_.Get().Inc(m_current->second.length());
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
  m_current = m_partitionStore.lower_bound(_searchKey);
  if (m_current == m_partitionStore.end()) {
    m_valid = false;
    LOG_TRACE(logger, "Key " << _searchKey << " not found");
    return KeyValuePair();
  }
  m_valid = true;
  auto &metrics = m_parentClient->getStorageMetrics();
  metrics.keys_reads_.Get().Inc();
  metrics.total_read_bytes_.Get().Inc(m_current->second.length());
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
  if (m_partitionStore.empty()) {
    m_valid = false;
    m_current = m_partitionStore.end();
    LOG_TRACE(logger, "Key " << _searchKey << " not found");
    return KeyValuePair();
  }

  // Find keys that are greater than or equal to the search key.
  m_current = m_partitionStore.lower_bound(_searchKey);
  if (m_current == m_partitionStore.end()) {
    // If there are no keys that are greater than or equal to the search key, then it means the last one is less than
    // the search key. Therefore, go back from the end iterator.
    --m_current;
  } else if (_searchKey != m_current->first) {
    // We have found a key that is greater than the search key. If it is not the first element, it means that the
    // previous one will be less than the search key. If it is the first element, it means there are no keys that are
    // less than the search key and we return an empty key/value pair.
    if (m_current != m_partitionStore.begin()) {
      --m_current;
    } else {
      m_valid = false;
      return KeyValuePair();
    }
  }
  m_valid = true;
  auto &metrics = m_parentClient->getStorageMetrics();
  metrics.keys_reads_.Get().Inc();
  metrics.total_read_bytes_.Get().Inc(m_current->second.length());
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
  if (!m_valid) {
    return KeyValuePair();
  }
  if (m_current == m_partitionStore.begin()) {
    m_valid = false;
    return KeyValuePair();
  }
  --m_current;
  auto &metrics = m_parentClient->getStorageMetrics();
  metrics.keys_reads_.Get().Inc();
  metrics.total_read_bytes_.Get().Inc(m_current->second.length());
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
  if (!m_valid) {
    return KeyValuePair();
  }
  ++m_current;
  if (m_current == m_partitionStore.end()) {
    m_valid = false;
    return KeyValuePair();
  }
  auto &metrics = m_parentClient->getStorageMetrics();
  metrics.keys_reads_.Get().Inc();
  metrics.total_read_bytes_.Get().Inc(m_current->second.length());
  return KeyValuePair(m_current->first, m_current->second);
}

/**
 * @brief Returns the key value pair at the current position of the iterator.
 *
 * @return Current key value pair.
 */
KeyValuePair ClientIterator::getCurrent() {
  if (!m_valid) {
    return KeyValuePair();
  }
  auto &metrics = m_parentClient->getStorageMetrics();
  metrics.keys_reads_.Get().Inc();
  metrics.total_read_bytes_.Get().Inc(m_current->second.length());
  return KeyValuePair(m_current->first, m_current->second);
}

/**
 * @return True if iterator is valid and points to a key-value pair, else False.
 */
bool ClientIterator::valid() const { return m_valid; }

/**
 * @brief Does nothing.
 *
 * @return Status OK.
 */
Status ClientIterator::getStatus() const {
  // TODO Should be used for sanity checks.
  return Status::OK();
}

}  // namespace memorydb
}  // namespace storage
}  // namespace concord
