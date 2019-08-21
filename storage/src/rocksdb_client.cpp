// Copyright 2018 VMware, all rights reserved

/**
 * @file RocksDBClient.cc
 *
 * @brief Contains helper functions for the RocksDBClient and
 * RocksDBClientIterator classes.
 *
 * Wrappers around RocksDB functions for standard database operations.
 * Functions are included for creating, using, and destroying iterators to
 * navigate through the RocksDB Database.
 */

#ifdef USE_ROCKSDB

#include "rocksdb/client.h"
#include "Logger.hpp"
#include "hash_defs.h"


using concordUtils::Sliver;
using concordUtils::Status;

namespace concord {
namespace storage {
namespace rocksdb {

// Counter for number of read requests
static unsigned int g_rocksdb_called_read;
static bool g_rocksdb_print_measurements;

/**
 * @brief Converts a Sliver object to a RocksDB Slice object.
 *
 * @param _s Sliver object.
 * @return A RocksDB Slice object.
 */
::rocksdb::Slice toRocksdbSlice(Sliver _s) {
  return ::rocksdb::Slice(reinterpret_cast<char *>(_s.data()), _s.length());
}

/**
 * @brief Wraps a RocksDB slice in a Sliver.
 *
 * Important: Sliver will release the data when it goes out of scope, so:
 *
 *  1. You must own the data inside the slice.
 *  2. Your use of that data must not happen after the Sliver is deleted.
 *
 * @param _s A RocksDB Slice object.
 * @return A Sliver object.
 */
Sliver fromRocksdbSlice(::rocksdb::Slice _s) {
  char *nonConstData = const_cast<char *>(_s.data());
  return Sliver(reinterpret_cast<uint8_t *>(nonConstData), _s.size());
}

/**
 * @brief Copies a RocksDB slice in a Sliver.
 *
 * @param _s A RocksDB Slice object.
 * @return A Sliver object.
 */
Sliver copyRocksdbSlice(::rocksdb::Slice _s) {
  uint8_t *copyData = new uint8_t[_s.size()];
  std::copy(_s.data(), _s.data() + _s.size(), copyData);
  return Sliver(copyData, _s.size());
}

bool Client::isNew() {
  ::rocksdb::DB *db;
  ::rocksdb::Options options;
  options.error_if_exists = true;
  ::rocksdb::Status s = ::rocksdb::DB::Open(options, m_dbPath, &db);
  return s.IsNotFound();
}

/**
 * @brief Opens a RocksDB database connection.
 *
 * Uses the RocksDBClient object variables m_dbPath and m_dbInstance to
 * establish a connection with RocksDB by creating a RocksDB object.
 *
 *  @return GeneralError in case of error in connection, else OK.
 */
Status Client::init(bool readOnly) {
  ::rocksdb::DB *db;
  ::rocksdb::Options options;
  options.create_if_missing = true;
  options.comparator = m_comparator;

  ::rocksdb::Status s;
  if (readOnly) {
    s = ::rocksdb::DB::OpenForReadOnly(options, m_dbPath, &db);
  } else {
    s = ::rocksdb::DB::Open(options, m_dbPath, &db);
  }
  m_dbInstance.reset(db);

  if (!s.ok()) {
    LOG_ERROR(logger, "Failed to open rocksdb database at "
                                << m_dbPath << " due to " << s.ToString());
    return Status::GeneralError("Database open error");
  }

  g_rocksdb_called_read = 0;

  // TODO(Shelly): Update for measurements. Remove when done as well as other
  // g_rocksdb_* variables.
  g_rocksdb_print_measurements = false;

  return Status::OK();
}

Status Client::get(Sliver _key, OUT std::string &_value) const {
  ++g_rocksdb_called_read;
  if (g_rocksdb_print_measurements) {
    LOG_DEBUG(logger, "Reading count = " << g_rocksdb_called_read
                                               << ", key " << _key);
  }
  ::rocksdb::Status s =
      m_dbInstance->Get(::rocksdb::ReadOptions(), toRocksdbSlice(_key), &_value);

  if (s.IsNotFound()) {
    return Status::NotFound("Not found");
  }

  if (!s.ok()) {
    LOG_DEBUG(logger,
                    "Failed to get key " << _key << " due to " << s.ToString());
    return Status::GeneralError("Failed to read key");
  }

  return Status::OK();
}

/**
 * @brief Services a read request from the RocksDB database.
 *
 * Fires a get request to the RocksDB client and stores the data of the
 * response in a new Sliver object.
 *
 * Note: the reference to the data is not stored, the data itself is.
 *
 * @param _key Sliver object of the key that needs to be looked up.
 * @param _outValue Sliver object in which the data of the Get response is
 *                  stored, if any.
 * @return Status NotFound if key is not present, Status GeneralError if error
 *         in Get, else Status OK.
 */
Status Client::get(Sliver _key, OUT Sliver &_outValue) const {
  std::string value;
  Status ret = get(_key, value);
  if (!ret.isOK()) return ret;

  size_t valueSize = value.size();
  // Must copy the string data
  uint8_t *stringCopy = new uint8_t[valueSize];
  memcpy(stringCopy, value.data(), valueSize);
  _outValue = Sliver(stringCopy, valueSize);

  return Status::OK();
}

// A memory for the output buffer is expected to be allocated by a caller.
Status Client::get(Sliver _key, OUT char *&buf, uint32_t bufSize,
                          OUT uint32_t &_realSize) const {
  std::string value;
  Status ret = get(_key, value);
  if (!ret.isOK()) return ret;

  _realSize = static_cast<uint32_t>(value.length());
  if (bufSize < _realSize) {
    LOG_ERROR(logger,
                    "Object value is bigger than specified buffer bufSize="
                        << bufSize << ", _realSize=" << _realSize);
    return Status::GeneralError("Object value is bigger than specified buffer");
  }
  memcpy(buf, value.data(), _realSize);
  return Status::OK();
}

/**
 * @brief Returns a RocksDBClientIterator object.
 *
 * @return RocksDBClientIterator object.
 */
IDBClient::IDBClientIterator *Client::getIterator() const {
  return new ClientIterator(this);
}

/**
 * @brief Frees the RocksDBClientIterator.
 *
 * @param _iter Pointer to object of class RocksDBClientIterator (the iterator)
 *              that needs to be freed.
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
 * @brief Returns an iterator.
 *
 * Returns a reference to a new object of RocksDbIterator.
 *
 * @return A pointer to RocksDbIterator object.
 */
::rocksdb::Iterator *Client::getNewRocksDbIterator() const {
  return m_dbInstance->NewIterator(::rocksdb::ReadOptions());
}

/**
 * @brief Currently used to check the number of read requests received.
 */
void Client::monitor() const {
  // TODO Can be used for additional sanity checks and debugging.

  if (g_rocksdb_print_measurements) {
    LOG_DEBUG(logger, "No. of times read: " << g_rocksdb_called_read);
  }
}

/**
 * @brief Constructor for the RocksDBClientIterator class.
 *
 * Calls the getNewRocksDbIterator function.
 */
ClientIterator::ClientIterator(const Client *_parentClient)
    : logger(concordlogger::Log::getLogger("com.vmware.concord.kvb")),
      m_parentClient(_parentClient),
      m_status(Status::OK()) {
  m_iter = m_parentClient->getNewRocksDbIterator();
}

/**
 * @brief Services a write request to the RocksDB database.
 *
 * Fires a put request to the RocksDB client.
 *
 * @param _key The key that needs to be stored.
 * @param _value The value that needs to be stored against the key.
 * @return Status GeneralError if error in Put, else Status OK.
 */
Status Client::put(Sliver _key, Sliver _value) {
  ::rocksdb::WriteOptions woptions = ::rocksdb::WriteOptions();

  ::rocksdb::Status s =
      m_dbInstance->Put(woptions, toRocksdbSlice(_key), toRocksdbSlice(_value));

  LOG_DEBUG(logger, "Rocksdb Put " << _key << " : " << _value);

  if (!s.ok()) {
    LOG_ERROR(logger,
                    "Failed to put key " << _key << ", value " << _value);
    return Status::GeneralError("Failed to put key");
  }

  return Status::OK();
}

/**
 * @brief Services a delete request to the RocksDB database.
 *
 *  Fires a Delete request to the RocksDB database.
 *
 *  @param _key The key corresponding to the key value pair which needs to be
 *              deleted.
 *  @return Status GeneralError if error in delete, else Status OK.
 */
Status Client::del(Sliver _key) {
  ::rocksdb::WriteOptions woptions = ::rocksdb::WriteOptions();
  ::rocksdb::Status s = m_dbInstance->Delete(woptions, toRocksdbSlice(_key));

  LOG_DEBUG(logger, "Rocksdb delete " << _key);

  if (!s.ok()) {
    LOG_ERROR(logger, "Failed to delete key " << _key);
    return Status::GeneralError("Failed to delete key");
  }

  return Status::OK();
}

Status Client::multiGet(const KeysVector &_keysVec,
                               OUT ValuesVector &_valuesVec) {
  std::vector<std::string> values;
  std::vector<::rocksdb::Slice> keys;
  for (auto const &it : _keysVec) keys.push_back(toRocksdbSlice(it));

  std::vector<::rocksdb::Status> statuses =
      m_dbInstance->MultiGet(::rocksdb::ReadOptions(), keys, &values);

  for (size_t i = 0; i < values.size(); i++) {
    if (statuses[i].IsNotFound()) return Status::NotFound("Not found");

    if (!statuses[i].ok()) {
      LOG_WARN(logger, "Failed to get key " << _keysVec[i] << " due to "
                                                  << statuses[i].ToString());
      return Status::GeneralError("Failed to read key");
    }
    size_t valueSize = values[i].size();
    auto valueStr = new uint8_t[valueSize];
    memcpy(valueStr, values[i].data(), valueSize);
    _valuesVec.push_back(Sliver(valueStr, valueSize));
  }
  return Status::OK();
}

std::ostringstream Client::collectKeysForPrint(
    const KeysVector &_keysVec) {
  std::ostringstream keys;
  for (auto const &it : _keysVec) keys << it << ", ";
  return keys;
}

Status Client::launchBatchJob(::rocksdb::WriteBatch &_batchJob,
                                     const KeysVector &_keysVec) {
  ::rocksdb::WriteOptions wOptions = ::rocksdb::WriteOptions();
  ::rocksdb::Status status = m_dbInstance->Write(wOptions, &_batchJob);
  if (!status.ok()) {
    LOG_ERROR(logger, "Execution of batch job failed; keys: "
                                << collectKeysForPrint(_keysVec).str());
    return Status::GeneralError("Execution of batch job failed");
  }
  LOG_DEBUG(logger, "Successfully executed a batch job for keys: "
                              << collectKeysForPrint(_keysVec).str());
  return Status::OK();
}

Status Client::multiPut(const SetOfKeyValuePairs &_keyValueMap) {
  ::rocksdb::WriteBatch batch;
  KeysVector keysVec;
  for (const auto &it : _keyValueMap) {
    batch.Put(toRocksdbSlice(it.first), toRocksdbSlice(it.second));
    keysVec.push_back(it.first);
    LOG_DEBUG(logger, "RocksDB Added entry: key ="
                                << it.first << ", value= " << it.second
                                << " to the batch job");
  }
  Status status = launchBatchJob(batch, keysVec);
  if (status.isOK())
    LOG_DEBUG(logger, "Successfully put all entries to the database");
  return status;
}

Status Client::multiDel(const KeysVector &_keysVec) {
  ::rocksdb::WriteBatch batch;
  std::ostringstream keys;
  for (auto const &it : _keysVec) {
    batch.Delete(toRocksdbSlice(it));
  }
  Status status = launchBatchJob(batch, _keysVec);
  if (status.isOK()) LOG_DEBUG(logger, "Successfully deleted entries");
  return status;
}

/**
 * @brief Returns the KeyValuePair object of the first key in the database.
 *
 * @return The KeyValuePair object of the first key.
 */
KeyValuePair ClientIterator::first() {
  ++g_rocksdb_called_read;
  if (g_rocksdb_print_measurements) {
    LOG_DEBUG(logger, "Reading count = " << g_rocksdb_called_read);
  }

  // Position at the first key in the database
  m_iter->SeekToFirst();

  if (!m_iter->Valid()) {
    LOG_ERROR(logger, "Did not find a first key");
    m_status = Status::NotFound("Empty database");
    return KeyValuePair();
  }

  Sliver key = copyRocksdbSlice(m_iter->key());
  Sliver value = copyRocksdbSlice(m_iter->value());

  m_status = Status::OK();
  return KeyValuePair(key, value);
}

/**
 * @brief Returns the key value pair of the key which is greater than or equal
 * to _searchKey.
 *
 * Returns the first key value pair whose key is not considered to go before
 * _searchKey. Also, moves the iterator to this position.
 *
 * @param _searchKey Key to search for.
 * @return Key value pair of the key which is greater than or equal to
 *         _searchKey.
 */
KeyValuePair ClientIterator::seekAtLeast(Sliver _searchKey) {
  ++g_rocksdb_called_read;
  if (g_rocksdb_print_measurements) {
    LOG_DEBUG(logger, "Reading count = " << g_rocksdb_called_read
                                               << ", key " << _searchKey);
  }

  m_iter->Seek(toRocksdbSlice(_searchKey));
  if (!m_iter->Valid()) {
    LOG_WARN(logger, "Did not find search key " << _searchKey);
    // TODO(SG): Status to exception?
    return KeyValuePair();
  }

  // We have to copy the data out of the iterator, so that we own it. The
  // pointers from the iterator will become invalid if the iterator is moved.
  Sliver key = copyRocksdbSlice(m_iter->key());
  Sliver value = copyRocksdbSlice(m_iter->value());

  LOG_DEBUG(logger, "Key " << key << " value " << value);
  m_status = Status::OK();
  return KeyValuePair(key, value);
}

/**
 * @brief Decrements the iterator.
 *
 * Decrements the iterator and returns the previous key value pair.
 *
 * @return The previous key value pair.
 */
KeyValuePair ClientIterator::previous() {
  m_iter->Prev();

  if (!m_iter->Valid()) {
    LOG_ERROR(logger, "Iterator out of bounds");
    return KeyValuePair();
  }

  Sliver key = copyRocksdbSlice(m_iter->key());
  Sliver value = copyRocksdbSlice(m_iter->value());

  LOG_DEBUG(logger, "Key " << key << " value " << value);
  m_status = Status::OK();

  return KeyValuePair(key, value);
}

/**
 * @brief Increments the iterator.
 *
 * Increments the iterator and returns the next key value pair.
 *
 * @return The next key value pair.
 */
KeyValuePair ClientIterator::next() {
  ++g_rocksdb_called_read;
  if (g_rocksdb_print_measurements) {
    LOG_DEBUG(logger, "Reading count = " << g_rocksdb_called_read);
  }

  m_iter->Next();
  if (!m_iter->Valid()) {
    LOG_ERROR(logger, "No next key");
    m_status = Status::GeneralError("No next key");
    return KeyValuePair();
  }

  Sliver key = copyRocksdbSlice(m_iter->key());
  Sliver value = copyRocksdbSlice(m_iter->value());

  LOG_DEBUG(logger, "Key " << key << " value " << value);
  m_status = Status::OK();
  return KeyValuePair(key, value);
}

/**
 * @brief Returns the key value pair at the current position of the iterator.
 *
 * @return Current key value pair.
 */
KeyValuePair ClientIterator::getCurrent() {
  if (!m_iter->Valid()) {
    LOG_ERROR(logger, "Iterator is not pointing at an element");
    m_status = Status::GeneralError("Iterator is not pointing at an element");
    return KeyValuePair();
  }

  Sliver key = copyRocksdbSlice(m_iter->key());
  Sliver value = copyRocksdbSlice(m_iter->value());

  m_status = Status::OK();
  return KeyValuePair(key, value);
}

/**
 * @brief Tells whether iterator has crossed the bounds of the last key value
 * pair.
 *
 * @return True if iterator is beyond the bounds, else False.
 */
bool ClientIterator::isEnd() { return !m_iter->Valid(); }

/**
 * @brief Returns the Status.
 *
 * @return The latest Status logged.
 */
Status ClientIterator::getStatus() { return m_status; }

}
}
}
#endif
