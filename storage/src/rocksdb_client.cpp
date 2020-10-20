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

#include <rocksdb/client.h>
#include <rocksdb/transaction.h>
#include <rocksdb/env.h>
#include <rocksdb/utilities/options_util.h>
#include <rocksdb/table.h>
#include "assertUtils.hpp"
#include "Logger.hpp"
#include <atomic>

using concordUtils::Sliver;
using concordUtils::Status;

namespace concord {
namespace storage {
namespace rocksdb {

// Counter for number of read requests
static unsigned int g_rocksdb_called_read = 0;
static bool g_rocksdb_print_measurements = false;
const unsigned int background_threads = 16;
/**
 * @brief Converts a Sliver object to a RocksDB Slice object.
 *
 * @param _s Sliver object.
 * @return A RocksDB Slice object.
 */
::rocksdb::Slice toRocksdbSlice(const Sliver &_s) {
  return ::rocksdb::Slice(reinterpret_cast<const char *>(_s.data()), _s.length());
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
Sliver fromRocksdbSlice(::rocksdb::Slice _s) { return Sliver(_s.data(), _s.size()); }

/**
 * @brief Copies a RocksDB slice in a Sliver.
 *
 * @param _s A RocksDB Slice object.
 * @return A Sliver object.
 */
Sliver copyRocksdbSlice(::rocksdb::Slice _s) {
  char *copyData = new char[_s.size()];
  std::copy(_s.data(), _s.data() + _s.size(), copyData);
  return Sliver(copyData, _s.size());
}

ITransaction *Client::beginTransaction() {
  static std::atomic_uint64_t current_transaction_id(0);
  ::rocksdb::WriteOptions wo;
  if (!txn_db_) throw std::runtime_error("Failed to start transaction, reason: RO mode");
  return new Transaction(txn_db_->BeginTransaction(wo), ++current_transaction_id);
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
 *  @throw GeneralError in case of error in connection, else OK.
 */
void Client::init(bool readOnly) {
  ::rocksdb::DB *db;
  ::rocksdb::Options options;
  ::rocksdb::TransactionDBOptions txn_options;
  std::vector<::rocksdb::ColumnFamilyDescriptor> cf_descs;
  ::rocksdb::BlockBasedTableOptions table_options;

  // Setting default rocksdb options
  options.enable_pipelined_write = true;
  options.IncreaseParallelism(background_threads);
  options.write_buffer_size = 1024 * 1024 * 512;
  options.max_write_buffer_number = 16;
  options.min_write_buffer_number_to_merge = 4;
  options.max_bytes_for_level_base = (uint64_t)1024 * 1024 * 2048;
  options.target_file_size_base = 1024 * 1024 * 256;
  options.max_background_flushes = 2;
  options.max_background_compactions = 48;
  options.max_subcompactions = 48;
  options.level0_file_num_compaction_trigger = 1;
  options.level0_slowdown_writes_trigger = 48;
  options.level0_stop_writes_trigger = 56;
  options.bytes_per_sync = 1024 * 2048;
  options.max_open_files = 50;

  table_options.block_size = 4 * 4096;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  // Try to read the stored options configuration file
  // Note that if we recover, then rocksdb should have its option configuration file stored in the rocksdb directory.
  // Thus, we don't need to persist our custom configuration file.
  auto s_opt = ::rocksdb::LoadLatestOptions(m_dbPath, ::rocksdb::Env::Default(), &options, &cf_descs);
  if (!s_opt.ok()) {
    const char kPathSeparator =
#ifdef _WIN32
        '\\';
#else
        '/';
#endif
    // If we couldn't read the stored configuration file, try to read the default configuration file.
    s_opt = ::rocksdb::LoadOptionsFromFile(
        m_dbPath + kPathSeparator + default_opt_config_name, ::rocksdb::Env::Default(), &options, &cf_descs);
  }
  if (!s_opt.ok()) {
    // If we couldn't read the stored configuration and not the default configuration file, then create
    // one.
    options.create_if_missing = true;
  }
  options.sst_file_manager.reset(::rocksdb::NewSstFileManager(::rocksdb::Env::Default()));
  options.statistics = ::rocksdb::CreateDBStatistics();
  options.statistics->set_stats_level(::rocksdb::StatsLevel::kExceptHistogramOrTimers);

  options.write_buffer_size = 512 << 20;  // set default memtable size to 512mb to improve perf

  // If a comparator is passed, use it. If not, use the default one.
  if (comparator_) {
    options.comparator = comparator_.get();
  }
  ::rocksdb::Status s;
  if (readOnly) {
    s = ::rocksdb::DB::OpenForReadOnly(options, m_dbPath, &db);
    if (!s.ok())
      throw std::runtime_error("Failed to open rocksdb database at " + m_dbPath + std::string(" reason: ") +
                               s.ToString());
    dbInstance_.reset(db);
  } else {
    s = ::rocksdb::TransactionDB::Open(options, txn_options, m_dbPath, &txn_db_);
    if (!s.ok())
      throw std::runtime_error("Failed to open rocksdb database at " + m_dbPath + std::string(" reason: ") +
                               s.ToString());
    dbInstance_.reset(txn_db_->GetBaseDB());
  }
  storage_metrics_.setMetricsDataSources(options.sst_file_manager, options.statistics);
}

Status Client::get(const Sliver &_key, OUT std::string &_value) const {
  ++g_rocksdb_called_read;
  if (g_rocksdb_print_measurements) {
    LOG_DEBUG(logger(), "Reading count = " << g_rocksdb_called_read << ", key " << _key);
  }
  ::rocksdb::Status s = dbInstance_->Get(::rocksdb::ReadOptions(), toRocksdbSlice(_key), &_value);

  if (s.IsNotFound()) {
    return Status::NotFound("Not found");
  }

  if (!s.ok()) {
    LOG_DEBUG(logger(), "Failed to get key " << _key << " due to " << s.ToString());
    return Status::GeneralError("Failed to read key");
  }
  storage_metrics_.tryToUpdateMetrics();
  return Status::OK();
}

bool Client::keyIsBefore(const Sliver &_lhs, const Sliver &_rhs) const {
  if (comparator_) {
    return comparator_->Compare(toRocksdbSlice(_lhs), toRocksdbSlice(_rhs)) < 0;
  }
  return _lhs.compare(_rhs) < 0;
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
Status Client::get(const Sliver &_key, OUT Sliver &_outValue) const {
  std::string value;
  Status ret = get(_key, value);
  if (!ret.isOK()) return ret;
  _outValue = Sliver(std::move(value));
  return Status::OK();
}

// A memory for the output buffer is expected to be allocated by a caller.
Status Client::get(const Sliver &_key, OUT char *&buf, uint32_t bufSize, OUT uint32_t &_realSize) const {
  std::string value;
  Status ret = get(_key, value);
  if (!ret.isOK()) return ret;

  _realSize = static_cast<uint32_t>(value.length());
  if (bufSize < _realSize) {
    LOG_ERROR(logger(),
              "Object value is bigger than specified buffer bufSize=" << bufSize << ", _realSize=" << _realSize);
    return Status::GeneralError("Object value is bigger than specified buffer");
  }
  memcpy(buf, value.data(), _realSize);
  return Status::OK();
}

Status Client::has(const Sliver &_key) const {
  Sliver dummy_out;
  return get(_key, dummy_out);
}

/**
 * @brief Returns a RocksDBClientIterator object.
 *
 * @return RocksDBClientIterator object.
 */
IDBClient::IDBClientIterator *Client::getIterator() const { return new ClientIterator(this, logger()); }

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
  return dbInstance_->NewIterator(::rocksdb::ReadOptions());
}

/**
 * @brief Constructor for the RocksDBClientIterator class.
 *
 * Calls the getNewRocksDbIterator function.
 */
ClientIterator::ClientIterator(const Client *_parentClient, logging::Logger logger)
    : logger(logger), m_parentClient(_parentClient), m_status(Status::OK()) {
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
Status Client::put(const Sliver &_key, const Sliver &_value) {
  ::rocksdb::WriteOptions woptions = ::rocksdb::WriteOptions();
  woptions.memtable_insert_hint_per_batch = true;
  ::rocksdb::Status s = dbInstance_->Put(woptions, toRocksdbSlice(_key), toRocksdbSlice(_value));

  LOG_TRACE(logger(), "Rocksdb Put " << _key << " : " << _value);

  if (!s.ok()) {
    LOG_ERROR(logger(), "Failed to put key " << _key << ", value " << _value);
    return Status::GeneralError("Failed to put key");
  }
  storage_metrics_.tryToUpdateMetrics();
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
Status Client::del(const Sliver &_key) {
  ::rocksdb::WriteOptions woptions = ::rocksdb::WriteOptions();
  ::rocksdb::Status s = dbInstance_->Delete(woptions, toRocksdbSlice(_key));

  LOG_TRACE(logger(), "Rocksdb delete " << _key);

  if (!s.ok()) {
    LOG_ERROR(logger(), "Failed to delete key " << _key);
    return Status::GeneralError("Failed to delete key");
  }
  storage_metrics_.tryToUpdateMetrics();
  return Status::OK();
}

Status Client::multiGet(const KeysVector &_keysVec, OUT ValuesVector &_valuesVec) {
  std::vector<std::string> values;
  std::vector<::rocksdb::Slice> keys;
  for (auto const &it : _keysVec) keys.push_back(toRocksdbSlice(it));

  std::vector<::rocksdb::Status> statuses = dbInstance_->MultiGet(::rocksdb::ReadOptions(), keys, &values);
  for (size_t i = 0; i < values.size(); i++) {
    if (statuses[i].IsNotFound()) return Status::NotFound("Not found");

    if (!statuses[i].ok()) {
      LOG_WARN(logger(), "Failed to get key " << _keysVec[i] << " due to " << statuses[i].ToString());
      return Status::GeneralError("Failed to read key");
    }
    _valuesVec.push_back(Sliver(std::move(values[i])));
  }
  storage_metrics_.tryToUpdateMetrics();
  return Status::OK();
}

Status Client::launchBatchJob(::rocksdb::WriteBatch &batch) {
  LOG_DEBUG(logger(), "launcBatchJob: batch data size=" << batch.GetDataSize() << " num updates=" << batch.Count());
  ::rocksdb::WriteOptions wOptions = ::rocksdb::WriteOptions();
  ::rocksdb::Status status = dbInstance_->Write(wOptions, &batch);
  if (!status.ok()) {
    LOG_ERROR(
        logger(),
        "Execution of batch job failed; batch data size=" << batch.GetDataSize() << " num updates=" << batch.Count());
    return Status::GeneralError("Execution of batch job failed");
  }
  LOG_DEBUG(
      logger(),
      "Successfully executed a batch job: batch data size=" << batch.GetDataSize() << " num updates=" << batch.Count());
  return Status::OK();
}

Status Client::multiPut(const SetOfKeyValuePairs &keyValueMap) {
  ::rocksdb::WriteBatch batch;
  LOG_DEBUG(logger(), "multiPut: keyValueMap.size() = " << keyValueMap.size());
  for (const auto &it : keyValueMap) {
    batch.Put(toRocksdbSlice(it.first), toRocksdbSlice(it.second));
    LOG_TRACE(logger(), "RocksDB Added entry: key =" << it.first << ", value= " << it.second << " to the batch job");
  }
  Status status = launchBatchJob(batch);
  if (status.isOK()) LOG_DEBUG(logger(), "Successfully put all entries to the database");
  storage_metrics_.tryToUpdateMetrics();
  return status;
}

Status Client::multiDel(const KeysVector &_keysVec) {
  ::rocksdb::WriteBatch batch;
  std::ostringstream keys;
  for (auto const &it : _keysVec) {
    batch.Delete(toRocksdbSlice(it));
  }
  Status status = launchBatchJob(batch);
  if (status.isOK()) LOG_DEBUG(logger(), "Successfully deleted entries");
  storage_metrics_.tryToUpdateMetrics();
  return status;
}

Status Client::rangeDel(const Sliver &_beginKey, const Sliver &_endKey) {
  if (_beginKey == _endKey) {
    return Status::OK();
  }

  // Make sure that _beginKey comes before _endKey .
  ConcordAssert(keyIsBefore(_beginKey, _endKey));

  const auto status =
      dbInstance_->DeleteRange(::rocksdb::WriteOptions(), nullptr, toRocksdbSlice(_beginKey), toRocksdbSlice(_endKey));
  if (!status.ok()) {
    LOG_ERROR(logger(), "RocksDB failed to delete range, begin=" << _beginKey << ", end=" << _endKey);
    return Status::GeneralError("Failed to delete range");
  }
  LOG_TRACE(logger(), "RocksDB successful range delete, begin=" << _beginKey << ", end=" << _endKey);
  storage_metrics_.tryToUpdateMetrics();
  return Status::OK();
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
 * @brief Returns the KeyValuePair object of the last key in the database.
 *
 * @return The KeyValuePair object of the last key.
 */
KeyValuePair ClientIterator::last() {
  ++g_rocksdb_called_read;
  if (g_rocksdb_print_measurements) {
    LOG_DEBUG(logger, "Reading count = " << g_rocksdb_called_read);
  }

  // Position at the last key in the database
  m_iter->SeekToLast();

  if (!m_iter->Valid()) {
    LOG_ERROR(logger, "Did not find a last key");
    m_status = Status::NotFound("Empty database");
    return KeyValuePair();
  }

  m_status = Status::OK();
  return KeyValuePair(copyRocksdbSlice(m_iter->key()), copyRocksdbSlice(m_iter->value()));
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
KeyValuePair ClientIterator::seekAtLeast(const Sliver &_searchKey) {
  ++g_rocksdb_called_read;
  if (g_rocksdb_print_measurements) {
    LOG_DEBUG(logger, "Reading count = " << g_rocksdb_called_read << ", key " << _searchKey);
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

  m_status = Status::OK();
  return KeyValuePair(key, value);
}

/**
 * @brief Returns the key value pair of the last key which is less than or equal
 * to _searchKey.
 *
 * @param _searchKey Key to search for.
 * @return Key value pair of the last key which is less than or equal to
 *         _searchKey.
 */
KeyValuePair ClientIterator::seekAtMost(const Sliver &_searchKey) {
  ++g_rocksdb_called_read;
  if (g_rocksdb_print_measurements) {
    LOG_DEBUG(logger, "Reading count = " << g_rocksdb_called_read << ", key " << _searchKey);
  }

  m_iter->SeekForPrev(toRocksdbSlice(_searchKey));
  if (!m_iter->Valid()) {
    return KeyValuePair();
  }

  m_status = Status::OK();
  return KeyValuePair(copyRocksdbSlice(m_iter->key()), copyRocksdbSlice(m_iter->value()));
}

/**
 * @brief Decrements the iterator.
 *
 * Decrements the iterator and returns the previous key value pair.
 *
 * @return The previous key value pair.
 */
KeyValuePair ClientIterator::previous() {
  if (!m_iter->Valid()) {
    LOG_ERROR(logger, "Iterator is not valid");
    m_status = Status::GeneralError("Iterator is not valid");
    return KeyValuePair();
  }
  m_iter->Prev();
  if (!m_iter->Valid()) {
    LOG_WARN(logger, "No previous key");
    m_status = Status::GeneralError("No previous key");
    return KeyValuePair();
  }

  Sliver key = copyRocksdbSlice(m_iter->key());
  Sliver value = copyRocksdbSlice(m_iter->value());

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

  if (!m_iter->Valid()) {
    LOG_ERROR(logger, "Iterator is not valid");
    m_status = Status::GeneralError("Iterator is not valid");
    return KeyValuePair();
  }
  m_iter->Next();
  if (!m_iter->Valid()) {
    LOG_WARN(logger, "No next key");
    m_status = Status::GeneralError("No next key");
    return KeyValuePair();
  }

  Sliver key = copyRocksdbSlice(m_iter->key());
  Sliver value = copyRocksdbSlice(m_iter->value());

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

}  // namespace rocksdb
}  // namespace storage
}  // namespace concord
#endif
