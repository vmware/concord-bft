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

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <stdexcept>
#include "util/filesystem.hpp"

#ifdef USE_ROCKSDB

#include <rocksdb/client.h>
#include <rocksdb/transaction.h>
#include <rocksdb/env.h>
#include <rocksdb/utilities/options_util.h>
#include <rocksdb/table.h>
#include <rocksdb/filter_policy.h>

#include <algorithm>
#include <atomic>
#include <utility>

#include "assertUtils.hpp"
#include "Logger.hpp"

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

void Client::Options::applyOptimizations() {
  // Setting optimized rocksdb options
  db_options.enable_pipelined_write = true;
  db_options.IncreaseParallelism(background_threads);
  db_options.write_buffer_size = 1024 * 1024 * 512;
  db_options.max_write_buffer_number = 16;
  db_options.min_write_buffer_number_to_merge = 4;
  db_options.max_bytes_for_level_base = (uint64_t)1024 * 1024 * 2048;
  db_options.target_file_size_base = 1024 * 1024 * 256;
  db_options.max_background_flushes = 2;
  db_options.max_background_compactions = 48;
  db_options.max_subcompactions = 48;
  db_options.level0_file_num_compaction_trigger = 1;
  db_options.level0_slowdown_writes_trigger = 48;
  db_options.level0_stop_writes_trigger = 56;
  db_options.bytes_per_sync = 1024 * 2048;

  ::rocksdb::BlockBasedTableOptions table_options;

  table_options.block_size = 4 * 4096;
  table_options.block_cache = ::rocksdb::NewLRUCache(1024 * 1024 * 1024 * 2ul);  // 2GB
  table_options.filter_policy.reset(::rocksdb::NewBloomFilterPolicy(10, false));

  db_options.table_factory.reset(NewBlockBasedTableFactory(table_options));
}

// Create a RocksDB client by loading all options from a user defined file.
//
// Throws `std::invalid_argument` if the file does not exist or it cannot be opened.
void Client::initDBFromFile(bool readOnly, const Options &user_options) {
  auto db_options = ::rocksdb::Options{};
  auto cf_descs = std::vector<::rocksdb::ColumnFamilyDescriptor>{};
  auto status =
      ::rocksdb::LoadOptionsFromFile(user_options.filepath, ::rocksdb::Env::Default(), &db_options, &cf_descs);
  if (!status.ok()) {
    const auto msg = "Failed to find RocksDB config file [" + user_options.filepath + "], reason: " + status.ToString();
    LOG_ERROR(logger(), msg);
    throw std::invalid_argument{msg};
  }

  auto default_db_options = ::rocksdb::Options{};
  auto default_cf_descs = std::vector<::rocksdb::ColumnFamilyDescriptor>{};
  status = ::rocksdb::LoadLatestOptions(m_dbPath, ::rocksdb::Env::Default(), &default_db_options, &default_cf_descs);
  if (status.ok()) {
    // We wouldn't want to force the user to define a complete *.ini file. In case the DB is not new, and there is a
    // mismatch between the default options and the user one, add the default to the user.
    // Note that if you add new CF and not define in the user .ini file, it will have the default rocksdb options.
    for (auto &ccf : default_cf_descs) {
      auto it = std::find_if(
          cf_descs.begin(), cf_descs.end(), [&ccf](const auto &cf_desc) { return ccf.name == cf_desc.name; });
      if (it == cf_descs.end()) {
        cf_descs.push_back(ccf);
      }
    }
  } else {
    LOG_WARN(logger(), "unable to open default rocksdb option file, reason: " << status.ToString());
  }

  // Add specific global options
  db_options.IncreaseParallelism(static_cast<int>(std::thread::hardware_concurrency()));
  db_options.sst_file_manager.reset(::rocksdb::NewSstFileManager(::rocksdb::Env::Default()));
  db_options.statistics = ::rocksdb::CreateDBStatistics();
  db_options.statistics->set_stats_level(::rocksdb::StatsLevel::kExceptTimeForMutex);

  // Some options, notably pointers, are not configurable via the config file. We set them in code here.
  user_options.completeInit(db_options, cf_descs);
  db_options.wal_dir = m_dbPath;
  db_options.create_missing_column_families = true;
  openRocksDB(readOnly, db_options, cf_descs);

  initialized_ = true;
  storage_metrics_.setMetricsDataSources(db_options.sst_file_manager, db_options.statistics);
}

// Create column family handles and call the appropriate RocksDB open functions.
void Client::openRocksDB(bool readOnly,
                         const ::rocksdb::Options &db_options,
                         std::vector<::rocksdb::ColumnFamilyDescriptor> &cf_descs) {
  ::rocksdb::Status s;
  auto raw_cf_handles = std::vector<::rocksdb::ColumnFamilyHandle *>{};
  auto unique_cf_handles = std::map<std::string, CfUniquePtr>{};
  // Ensure that we always delete column family handles, including error returns from the open calls.
  const auto raw_to_unique_cf_handles = [this](const auto &raw_cf_handles) {
    auto ret = std::map<std::string, CfUniquePtr>{};
    for (auto cf_handle : raw_cf_handles) {
      ret[cf_handle->GetName()] = CfUniquePtr{cf_handle, CfDeleter{this}};
    }
    return ret;
  };
  if (cf_descs.empty()) {
    // Make sure we always get a handle for the default column family. Use the DB options to configure it.
    cf_descs.push_back(::rocksdb::ColumnFamilyDescriptor{::rocksdb::kDefaultColumnFamilyName, db_options});
  } else if (comparator_) {
    // Make sure we always set the user-supplied comparator for the default family.
    for (auto &cf_desc : cf_descs) {
      if (cf_desc.name == ::rocksdb::kDefaultColumnFamilyName) {
        cf_desc.options.comparator = comparator_.get();
      }
    }
  }

  if (readOnly) {
    ::rocksdb::DB *db;
    s = ::rocksdb::DB::OpenForReadOnly(db_options, m_dbPath, cf_descs, &raw_cf_handles, &db);
    unique_cf_handles = raw_to_unique_cf_handles(raw_cf_handles);
    if (!s.ok())
      throw std::runtime_error("Failed to open rocksdb database at " + m_dbPath + std::string(" reason: ") +
                               s.ToString());
    dbInstance_.reset(db);
  } else {
    ::rocksdb::OptimisticTransactionDBOptions txn_options;
    s = ::rocksdb::OptimisticTransactionDB::Open(
        db_options, txn_options, m_dbPath, cf_descs, &raw_cf_handles, &txn_db_);
    unique_cf_handles = raw_to_unique_cf_handles(raw_cf_handles);
    if (!s.ok())
      throw std::runtime_error("Failed to open rocksdb database at " + m_dbPath + std::string(" reason: ") +
                               s.ToString());
    dbInstance_.reset(txn_db_->GetBaseDB());
    // create checkpoint instance
    ::rocksdb::Checkpoint *checkpoint;
    s = ::rocksdb::Checkpoint::Create(dbInstance_.get(), &checkpoint);
    if (!s.ok()) throw std::runtime_error("Failed to create checkpoint instance for incremental db snapshot");
    dbCheckPoint_.reset(checkpoint);
  }
  cf_handles_ = std::move(unique_cf_handles);
}

/**
 * @brief Opens a RocksDB database connection.
 *
 * Uses the RocksDBClient object variables m_dbPath and m_dbInstance to
 * establish a connection with RocksDB by creating a RocksDB object.
 *
 *  @throw GeneralError in case of error in connection, else OK.
 */
void Client::initDB(bool readOnly, const std::optional<Options> &userOptions, bool applyOptimizations) {
  if (initialized_) {
    return;
  }

  if (userOptions) {
    return initDBFromFile(readOnly, *userOptions);
  }

  Options options;
  std::vector<::rocksdb::ColumnFamilyDescriptor> cf_descs;

  auto cf_names = std::vector<std::string>{};
  if (const auto s = ::rocksdb::DB::ListColumnFamilies(options.db_options, m_dbPath, &cf_names); !s.ok()) {
    // There's no way to reliably check for non-existing DB. If there's any kind of an error, just clear the list of cf
    // names. If the DB was actually non-existent, the Open() call will fail anyway.
    cf_names.clear();
  }

  // RocksDB creates column families in 2 non-atomic steps:
  //  1. Create the column family in the DB.
  //  2. Create the column family in the options file by persisting the respective options provided by the user.
  // If the process is stopped between these steps, there could be a mismatch between them. That could lead to losing
  // the column family options the user provided. In that case, track incompletely created column families and if they
  // are empty, drop them so that user code can re-create them.
  //
  // Reference: https://github.com/facebook/rocksdb/blob/v6.8.1/db/db_impl/db_impl.cc#L2187
  auto incompletelyCreatedColumnFamilies = std::vector<std::string>{};

  auto s_opt = ::rocksdb::LoadLatestOptions(m_dbPath, ::rocksdb::Env::Default(), &options.db_options, &cf_descs);
  if (!s_opt.ok()) {
    // If we couldn't read the stored configuration and not the default configuration file, then create
    // one.
    options.db_options.create_if_missing = true;
  }
  options.db_options.sst_file_manager.reset(::rocksdb::NewSstFileManager(::rocksdb::Env::Default()));
  options.db_options.statistics = ::rocksdb::CreateDBStatistics();
  options.db_options.statistics->set_stats_level(::rocksdb::StatsLevel::kExceptTimeForMutex);

  // Fill any missing column family descriptors. That may happen as there can be a mismatch between the column
  // families in the DB and the ones in the options file due to the non-atomic way of creating a column family in
  // terms of DB and options file.
  for (const auto &cf_name : cf_names) {
    auto it = std::find_if(
        cf_descs.begin(), cf_descs.end(), [&cf_name](const auto &cf_desc) { return cf_name == cf_desc.name; });
    if (it == cf_descs.end()) {
      // Open with default options and mark as incompletely created.
      cf_descs.push_back(::rocksdb::ColumnFamilyDescriptor{cf_name, ::rocksdb::ColumnFamilyOptions{}});
      incompletelyCreatedColumnFamilies.push_back(cf_name);
    }
  }

  // If a comparator is passed, use it. If not, use the default one.
  if (comparator_) {
    options.db_options.comparator = comparator_.get();
  }

  if (applyOptimizations) {
    options.applyOptimizations();
  }
  options.db_options.wal_dir = m_dbPath;
  openRocksDB(readOnly, options.db_options, cf_descs);

  // If an incomplete column family is empty (i.e. has no keys), drop it so that user code can re-create it with the
  // correct options. Otherwise, warn and continue.
  for (const auto &cf : incompletelyCreatedColumnFamilies) {
    if (cf == ::rocksdb::kDefaultColumnFamilyName) {
      continue;
    }
    auto cf_iter = cf_handles_.find(cf);
    ConcordAssertNE(cf_iter, cf_handles_.end());
    if (columnFamilyIsEmpty(cf_iter->second.get())) {
      const auto s = dbInstance_->DropColumnFamily(cf_iter->second.get());
      if (!s.ok()) {
        const auto msg =
            "Failed to drop incompletely created RocksDB column family [" + cf + "], reason: " + s.ToString();
        LOG_ERROR(logger(), msg);
        throw std::runtime_error{msg};
      }
      cf_handles_.erase(cf_iter);
      LOG_WARN(logger(), "Dropped incompletely created and empty RocksDB column family [" << cf << ']');
    } else {
      const auto msg = "RocksDB column family [" + cf +
                       "] has no persisted options, yet there is data inside - cannot continue with unknown options";
      LOG_ERROR(logger(), msg);
      throw std::runtime_error{msg};
    }
  }

  initialized_ = true;
  storage_metrics_.setMetricsDataSources(options.db_options.sst_file_manager, options.db_options.statistics);
}

void Client::init(bool readOnly) {
  const auto applyOptimizations = true;
  initDB(readOnly, std::nullopt, applyOptimizations);
}

Status Client::get(const Sliver &_key, std::string &_value) const {
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
  return Status::OK();
}

bool Client::keyIsBefore(const Sliver &_lhs, const Sliver &_rhs) const {
  if (comparator_) {
    return comparator_->Compare(toRocksdbSlice(_lhs), toRocksdbSlice(_rhs)) < 0;
  }
  return _lhs.compare(_rhs) < 0;
}

bool Client::columnFamilyIsEmpty(::rocksdb::ColumnFamilyHandle *cf) const {
  auto it = std::unique_ptr<::rocksdb::Iterator>{dbInstance_->NewIterator(::rocksdb::ReadOptions{}, cf)};
  it->SeekToFirst();
  const auto s = it->status();
  if (!s.ok()) {
    throw std::runtime_error{"Failed to seek to first during columnFamilyIsEmpty(): " + s.ToString()};
  }
  return !it->Valid();
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
Status Client::get(const Sliver &_key, Sliver &_outValue) const {
  std::string value;
  Status ret = get(_key, value);
  if (!ret.isOK()) return ret;
  _outValue = Sliver(std::move(value));
  return Status::OK();
}

// A memory for the output buffer is expected to be allocated by a caller.
Status Client::get(const Sliver &_key, char *&buf, uint32_t bufSize, uint32_t &_realSize) const {
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
    LOG_ERROR(logger(), "Failed to put key " << _key << ", value " << _value << ", Error: " << s.ToString());
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
Status Client::del(const Sliver &_key) {
  ::rocksdb::WriteOptions woptions = ::rocksdb::WriteOptions();
  ::rocksdb::Status s = dbInstance_->Delete(woptions, toRocksdbSlice(_key));

  LOG_TRACE(logger(), "Rocksdb delete " << _key);

  if (!s.ok()) {
    LOG_ERROR(logger(), "Failed to delete key " << _key << ", Error: " << s.ToString());
    return Status::GeneralError("Failed to delete key");
  }
  return Status::OK();
}

Status Client::multiGet(const KeysVector &_keysVec, ValuesVector &_valuesVec) {
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
  return Status::OK();
}

Status Client::launchBatchJob(::rocksdb::WriteBatch &batch, bool sync) {
  LOG_DEBUG(logger(), "launcBatchJob: batch data size=" << batch.GetDataSize() << " num updates=" << batch.Count());
  ::rocksdb::WriteOptions wOptions = ::rocksdb::WriteOptions();
  wOptions.sync = sync;
  ::rocksdb::Status status = dbInstance_->Write(wOptions, &batch);
  if (!status.ok()) {
    LOG_ERROR(logger(),
              "Execution of batch job failed; batch data size=" << batch.GetDataSize() << " num updates="
                                                                << batch.Count() << " Error:" << status.ToString());
    return Status::GeneralError("Execution of batch job failed");
  }
  LOG_DEBUG(
      logger(),
      "Successfully executed a batch job: batch data size=" << batch.GetDataSize() << " num updates=" << batch.Count());
  return Status::OK();
}

Status Client::multiPut(const SetOfKeyValuePairs &keyValueMap, bool sync) {
  ::rocksdb::WriteBatch batch;
  LOG_DEBUG(logger(), "multiPut: keyValueMap.size() = " << keyValueMap.size());
  for (const auto &it : keyValueMap) {
    batch.Put(toRocksdbSlice(it.first), toRocksdbSlice(it.second));
    LOG_TRACE(logger(), "RocksDB Added entry: key =" << it.first << ", value= " << it.second << " to the batch job");
  }
  Status status = launchBatchJob(batch, sync);
  if (status.isOK()) LOG_DEBUG(logger(), "Successfully put all entries to the database");
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
  return Status::OK();
}

std::string Client::getPathForCheckpoint(std::uint64_t checkpointId) const {
  return (fs::path{dbCheckpointPath_} / std::to_string(checkpointId)).string();
}

Status Client::createCheckpoint(const uint64_t &checkPointId) {
  if (!dbCheckPoint_.get()) return Status::GeneralError("Checkpoint instance is not initialized");
  // create dir(remove if already exist)
  ConcordAssertNE(dbCheckpointPath_, m_dbPath);

  try {
    if (dbCheckpointPath_.empty()) {
      LOG_ERROR(logger(), "RocksDb checkpoint dir path not set");
      return Status::GeneralError("checkpoint creation failed");
    }
    fs::path path{dbCheckpointPath_};
    if (!fs::exists(path)) {
      fs::create_directory(path);
    }
    fs::path chkptDirPath = getPathForCheckpoint(checkPointId);
    // rocksDb create the dir for the checkpoint
    if (fs::exists(chkptDirPath)) fs::remove_all(chkptDirPath);

    ::rocksdb::Status s = dbCheckPoint_.get()->CreateCheckpoint(chkptDirPath.string());
    if (s.ok()) {
      LOG_INFO(logger(), "created rocks db checkpoint: " << KVLOG(checkPointId));
      return Status::OK();
    }
    LOG_ERROR(logger(),
              "RocksDB checkpoint creation failed for " << KVLOG(checkPointId, chkptDirPath.string(), s.ToString()));
    if (fs::exists(chkptDirPath)) fs::remove_all(chkptDirPath);
    return Status::GeneralError("checkpoint creation failed");
  } catch (std::exception &e) {
    LOG_FATAL(logger(), "Failed to create rocksdb checkpoint: " << e.what());
    return Status::GeneralError("checkpoint creation failed");
  }
}
std::vector<uint64_t> Client::getListOfCreatedCheckpoints() const {
  std::vector<uint64_t> createdCheckPoints_;
  try {
    fs::path path{dbCheckpointPath_};
    if (fs::exists(path)) {
      for (const auto &entry : fs::directory_iterator(path)) {
        auto dirName = entry.path().filename();
        createdCheckPoints_.emplace_back(std::stoull(dirName));
      }
    }
  } catch (std::exception &e) {
    LOG_ERROR(logger(), "Failed to get list of checkpoints dir: " << e.what());
  }
  return createdCheckPoints_;
}
void Client::removeCheckpoint(const uint64_t &checkPointId) const {
  ConcordAssertNE(dbCheckpointPath_, m_dbPath);
  try {
    fs::path path{dbCheckpointPath_};
    fs::path chkptDirPath = path / std::to_string(checkPointId);
    if (fs::exists(chkptDirPath)) fs::remove_all(chkptDirPath);
  } catch (std::exception &e) {
    LOG_ERROR(logger(), "Failed to remove checkpoint id: " << checkPointId << " - " << e.what());
  }
}
void Client::removeAllCheckpoints() const {
  ConcordAssertNE(dbCheckpointPath_, m_dbPath);
  try {
    fs::path path{dbCheckpointPath_};
    if (fs::exists(path)) {
      for (const auto &entry : fs::directory_iterator(path)) {
        fs::remove_all(entry.path());
      }
      fs::remove_all(path);
    }
  } catch (std::exception &e) {
    LOG_FATAL(logger(), "Failed remove rocksdb checkpoint: " << e.what());
  }
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
