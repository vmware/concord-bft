// Copyright 2018 VMware, all rights reserved

/**
 * @file RocksDBClient.h
 *
 *  @brief Header file containing the RocksDBClientIterator and RocksDBClient
 *  class definitions.
 *
 *  Objects of RocksDBClientIterator contain an iterator for database along with
 *  a pointer to the client object.
 *
 *  Objects of RocksDBClient signify connections with RocksDB database. They
 *  contain variables for storing the database directory path, connection object
 *  and comparator.
 *
 */

#ifndef CONCORD_STORAGE_ROCKSDB_CLIENT_H_
#define CONCORD_STORAGE_ROCKSDB_CLIENT_H_

#ifdef USE_ROCKSDB
#include "Logger.hpp"
#include "rocksdb/db.h"
#include "kv_types.hpp"
#include "../database_interface.h"

namespace concordStorage {
namespace rocksdb {

class RocksDBClient;

class RocksDBClientIterator
    : public concordStorage::IDBClient::IDBClientIterator {
  friend class RocksDBClient;

 public:
  RocksDBClientIterator(const RocksDBClient *_parentClient);
  ~RocksDBClientIterator() { delete m_iter; }

  // Inherited via IDBClientIterator
  concordUtils::KeyValuePair first() override;
  concordUtils::KeyValuePair seekAtLeast(concordUtils::Sliver _searchKey) override;
  concordUtils::KeyValuePair previous() override;
  KeyValuePair next() override;
  KeyValuePair getCurrent() override;
  bool isEnd() override;
  concordUtils::Status getStatus() override;

 private:
  concordlogger::Logger logger;

  ::rocksdb::Iterator *m_iter;

  // Reference to the RocksDBClient
  const RocksDBClient *m_parentClient;

  concordUtils::Status m_status;
};

class RocksDBClient : public concordStorage::IDBClient {
 public:
  RocksDBClient(std::string _dbPath, ::rocksdb::Comparator *_comparator)
      : logger(concordlogger::Log::getLogger("com.concord.vmware.kvb")),
        m_dbPath(_dbPath),
        m_comparator(_comparator) {}

  concordUtils::Status init(bool readOnly = false) override;
  concordUtils::Status get(
      concordUtils::Sliver _key,
      OUT concordUtils::Sliver &_outValue) const override;
  concordUtils::Status get(concordUtils::Sliver _key,
                                 OUT char *&buf, uint32_t bufSize,
                                 OUT uint32_t &_realSize) const override;
  IDBClientIterator *getIterator() const override;
  concordUtils::Status freeIterator(
      IDBClientIterator *_iter) const override;
  concordUtils::Status put(concordUtils::Sliver _key,
                                 concordUtils::Sliver _value) override;
  concordUtils::Status del(concordUtils::Sliver _key) override;
  concordUtils::Status multiGet(
      const KeysVector &_keysVec,
      OUT ValuesVector &_valuesVec) override;
  concordUtils::Status multiPut(
      const SetOfKeyValuePairs &_keyValueMap) override;
  concordUtils::Status multiDel(
      const KeysVector &_keysVec) override;
  ::rocksdb::Iterator *getNewRocksDbIterator() const;
  void monitor() const override;
  bool isNew() override;

 private:
  concordUtils::Status launchBatchJob(
      ::rocksdb::WriteBatch &_batchJob,
      const KeysVector &_keysVec);
  std::ostringstream collectKeysForPrint(
      const KeysVector &_keysVec);
  concordUtils::Status get(concordUtils::Sliver _key,
                                 OUT std::string &_value) const;

 private:
  concordlogger::Logger logger;

  // Database path on directory (used for connection).
  std::string m_dbPath;

  // Database object (created on connection).
  std::unique_ptr<::rocksdb::DB> m_dbInstance;

  // Comparator object.
  ::rocksdb::Comparator *m_comparator;
};

::rocksdb::Slice toRocksdbSlice(concordUtils::Sliver _s);
concordUtils::Sliver fromRocksdbSlice(::rocksdb::Slice _s);
concordUtils::Sliver copyRocksdbSlice(::rocksdb::Slice _s);

}  // namespace rocksdb
}  // namespace concordStorage

#endif  // USE_ROCKSDB
#endif  // CONCORD_STORAGE_ROCKSDB_CLIENT_H_
