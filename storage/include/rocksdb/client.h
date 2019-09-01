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

#pragma once

#ifdef USE_ROCKSDB
#include "Logger.hpp"
#include <rocksdb/utilities/transaction_db.h>
#include "kv_types.hpp"
#include "storage/db_interface.h"

namespace concord {
namespace storage {
namespace rocksdb {

class Client;

class ClientIterator
    : public concord::storage::IDBClient::IDBClientIterator {
  friend class Client;

 public:
  ClientIterator(const Client *_parentClient);
  ~ClientIterator() { delete m_iter; }

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
  const Client *m_parentClient;

  concordUtils::Status m_status;
};

class Client : public concord::storage::IDBClient {
 public:
  Client(std::string _dbPath, ::rocksdb::Comparator *_comparator)
      : logger(concordlogger::Log::getLogger("com.concord.vmware.kvb")),
        m_dbPath(_dbPath),
        m_comparator(_comparator) {}

  void init(bool readOnly = false) override;
  concordUtils::Status get(concordUtils::Sliver _key, concordUtils::Sliver &_outValue) const override;
  concordUtils::Status get(concordUtils::Sliver _key, char *&buf, uint32_t bufSize, uint32_t &_realSize) const override;
  IDBClientIterator*   getIterator() const override;
  concordUtils::Status freeIterator(IDBClientIterator *_iter) const override;
  concordUtils::Status put(concordUtils::Sliver _key, concordUtils::Sliver _value) override;
  concordUtils::Status del(concordUtils::Sliver _key) override;
  concordUtils::Status multiGet(const KeysVector &_keysVec, ValuesVector &_valuesVec) override;
  concordUtils::Status multiPut(const SetOfKeyValuePairs &_keyValueMap) override;
  concordUtils::Status multiDel(const KeysVector &_keysVec) override;
  ::rocksdb::Iterator* getNewRocksDbIterator() const;
  void monitor() const override;
  bool isNew() override;
  ITransaction* beginTransaction() override;

 private:
  concordUtils::Status launchBatchJob(::rocksdb::WriteBatch &_batchJob, const KeysVector &_keysVec);
  std::ostringstream   collectKeysForPrint( const KeysVector &_keysVec);
  concordUtils::Status get(concordUtils::Sliver _key, std::string &_value) const;

 private:
  concordlogger::Logger logger;
  // Database path on directory (used for connection).
  std::string m_dbPath;
  // Database object (created on connection).
  std::unique_ptr<::rocksdb::DB> m_dbInstance;
  ::rocksdb::TransactionDB*      txn_db_ = nullptr;;
  ::rocksdb::Comparator*         m_comparator = nullptr;
};

::rocksdb::Slice toRocksdbSlice(concordUtils::Sliver _s);
concordUtils::Sliver fromRocksdbSlice(::rocksdb::Slice _s);
concordUtils::Sliver copyRocksdbSlice(::rocksdb::Slice _s);

}
}
}
#endif  // USE_ROCKSDB
