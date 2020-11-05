// Copyright 2018-2019 VMware, all rights reserved

// Objects of ClientIterator contain an iterator for the in memory
// object store (implemented as a map) along with a pointer to the map.
//
// Objects of Client are implementations of an in memory database
// (implemented as a map).
//
// The map contains key value pairs of the type KeyValuePair. Keys and values
// are of type Sliver.

#pragma once

#include "Logger.hpp"
#include <map>
#include "sliver.hpp"
#include "key_comparator.h"
#include "storage/partitioned_db_interface.h"
#include <functional>
#include "storage/storage_metrics.h"

namespace concord {
namespace storage {
namespace memorydb {

class Client;

typedef std::function<bool(const Sliver &, const Sliver &)> Compare;
typedef std::map<Sliver, Sliver, Compare> TKVStore;
typedef std::map<std::string, TKVStore> PartitionMap;

class ClientIterator : public concord::storage::IDBClient::IDBClientIterator {
  friend class Client;

 public:
  ClientIterator(const Client *parentClient, const TKVStore &partitionStore)
      : logger(logging::getLogger("concord.storage.memorydb")),
        m_parentClient(parentClient),
        m_partitionStore{partitionStore} {}
  virtual ~ClientIterator() {}

  // Inherited via IDBClientIterator
  KeyValuePair first() override;
  KeyValuePair last() override;
  KeyValuePair seekAtLeast(const Sliver &_searchKey) override;
  KeyValuePair seekAtMost(const Sliver &_searchKey) override;
  KeyValuePair previous() override;
  KeyValuePair next() override;
  KeyValuePair getCurrent() override;
  bool valid() const override;
  Status getStatus() const override;

 private:
  logging::Logger logger;

  // Pointer to the Client.
  const Client *m_parentClient;

  // Current iterator inside the partition store.
  TKVStore::const_iterator m_current;

  // Needed to handle the case of calling previous() when pointing to the first element.
  bool m_valid{false};

  // The DB partition this iterator refers to.
  const TKVStore &m_partitionStore;
};

// In-memory IO operations below are not thread-safe.
// get/put/del/multiGet/multiPut/multiDel operations are not synchronized and
// not guarded by locks. The caller is expected to use those APIs via a
// single thread.
//
// The default KeyComparator provides lexicographical ordering. If 'a' is shorter than 'b' and they match up to the
// length of 'a', then 'a' is considered to precede 'b'.
class Client : public IPartitionedDBClient {
 public:
  Client(KeyComparator comp = KeyComparator{}) : logger(logging::getLogger("concord.storage.memorydb")), comp_(comp) {
    // Ensure we always have the default partition.
    addPartition(kDefaultPartition);
  }

  void init(bool readOnly = false) override;
  concordUtils::Status get(const Sliver &_key, OUT Sliver &_outValue) const override;
  concordUtils::Status get(const Sliver &_key, OUT char *&buf, uint32_t bufSize, OUT uint32_t &_size) const override;
  concordUtils::Status has(const Sliver &_key) const override;
  std::unique_ptr<IDBClientIterator> getIterator() const override;
  virtual concordUtils::Status put(const Sliver &_key, const Sliver &_value) override;
  virtual concordUtils::Status del(const Sliver &_key) override;
  concordUtils::Status multiGet(const KeysVector &_keysVec, OUT ValuesVector &_valuesVec) const override;
  concordUtils::Status multiPut(const SetOfKeyValuePairs &_keyValueMap) override;
  concordUtils::Status multiDel(const KeysVector &_keysVec) override;
  Status multiGet(const std::vector<PartitionedKey> &keys, ValuesVector &values) const override;
  Status multiPut(const std::vector<PartitionedKeyValue> &keyValues) override;
  Status multiDel(const std::vector<PartitionedKey> &keys) override;
  concordUtils::Status rangeDel(const Sliver &_beginKey, const Sliver &_endKey) override;
  bool isNew() override { return true; }
  ITransaction *beginTransaction() override;
  std::unique_ptr<ITransaction> startTransaction() override;
  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) override {
    storage_metrics_.setAggregator(aggregator);
  }
  InMemoryStorageMetrics &getStorageMetrics() const { return storage_metrics_; }

  std::string defaultPartition() const override { return kDefaultPartition; }
  std::set<std::string> partitions() const override;
  bool hasPartition(const std::string &partition) const override;
  Status addPartition(const std::string &partition) override;
  Status dropPartition(const std::string &partition) override;

  Status get(const std::string &partition, const Sliver &key, Sliver &outValue) const override;
  Status has(const std::string &partition, const Sliver &key) const override;
  Status put(const std::string &partition, const Sliver &key, const Sliver &value) override;
  Status del(const std::string &partition, const Sliver &key) override;
  Status multiGet(const std::string &partition, const KeysVector &keys, ValuesVector &values) const override;
  Status multiPut(const std::string &partition, const SetOfKeyValuePairs &keyValues) override;
  Status multiDel(const std::string &partition, const KeysVector &keys) override;
  Status rangeDel(const std::string &partition, const Sliver &beginKey, const Sliver &endKey) override;

  std::unique_ptr<IDBClientIterator> getIterator(const std::string &partition) const override;
  std::unique_ptr<IPartitionedTransaction> startPartitionedTransaction() override;

  static inline const std::string kDefaultPartition{"default"};

 private:
  logging::Logger logger;

  // Keep a copy of comp_ so that it lives as long as map_
  KeyComparator comp_;

  // map that stores the in memory database.
  // maps partition -> key-value store
  PartitionMap map_;

  // Metrics
  mutable InMemoryStorageMetrics storage_metrics_;
};

}  // namespace memorydb
}  // namespace storage
}  // namespace concord
