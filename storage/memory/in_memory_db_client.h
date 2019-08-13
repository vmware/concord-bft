// Copyright 2018 VMware, all rights reserved

// Objects of InMemoryDBClientIterator contain an iterator for the in memory
// object store (implemented as a map) along with a pointer to the map.
//
// Objects of InMemoryDBClient are implementations of an in memory database
// (implemented as a map).
//
// The map contains key value pairs of the type KeyValuePair. Keys and values
// are of type Sliver.

#pragma once

#include "Logger.hpp"
#include <map>
#include "storage/db_interface.h"

namespace concord {
namespace storage {
namespace memory {

class InMemoryDBClient;

typedef std::map<concordUtils::Sliver, concordUtils::Sliver, IDBClient::KeyComparator> TKVStore;

class InMemoryDBClientIterator : public IDBClient::IDBClientIterator {
  friend class InMemoryDBClient;

 public:
  InMemoryDBClientIterator(InMemoryDBClient *_parentClient)
      : logger(concordlogger::Log::getLogger("concord.storage.memory")), m_parentClient(_parentClient) {}
  virtual ~InMemoryDBClientIterator() {}

  // Inherited via IDBClientIterator
  virtual KeyValuePair first() override;
  virtual KeyValuePair seekAtLeast(Sliver _searchKey) override;
  virtual KeyValuePair previous() override;
  virtual KeyValuePair next() override;
  virtual KeyValuePair getCurrent() override;
  virtual bool isEnd() override;
  virtual concordUtils::Status getStatus() override;

 private:
  concordlogger::Logger logger;

  // Pointer to the InMemoryDBClient.
  InMemoryDBClient *m_parentClient;

  // Current iterator inside the map.
  TKVStore::const_iterator m_current;
};

// In-memory IO operations below are not thread-safe.
// get/put/del/multiGet/multiPut/multiDel operations are not synchronized and
// not guarded by locks. The caller is expected to use those APIs via a
// single thread.
class InMemoryDBClient : public IDBClient {
 public:
  InMemoryDBClient(KeyComparator comp) { setComparator(comp); }

  virtual Status init(bool readOnly) override;
  virtual Status get(Sliver _key, OUT Sliver &_outValue) const override;
  Status get(Sliver _key, OUT char *&buf, uint32_t bufSize, OUT uint32_t &_size) const override;
  virtual IDBClientIterator *getIterator() const override;
  virtual concordUtils::Status freeIterator(IDBClientIterator *_iter) const override;
  virtual concordUtils::Status put(Sliver _key, Sliver _value) override;
  virtual concordUtils::Status del(Sliver _key) override;
  concordUtils::Status multiGet(const KeysVector &_keysVec, OUT ValuesVector &_valuesVec) override;
  concordUtils::Status multiPut(const SetOfKeyValuePairs &_keyValueMap) override;
  concordUtils::Status multiDel(const KeysVector &_keysVec) override;
  virtual void monitor() const override{};
  bool isNew() override { return true; }

  TKVStore &getMap() { return map; }
  void setComparator(KeyComparator comp) { map = TKVStore(comp); }

 private:
  // map that stores the in memory database.
  TKVStore map;
};

}  // namespace memory
}  // namespace storage
}  // namespace concord
