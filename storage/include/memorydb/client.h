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
#include "storage/db_interface.h"
#include <functional>

namespace concord {
namespace storage {
namespace memorydb {

class Client;

typedef std::function<bool(const Sliver &, const Sliver &)> Compare;
typedef std::map<Sliver, Sliver, Compare> TKVStore;

class ClientIterator : public concord::storage::IDBClient::IDBClientIterator {
  friend class Client;

 public:
  ClientIterator(Client *_parentClient)
      : logger(concordlogger::Log::getLogger("concord.storage.memorydb")), m_parentClient(_parentClient) {}
  virtual ~ClientIterator() {}

  // Inherited via IDBClientIterator
  virtual KeyValuePair first() override;
  virtual KeyValuePair seekAtLeast(const Sliver &_searchKey) override;
  virtual KeyValuePair previous() override;
  virtual KeyValuePair next() override;
  virtual KeyValuePair getCurrent() override;
  virtual bool isEnd() override;
  virtual concordUtils::Status getStatus() override;

 private:
  concordlogger::Logger logger;

  // Pointer to the Client.
  Client *m_parentClient;

  // Current iterator inside the map.
  TKVStore::const_iterator m_current;
};

// In-memory IO operations below are not thread-safe.
// get/put/del/multiGet/multiPut/multiDel operations are not synchronized and
// not guarded by locks. The caller is expected to use those APIs via a
// single thread.
class Client : public IDBClient {
 public:
  Client(KeyComparator comp) : comp_(comp), map_([this](const Sliver &a, const Sliver &b) { return comp_(a, b); }) {}

  virtual void init(bool readOnly) override;
  virtual Status get(const Sliver &_key, OUT Sliver &_outValue) const override;
  Status get(const Sliver &_key, OUT char *&buf, uint32_t bufSize, OUT uint32_t &_size) const override;
  virtual IDBClientIterator *getIterator() const override;
  virtual concordUtils::Status freeIterator(IDBClientIterator *_iter) const override;
  virtual concordUtils::Status put(const Sliver &_key, const Sliver &_value) override;
  virtual concordUtils::Status del(const Sliver &_key) override;
  concordUtils::Status multiGet(const KeysVector &_keysVec, OUT ValuesVector &_valuesVec) override;
  concordUtils::Status multiPut(const SetOfKeyValuePairs &_keyValueMap) override;
  concordUtils::Status multiDel(const KeysVector &_keysVec) override;
  virtual void monitor() const override{};
  bool isNew() override { return true; }
  ITransaction *beginTransaction() override { return nullptr; }  // TODO [TK] implement in-memory transaction?
  TKVStore &getMap() { return map_; }

 private:
  // Keep a copy of comp_ so that it lives as long as map_
  KeyComparator comp_;

  // map that stores the in memory database.
  TKVStore map_;
};

}  // namespace memorydb
}  // namespace storage
}  // namespace concord
