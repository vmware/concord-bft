// Copyright 2018 VMware, all rights reserved

#pragma once

#include "sliver.hpp"
#include "status.hpp"
#include <unordered_map>
#include <vector>
#include "Metrics.hpp"

#define OUT

namespace concord {
namespace storage {

using concordUtils::Sliver;
using concordUtils::Status;

typedef std::pair<Sliver, Sliver> KeyValuePair;
typedef std::unordered_map<Sliver, Sliver> SetOfKeyValuePairs;
typedef std::vector<Sliver> KeysVector;
typedef std::vector<Sliver> ValuesVector;

class ITransaction {
 public:
  typedef uint64_t ID;
  ITransaction(ID id) : id_(id) {}
  virtual ~ITransaction() = default;
  virtual void commit() = 0;
  virtual void rollback() = 0;
  virtual void put(const Sliver& key, const Sliver& value) = 0;
  virtual std::string get(const Sliver& key) = 0;
  virtual void del(const Sliver& key) = 0;

  ID getId() const { return id_; }
  std::string getIdStr() const { return std::to_string(id_); }
  class Guard {
   public:
    Guard(ITransaction* t) : txn_(t) {}
    virtual ~Guard() noexcept(false) {
      if (!std::uncaught_exception()) {
        txn_->commit();
      }
      delete txn_;
    }
    ITransaction* txn() const { return txn_; }

   protected:
    ITransaction* txn_;
  };

 private:
  ID id_;
};

class IDBClient {
 public:
  typedef std::shared_ptr<IDBClient> ptr;
  virtual ~IDBClient() = default;
  virtual void init(bool readOnly = false) = 0;
  virtual Status get(const Sliver& _key, OUT Sliver& _outValue) const = 0;
  virtual Status get(const Sliver& _key, OUT char*& buf, uint32_t bufSize, OUT uint32_t& _size) const = 0;
  virtual Status has(const Sliver& _key) const = 0;
  virtual Status put(const Sliver& _key, const Sliver& _value) = 0;
  virtual Status del(const Sliver& _key) = 0;
  virtual Status multiGet(const KeysVector& _keysVec, OUT ValuesVector& _valuesVec) = 0;
  virtual Status multiPut(const SetOfKeyValuePairs& _keyValueMap) = 0;
  virtual Status multiDel(const KeysVector& _keysVec) = 0;
  // Delete keys in the [_beginKey, _endKey) range (_beginKey included and _endKey excluded). If an inavlid range has
  // been passed (i.e. _endKey < _beginKey), the behavior is undefined. If _beginKey == _endKey, the call will not have
  // an effect. If no keys have been deleted, the operation is still successful. If there is an error, the returned
  // status will reflect it.
  virtual Status rangeDel(const Sliver& _beginKey, const Sliver& _endKey) = 0;
  virtual bool isNew() = 0;

  // the caller is responsible for transaction object lifetime
  // possible options: ITransaction::Guard or std::shared_ptr
  virtual ITransaction* beginTransaction() = 0;

  virtual void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) = 0;
  class IDBClientIterator {
   public:
    virtual KeyValuePair first() = 0;
    virtual KeyValuePair last() = 0;
    // Returns next keys if not found for this key
    virtual KeyValuePair seekAtLeast(const Sliver& _searchKey) = 0;
    // Returns the key value pair of the last key which is less than or equal to _searchKey
    virtual KeyValuePair seekAtMost(const Sliver& _searchKey) = 0;
    virtual KeyValuePair previous() = 0;
    virtual KeyValuePair next() = 0;
    virtual KeyValuePair getCurrent() = 0;
    virtual bool isEnd() = 0;
    // Status of last operation
    virtual Status getStatus() = 0;
    virtual ~IDBClientIterator() = default;

    class Guard {
     public:
      Guard(const IDBClient& db) : db_{db}, iter_{*(db.getIterator())} {}
      Guard(const IDBClient* db) : db_{*db}, iter_{*(db->getIterator())} {}
      Guard(const Guard&) = delete;
      Guard& operator=(const Guard&) = delete;
      Guard(Guard&&) = default;
      ~Guard() noexcept {
        try {
          db_.freeIterator(&iter_);
        } catch (...) {
        }
      }

      IDBClientIterator* operator->() const noexcept { return &iter_; }

     private:
      const IDBClient& db_;
      IDBClientIterator& iter_;
    };
  };

  class IKeyComparator {
   public:
    virtual int composedKeyComparison(const char* _a_data, size_t _a_length, const char* _b_data, size_t _b_length) = 0;
    virtual ~IKeyComparator() = default;
  };

  virtual IDBClientIterator* getIterator() const = 0;
  virtual Status freeIterator(IDBClientIterator* _iter) const = 0;
  IDBClientIterator::Guard getIteratorGuard() const { return IDBClientIterator::Guard{this}; }
};

}  // namespace storage
}  // namespace concord
