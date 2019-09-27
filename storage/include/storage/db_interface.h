// Copyright 2018 VMware, all rights reserved

#pragma once

#include "sliver.hpp"
#include "status.hpp"
#include "kv_types.hpp"

#define OUT

namespace concord {
namespace storage {

using concordUtils::Sliver;
using concordUtils::Status;
using concordUtils::KeysVector;
using concordUtils::ValuesVector;
using concordUtils::KeyValuePair;
using concordUtils::SetOfKeyValuePairs;


class ITransaction {
 public:
  typedef uint64_t ID;
  ITransaction(ID id):id_(id){}
  virtual ~ITransaction() = default;
  virtual void commit() = 0;
  virtual void rollback() = 0;
  virtual void put(const Sliver& key, const Sliver& value) = 0;
  virtual std::string get(const Sliver& key) = 0;
  virtual void del(const Sliver& key) = 0;

  ID getId() const {return id_;}
  std::string getIdStr() const {return std::to_string(id_);}
  class Guard{
   public:
     Guard(ITransaction* t):txn_(t){}
     virtual ~Guard(){
       if (!std::uncaught_exception())
         txn_->commit();
       delete txn_;
     }
     ITransaction* txn() const {return txn_;}
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
  virtual void   init(bool readOnly = false) = 0;
  virtual Status get(const Sliver& _key, OUT Sliver &_outValue) const = 0;
  virtual Status get(const Sliver& _key, OUT char *&buf, uint32_t bufSize, OUT uint32_t &_size) const = 0;
  virtual Status put(const Sliver& _key, const Sliver& _value) = 0;
  virtual Status del(const Sliver& _key) = 0;
  virtual Status multiGet(const KeysVector &_keysVec, OUT ValuesVector &_valuesVec) = 0;
  virtual Status multiPut(const SetOfKeyValuePairs &_keyValueMap) = 0;
  virtual Status multiDel(const KeysVector &_keysVec) = 0;
  virtual void   monitor() const = 0;
  virtual bool   isNew() = 0;

  // the caller is responsible for transaction object lifetime
  // possible options: ITransaction::Guard or std::shared_ptr
  virtual ITransaction* beginTransaction() = 0;

  class IDBClientIterator {
   public:
    virtual KeyValuePair first() = 0;
    // Returns next keys if not found for this key
    virtual KeyValuePair seekAtLeast(const Sliver& _searchKey) = 0;
    virtual KeyValuePair previous() = 0;
    virtual KeyValuePair next() = 0;
    virtual KeyValuePair getCurrent() = 0;
    virtual bool isEnd() = 0;
    // Status of last operation
    virtual Status getStatus() = 0;
    virtual ~IDBClientIterator() = default;
  };

  class IKeyManipulator{
    public:
    virtual int composedKeyComparison(const uint8_t* _a_data, size_t _a_length,
                                      const uint8_t* _b_data, size_t _b_length) = 0;
      virtual ~IKeyManipulator() = default;
  };

  virtual IDBClientIterator *getIterator() const = 0;
  virtual Status freeIterator(IDBClientIterator *_iter) const = 0;
};

}
}
