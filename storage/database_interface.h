// Copyright 2018 VMware, all rights reserved

#ifndef CONCORD_STORAGE_DATABASE_INTERFACE_H_
#define CONCORD_STORAGE_DATABASE_INTERFACE_H_

#include "sliver.hpp"
#include "status.hpp"
#include "kv_types.hpp"

#define OUT

namespace concordStorage {

using concordUtils::Sliver;
using concordUtils::Status;
using concordUtils::KeysVector;
using concordUtils::ValuesVector;
using concordUtils::KeyValuePair;
using concordUtils::SetOfKeyValuePairs;

class IDBClient {
 public:
  typedef bool (*KeyComparator)(const Sliver &, const Sliver &);

  virtual ~IDBClient() = default;
  virtual Status init(bool readOnly = false) = 0;
  virtual Status get(Sliver _key, OUT Sliver &_outValue) const = 0;
  virtual Status get(Sliver _key, OUT char *&buf, uint32_t bufSize,
                     OUT uint32_t &_size) const = 0;
  virtual Status put(Sliver _key, Sliver _value) = 0;
  virtual Status del(Sliver _key) = 0;
  virtual Status multiGet(const KeysVector &_keysVec,
                          OUT ValuesVector &_valuesVec) = 0;
  virtual Status multiPut(const SetOfKeyValuePairs &_keyValueMap) = 0;
  virtual Status multiDel(const KeysVector &_keysVec) = 0;
  virtual void monitor() const = 0;
  virtual bool isNew() = 0;

  class IDBClientIterator {
   public:
    virtual KeyValuePair first() = 0;

    // Returns next keys if not found for this key
    virtual KeyValuePair seekAtLeast(Sliver _searchKey) = 0;
    virtual KeyValuePair previous() = 0;
    virtual KeyValuePair next() = 0;
    virtual KeyValuePair getCurrent() = 0;
    virtual bool isEnd() = 0;

    // Status of last operation
    virtual Status getStatus() = 0;

    virtual ~IDBClientIterator() {}
  };

  virtual IDBClientIterator *getIterator() const = 0;
  virtual Status freeIterator(IDBClientIterator *_iter) const = 0;
};

}  // namespace concordStorage

#endif  // CONCORD_STORAGE_DATABASE_INTERFACE_H_
