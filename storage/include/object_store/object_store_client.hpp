// Copyright 2018 VMware, all rights reserved

#pragma once

#include <memory>
#include "storage/db_interface.h"

namespace concord::storage {

using namespace concordUtils;

/**
 * @brief This class implements Abstract object store client assuming S3
 * protocol
 *
 */
class ObjectStoreClient : public IDBClient {
 public:
  ObjectStoreClient(ObjectStoreClient&) = delete;
  ObjectStoreClient() = delete;
  ~ObjectStoreClient() = default;

  ObjectStoreClient(IDBClient* impl) : pImpl_(impl) {}

  void init(bool readOnly) { return pImpl_->init(readOnly); }

  Status get(const Sliver& _key, OUT Sliver& _outValue) const { return pImpl_->get(_key, _outValue); }

  Status get(const Sliver& _key, OUT char*& buf, uint32_t bufSize, OUT uint32_t& _size) const {
    return pImpl_->get(_key, buf, bufSize, _size);
  }

  Status put(const Sliver& _key, const Sliver& _value) { return pImpl_->put(_key, _value); }

  Status del(const Sliver& _key) { return pImpl_->del(_key); }

  Status multiGet(const KeysVector& _keysVec, OUT ValuesVector& _valuesVec) {
    return pImpl_->multiGet(_keysVec, _valuesVec);
  }

  Status multiPut(const SetOfKeyValuePairs& _keyValueMap) { return pImpl_->multiPut(_keyValueMap); }

  Status multiDel(const KeysVector& _keysVec) { return pImpl_->multiDel(_keysVec); }

  bool isNew() { return pImpl_->isNew(); }

  IDBClient::IDBClientIterator* getIterator() const { return pImpl_->getIterator(); }

  Status freeIterator(IDBClientIterator* _iter) const { return pImpl_->freeIterator(_iter); }

  ITransaction* beginTransaction() { return pImpl_->beginTransaction(); }

  Status has(const Sliver& key) const { return pImpl_->has(key); }

 protected:
  std::shared_ptr<IDBClient> pImpl_ = nullptr;
};

}  // namespace concord::storage
