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

  void init(bool readOnly) override { return pImpl_->init(readOnly); }

  Status get(const Sliver& _key, OUT Sliver& _outValue) const override { return pImpl_->get(_key, _outValue); }

  Status get(const Sliver& _key, OUT char*& buf, uint32_t bufSize, OUT uint32_t& _size) const override {
    return pImpl_->get(_key, buf, bufSize, _size);
  }

  Status put(const Sliver& _key, const Sliver& _value) override { return pImpl_->put(_key, _value); }

  Status del(const Sliver& _key) override { return pImpl_->del(_key); }

  Status multiGet(const KeysVector& _keysVec, OUT ValuesVector& _valuesVec) const override {
    return pImpl_->multiGet(_keysVec, _valuesVec);
  }

  Status multiPut(const SetOfKeyValuePairs& _keyValueMap) override { return pImpl_->multiPut(_keyValueMap); }

  Status multiDel(const KeysVector& _keysVec) override { return pImpl_->multiDel(_keysVec); }

  bool isNew() override { return pImpl_->isNew(); }

  std::unique_ptr<IDBClient::IDBClientIterator> getIterator() const override { return pImpl_->getIterator(); }

  ITransaction* beginTransaction() override { return pImpl_->beginTransaction(); }

  std::unique_ptr<ITransaction> startTransaction() override {
    return std::unique_ptr<ITransaction>{beginTransaction()};
  }

  Status has(const Sliver& key) const override { return pImpl_->has(key); }

  Status rangeDel(const Sliver& _beginKey, const Sliver& _endKey) override {
    return pImpl_->rangeDel(_beginKey, _endKey);
  }

  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) override {
    pImpl_->setAggregator(aggregator);
  }

 protected:
  std::shared_ptr<IDBClient> pImpl_ = nullptr;
};

}  // namespace concord::storage
