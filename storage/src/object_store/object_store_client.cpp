// Copyright 2019 VMware, all rights reserved

#include "object_store/object_store_client.hpp"
#include "ecs_s3_client.hpp"

using namespace concord::storage;

ObjectStoreClient::ObjectStoreClient(const S3_StoreConfig& config,
                                     const ObjectStoreBehavior& bh) {
  pImpl_ = std::shared_ptr<EcsS3ClientImpl>(new EcsS3ClientImpl(config, bh));
}

void ObjectStoreClient::init(bool readOnly) { return pImpl_->init(); }

Status ObjectStoreClient::get(const Sliver& _key, OUT Sliver& _outValue) const {
  return pImpl_->get(_key, _outValue);
}

Status ObjectStoreClient::get(const Sliver& _key, OUT char*& buf,
                              uint32_t bufSize, OUT uint32_t& _size) const {
  return pImpl_->get(_key, buf, bufSize, _size);
}

Status ObjectStoreClient::put(const Sliver& _key, const Sliver& _value) {
  return pImpl_->put(_key, _value);
}

Status ObjectStoreClient::del(const Sliver& _key) { return pImpl_->del(_key); }

Status ObjectStoreClient::multiGet(const KeysVector& _keysVec,
                                   OUT ValuesVector& _valuesVec) {
  return pImpl_->multiGet(_keysVec, _valuesVec);
}

Status ObjectStoreClient::multiPut(const SetOfKeyValuePairs& _keyValueMap) {
  return pImpl_->multiPut(_keyValueMap);
}

Status ObjectStoreClient::multiDel(const KeysVector& _keysVec) {
  return pImpl_->multiDel(_keysVec);
}

void ObjectStoreClient::monitor() const { return pImpl_->monitor(); }

bool ObjectStoreClient::isNew() { return pImpl_->isNew(); }

IDBClient::IDBClientIterator* ObjectStoreClient::getIterator() const {
  return pImpl_->getIterator();
}

Status ObjectStoreClient::freeIterator(IDBClientIterator* _iter) const {
  return pImpl_->freeIterator(_iter);
}

ITransaction* ObjectStoreClient::beginTransaction() {
  return pImpl_->beginTransaction();
}

void ObjectStoreClient::wait_for_storage() {
  return static_pointer_cast<EcsS3ClientImpl>(pImpl_)->wait_for_storage();
}

Status ObjectStoreClient::object_exists(const Sliver& key) {
  return static_pointer_cast<EcsS3ClientImpl>(pImpl_)->object_exists(key);
}

Status ObjectStoreClient::test_bucket() {
  return static_pointer_cast<EcsS3ClientImpl>(pImpl_)->test_bucket();
}

ObjectStoreBehavior::ObjectStoreBehavior(bool canDelete, bool canSimulateNetFailure)
    : allowDelete_{canDelete}, allowNetworkFailure_{canSimulateNetFailure} {}

bool ObjectStoreBehavior::can_delete() const { return allowDelete_; }

bool ObjectStoreBehavior::can_simulate_net_failure() const { 
  return allowNetworkFailure_; }

bool ObjectStoreBehavior::get_do_net_failure() const {
  return doNetworkFailure_;
}

void ObjectStoreBehavior::set_do_net_failure(bool value, uint64_t durationMilli) {
  doNetworkFailure_ = value;
  doNetworkFailureDurMilli_ = durationMilli;
  startTimeForNetworkFailure_ = std::chrono::steady_clock::now();
}

bool ObjectStoreBehavior::has_finished() const {
  std::chrono::time_point<std::chrono::steady_clock> time = std::chrono::steady_clock::now();
  auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(time - startTimeForNetworkFailure_).count();
  return diff > 0 && (uint64_t)diff > doNetworkFailureDurMilli_;
}

