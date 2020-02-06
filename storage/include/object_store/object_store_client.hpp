// Copyright 2018 VMware, all rights reserved

#pragma once

#include <memory>
#include "storage/db_interface.h"
#include <chrono>

namespace concord {
namespace storage {

struct S3_StoreConfig {
  std::string bucketName;  // assuminh pre configured, no need to create
  std::string url;         // from the customer
  std::string protocol;    // currently tested with HTTP
  std::string secretKey;   // from the customer
  std::string accessKey;   // from the customer
  std::uint32_t maxWaitTime; // in milliseconds
  virtual ~S3_StoreConfig() {}
};

// For using in tests. This class is used to tell the ObjectStore client impl to simulate network failure for predefined
// ammount of time. Also it can be used for controlling deletion of objects (useful for tests)
class ObjectStoreBehavior {
 private:
  bool allowDelete_ = false;
  bool allowNetworkFailure_ = false;
  std::chrono::time_point<std::chrono::steady_clock> startTimeForNetworkFailure_;
  bool doNetworkFailure_ = false;
  uint64_t doNetworkFailureDurMilli_ = 0;
 
 public:
  ObjectStoreBehavior(bool canDelete, bool canSimulateNetFailure);
  // set network failure flag and duration
  void set_do_net_failure(bool value, uint64_t durationMilli);
  bool get_do_net_failure() const;
  bool can_delete() const;
  // used to assert whether its possible to simulate network failure
  bool can_simulate_net_failure() const;
  // returns whether the network failure period has been elapsed
  bool has_finished() const;
};

/**
 * @brief This class implements Abstract object store client assuming S3
 * protocol
 *
 */
class ObjectStoreClient : public IDBClient {
 public:
  ObjectStoreClient(ObjectStoreClient&) = delete;
  ObjectStoreClient() = delete;

  ObjectStoreClient(const S3_StoreConfig& config, const ObjectStoreBehavior& b);

  virtual ~ObjectStoreClient() = default;
  virtual void init(bool readOnly = false) override;
  virtual Status get(const Sliver& _key, OUT Sliver& _outValue) const override;
  virtual Status get(const Sliver& _key, OUT char*& buf, uint32_t bufSize,
                     OUT uint32_t& _size) const override;
  virtual Status put(const Sliver& _key, const Sliver& _value) override;
  virtual Status del(const Sliver& _key) override;
  virtual Status multiGet(const KeysVector& _keysVec,
                          OUT ValuesVector& _valuesVec) override;
  virtual Status multiPut(const SetOfKeyValuePairs& _keyValueMap) override;
  virtual Status multiDel(const KeysVector& _keysVec) override;
  virtual void monitor() const override;
  virtual bool isNew() override;
  virtual IDBClient::IDBClientIterator* getIterator() const override;
  virtual Status freeIterator(IDBClientIterator* _iter) const override;
  virtual ITransaction* beginTransaction() override;

  void wait_for_storage();
  Status object_exists(const Sliver& key);
  Status test_bucket();

 private:
  std::shared_ptr<IDBClient> pImpl_ = nullptr;
};  // class ObjectStoreClient

}  // namespace storage
}  // namespace concord
