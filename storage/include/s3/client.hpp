// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
//
// This convenience header combines different block implementations.

#include <libs3.h>
#include <cstring>
#include <mutex>
#include <thread>
#include <mutex>
#include "Logger.hpp"
#include "assertUtils.hpp"
#include "storage/db_interface.h"
#include "s3_metrics.hpp"

#pragma once

namespace concord::storage::s3 {

struct StoreConfig {
  std::string bucketName;     // assuming pre-configured, no need to create
  std::string url;            // from the customer
  std::string protocol;       // currently tested with HTTP
  std::string secretKey;      // from the customer
  std::string accessKey;      // from the customer
  std::uint32_t maxWaitTime;  // in milliseconds
  std::string pathPrefix;     // optional path prefix used in the bucket
};

/**
 * @brief Internal implementation of ECS EMC S3 client based on libs3
 * Most of the method are irrelevant for the object store and throw exception if
 * called. We assume that only ObjectStoreAppState class will use this class
 *
 */
class Client : public concord::storage::IDBClient {
 public:
  /**
   * Transaction class. Is used also internally to implement multiPut()
   */
  class Transaction : public ITransaction {
   public:
    Transaction(Client* client) : ITransaction(nextId()), client_{client} {}
    void commit() override {
      for (auto& pair : multiput_)
        if (concordUtils::Status s = client_->put(pair.first, pair.second); !s.isOK())
          throw std::runtime_error("S3 commit failed while putting a value for key: " + pair.first.toString() +
                                   std::string(" txn id[") + getIdStr() + std::string("], reason: ") + s.toString());
      for (auto& key : keys_to_delete_)
        if (concordUtils::Status s = client_->del(key); !s.isOK())
          throw std::runtime_error("S3 commit failed while deleting a vallue for key: " + key.toString() +
                                   std::string(" txn id[") + getIdStr() + std::string("], reason: ") + s.toString());
    }
    void rollback() override { multiput_.clear(); }
    void put(const concordUtils::Sliver& key, const concordUtils::Sliver& value) override { multiput_[key] = value; }
    std::string get(const concordUtils::Sliver& key) override { return multiput_[key].toString(); }
    void del(const concordUtils::Sliver& key) override {
      multiput_.erase(key);
      keys_to_delete_.insert(key);
    }

   protected:
    Client* client_;
    SetOfKeyValuePairs multiput_;
    std::set<concordUtils::Sliver> keys_to_delete_;
    ID nextId() {
      static ID id_ = 0;
      return ++id_;
    }
  };

  Client(const StoreConfig& config) : config_{config} { LOG_INFO(logger_, "S3 client created"); }

  ~Client() {
    /* Destroy LibS3 */
    S3_deinitialize();
    init_ = false;
    LOG_INFO(logger_, "libs3 deinit");
  }
  Status reclaimDiskSpace() override { return Status::IllegalOperation("not supported with s3 storage"); }

  /**
   * @brief used only from the test! name should be immutable string literal
   *
   * @param name
   */
  void set_bucket_name(const std::string& name) { config_.bucketName = name; }

  /**
   * @brief Initializing underlying libs3. The S3_initialize function must be
   * called exactly once and only from 1 thread
   *
   * @param readOnly
   */
  void init(bool readOnly) override;
  /**
   * @brief Get object from the store.
   *
   * @param _key key to be retrieved
   * @param _outValue returned object
   * @return OK if success, otherwise GeneralError with underlying error status
   */
  concordUtils::Status get(const concordUtils::Sliver& _key, OUT concordUtils::Sliver& _outValue) const override {
    LOG_DEBUG(logger_, _key.toString());
    using namespace std::placeholders;
    concordUtils::Status res = concordUtils::Status::OK();
    std::function<Status(const concordUtils::Sliver&, OUT concordUtils::Sliver&)> f =
        std::bind(&Client::get_internal, this, _1, _2);
    do_with_retry("get_internal", res, f, _key, _outValue);
    return res;
  }

  concordUtils::Status get(const concordUtils::Sliver& _key,
                           OUT char*& buf,
                           uint32_t bufSize,
                           OUT uint32_t& _size) const override {
    concordUtils::Sliver res;
    if (Status s = get(_key, res); !s.isOK()) return s;
    size_t len = std::min(res.length(), (size_t)bufSize);
    memcpy(buf, res.data(), len);
    _size = len;
    return concordUtils::Status::OK();
  }

  /**
   * @brief Put object to the store.
   *
   * @param _key object's key
   * @param _value object
   * @return OK if success, otherwise GeneralError with underlying error status
   */
  concordUtils::Status put(const concordUtils::Sliver& _key, const concordUtils::Sliver& _value) override {
    using namespace std::placeholders;
    std::function<Status(const concordUtils::Sliver&, const concordUtils::Sliver&)> f =
        std::bind(&Client::put_internal, this, _1, _2);
    concordUtils::Status res = concordUtils::Status::OK();
    do_with_retry("put_internal", res, f, _key, _value);
    return res;
  }

  concordUtils::Status test_bucket() {
    using namespace std::placeholders;
    std::function<Status()> f = std::bind(&Client::test_bucket_internal, this);
    concordUtils::Status res = concordUtils::Status::OK();

    do_with_retry("test_bucket_internal", res, f);
    return res;
  }

  concordUtils::Status has(const concordUtils::Sliver& key) const override {
    using namespace std::placeholders;
    std::function<Status(const concordUtils::Sliver&)> f = std::bind(&Client::object_exists_internal, this, _1);
    concordUtils::Status res = concordUtils::Status::OK();
    do_with_retry("object_exists_internal", res, f, key);
    return res;
  }

  concordUtils::Status del(const concordUtils::Sliver& key) override;

  concordUtils::Status multiGet(const KeysVector& _keysVec, OUT ValuesVector& _valuesVec) override {
    ConcordAssert(_keysVec.size() == _valuesVec.size());

    for (KeysVector::size_type i = 0; i < _keysVec.size(); ++i)
      if (Status s = get(_keysVec[i], _valuesVec[i]); !s.isOK()) return s;

    return concordUtils::Status::OK();
  }

  concordUtils::Status multiPut(const SetOfKeyValuePairs& _keyValueMap) override {
    ITransaction::Guard g(beginTransaction());
    for (auto&& pair : _keyValueMap) g.txn()->put(pair.first, pair.second);

    return concordUtils::Status::OK();
  }

  concordUtils::Status multiDel(const KeysVector& _keysVec) override {
    for (auto&& key : _keysVec)
      if (Status s = del(key); !s.isOK()) return s;
    return concordUtils::Status::OK();
  }

  bool isNew() override { throw std::logic_error("isNew()  Not implemented for S3 object store"); }

  IDBClient::IDBClientIterator* getIterator() const override {
    ConcordAssert("getIterator() Not implemented for ECS S3 object store" && false);
    throw std::logic_error("getIterator() Not implemented for ECS S3 object store");
  }

  concordUtils::Status freeIterator(IDBClientIterator* _iter) const override {
    throw std::logic_error("freeIterator() Not implemented for ECS S3 object store");
  }

  ITransaction* beginTransaction() override { return new Transaction(this); }

  concordUtils::Status rangeDel(const Sliver& _beginKey, const Sliver& _endKey) override {
    throw std::logic_error("rangeDel()  Not implemented for S3 object store");
  }

  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) override {
    metrics_.metrics_component.SetAggregator(aggregator);
    metrics_.metrics_component.UpdateAggregator();
  }

  ///////////////////////// protected /////////////////////////////
 protected:
  // retry forever, increasing the waiting timeout until it reaches the defined maximum
  template <typename F, typename... Args>
  void do_with_retry(const std::string_view msg, Status& r, F&& f, Args&&... args) const {
    uint16_t delay = initialDelay_;
    do {
      r = std::forward<F>(f)(std::forward<Args>(args)...);
      if (!r.isGeneralError()) break;
      if (delay < config_.maxWaitTime) delay *= delayFactor_;
      LOG_INFO(logger_, "retrying " << msg << " after delay: " << delay);
      std::this_thread::sleep_for(std::chrono::milliseconds(delay));
    } while (!r.isOK() || !r.isNotFound());
  }

  concordUtils::Status get_internal(const concordUtils::Sliver& _key, OUT concordUtils::Sliver& _outValue) const;

  concordUtils::Status put_internal(const concordUtils::Sliver& _key, const concordUtils::Sliver& _value);

  concordUtils::Status object_exists_internal(const concordUtils::Sliver& key) const;

  concordUtils::Status test_bucket_internal();

  struct ResponseData {
    S3Status status = S3Status::S3StatusOK;
    std::string errorMessage;
  };

  /**
   * @brief Represents intermidiate data for get operation and is passed to
   * the lambda callback.
   *
   */
  struct GetObjectResponseData : public ResponseData {
    GetObjectResponseData(size_t linitialLength) : data(new char[linitialLength]), dataLength(linitialLength) {}

    void CheckAndResize(int& nextReadLength) {
      if (readLength + nextReadLength <= dataLength) return;

      dataLength *= 2;
      char* newData = new char[dataLength];
      memcpy(newData, data, readLength);
      delete[] data;
      data = newData;
    }

    void Write(const char* _data, int& dataSize) {
      CheckAndResize(dataSize);
      memcpy(data + readLength, _data, dataSize);
      readLength += dataSize;
    }

    ~GetObjectResponseData() { delete[] data; }

    char* data = nullptr;
    size_t dataLength = 0;
    size_t readLength = 0;
  };

  /**
   * @brief Represents intermidiate data for put operation and is passed to
   * the lambda callback.
   *
   */
  struct PutObjectResponseData : public ResponseData {
    PutObjectResponseData(const char* _data, size_t&& _dataLength) : data(_data), dataLength(_dataLength) {}

    const char* data;
    size_t dataLength;
    size_t putCount = 0;
  };

  static void responseCompleteCallback(S3Status status, const S3ErrorDetails* error, void* callbackData) {
    ResponseData* cb = nullptr;
    if (callbackData) {
      cb = static_cast<ResponseData*>(callbackData);
      cb->status = status;
    }
    if (error) {
      if (error->message) {
        if (cb) cb->errorMessage = std::string(error->message);
      }
    }
  }

  static S3Status propertiesCallback(const S3ResponseProperties* properties, void* callbackData) {
    return S3Status::S3StatusOK;
  }

  S3ResponseHandler responseHandler = {NULL, &responseCompleteCallback};
  StoreConfig config_;
  S3BucketContext context_;
  bool init_ = false;
  const uint32_t kInitialGetBufferSize_ = 25000;
  std::mutex initLock_;
  logging::Logger logger_ = logging::getLogger("concord.storage.s3");

  uint16_t initialDelay_ = 100;
  const double delayFactor_ = 1.5;

  Metrics metrics_;
};

}  // namespace concord::storage::s3
