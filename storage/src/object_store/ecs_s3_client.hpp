// Copyright 2019 VMware, all rights reserved.

#pragma once
#include <libs3.h>
#include <cassert>
#include <cstring>
#include <functional>
#include <mutex>
#include <thread>
#include <functional>
#include "Logger.hpp"
#include "object_store/object_store_client.hpp"

using namespace concord::storage;
using namespace std;

/**
 * @brief Internal implementation of ECS EMC S3 client based on libs3
 * Most of the method are irrelevant for the object store and throw exception if
 * called. We assume that only ObjectStoreAppState class will use this class
 *
 */
class EcsS3ClientImpl : public IDBClient {
 public:
  EcsS3ClientImpl(const S3_StoreConfig& config, const ObjectStoreBehavior& b) : config_{config}, bh_{b} {
    LOG_INFO(logger_, "ECS S3 client created");
  }

  ~EcsS3ClientImpl() {
    /* Destroy LibS3 */
    S3_deinitialize();
    init_ = false;
    LOG_INFO(logger_, "libs3 deinit");
  }

  /**
   * @brief used only from the test! name should be immutable string literal
   *
   * @param name
   */
  void set_bucket_name(string name) { config_.bucketName = name; }

  /**
   * @brief Initializing underlying libs3. The S3_initialize function must be
   * called exactly once and only from 1 thread
   *
   * @param readOnly
   */
  virtual void init(bool readOnly) override {
    std::lock_guard<std::mutex> g(initLock_);
    assert(!init_);  // we may return here instead of firing assert failure, but
                     // probably we want to catch bugs?
    context_.hostName = config_.url.c_str();
    context_.protocol = (config_.protocol == "HTTP" ? S3ProtocolHTTP : S3ProtocolHTTPS);
    context_.uriStyle = S3UriStylePath;
    /* In ECS terms, this is your object user */
    context_.accessKeyId = config_.accessKey.c_str();
    context_.secretAccessKey = config_.secretKey.c_str();
    /* The name of a bucket to use */
    context_.bucketName = config_.bucketName.c_str();

    S3Status st = S3_initialize(NULL, S3_INIT_ALL, NULL);
    if (S3Status::S3StatusOK != st) {
      LOG_ERROR(logger_, "libs3 init failed, status: " + to_string(st));
      throw std::runtime_error("libs3 init failed, status: " + to_string(st));
    }
    LOG_INFO(logger_, "libs3 initialized");
    init_ = true;
  }

  /**
   * @brief GEt object from the store.
   *
   * @param _key key to be retrieved
   * @param _outValue returned object
   * @return OK if success, otherwise GeneralError with underlying error status
   */
  virtual Status get(const Sliver& _key, OUT Sliver& _outValue) const override {
    using namespace std::placeholders;
    Status res = Status::OK();
    std::function<Status(const Sliver&, OUT Sliver&)> f = std::bind(&EcsS3ClientImpl::get_internal, this, _1, _2);
    do_with_retry(res, f, _key, _outValue);
    return res;
  }

  virtual Status get(const Sliver& _key, OUT char*& buf, uint32_t bufSize, OUT uint32_t& _size) const override {
    throw std::logic_error("Not implemented for ECS S3 object store");
  }

  /**
   * @brief Put object to the store.
   *
   * @param _key object's key
   * @param _value object
   * @return OK if success, otherwise GeneralError with underlying error status
   */
  virtual Status put(const Sliver& _key, const Sliver& _value) override {
    using namespace std::placeholders;
    std::function<Status(const Sliver&, const Sliver&)> f = std::bind(&EcsS3ClientImpl::put_internal, this, _1, _2);
    Status res = Status::OK();
    do_with_retry(res, f, _key, _value);
    return res;
  }

  Status test_bucket() {
    using namespace std::placeholders;
    std::function<Status()> f = std::bind(&EcsS3ClientImpl::test_bucket_internal, this);
    Status res = Status::OK();

    do_with_retry(res, f);
    return res;
  }

  Status object_exists(const Sliver& key) {
    using namespace std::placeholders;
    std::function<Status(const Sliver&)> f = std::bind(&EcsS3ClientImpl::object_exists_internal, this, _1);
    Status res = Status::OK();
    do_with_retry(res, f, key);
    return res;
  }

  void wait_for_storage() {
    Status res = Status::OK();
    do {
      res = test_bucket();
      std::this_thread::sleep_for(std::chrono::seconds(5));
    } while (!res.isOK());
  }

  virtual Status del(const Sliver& key) override {
    if (!bh_.can_delete()) throw std::logic_error("Not implemented for ECS S3 object store");
    assert(init_);
    if (should_net_fail()) return Status::GeneralError("Network failure simulated");

    ResponseData rData;
    S3ResponseHandler rHandler;
    rHandler.completeCallback = responseCompleteCallback;
    rHandler.propertiesCallback = propertiesCallback;
    S3_delete_object(&context_, string(key.data()).c_str(), NULL, &rHandler, &rData);
    LOG_DEBUG(logger_,
              "del key: " << string(key.data()) << ", status: " << rData.status << ", msg: " << rData.errorMessage);
    if (rData.status == S3Status::S3StatusOK)
      return Status::OK();
    else {
      LOG_ERROR(logger_,
                "del key: " << string(key.data()) << ", status: " << rData.status << ", msg: " << rData.errorMessage);
      if (rData.status == S3Status::S3StatusHttpErrorNotFound || rData.status == S3Status::S3StatusErrorNoSuchBucket ||
          rData.status == S3Status::S3StatusErrorNoSuchKey)
        return Status::NotFound("Status: " + to_string(rData.status) + "msg: " + rData.errorMessage);

      return Status::GeneralError("Status: " + to_string(rData.status) + "msg: " + rData.errorMessage);
    }
  }

  virtual Status multiGet(const KeysVector& _keysVec, OUT ValuesVector& _valuesVec) override {
    throw std::logic_error("Not implemented for ECS S3 object store");
  }

  virtual Status multiPut(const SetOfKeyValuePairs& _keyValueMap) override {
    throw std::logic_error("Not implemented for ECS S3 object store");
  }

  virtual Status multiDel(const KeysVector& _keysVec) override {
    throw std::logic_error("Not implemented for ECS S3 object store");
  }

  virtual void monitor() const override { throw std::logic_error("Not implemented for ECS S3 object store"); }

  virtual bool isNew() override { throw std::logic_error("Not implemented for ECS S3 object store"); }

  virtual IDBClient::IDBClientIterator* getIterator() const override {
    throw std::logic_error("Not implemented for ECS S3 object store");
  }

  virtual Status freeIterator(IDBClientIterator* _iter) const override {
    throw std::logic_error("Not implemented for ECS S3 object store");
  }

  virtual ITransaction* beginTransaction() override {
    throw std::logic_error("Not implemented for ECS S3 object store");
  }

  ///////////////////////// private /////////////////////////////
 private:
  template <typename F, typename... Args>
  void do_with_retry(Status& r, F&& f, Args&&... args) const {
    uint16_t delay = initialDelay_;
    auto start = std::chrono::steady_clock::now();
    auto now = std::chrono::steady_clock::now();
    do {
      r = std::forward<F>(f)(std::forward<Args>(args)...);
      LOG_DEBUG(logger_, "do_with_retry, delay: " << delay);
      if (!r.isGeneralError()) break;
      if (delay < config_.maxWaitTime) delay *= delayFactor_;
      std::this_thread::sleep_for(std::chrono::milliseconds(delay));
      now = std::chrono::steady_clock::now();
    } while (std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count() < (int32_t)config_.maxWaitTime);
  }

  Status get_internal(const Sliver& _key, OUT Sliver& _outValue) const {
    assert(init_);
    if (should_net_fail()) return Status::GeneralError("Network failure simulated");
    GetObjectResponseData cbData(kInitialGetBufferSize_);
    S3GetObjectHandler getObjectHandler;
    getObjectHandler.responseHandler = responseHandler;

    // libs3 uses multiple calls to this callaback to append chunks of data from
    // the stream
    auto f = [](int buf_len, const char* buf, void* cb) -> S3Status {
      GetObjectResponseData* cbData = static_cast<GetObjectResponseData*>(cb);
      assert(cbData->data);
      cbData->Write(buf, buf_len);
      return S3Status::S3StatusOK;
    };
    getObjectHandler.getObjectDataCallback = f;
    S3_get_object(&context_, _key.data(), NULL, 0, 0, NULL, &getObjectHandler, &cbData);
    if (cbData.status == S3Status::S3StatusOK) {
      _outValue = Sliver::copy(reinterpret_cast<const char*>(cbData.data), cbData.readLength);
      return Status::OK();
    } else {
      LOG_ERROR(logger_, "get status: " << cbData.status);
      if (cbData.status == S3Status::S3StatusHttpErrorNotFound ||
          cbData.status == S3Status::S3StatusErrorNoSuchBucket || cbData.status == S3Status::S3StatusErrorNoSuchKey)
        return Status::NotFound("Status: " + to_string(cbData.status) + "msg: " + cbData.errorMessage);

      return Status::GeneralError("Status: " + to_string(cbData.status) + "msg: " + cbData.errorMessage);
    }
  }

  Status put_internal(const Sliver& _key, const Sliver& _value) {
    assert(init_);
    if (this->should_net_fail()) return Status::GeneralError("Network failure simulated");
    PutObjectResponseData cbData(_value.data(), _value.length());
    S3PutObjectHandler putObjectHandler;
    putObjectHandler.responseHandler = responseHandler;

    // libs3 uses multiple calls to this callaback to append chunks of data to
    // the stream
    auto f = [](int buf_len, char* buf, void* cb) {
      PutObjectResponseData* cbData = static_cast<PutObjectResponseData*>(cb);
      int toSend = 0;
      if (cbData->dataLength) {
        toSend = std::min(cbData->dataLength, (size_t)buf_len);
        std::memcpy(buf, cbData->data + cbData->putCount, toSend);
      }
      cbData->putCount += toSend;
      cbData->dataLength -= toSend;
      return toSend;
    };
    putObjectHandler.putObjectDataCallback = f;
    string s = string(_key.data());
    S3_put_object(&context_, string(_key.data()).c_str(), _value.length(), NULL, NULL, &putObjectHandler, &cbData);
    if (cbData.status == S3Status::S3StatusOK)
      return Status::OK();
    else {
      LOG_ERROR(logger_, "put status: " << cbData.status);
      if (cbData.status == S3Status::S3StatusHttpErrorNotFound || cbData.status == S3Status::S3StatusErrorNoSuchBucket)
        return Status::NotFound("Status: " + to_string(cbData.status) + "msg: " + cbData.errorMessage);

      return Status::GeneralError("Status: " + to_string(cbData.status) + "msg: " + cbData.errorMessage);
    }
  }

  Status object_exists_internal(const Sliver& key) {
    assert(init_);
    if (should_net_fail()) return Status::GeneralError("Network failure simulated");
    ResponseData rData;
    S3ResponseHandler rHandler;
    rHandler.completeCallback = responseCompleteCallback;
    rHandler.propertiesCallback = propertiesCallback;

    S3_head_object(&context_, string(key.data()).c_str(), NULL, &rHandler, &rData);
    LOG_DEBUG(
        logger_,
        "object_exist key: " << string(key.data()) << ", status: " << rData.status << ", msg: " << rData.errorMessage);
    if (rData.status == S3Status::S3StatusOK)
      return Status::OK();
    else {
      LOG_ERROR(logger_,
                "object_exist key: " << string(key.data()) << ", status: " << rData.status
                                     << ", msg: " << rData.errorMessage);
      if (rData.status == S3Status::S3StatusHttpErrorNotFound || rData.status == S3Status::S3StatusErrorNoSuchBucket ||
          rData.status == S3Status::S3StatusErrorNoSuchKey)
        return Status::NotFound("Status: " + to_string(rData.status) + "msg: " + rData.errorMessage);

      return Status::GeneralError("Status: " + to_string(rData.status) + "msg: " + rData.errorMessage);
    }
  }

  Status test_bucket_internal() {
    assert(init_);
    if (should_net_fail()) return Status::GeneralError("Network failure simulated");
    ResponseData rData;
    S3ResponseHandler rHandler;
    rHandler.completeCallback = responseCompleteCallback;
    rHandler.propertiesCallback = propertiesCallback;
    char location[100];

    S3_test_bucket(context_.protocol,
                   context_.uriStyle,
                   context_.accessKeyId,
                   context_.secretAccessKey,
                   context_.hostName,
                   context_.bucketName,
                   100,
                   location,
                   NULL,
                   &rHandler,
                   &rData);
    LOG_DEBUG(logger_,
              "test_bucket bucket: " << context_.bucketName << ", status: " << rData.status
                                     << ", msg: " << rData.errorMessage);
    if (rData.status == S3Status::S3StatusOK)
      return Status::OK();
    else {
      LOG_ERROR(logger_,
                "test_bucket bucket: " << context_.bucketName << ", status: " << rData.status
                                       << ", msg: " << rData.errorMessage);
      if (rData.status == S3Status::S3StatusHttpErrorNotFound || rData.status == S3Status::S3StatusErrorNoSuchBucket)
        return Status::NotFound("Status: " + to_string(rData.status) + "msg: " + rData.errorMessage);

      return Status::GeneralError("Status: " + to_string(rData.status) + "msg: " + rData.errorMessage);
    }
  }

  struct ResponseData {
    S3Status status = S3Status::S3StatusOK;
    string errorMessage;
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
        if (cb) cb->errorMessage = string(error->message);
      }
    }
  }

  static S3Status propertiesCallback(const S3ResponseProperties* properties, void* callbackData) {
    return S3Status::S3StatusOK;
  }

  bool should_net_fail() const {
    bool b1 = bh_.can_simulate_net_failure();
    if (!b1) return false;
    bool b3 = bh_.get_do_net_failure();
    if (!b3) return false;
    bool b2 = bh_.has_finished();
    if (b2) return false;
    return true;
  }

  S3ResponseHandler responseHandler = {NULL, &responseCompleteCallback};
  S3_StoreConfig config_;
  S3BucketContext context_;
  bool init_ = false;
  const uint32_t kInitialGetBufferSize_ = 25000;
  std::mutex initLock_;
  concordlogger::Logger logger_ = concordlogger::Log::getLogger("concord::storage::EcsS3ClientImpl");
  ObjectStoreBehavior bh_;
  uint16_t initialDelay_ = 100;
  const double delayFactor_ = 1.5;
};
