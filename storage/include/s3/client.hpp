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
#include <functional>
#include "Logger.hpp"
#include "kvstream.h"
#include "assertUtils.hpp"
#include "storage/db_interface.h"
#include "s3_metrics.hpp"
#include "thread_pool.hpp"

#pragma once

namespace concord::storage::s3 {

using namespace std::placeholders;

struct StoreConfig {
  std::string bucketName;          // assuming pre-configured, no need to create
  std::string url;                 // S3UriStylePath Path: ${protocol}://s3.amazonaws.com/${bucket}/[${key}]
  std::string protocol;            // HTTP or HTTPS
  std::string accessKey;           // user name
  std::string secretKey;           // password
  std::string pathPrefix;          // optional path prefix used in the bucket
  std::uint32_t operationTimeout;  // max timeout for an operation in milliseconds

  std::string toURL() const {
    std::ostringstream oss;
    oss << protocol << "://" << url << "/" << bucketName;
    return oss.str();
  }
  friend std::ostream& operator<<(std::ostream&, const StoreConfig&);
};

/** @brief IDBClient implementation for S3 compatible object store using libs3. */
class Client : public concord::storage::IDBClient {
 protected:
  /** Base class for response callback data */
  struct ResponseData {
    S3Status status = S3Status::S3StatusOK;
    std::string errorMessage;
  };

  friend void responseCompleteCallback(S3Status status, const S3ErrorDetails* error, void* callbackData);

 public:
  /** Transaction class. Is used also internally to implement multiPut()*/
  class Transaction : public ITransaction {
   public:
    Transaction(Client* client) : ITransaction(nextId()), client_{client->shared_from_this()} {}
    void commit() override;
    void rollback() override { multiput_.clear(); }
    void put(const concordUtils::Sliver& key, const concordUtils::Sliver& value) override {
      multiput_[key.clone()] = value.clone();
    }
    std::string get(const concordUtils::Sliver& key) override { return multiput_[key].toString(); }
    void del(const concordUtils::Sliver& key) override {
      multiput_.erase(key);
      keys_to_delete_.insert(key);
    }

   protected:
    std::shared_ptr<IDBClient> client_;
    SetOfKeyValuePairs multiput_;
    std::set<concordUtils::Sliver> keys_to_delete_;
    ID nextId() {
      static ID id_ = 0;
      return ++id_;
    }
  };
  /** S3 iterator.
   *  Iterates over the object headers only. Once an object is found,
   *  a get operation fir a specific key should be performed.
   *  Iterator access functions returning KeyValuePair return key-eTag instead of key-value,
   *  where eTag is a md5 of a value.
   */
  class Iterator : public concord::storage::IDBClient::IDBClientIterator {
    friend class Client;

   public:
    Iterator(const Client* client) : client_{client} {}
    ~Iterator() { LOG_INFO(GL, ""); }

    KeyValuePair first() override { return toKvPair(cb_data_.results.begin()); }
    KeyValuePair last() override {
      while (!isEnd()) seek(prefix_);
      return toKvPair(--(cb_data_.results.end()));
    }
    KeyValuePair seekAtLeast(const concordUtils::Sliver& searchKey) override {
      prefix_ = searchKey.clone();
      return seek(prefix_);
    }
    KeyValuePair seekAtMost(const concordUtils::Sliver& searchKey) override { return KeyValuePair(); }
    KeyValuePair previous() override { return toKvPair(--iterator_); }
    KeyValuePair next() override {
      ++iterator_;
      if (iterator_ != cb_data_.results.end()) return toKvPair(iterator_);
      if (!cb_data_.isTruncated) return toKvPair(iterator_);
      // more left in S3
      return seek(prefix_);
    }
    KeyValuePair getCurrent() override { return toKvPair(iterator_); }
    bool isEnd() override { return (iterator_ == cb_data_.results.end()) && !cb_data_.isTruncated; }
    concordUtils::Status getStatus() override { return Status::OK(); }

   protected:
    struct Result {
      Result(const S3ListBucketContent& c)
          : key(c.key),
            lastModified(c.lastModified),
            eTag(c.eTag),
            size(c.size),
            ownerId(c.ownerId),
            ownerDisplayName(c.ownerDisplayName) {}
      ~Result() {}
      std::string key;
      std::int64_t lastModified;
      std::string eTag;
      std::uint64_t size;
      std::string ownerId;
      std::string ownerDisplayName;
    };
    KeyValuePair seek(const concordUtils::Sliver& prefix);
    KeyValuePair toKvPair(const std::vector<Result>::const_iterator& it) {
      if (it == cb_data_.results.end()) return KeyValuePair();
      std::string lm = std::to_string(it->lastModified);
      return KeyValuePair(Sliver::copy(it->key.data(), it->key.length()), Sliver::copy(lm.data(), lm.length()));
    }
    static int sort_by_modified_desc(const Result& a, const Result& b) { return a.lastModified > b.lastModified; }
    static int sort_by_modified_asc(const Result& a, const Result& b) { return a.lastModified < b.lastModified; };
    static int sort_by_key_desc(const Result& a, const Result& b) { return a.key > b.key; };
    static int sort_by_key_asc(const Result& a, const Result& b) { return a.key < b.key; };

   public:
    struct ListBucketCallbackData : public ResponseData {
      int isTruncated = 0;
      std::string nextMarker;
      int keyCount = 0;
      int allDetails = 0;
      int maxKeys = 1000;
      std::vector<Result> results;
    };

   protected:
    std::vector<Result>::const_iterator iterator_;
    const Client* client_;
    ListBucketCallbackData cb_data_;
    Sliver prefix_;  // for subsequent seeks;
    logging::Logger logger_ = logging::getLogger("concord.storage.s3");
  };

  template <int (*F)(const Iterator::Result&, const Iterator::Result&)>
  class SortableIterator : public concord::storage::s3::Client::Iterator {
   public:
    SortableIterator(const Client* client) : Iterator(client) {}
    concord::storage::KeyValuePair first() override {
      Iterator::last();
      sort();
      return toKvPair(cb_data_.results.begin());
    }
    concord::storage::KeyValuePair last() override {
      Iterator::last();
      sort();
      return toKvPair(--(cb_data_.results.end()));
    }
    KeyValuePair seekAtLeast(const concordUtils::Sliver& searchKey) override {
      Iterator::seekAtLeast(searchKey);
      if (!isEnd()) {
        Iterator::last();
        sort();
        iterator_ = cb_data_.results.begin();
      }
      return getCurrent();
    }

   protected:
    void sort() {
      if (sorted_) return;
      std::sort(cb_data_.results.begin(), cb_data_.results.end(), F);
      sorted_ = true;
    }

   protected:
    bool sorted_ = false;
  };

  typedef SortableIterator<Iterator::sort_by_modified_desc> SortByModifiedDescIterator;
  typedef SortableIterator<Iterator::sort_by_modified_asc> SortByModifiedAscIterator;
  typedef SortableIterator<Iterator::sort_by_key_desc> SortByKeyDescIterator;
  typedef SortableIterator<Iterator::sort_by_key_asc> SortByKeyAscIterator;

  template <typename T>
  T* getIterator() {
    return new T(this);
  }

  Client(const StoreConfig& config) : config_{config} { LOG_INFO(logger_, "S3 client created"); }

  ~Client() {
    /* Destroy LibS3 */
    S3_deinitialize();
    init_ = false;
    LOG_INFO(logger_, "libs3 deinit");
  }

  void init(bool readOnly) override;

  concordUtils::Status get(const concordUtils::Sliver& key, concordUtils::Sliver& outValue) const override {
    LOG_DEBUG(logger_, key.toString());
    return do_with_retry("get_internal", std::bind(&Client::get_internal, this, _1, _2), key, outValue);
  }

  concordUtils::Status get(const concordUtils::Sliver& _key,
                           char*& buf,
                           uint32_t bufSize,
                           uint32_t& _size) const override {
    concordUtils::Sliver res;
    if (Status s = get(_key, res); !s.isOK()) return s;
    size_t len = std::min(res.length(), (size_t)bufSize);
    memcpy(buf, res.data(), len);
    _size = len;
    return concordUtils::Status::OK();
  }

  concordUtils::Status put(const concordUtils::Sliver& key, const concordUtils::Sliver& value) override {
    LOG_DEBUG(logger_, key.toString());
    return do_with_retry("put_internal", std::bind(&Client::put_internal, this, _1, _2), key, value);
  }

  concordUtils::Status create_bucket() {
    LOG_DEBUG(logger_, config_.bucketName);
    return do_with_retry("create_bucket_internal", std::bind(&Client::create_bucket_internal, this));
  }
  concordUtils::Status test_bucket() {
    LOG_DEBUG(logger_, config_.bucketName);
    return do_with_retry("test_bucket_internal", std::bind(&Client::test_bucket_internal, this));
  }

  concordUtils::Status has(const concordUtils::Sliver& key) const override {
    using namespace std::placeholders;
    std::function<ResponseData(const concordUtils::Sliver&)> f = std::bind(&Client::object_exists_internal, this, _1);
    return do_with_retry("object_exists_internal", f, key);
  }

  concordUtils::Status del(const concordUtils::Sliver& key) override {
    LOG_DEBUG(logger_, key.toString());
    return do_with_retry("delete_internal", std::bind(&Client::delete_internal, this, _1), key);
  }

  concordUtils::Status multiGet(const KeysVector& _keysVec, ValuesVector& _valuesVec) override {
    ConcordAssert(_keysVec.size() == _valuesVec.size());
    for (KeysVector::size_type i = 0; i < _keysVec.size(); ++i)
      if (Status s = get(_keysVec[i], _valuesVec[i]); !s.isOK()) return s;

    return concordUtils::Status::OK();
  }

  concordUtils::Status multiPut(const SetOfKeyValuePairs& _keyValueMap, bool sync = false) override {
    ITransaction::Guard g(beginTransaction());
    for (auto&& pair : _keyValueMap) g.txn()->put(pair.first, pair.second);
    return concordUtils::Status::OK();
  }

  concordUtils::Status multiDel(const KeysVector& _keysVec) override {
    ITransaction::Guard g(beginTransaction());
    for (const auto& key : _keysVec) g.txn()->del(key);
    return concordUtils::Status::OK();
  }

  bool isNew() override { throw std::logic_error("isNew()  Not implemented for S3 object store"); }

  IDBClient::IDBClientIterator* getIterator() const override { return new Iterator(this); }

  concordUtils::Status freeIterator(IDBClientIterator* _iter) const override {
    delete _iter;
    return concordUtils::Status::OK();
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
  /**
   * If status is not NotFound, retry until operation timeout reached, increasing the waiting interval between attempts.
   * TODO [TK] finer error check granularity.
   */
  template <typename F, typename... Args>
  Status do_with_retry(const std::string_view msg, F&& f, Args&&... args) const {
    uint16_t delay = initialDelay_;
    ResponseData rd{S3Status::S3StatusErrorUnknown, ""};

    for (uint16_t retries = 0;
         rd.status != S3Status::S3StatusOK && !S3_status_is_not_found(rd.status) && delay < config_.operationTimeout;
         ++retries) {
      if (retries > 0) {
        delay += retries * initialDelay_;
        LOG_ERROR(logger_,
                  msg << " status: " << S3_get_status_name(rd.status) << "(" << rd.status
                      << ") error: " << rd.errorMessage << ", retrying after " << delay << " ms.");
        std::this_thread::sleep_for(std::chrono::milliseconds(delay));
      }
      rd = std::forward<F>(f)(std::forward<Args>(args)...);
    }

    if (rd.status == S3Status::S3StatusOK) {
      LOG_DEBUG(logger_, msg << " status: " << rd.status);
      return Status::OK();
    }
    if (S3_status_is_not_found(rd.status)) {
      LOG_DEBUG(logger_, msg << "not found, status: " << rd.status << " error: " << rd.errorMessage);
      return Status::NotFound("Status: " + rd.errorMessage);
    }
    LOG_ERROR(logger_, msg << " status: " << rd.status << " error: " << rd.errorMessage);
    return Status::GeneralError("Status: " + rd.errorMessage);
  }

  bool S3_status_is_not_found(S3Status s) const {
    return s == S3Status::S3StatusHttpErrorNotFound || s == S3Status::S3StatusErrorNoSuchBucket ||
           s == S3Status::S3StatusErrorNoSuchKey;
  }

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
  struct PutObjectResponseData : public ResponseData {
    PutObjectResponseData(const char* _data, size_t&& _dataLength) : data(_data), dataLength(_dataLength) {}

    const char* data;
    size_t dataLength;
    size_t putCount = 0;
  };

  GetObjectResponseData get_internal(const concordUtils::Sliver& _key, concordUtils::Sliver& _outValue) const;
  PutObjectResponseData put_internal(const concordUtils::Sliver& _key, const concordUtils::Sliver& _value);
  ResponseData object_exists_internal(const concordUtils::Sliver& key) const;
  ResponseData delete_internal(const concordUtils::Sliver& key);
  ResponseData create_bucket_internal();
  ResponseData test_bucket_internal();

  StoreConfig config_;
  S3BucketContext context_;
  bool init_ = false;
  const uint32_t kInitialGetBufferSize_ = 25000;
  std::mutex initLock_;
  logging::Logger logger_ = logging::getLogger("concord.storage.s3");
  uint16_t initialDelay_ = 100;
  Metrics metrics_;
  util::ThreadPool thread_pool_{std::thread::hardware_concurrency()};
};

}  // namespace concord::storage::s3
