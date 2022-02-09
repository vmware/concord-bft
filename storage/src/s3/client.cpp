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

#include "s3/client.hpp"
#include <cstring>
#include <functional>
#include <mutex>
#include <thread>
#include <functional>
#include "Logger.hpp"
#include "assertUtils.hpp"
#include "storage/db_interface.h"

namespace concord::storage::s3 {

using namespace std;
using namespace concordUtils;

static S3Status propertiesCallback(const S3ResponseProperties* properties, void* callbackData) {
  return S3Status::S3StatusOK;
}
// This callback does the same thing for every request type: saves the status
// and error stuff in global variables
void responseCompleteCallback(S3Status status, const S3ErrorDetails* error, void* callbackData) {
  Client::ResponseData* data = (Client::ResponseData*)callbackData;
  data->status = status;
  if (data->status == S3StatusOK || !error) return;

  std::ostringstream oss;
  if (error->message) oss << " Message: " << error->message;
  if (error->resource) oss << " Resource: " << error->resource;
  if (error->furtherDetails) oss << " Further Details: " << error->furtherDetails;
  if (error->extraDetailsCount) {
    oss << "  Extra Details: ";
    for (int i = 0; i < error->extraDetailsCount; i++)
      oss << error->extraDetails[i].name << ": " << error->extraDetails[i].value << " ";
  }
  data->errorMessage = oss.str();
}

void Client::init(bool readOnly) {
  std::lock_guard<std::mutex> g(initLock_);
  ConcordAssert(!init_);  // we may return here instead of firing assert failure, but
                          // probably we want to catch bugs?

  // protocol is provided by the user and can be in any case
  std::string s3_protocol;
  for (auto& c : config_.protocol) s3_protocol.push_back(tolower(c));

  context_.hostName = config_.url.c_str();
  if (s3_protocol == "http") {
    context_.protocol = S3ProtocolHTTP;
  } else if (s3_protocol == "https") {
    context_.protocol = S3ProtocolHTTPS;
  } else {
    LOG_FATAL(logger_, "Invalid protocol: " + s3_protocol + ". Supported values are http or https (case insensitive).");
    ConcordAssert("Invalid S3 protocol" && false);
  }

  context_.uriStyle = S3UriStylePath;
  /* In ECS terms, this is your object user */
  context_.accessKeyId = config_.accessKey.c_str();
  context_.secretAccessKey = config_.secretKey.c_str();
  /* The name of a bucket to use */
  context_.bucketName = config_.bucketName.c_str();

  S3Status st = S3_initialize(nullptr, S3_INIT_ALL, nullptr);
  if (S3Status::S3StatusOK != st) {
    LOG_FATAL(logger_, "libs3 init failed, status: " + to_string(st));
    ConcordAssert("libs3 init failed" && false);
  }
  LOG_INFO(logger_, "libs3 initialized");
  init_ = true;
  auto res = test_bucket();
  if (res.isOK()) return;
  if (!res.isNotFound()) throw std::runtime_error("s3::Client failed to test bucket: " + res.toString());

  res = create_bucket();
  if (!res.isOK()) throw std::runtime_error("s3::Client failed to create bucket: " + res.toString());
  LOG_INFO(logger_, "initialization complete");
}

Client::ResponseData Client::delete_internal(const Sliver& key) {
  ConcordAssert(init_);
  ResponseData rData;
  S3ResponseHandler rHandler{propertiesCallback, responseCompleteCallback};
  LOG_DEBUG(logger_, "calling S3_delete_object, key: " << key.toString());
  S3_delete_object(&context_, key.toString().c_str(), nullptr, &rHandler, &rData);

  return rData;
}

Client::GetObjectResponseData Client::get_internal(const Sliver& key, Sliver& outValue) const {
  ConcordAssert(init_);
  LOG_DEBUG(logger_, "key: " << key.toString());
  GetObjectResponseData cbData(kInitialGetBufferSize_);
  S3GetObjectHandler getObjectHandler;
  getObjectHandler.responseHandler = {nullptr, &responseCompleteCallback};

  // libs3 uses multiple calls to this callaback to append chunks of data from the stream
  auto f = [](int buf_len, const char* buf, void* cb) -> S3Status {
    GetObjectResponseData* cbData = static_cast<GetObjectResponseData*>(cb);
    ConcordAssert(cbData->data != nullptr);
    cbData->Write(buf, buf_len);
    return S3Status::S3StatusOK;
  };
  getObjectHandler.getObjectDataCallback = f;
  LOG_DEBUG(logger_, "calling S3_get_object, key: " << key.toString());
  S3_get_object(&context_, key.toString().c_str(), nullptr, 0, 0, nullptr, &getObjectHandler, &cbData);
  if (cbData.status == S3Status::S3StatusOK)
    outValue = Sliver::copy(reinterpret_cast<const char*>(cbData.data), cbData.readLength);

  return cbData;
}

Client::PutObjectResponseData Client::put_internal(const Sliver& key, const Sliver& value) {
  ConcordAssert(init_);
  PutObjectResponseData cbData(value.data(), value.length());
  S3PutObjectHandler putObjectHandler;
  putObjectHandler.responseHandler = {nullptr, &responseCompleteCallback};

  // libs3 uses multiple calls to this callaback to append chunks of data to the stream
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
  LOG_DEBUG(logger_, "calling S3_put_object, key: " << key.toString());
  S3_put_object(&context_, key.toString().c_str(), value.length(), nullptr, nullptr, &putObjectHandler, &cbData);
  if (cbData.status == S3Status::S3StatusOK) {
    metrics_.num_keys_transferred++;
    metrics_.bytes_transferred += (key.length() + value.length());
    metrics_.metrics_component.UpdateAggregator();
  }
  return cbData;
}

Client::ResponseData Client::object_exists_internal(const Sliver& key) const {
  ConcordAssert(init_);
  ResponseData rData;
  S3ResponseHandler rHandler{propertiesCallback, responseCompleteCallback};
  LOG_DEBUG(logger_, "calling S3_head_object, key: " << key.toString());
  S3_head_object(&context_, key.toString().c_str(), nullptr, &rHandler, &rData);
  return rData;
}

Client::ResponseData Client::test_bucket_internal() {
  ConcordAssert(init_);
  ResponseData rData;
  S3ResponseHandler rHandler{propertiesCallback, responseCompleteCallback};
  char location[100];
  LOG_DEBUG(logger_, "calling S3_test_bucket for : " << config_.toURL());
  S3_test_bucket(context_.protocol,
                 context_.uriStyle,
                 context_.accessKeyId,
                 context_.secretAccessKey,
                 context_.hostName,
                 context_.bucketName,
                 100,
                 location,
                 nullptr,
                 &rHandler,
                 &rData);
  return rData;
}

Client::ResponseData Client::create_bucket_internal() {
  ConcordAssert(init_);
  ResponseData rData;
  S3ResponseHandler rHandler{propertiesCallback, responseCompleteCallback};
  LOG_DEBUG(logger_, "calling S3_test_bucket for : " << config_.toURL());
  S3_create_bucket(context_.protocol,
                   context_.accessKeyId,
                   context_.secretAccessKey,
                   context_.hostName,
                   context_.bucketName,
                   S3CannedAclPrivate,
                   nullptr,
                   nullptr,
                   &rHandler,
                   &rData);
  return rData;
}

std::ostream& operator<<(std::ostream& os, const concord::storage::s3::StoreConfig& c) {
  os << KVLOG(c.url, c.bucketName, c.pathPrefix, c.protocol, c.operationTimeout);
  return os;
}

S3Status listBucketCallback(int isTruncated,
                            const char* nextMarker,
                            int contentsCount,
                            const S3ListBucketContent* contents,
                            int commonPrefixesCount,
                            const char** commonPrefixes,
                            void* callbackData) {
  Client::Iterator::ListBucketCallbackData* data = (Client::Iterator::ListBucketCallbackData*)callbackData;

  data->isTruncated = isTruncated;
  // This is tricky.  S3 doesn't return the NextMarker if there is no
  // delimiter.  Why, I don't know, since it's still useful for paging
  // through results.  We want NextMarker to be the last content in the
  // list, so set it to that if necessary.
  if ((!nextMarker || !nextMarker[0]) && contentsCount) nextMarker = contents[contentsCount - 1].key;

  if (nextMarker) data->nextMarker = nextMarker;

  data->results.reserve(contentsCount);
  for (int i = 0; i < contentsCount; i++) data->results.emplace_back(contents[i]);
  data->keyCount += contentsCount;
  return S3StatusOK;
}

KeyValuePair Client::Iterator::seek(const Sliver& prefix) {
  LOG_DEBUG(logger_, "prefix: " << prefix.toString() << " nextMarker: " << cb_data_.nextMarker);
  S3ListBucketHandler listBucketHandler = {{propertiesCallback, responseCompleteCallback}, listBucketCallback};
  int prevKeyCount = cb_data_.keyCount;
  do {
    do {
      S3_list_bucket(&client_->context_,
                     prefix.toString().c_str(),
                     cb_data_.nextMarker.length() ? cb_data_.nextMarker.c_str() : prefix.toString().c_str(),
                     0,
                     cb_data_.maxKeys,
                     0,
                     &listBucketHandler,
                     &cb_data_);
    } while (S3_status_is_retryable(cb_data_.status));
    if (cb_data_.status != S3StatusOK) break;
  } while (cb_data_.isTruncated && (!cb_data_.maxKeys || (cb_data_.keyCount < cb_data_.maxKeys)));

  if (cb_data_.status != S3StatusOK)
    LOG_ERROR(logger_, "ERROR: " << S3_get_status_name(cb_data_.status) << cb_data_.errorMessage);
  iterator_ = cb_data_.results.begin() + prevKeyCount;
  return getCurrent();
}
void Client::Transaction::commit() {
  static logging::Logger logger_ = logging::getLogger("concord.storage.s3");
  std::vector<std::future<concordUtils::Status>> futures;
  for (auto& pair : multiput_)
    futures.emplace_back(
        client_->put_thread_pool_.async([this, pair] { return client_->put(pair.first, pair.second); }));
  for (auto& key : keys_to_delete_)
    futures.emplace_back(client_->put_thread_pool_.async([this, key] { return client_->del(key); }));
  for (auto& f : futures) {
    concordUtils::Status s = f.get();
    LOG_TRACE(logger_, "status: " << s.toString());
    if (s != concordUtils::Status::OK()) {
      throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(" txn id[") + getIdStr() +
                               std::string("], failed, status: ") + s.toString());
    }
  }
  LOG_DEBUG(logger_, "txn id[" + getIdStr() + std::string("]"));
}

}  // namespace concord::storage::s3
