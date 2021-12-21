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

/**
 * @brief Initializing underlying libs3. The S3_initialize function must be
 * called exactly once and only from 1 thread
 *
 * @param readOnly
 */
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

  S3Status st = S3_initialize(NULL, S3_INIT_ALL, NULL);
  if (S3Status::S3StatusOK != st) {
    LOG_FATAL(logger_, "libs3 init failed, status: " + to_string(st));
    ConcordAssert("libs3 init failed" && false);
  }
  LOG_INFO(logger_, "libs3 initialized");
  init_ = true;
}

Status Client::del(const Sliver& key) {
  ConcordAssert(init_);
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

Status Client::get_internal(const Sliver& _key, OUT Sliver& _outValue) const {
  ConcordAssert(init_);
  LOG_DEBUG(logger_, "key: " << _key.toString());
  GetObjectResponseData cbData(kInitialGetBufferSize_);
  S3GetObjectHandler getObjectHandler;
  getObjectHandler.responseHandler = responseHandler;

  // libs3 uses multiple calls to this callaback to append chunks of data from
  // the stream
  auto f = [](int buf_len, const char* buf, void* cb) -> S3Status {
    GetObjectResponseData* cbData = static_cast<GetObjectResponseData*>(cb);
    ConcordAssert(cbData->data != nullptr);
    cbData->Write(buf, buf_len);
    return S3Status::S3StatusOK;
  };
  getObjectHandler.getObjectDataCallback = f;
  S3_get_object(&context_, _key.data(), NULL, 0, 0, NULL, &getObjectHandler, &cbData);
  if (cbData.status == S3Status::S3StatusOK) {
    _outValue = Sliver::copy(reinterpret_cast<const char*>(cbData.data), cbData.readLength);
    return Status::OK();
  } else {
    if (cbData.status == S3Status::S3StatusHttpErrorNotFound || cbData.status == S3Status::S3StatusErrorNoSuchBucket ||
        cbData.status == S3Status::S3StatusErrorNoSuchKey)
      return Status::NotFound("Status: " + std::string(S3_get_status_name(cbData.status)) +
                              "msg: " + cbData.errorMessage);

    return Status::GeneralError("Status: " + std::string(S3_get_status_name(cbData.status)) +
                                "msg: " + cbData.errorMessage);
  }
}

Status Client::put_internal(const Sliver& _key, const Sliver& _value) {
  ConcordAssert(init_);
  LOG_DEBUG(logger_, "key: " << _key.toString());
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
  if (cbData.status == S3Status::S3StatusOK) {
    metrics_.num_keys_transferred++;
    metrics_.bytes_transferred += (_key.length() + _value.length());
    // metrics_.updateLastSavedBlockId(_key);
    metrics_.metrics_component.UpdateAggregator();
    return Status::OK();
  } else {
    LOG_ERROR(logger_,
              "key: " << _key.toString() << " status: " << S3_get_status_name(cbData.status) << " ("
                      << cbData.errorMessage << ")");
    if (cbData.status == S3Status::S3StatusHttpErrorNotFound || cbData.status == S3Status::S3StatusErrorNoSuchBucket)
      return Status::NotFound("Status: " + to_string(cbData.status) + "msg: " + cbData.errorMessage);

    return Status::GeneralError("Status: " + to_string(cbData.status) + "msg: " + cbData.errorMessage);
  }
}

Status Client::object_exists_internal(const Sliver& key) const {
  ConcordAssert(init_);
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
    LOG_DEBUG(
        logger_,
        "object_exist key: " << string(key.data()) << ", status: " << rData.status << ", msg: " << rData.errorMessage);
    if (rData.status == S3Status::S3StatusHttpErrorNotFound || rData.status == S3Status::S3StatusErrorNoSuchBucket ||
        rData.status == S3Status::S3StatusErrorNoSuchKey)
      return Status::NotFound("Status: " + to_string(rData.status) + "msg: " + rData.errorMessage);

    return Status::GeneralError("Status: " + to_string(rData.status) + "msg: " + rData.errorMessage);
  }
}

Status Client::test_bucket_internal() {
  ConcordAssert(init_);
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
  LOG_DEBUG(
      logger_,
      "test_bucket bucket: " << context_.bucketName << ", status: " << rData.status << ", msg: " << rData.errorMessage);
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

}  // namespace concord::storage::s3
