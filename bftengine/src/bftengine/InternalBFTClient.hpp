// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "IncomingMsgsStorage.hpp"
#include "PrimitiveTypes.hpp"
#include "MsgsCommunicator.hpp"

namespace bftEngine {
namespace impl {
class IInternalBFTClient {
 public:
  virtual ~IInternalBFTClient() {}
  virtual NodeIdType getClientId() const = 0;

  // send* methods return the sent client request sequence number.
  // The optional `onPoppedFromQueue` function is called in the consumer/replica thread(s) when the given request is
  // popped.
  // Note: users should be aware that if they push a request from the consumer/replica thread(s), the given callback
  // will be called in the same thread.
  virtual uint64_t sendRequest(uint64_t flags, uint32_t requestLength, const char* request, const std::string& cid) = 0;
  virtual uint64_t sendRequest(uint64_t flags,
                               uint32_t requestLength,
                               const char* request,
                               const std::string& cid,
                               IncomingMsgsStorage::Callback onPoppedFromQueue) = 0;

  virtual uint32_t numOfConnectedReplicas(uint32_t clusterSize) = 0;
  virtual bool isUdp() const = 0;
};

class InternalBFTClient : public IInternalBFTClient {
 public:
  InternalBFTClient(NodeIdType id, std::shared_ptr<MsgsCommunicator>& msgComm) : id_{id}, msgComm_(msgComm) {}
  NodeIdType getClientId() const override { return id_; }
  uint64_t sendRequest(uint64_t flags, uint32_t requestLength, const char* request, const std::string& cid) override;
  uint64_t sendRequest(uint64_t flags,
                       uint32_t requestLength,
                       const char* request,
                       const std::string& cid,
                       IncomingMsgsStorage::Callback onPoppedFromQueue) override;
  uint32_t numOfConnectedReplicas(uint32_t clusterSize) override {
    return msgComm_->numOfConnectedReplicas(clusterSize);
  }
  bool isUdp() const override { return msgComm_->isUdp(); }

 private:
  NodeIdType id_;
  std::shared_ptr<MsgsCommunicator> msgComm_;
};

}  // namespace impl
}  // namespace bftEngine
