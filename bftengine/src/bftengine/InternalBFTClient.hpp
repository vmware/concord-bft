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

#include "PrimitiveTypes.hpp"
#include "MsgsCommunicator.hpp"

namespace bftEngine {
namespace impl {
class IInternalBFTClient {
 public:
  virtual ~IInternalBFTClient() {}
  virtual NodeIdType getClientId() const = 0;
  // Returns the sent client request sequence number.
  virtual uint64_t sendRequest(uint64_t flags, uint32_t requestLength, const char* request, const std::string& cid) = 0;
  virtual uint32_t numOfConnectedReplicas(uint32_t clusterSize) = 0;
  virtual bool isUdp() const = 0;
};

class InternalBFTClient : public IInternalBFTClient {
 public:
  InternalBFTClient(NodeIdType id, std::shared_ptr<MsgsCommunicator>& msgComm) : id_{id}, msgComm_(msgComm) {}
  NodeIdType getClientId() const { return id_; }
  uint64_t sendRequest(uint64_t flags, uint32_t requestLength, const char* request, const std::string& cid);
  uint32_t numOfConnectedReplicas(uint32_t clusterSize) { return msgComm_->numOfConnectedReplicas(clusterSize); }
  bool isUdp() const { return msgComm_->isUdp(); }

 private:
  NodeIdType id_;
  std::shared_ptr<MsgsCommunicator> msgComm_;
};

}  // namespace impl
}  // namespace bftEngine
