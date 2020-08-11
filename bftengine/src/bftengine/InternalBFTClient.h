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

class InternalBFTClient {
 public:
  InternalBFTClient(const int& id, const NodeIdType& nonInternalNum, std::shared_ptr<MsgsCommunicator>& msgComm);
  inline NodeIdType getClientId() const { return repID_ + startIdForInternalClient_; };
  void sendRquest(uint8_t flags, uint32_t requestLength, const char* request, const std::string& cid);

 private:
  uint32_t repID_{};
  NodeIdType startIdForInternalClient_{};
  std::shared_ptr<MsgsCommunicator> msgComm_;
};

}  // namespace impl
}  // namespace bftEngine
