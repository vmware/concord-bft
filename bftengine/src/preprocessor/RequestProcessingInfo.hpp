// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include "PrimitiveTypes.hpp"
#include "sha3_256.h"
#include "messages/PreProcessReplyMsg.hpp"
#include "messages/ClientPreProcessRequestMsg.hpp"
#include "sliver.hpp"

#include <vector>
#include <memory>

namespace preprocessor {

struct ReplicaDataForRequest {
  std::unique_ptr<PreProcessReplyMsg> preProcessReplyMsg_;
};

class RequestProcessingInfo {
 public:
  RequestProcessingInfo(uint16_t numOfReplicas, ReqId reqSeqNum);
  ~RequestProcessingInfo() = default;

  void saveClientPreProcessRequestMsg(const ClientPreProcessReqMsgSharedPtr &clientPreProcessRequestMsg);
  void savePreProcessResult(const concordUtils::Sliver &preProcessResult, uint32_t preProcessResultLen);

 private:
  const uint16_t numOfReplicas_;
  const ReqId reqSeqNum_;
  ClientPreProcessReqMsgSharedPtr clientPreProcessRequestMsg_;  // Original client message
  concord::util::SHA3_256::Digest myPreProcessResultHash_ = {0};
  concordUtils::Sliver myPreProcessResult_;
  std::vector<std::unique_ptr<ReplicaDataForRequest>> replicasDataForRequest_;
};

}  // namespace preprocessor
