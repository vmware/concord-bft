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

#include "RequestProcessingInfo.hpp"
#include "ReplicaConfig.hpp"
#include "Logger.hpp"

namespace preprocessor {

using namespace std;
using namespace concord::util;
using namespace concordUtils;

RequestProcessingInfo::RequestProcessingInfo(uint16_t numOfReplicas, ReqId reqSeqNum)
    : numOfReplicas_(numOfReplicas), reqSeqNum_(reqSeqNum) {
  for (auto i = 0; i < numOfReplicas; i++)
    // Placeholders for all replicas
    replicasDataForRequest_.push_back(nullptr);
  LOG_DEBUG(GL, "Created RequestProcessingInfo with reqSeqNum=" << reqSeqNum_ << ", numOfReplicas= " << numOfReplicas_);
}

void RequestProcessingInfo::saveClientPreProcessRequestMsg(const ClientPreProcessReqMsgSharedPtr& clientPreProcessReq) {
  clientPreProcessRequestMsg_ = clientPreProcessReq;
}

void RequestProcessingInfo::savePreProcessResult(const Sliver& preProcessResult, uint32_t preProcessResultLen) {
  myPreProcessResult_ = preProcessResult.subsliver(0, preProcessResultLen);
  myPreProcessResultHash_ = SHA3_256().digest(myPreProcessResult_.data(), myPreProcessResult_.length());
}

}  // namespace preprocessor
