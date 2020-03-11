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
#include "Logger.hpp"

namespace preprocessor {

using namespace std;
using namespace concord::util;

uint16_t RequestProcessingInfo::numOfRequiredEqualReplies_ = 0;

RequestProcessingInfo::RequestProcessingInfo(uint16_t numOfReplicas,
                                             uint16_t numOfRequiredReplies,
                                             ReqId reqSeqNum,
                                             ClientPreProcessReqMsgUniquePtr clientReqMsg,
                                             PreProcessRequestMsgSharedPtr preProcessRequestMsg)
    : numOfReplicas_(numOfReplicas),
      reqSeqNum_(reqSeqNum),
      clientPreProcessReqMsg_(move(clientReqMsg)),
      preProcessRequestMsg_(preProcessRequestMsg) {
  numOfRequiredEqualReplies_ = numOfRequiredReplies;
  LOG_DEBUG(GL, "Created RequestProcessingInfo with reqSeqNum=" << reqSeqNum_ << ", numOfReplicas= " << numOfReplicas_);
}

void RequestProcessingInfo::handlePrimaryPreProcessed(const char *preProcessResult, uint32_t preProcessResultLen) {
  myPreProcessResult_ = preProcessResult;
  myPreProcessResultLen_ = preProcessResultLen;
  myPreProcessResultHash_ = convertToArray(SHA3_256().digest(myPreProcessResult_, myPreProcessResultLen_).data());
}

void RequestProcessingInfo::handlePreProcessReplyMsg(PreProcessReplyMsgSharedPtr preProcessReplyMsg) {
  numOfReceivedReplies_++;
  preProcessingResultHashes_[convertToArray(preProcessReplyMsg->resultsHash())]++;  // Count equal hashes
}

SHA3_256::Digest RequestProcessingInfo::convertToArray(const uint8_t resultsHash[SHA3_256::SIZE_IN_BYTES]) {
  SHA3_256::Digest hashArray;
  for (uint64_t i = 0; i < SHA3_256::SIZE_IN_BYTES; i++) hashArray[i] = resultsHash[i];
  return hashArray;
}

PreProcessingResult RequestProcessingInfo::getPreProcessingConsensusResult() const {
  if (numOfReceivedReplies_ < numOfRequiredEqualReplies_) return CONTINUE;

  uint16_t maxNumOfEqualHashes = 0;
  auto itOfChosenHash = preProcessingResultHashes_.begin();
  // Calculate a maximum number of the same hashes calculated by non-primary replicas
  for (auto it = preProcessingResultHashes_.begin(); it != preProcessingResultHashes_.end(); it++) {
    if (it->second > maxNumOfEqualHashes) {
      maxNumOfEqualHashes = it->second;
      itOfChosenHash = it;
    }
  }

  if (maxNumOfEqualHashes >= numOfRequiredEqualReplies_) {
    if (itOfChosenHash->first == myPreProcessResultHash_)
      return COMPLETE;  // Pre-execution consensus reached
    else if (myPreProcessResultLen_ != 0) {
      // Primary replica calculated hash is different from a hash that passed pre-execution consensus => we don't have
      // correct pre-processed results. Let's launch a pre-processing retry.
      LOG_WARN(GL,
               "Primary replica pre-processing result hash is different from one passed the consensus for reqSeqNum="
                   << reqSeqNum_ << "; retry pre-processing on primary replica");
      return RETRY_PRIMARY;
    } else {
      LOG_DEBUG(GL, "Primary replica did not complete pre-processing yet for reqSeqNum=" << reqSeqNum_ << "; continue");
      return CONTINUE;
    }
  }

  if (numOfReceivedReplies_ == numOfReplicas_ - 1) {
    // Replies from all replicas received, but not enough equal hashes collected => pre-execution consensus not
    // reached => cancel request.
    LOG_WARN(GL, "Not enough equal hashes collected for reqSeqNum=" << reqSeqNum_ << ", cancel request");
    return CANCEL;
  }
  return CONTINUE;
}

unique_ptr<MessageBase> RequestProcessingInfo::convertClientPreProcessToClientMsg(bool resetPreProcessFlag) {
  unique_ptr<MessageBase> retMsg = clientPreProcessReqMsg_->convertToClientRequestMsg(resetPreProcessFlag);
  clientPreProcessReqMsg_.release();
  return retMsg;
}

}  // namespace preprocessor
