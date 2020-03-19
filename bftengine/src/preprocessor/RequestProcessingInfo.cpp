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
using namespace chrono;
using namespace concord::util;

uint16_t RequestProcessingInfo::numOfRequiredEqualReplies_ = 0;

uint64_t RequestProcessingInfo::getMonotonicTimeMilli() {
  steady_clock::time_point curTimePoint = steady_clock::now();
  return duration_cast<milliseconds>(curTimePoint.time_since_epoch()).count();
}

RequestProcessingInfo::RequestProcessingInfo(uint16_t numOfReplicas,
                                             uint16_t numOfRequiredReplies,
                                             ReqId reqSeqNum,
                                             ClientPreProcessReqMsgUniquePtr clientReqMsg,
                                             PreProcessRequestMsgSharedPtr preProcessRequestMsg)
    : numOfReplicas_(numOfReplicas),
      reqSeqNum_(reqSeqNum),
      entryTime_(getMonotonicTimeMilli()),
      clientPreProcessReqMsg_(move(clientReqMsg)),
      preProcessRequestMsg_(preProcessRequestMsg) {
  numOfRequiredEqualReplies_ = numOfRequiredReplies;
  LOG_DEBUG(GL, "Created RequestProcessingInfo with reqSeqNum=" << reqSeqNum_ << ", numOfReplicas= " << numOfReplicas_);
}

void RequestProcessingInfo::handlePrimaryPreProcessed(const char *preProcessResult, uint32_t preProcessResultLen) {
  primaryPreProcessResult_ = preProcessResult;
  primaryPreProcessResultLen_ = preProcessResultLen;
  primaryPreProcessResultHash_ =
      convertToArray(SHA3_256().digest(primaryPreProcessResult_, primaryPreProcessResultLen_).data());
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

auto RequestProcessingInfo::calculateMaxNbrOfEqualHashes(uint16_t &maxNumOfEqualHashes) const {
  auto itOfChosenHash = preProcessingResultHashes_.begin();
  // Calculate a maximum number of the same hashes received from non-primary replicas
  for (auto it = preProcessingResultHashes_.begin(); it != preProcessingResultHashes_.end(); it++) {
    if (it->second > maxNumOfEqualHashes) {
      maxNumOfEqualHashes = it->second;
      itOfChosenHash = it;
    }
  }
  return itOfChosenHash;
}

// Primary replica logic
bool RequestProcessingInfo::isReqTimedOut() const {
  // Check request timeout once asynchronous primary pre-execution completed (to not abort the execution thread)
  if (primaryPreProcessResultLen_ != 0) {
    auto reqProcessingTime = getMonotonicTimeMilli() - entryTime_;
    if (reqProcessingTime > clientPreProcessReqMsg_->requestTimeoutMilli()) {
      LOG_WARN(GL,
               "Request timeout of " << clientPreProcessReqMsg_->requestTimeoutMilli() << " ms expired for reqSeqNum="
                                     << reqSeqNum_ << "; reqProcessingTime=" << reqProcessingTime);
      return true;
    }
  }
  return false;
}

// Non-primary replica logic
bool RequestProcessingInfo::isPreProcessReqMsgReceivedInTime(const uint16_t preProcessReqWaitTimeMilli) const {
  // Check if request was registered for too long after been received from the client
  auto clientRequestWaitingTime = getMonotonicTimeMilli() - entryTime_;
  if (clientRequestWaitingTime > preProcessReqWaitTimeMilli) {
    LOG_WARN(GL,
             "PreProcessRequestMsg did not arrive in time: preProcessReqWaitTimeMilli="
                 << preProcessReqWaitTimeMilli << " ms expired for reqSeqNum=" << reqSeqNum_
                 << "; clientRequestWaitingTime=" << clientRequestWaitingTime);
    return true;
  }
  return false;
}

PreProcessingResult RequestProcessingInfo::getPreProcessingConsensusResult() const {
  if (numOfReceivedReplies_ < numOfRequiredEqualReplies_) return CONTINUE;

  uint16_t maxNumOfEqualHashes = 0;
  auto itOfChosenHash = calculateMaxNbrOfEqualHashes(maxNumOfEqualHashes);
  if (maxNumOfEqualHashes >= numOfRequiredEqualReplies_) {
    if (itOfChosenHash->first == primaryPreProcessResultHash_) return COMPLETE;  // Pre-execution consensus reached

    if (primaryPreProcessResultLen_ != 0) {
      // Primary replica calculated hash is different from a hash that passed pre-execution consensus => we don't have
      // correct pre-processed results. Let's launch a pre-processing retry.
      LOG_WARN(GL,
               "Primary replica pre-processing result hash is different from one passed the consensus for reqSeqNum="
                   << reqSeqNum_ << "; retry pre-processing on primary replica");
      return RETRY_PRIMARY;
    }

    LOG_DEBUG(GL, "Primary replica did not complete pre-processing yet for reqSeqNum=" << reqSeqNum_ << "; continue");
    return CONTINUE;
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
