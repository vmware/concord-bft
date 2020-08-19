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

#include "RequestProcessingState.hpp"

namespace preprocessor {

using namespace std;
using namespace chrono;
using namespace concord::util;

uint16_t RequestProcessingState::numOfRequiredEqualReplies_ = 0;

uint64_t RequestProcessingState::getMonotonicTimeMilli() {
  steady_clock::time_point curTimePoint = steady_clock::now();
  return duration_cast<milliseconds>(curTimePoint.time_since_epoch()).count();
}

void RequestProcessingState::init(uint16_t numOfRequiredReplies) { numOfRequiredEqualReplies_ = numOfRequiredReplies; }

RequestProcessingState::RequestProcessingState(uint16_t numOfReplicas,
                                               uint16_t clientId,
                                               const string &cid,
                                               ReqId reqSeqNum,
                                               ClientPreProcessReqMsgUniquePtr clientReqMsg,
                                               PreProcessRequestMsgSharedPtr preProcessRequestMsg)
    : numOfReplicas_(numOfReplicas),
      clientId_(clientId),
      cid_(cid),
      reqSeqNum_(reqSeqNum),
      entryTime_(getMonotonicTimeMilli()),
      clientPreProcessReqMsg_(move(clientReqMsg)),
      preProcessRequestMsg_(preProcessRequestMsg) {
  LOG_DEBUG(logger(), "Created RequestProcessingState with " << KVLOG(reqSeqNum, numOfReplicas_));
}

void RequestProcessingState::setPreProcessRequest(PreProcessRequestMsgSharedPtr preProcessReqMsg) {
  if (preProcessRequestMsg_ != nullptr) {
    SCOPED_MDC_CID(preProcessReqMsg->getCid());
    const auto reqSeqNum = preProcessRequestMsg_->reqSeqNum();
    LOG_ERROR(logger(), "preProcessRequestMsg_ is already set; " << KVLOG(reqSeqNum, clientId_));
    return;
  }
  preProcessRequestMsg_ = preProcessReqMsg;
}

void RequestProcessingState::handlePrimaryPreProcessed(const char *preProcessResult, uint32_t preProcessResultLen) {
  primaryPreProcessResult_ = preProcessResult;
  primaryPreProcessResultLen_ = preProcessResultLen;
  primaryPreProcessResultHash_ =
      convertToArray(SHA3_256().digest(primaryPreProcessResult_, primaryPreProcessResultLen_).data());
}

void RequestProcessingState::detectNonDeterministicPreProcessing(const SHA3_256::Digest &newHash,
                                                                 NodeIdType newSenderId) const {
  SCOPED_MDC_CID(cid_);
  for (auto &hashArray : preProcessingResultHashes_)
    if (newHash != hashArray.first) {
      LOG_WARN(logger(),
               "Received pre-processing result hash is different from calculated by other replica "
                   << KVLOG(reqSeqNum_, clientId_, newSenderId) << " newHash: " << newHash.data()
                   << " hash: " << hashArray.first.data());
    }
}

void RequestProcessingState::detectNonDeterministicPreProcessing(const uint8_t *newHash, NodeIdType senderId) const {
  detectNonDeterministicPreProcessing(convertToArray(newHash), senderId);
}

void RequestProcessingState::handlePreProcessReplyMsg(const PreProcessReplyMsgSharedPtr &preProcessReplyMsg) {
  numOfReceivedReplies_++;
  const auto newHashArray = convertToArray(preProcessReplyMsg->resultsHash());
  preProcessingResultHashes_[newHashArray]++;  // Count equal hashes
  detectNonDeterministicPreProcessing(newHashArray, preProcessReplyMsg->senderId());
}

SHA3_256::Digest RequestProcessingState::convertToArray(const uint8_t resultsHash[SHA3_256::SIZE_IN_BYTES]) {
  SHA3_256::Digest hashArray;
  for (uint64_t i = 0; i < SHA3_256::SIZE_IN_BYTES; i++) hashArray[i] = resultsHash[i];
  return hashArray;
}

auto RequestProcessingState::calculateMaxNbrOfEqualHashes(uint16_t &maxNumOfEqualHashes) const {
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

bool RequestProcessingState::isReqTimedOut(bool isPrimary) const {
  if (!clientPreProcessReqMsg_) return false;

  SCOPED_MDC_CID(cid_);
  if (!isPrimary || primaryPreProcessResultLen_ != 0) {
    // On the primary: check request timeout once an asynchronous pre-execution completed (to not abort the execution
    // thread)
    auto reqProcessingTime = getMonotonicTimeMilli() - entryTime_;
    if (reqProcessingTime > clientPreProcessReqMsg_->requestTimeoutMilli()) {
      LOG_WARN(logger(),
               "Request timeout of " << clientPreProcessReqMsg_->requestTimeoutMilli() << " ms expired for "
                                     << KVLOG(reqSeqNum_, clientId_, reqProcessingTime));
      return true;
    }
  }
  return false;
}

// The primary replica logic
PreProcessingResult RequestProcessingState::definePreProcessingConsensusResult() {
  if (numOfReceivedReplies_ < numOfRequiredEqualReplies_) return CONTINUE;

  uint16_t maxNumOfEqualHashes = 0;
  auto itOfChosenHash = calculateMaxNbrOfEqualHashes(maxNumOfEqualHashes);
  SCOPED_MDC_CID(cid_);
  if (maxNumOfEqualHashes >= numOfRequiredEqualReplies_) {
    if (itOfChosenHash->first == primaryPreProcessResultHash_) return COMPLETE;  // Pre-execution consensus reached
    if (primaryPreProcessResultLen_ != 0 && !retrying_) {
      // Primary replica calculated hash is different from a hash that passed pre-execution consensus => we don't have
      // correct pre-processed results. Let's launch a pre-processing retry.
      LOG_WARN(logger(),
               "Primary replica pre-processing result hash: "
                   << primaryPreProcessResultHash_.data() << " is different from one passed the consensus: "
                   << itOfChosenHash->first.data() << KVLOG(reqSeqNum_) << "; retry pre-processing on primary replica");
      retrying_ = true;
      return RETRY_PRIMARY;
    }
    LOG_DEBUG(logger(),
              "Primary replica did not complete pre-processing yet for " << KVLOG(reqSeqNum_) << "; continue");
    return CONTINUE;
  }

  if (numOfReceivedReplies_ == numOfReplicas_ - 1) {
    // Replies from all replicas received, but not enough equal hashes collected => pre-execution consensus not
    // reached => cancel request.
    LOG_WARN(logger(), "Not enough equal hashes collected for " << KVLOG(reqSeqNum_) << ", cancel request");
    return CANCEL;
  }
  return CONTINUE;
}

unique_ptr<MessageBase> RequestProcessingState::buildClientRequestMsg(bool resetPreProcessFlag) {
  return clientPreProcessReqMsg_->convertToClientRequestMsg(resetPreProcessFlag);
}

}  // namespace preprocessor