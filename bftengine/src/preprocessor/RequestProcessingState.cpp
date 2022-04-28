// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "RequestProcessingState.hpp"
#include "sparse_merkle/base_types.h"
#include "SigManager.hpp"
#include "TimeUtils.hpp"
#include "messages/PreProcessResultHashCreator.hpp"

namespace preprocessor {

using namespace std;
using namespace bftEngine;
using namespace chrono;
using namespace concord::util;
using namespace concord::kvbc::sparse_merkle;

uint16_t RequestProcessingState::numOfRequiredEqualReplies_ = 0;
PreProcessorRecorder *RequestProcessingState::preProcessorHistograms_ = nullptr;

void RequestProcessingState::init(uint16_t numOfRequiredReplies, PreProcessorRecorder *histograms) {
  LOG_INFO(logger(), "RequestProcessingState init:" << KVLOG(numOfRequiredReplies));
  numOfRequiredEqualReplies_ = numOfRequiredReplies;
  preProcessorHistograms_ = histograms;
}

RequestProcessingState::RequestProcessingState(ReplicaId myReplicaId,
                                               uint16_t numOfReplicas,
                                               const string &batchCid,
                                               uint16_t clientId,
                                               uint16_t reqOffsetInBatch,
                                               const string &reqCid,
                                               ReqId reqSeqNum,
                                               ClientPreProcessReqMsgUniquePtr clientReqMsg,
                                               PreProcessRequestMsgSharedPtr preProcessRequestMsg,
                                               const char *signature,
                                               const uint32_t signatureLen)
    : myReplicaId_{myReplicaId},
      numOfReplicas_(numOfReplicas),
      batchCid_(batchCid),
      clientId_(clientId),
      reqOffsetInBatch_(reqOffsetInBatch),
      reqCid_(reqCid),
      reqSeqNum_(reqSeqNum),
      entryTime_(getMonotonicTimeMilli()),
      clientRequestSignature_(signature ? signature : "", signature ? signatureLen : 0),
      clientPreProcessReqMsg_(move(clientReqMsg)),
      preProcessRequestMsg_(preProcessRequestMsg) {
  LOG_DEBUG(logger(), "Created RequestProcessingState with" << KVLOG(clientId, reqSeqNum, reqCid, numOfReplicas_));
}

void RequestProcessingState::setPreProcessRequest(PreProcessRequestMsgSharedPtr preProcessReqMsg) {
  if (preProcessRequestMsg_ != nullptr) {
    const auto reqSeqNum = preProcessRequestMsg_->reqSeqNum();
    const auto &reqCid = preProcessRequestMsg_->getCid();
    LOG_ERROR(logger(), "preProcessRequestMsg_ is already set;" << KVLOG(clientId_, reqSeqNum, reqCid));
    return;
  }
  preProcessRequestMsg_ = preProcessReqMsg;
  reqRetryId_ = preProcessRequestMsg_->reqRetryId();
}

uint32_t RequestProcessingState::sizeOfPreProcessResultData() const {
  return sizeof(OperationResult) + sizeof(clientId_) + sizeof(reqSeqNum_);
}

void RequestProcessingState::setupPreProcessResultData(OperationResult preProcessResult) {
  memcpy((void *)primaryPreProcessResultData_, &preProcessResult, sizeof(preProcessResult));
  primaryPreProcessResultData_ += sizeof(preProcessResult);
  memcpy((void *)primaryPreProcessResultData_, &clientId_, sizeof(clientId_));
  primaryPreProcessResultData_ += sizeof(clientId_);
  memcpy((void *)primaryPreProcessResultData_, &reqSeqNum_, sizeof(reqSeqNum_));
  primaryPreProcessResultLen_ = sizeOfPreProcessResultData();
}

void RequestProcessingState::updatePreProcessResultData(OperationResult preProcessResult) {
  memcpy(&primaryPreProcessResultData_, &preProcessResult, sizeof(preProcessResult));
  primaryPreProcessResultLen_ = sizeOfPreProcessResultData();
}

void RequestProcessingState::handlePrimaryPreProcessed(const char *preProcessResultData,
                                                       uint32_t preProcessResultLen,
                                                       OperationResult preProcessResult) {
  primaryPreProcessResult_ = preProcessResult;
  if (preProcessResult != OperationResult::UNKNOWN) {
    primaryPreProcessResultData_ = preProcessResultData;
    primaryPreProcessResultLen_ = preProcessResultLen;
  }

  primaryPreProcessResultHash_ = PreProcessResultHashCreator::create(
      primaryPreProcessResultData_, primaryPreProcessResultLen_, primaryPreProcessResult_, clientId_, reqSeqNum_);

  // In case the pre-processing failed on the primary replica, fill primaryPreProcessResultData_ by the result-related
  // information.
  if (preProcessResult != OperationResult::SUCCESS) setupPreProcessResultData(preProcessResult);
  auto sm = SigManager::instance();
  std::vector<char> sig(sm->getMySigLength());
  sm->sign(reinterpret_cast<const char *>(primaryPreProcessResultHash_.data()),
           primaryPreProcessResultHash_.size(),
           sig.data(),
           sig.size());
  if (!preProcessingResultHashes_[primaryPreProcessResultHash_]
           .emplace(std::move(sig), myReplicaId_, preProcessResult)
           .second) {
    LOG_INFO(logger(), "Failed to add signature, primary already has a signature");
    return;
  }
  // Decision when PreProcessing is complete is made based on the value of received replies. The value should be
  // increased for the primary hash, too.
  numOfReceivedReplies_++;
}

void RequestProcessingState::releaseResources() {
  clientPreProcessReqMsg_.reset();
  preProcessRequestMsg_.reset();
}

void RequestProcessingState::detectNonDeterministicPreProcessing(const SHA3_256::Digest &newHash,
                                                                 NodeIdType newSenderId,
                                                                 uint64_t reqRetryId) const {
  for (auto &hashArray : preProcessingResultHashes_)
    if ((newHash != hashArray.first) && reqRetryId_ && (reqRetryId_ == reqRetryId)) {
      // Compare only between matching request/reply retry ids
      LOG_INFO(logger(),
               "Received pre-processing result hash is different from calculated by other replica"
                   << KVLOG(clientId_, batchCid_, reqSeqNum_, reqCid_, newSenderId));
    }
}

void RequestProcessingState::detectNonDeterministicPreProcessing(const uint8_t *newHash,
                                                                 NodeIdType senderId,
                                                                 uint64_t reqRetryId) const {
  detectNonDeterministicPreProcessing(convertToArray(newHash), senderId, reqRetryId);
}

void RequestProcessingState::handlePreProcessReplyMsg(const PreProcessReplyMsgSharedPtr &preProcessReplyMsg) {
  const auto &senderId = preProcessReplyMsg->senderId();
  if (preProcessReplyMsg->status() != STATUS_REJECT) {
    const auto &newHashArray = convertToArray(preProcessReplyMsg->resultsHash());
    // Counts equal hashes and saves the signatures with the replica ID. They will be used as a proof that the primary
    // is sending correct preexecution result to the rest of the replicas.
    if (!preProcessingResultHashes_[newHashArray]
             .emplace(preProcessReplyMsg->getResultHashSignature(),
                      preProcessReplyMsg->senderId(),
                      preProcessReplyMsg->preProcessResult())
             .second) {
      LOG_INFO(logger(),
               "Signature was already accepted for this sender"
                   << KVLOG(senderId, clientId_, batchCid_, reqSeqNum_, reqCid_));
      return;
    }
    numOfReceivedReplies_++;
    detectNonDeterministicPreProcessing(newHashArray, senderId, preProcessReplyMsg->reqRetryId());
  } else {
    LOG_DEBUG(logger(),
              "Register rejected PreProcessReplyMsg" << KVLOG(senderId, clientId_, batchCid_, reqSeqNum_, reqCid_));
    rejectedReplicaIds_.push_back(preProcessReplyMsg->senderId());
  }
}

SHA3_256::Digest RequestProcessingState::convertToArray(const uint8_t resultsHash[SHA3_256::SIZE_IN_BYTES]) {
  SHA3_256::Digest hashArray;
  for (uint64_t i = 0; i < SHA3_256::SIZE_IN_BYTES; i++) hashArray[i] = resultsHash[i];
  return hashArray;
}

auto RequestProcessingState::calculateMaxNbrOfEqualHashes(uint16_t &maxNumOfEqualHashes) const {
  auto itOfChosenHash = preProcessingResultHashes_.begin();
  // Calculate a maximum number of the same hashes
  for (auto it = preProcessingResultHashes_.begin(); it != preProcessingResultHashes_.end(); it++) {
    if (it->second.size() > maxNumOfEqualHashes) {
      maxNumOfEqualHashes = it->second.size();
      itOfChosenHash = it;
    }
  }
  return itOfChosenHash;
}

bool RequestProcessingState::isReqTimedOut() const {
  if (!clientPreProcessReqMsg_) return false;

  LOG_DEBUG(logger(), KVLOG(preprocessingRightNow_));
  if (!preprocessingRightNow_) {
    // Check request timeout once an asynchronous pre-execution completed (to not abort the execution thread)
    auto reqProcessingTime = getMonotonicTimeMilli() - entryTime_;
    if (reqProcessingTime > clientPreProcessReqMsg_->requestTimeoutMilli()) {
      LOG_WARN(logger(),
               "Request timeout of " << clientPreProcessReqMsg_->requestTimeoutMilli() << " ms expired for"
                                     << KVLOG(clientId_, batchCid_, reqSeqNum_, reqCid_, reqProcessingTime));
      return true;
    }
  }
  return false;
}

std::pair<std::string, concord::util::SHA3_256::Digest> RequestProcessingState::detectFailureDueToBlockID(
    const concord::util::SHA3_256::Digest &other, uint64_t blockId) {
  // Since this scenario is rare, a new string is allocated for safety.
  std::string modifiedResult(primaryPreProcessResultData_, primaryPreProcessResultLen_);
  ConcordAssertGT(modifiedResult.size(), sizeof(uint64_t));
  memcpy(modifiedResult.data() + modifiedResult.size() - sizeof(uint64_t),
         reinterpret_cast<char *>(&blockId),
         sizeof(uint64_t));
  auto modifiedHash = PreProcessResultHashCreator::create(
      modifiedResult.data(), modifiedResult.size(), OperationResult::SUCCESS, clientId_, reqSeqNum_);
  if (other == modifiedHash) {
    LOG_INFO(
        logger(),
        "Primary hash is different from quorum due to mismatch in block id" << KVLOG(batchCid_, reqSeqNum_, reqCid_));
    return {modifiedResult, modifiedHash};
  }
  return {"", concord::util::SHA3_256::Digest{}};
}

void RequestProcessingState::modifyPrimaryResult(
    const std::pair<std::string, concord::util::SHA3_256::Digest> &result) {
  memcpy(const_cast<char *>(primaryPreProcessResultData_), result.first.c_str(), primaryPreProcessResultLen_);
  primaryPreProcessResultHash_ = result.second;
  auto sm = SigManager::instance();
  std::vector<char> sig(sm->getMySigLength());
  sm->sign(reinterpret_cast<const char *>(primaryPreProcessResultHash_.data()),
           primaryPreProcessResultHash_.size(),
           sig.data(),
           sig.size());
  if (!preProcessingResultHashes_[primaryPreProcessResultHash_]
           .emplace(std::move(sig), myReplicaId_, primaryPreProcessResult_)
           .second) {
    LOG_INFO(logger(), "Failed to add signature, primary already has a signature");
  }
}

void RequestProcessingState::reportNonEqualHashes(const unsigned char *chosenData, uint32_t chosenSize) const {
  // Primary replica calculated hash is different from a hash that passed pre-execution consensus => we don't have
  // correct pre-processed results.
  const auto &primaryHash =
      Hash(SHA3_256().digest(primaryPreProcessResultHash_.data(), primaryPreProcessResultHash_.size())).toString();
  const auto &hashPassedConsensus = Hash(SHA3_256().digest(chosenData, chosenSize)).toString();
  LOG_WARN(logger(),
           "Primary replica pre-processing result hash: "
               << primaryHash << " is different from one passed the consensus: " << hashPassedConsensus
               << KVLOG(batchCid_, reqSeqNum_, reqCid_));
}

// The primary replica logic
PreProcessingResult RequestProcessingState::definePreProcessingConsensusResult() {
  if (numOfReceivedReplies_ < numOfRequiredEqualReplies_) {
    LOG_INFO(logger(),
             "Not enough replies received, continue waiting"
                 << KVLOG(batchCid_, reqSeqNum_, reqCid_, numOfReceivedReplies_, numOfRequiredEqualReplies_));
    return CONTINUE;
  }
  if (primaryPreProcessResultLen_ == 0) {
    LOG_INFO(logger(),
             "Primary replica did not complete pre-processing yet, continue waiting"
                 << KVLOG(batchCid_, reqSeqNum_, reqCid_));
    return CONTINUE;
  }
  uint16_t maxNumOfEqualHashes = 0;
  auto itOfChosenHash = calculateMaxNbrOfEqualHashes(maxNumOfEqualHashes);
  LOG_INFO(logger(), KVLOG(maxNumOfEqualHashes, numOfRequiredEqualReplies_));
  if (maxNumOfEqualHashes >= numOfRequiredEqualReplies_) {
    agreedPreProcessResult_ = itOfChosenHash->second.cbegin()->getPreProcessResult();
    if (itOfChosenHash->first == primaryPreProcessResultHash_) {
      if (agreedPreProcessResult_ != OperationResult::SUCCESS)
        LOG_INFO(logger(),
                 "The replicas agreed on an error execution result:"
                     << KVLOG(static_cast<uint32_t>(agreedPreProcessResult_)));
      return COMPLETE;  // Pre-execution consensus reached
    }
    if (primaryPreProcessResult_ == OperationResult::SUCCESS && agreedPreProcessResult_ != OperationResult::SUCCESS) {
      // The pre-execution succeeded on a primary replica while failed on non-primaries. The consensus for an error
      // execution result has been reached => update preProcess result data.
      updatePreProcessResultData(agreedPreProcessResult_);
      LOG_INFO(logger(),
               "The replicas (except the primary) agreed on an error execution result:"
                   << KVLOG(static_cast<uint32_t>(agreedPreProcessResult_)) << ", we are done");
      return COMPLETE;
    }
    // A known scenario that can cause a mismatch, is due to rejection of the block id sent by the primary.
    // In this case the difference should be only the last 64 bits that encodes the `0` as the rejection value.
    if (primaryPreProcessResult_ == OperationResult::SUCCESS) {
      const auto modifiedResult = detectFailureDueToBlockID(itOfChosenHash->first, 0);
      if (modifiedResult.first.size() > 0) {
        modifyPrimaryResult(modifiedResult);
        return COMPLETE;
      }
      reportNonEqualHashes(itOfChosenHash->first.data(), itOfChosenHash->first.size());
      return CANCEL;
    }
  } else
    LOG_INFO(logger(),
             "Not enough equal hashes collected yet"
                 << KVLOG(batchCid_, reqSeqNum_, reqCid_, maxNumOfEqualHashes, numOfRequiredEqualReplies_));

  if (numOfReceivedReplies_ == numOfReplicas_) {
    // Replies from all replicas received, but not enough equal hashes collected => pre-execution consensus not
    // reached => cancel request.
    LOG_WARN(logger(), "Not enough equal hashes collected, cancel request" << KVLOG(batchCid_, reqSeqNum_, reqCid_));
    return CANCEL;
  }
  LOG_INFO(logger(),
           "Continue waiting for replies to arrive" << KVLOG(
               batchCid_, reqSeqNum_, reqCid_, numOfReceivedReplies_, maxNumOfEqualHashes, numOfRequiredEqualReplies_));
  return CONTINUE;
}

unique_ptr<MessageBase> RequestProcessingState::buildClientRequestMsg(bool emptyReq) {
  return clientPreProcessReqMsg_->convertToClientRequestMsg(emptyReq);
}

const std::set<PreProcessResultSignature> &RequestProcessingState::getPreProcessResultSignatures() {
  const auto &r = preProcessingResultHashes_.find(primaryPreProcessResultHash_);
  if (r != preProcessingResultHashes_.end()) return r->second;
  LOG_FATAL(logger(),
            "Primary replica pre-processing has not been completed" << KVLOG(clientId_,
                                                                             batchCid_,
                                                                             reqSeqNum_,
                                                                             reqCid_,
                                                                             reqOffsetInBatch_,
                                                                             preProcessingResultHashes_.size(),
                                                                             preprocessingRightNow_,
                                                                             (int)agreedPreProcessResult_,
                                                                             (int)primaryPreProcessResult_,
                                                                             numOfReceivedReplies_));
  ConcordAssert(false);
}

}  // namespace preprocessor
