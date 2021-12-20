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

namespace preprocessor {

using namespace std;
using namespace chrono;
using namespace concord::util;
using namespace concord::kvbc::sparse_merkle;

uint16_t RequestProcessingState::numOfRequiredEqualReplies_ = 0;
PreProcessorRecorder *RequestProcessingState::preProcessorHistograms_ = nullptr;

void RequestProcessingState::init(uint16_t numOfRequiredReplies, PreProcessorRecorder *histograms) {
  LOG_INFO(logger(), KVLOG(numOfRequiredReplies));
  numOfRequiredEqualReplies_ = numOfRequiredReplies;
  preProcessorHistograms_ = histograms;
}

RequestProcessingState::RequestProcessingState(ReplicaId myReplicaId,
                                               uint16_t numOfReplicas,
                                               const string &batchCid,
                                               uint16_t clientId,
                                               uint16_t reqOffsetInBatch,
                                               const string &cid,
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
      cid_(cid),
      reqSeqNum_(reqSeqNum),
      entryTime_(getMonotonicTimeMilli()),
      clientRequestSignature_(signature ? signature : "", signature ? signatureLen : 0),
      clientPreProcessReqMsg_(move(clientReqMsg)),
      preProcessRequestMsg_(preProcessRequestMsg) {
  SCOPED_MDC_CID(cid);
  LOG_DEBUG(logger(), "Created RequestProcessingState with" << KVLOG(reqSeqNum, clientId, numOfReplicas_));
}

void RequestProcessingState::setPreProcessRequest(PreProcessRequestMsgSharedPtr preProcessReqMsg) {
  if (preProcessRequestMsg_ != nullptr) {
    SCOPED_MDC_CID(preProcessReqMsg->getCid());
    const auto reqSeqNum = preProcessRequestMsg_->reqSeqNum();
    LOG_ERROR(logger(), "preProcessRequestMsg_ is already set;" << KVLOG(reqSeqNum, clientId_));
    return;
  }
  preProcessRequestMsg_ = preProcessReqMsg;
  reqRetryId_ = preProcessRequestMsg_->reqRetryId();
}

void RequestProcessingState::handlePrimaryPreProcessed(const char *preProcessResult, uint32_t preProcessResultLen) {
  preprocessingRightNow_ = false;
  primaryPreProcessResult_ = preProcessResult;
  primaryPreProcessResultLen_ = preProcessResultLen;
  primaryPreProcessResultHash_ =
      convertToArray(SHA3_256().digest(primaryPreProcessResult_, primaryPreProcessResultLen_).data());

  auto sm = SigManager::instance();
  std::vector<char> sig(sm->getMySigLength());
  sm->sign(reinterpret_cast<const char *>(primaryPreProcessResultHash_.data()),
           primaryPreProcessResultHash_.size(),
           sig.data(),
           sig.size());
  preProcessingResultHashes_[primaryPreProcessResultHash_].emplace_back(std::move(sig), myReplicaId_);
  // Decision when PreProcessing is complete is made based on the value of received replies. The value should be
  // increased for the primary hash too.
  numOfReceivedReplies_++;
}

void RequestProcessingState::releaseResources() {
  clientPreProcessReqMsg_.reset();
  preProcessRequestMsg_.reset();
}

void RequestProcessingState::detectNonDeterministicPreProcessing(const SHA3_256::Digest &newHash,
                                                                 NodeIdType newSenderId,
                                                                 uint64_t reqRetryId) const {
  SCOPED_MDC_CID(cid_);
  for (auto &hashArray : preProcessingResultHashes_)
    if ((newHash != hashArray.first) && reqRetryId_ && (reqRetryId_ == reqRetryId)) {
      // Compare only between matching request/reply retry ids
      LOG_INFO(logger(),
               "Received pre-processing result hash is different from calculated by other replica"
                   << KVLOG(reqSeqNum_, clientId_, newSenderId));
    }
}

void RequestProcessingState::detectNonDeterministicPreProcessing(const uint8_t *newHash,
                                                                 NodeIdType senderId,
                                                                 uint64_t reqRetryId) const {
  detectNonDeterministicPreProcessing(convertToArray(newHash), senderId, reqRetryId);
}

void RequestProcessingState::handlePreProcessReplyMsg(const PreProcessReplyMsgSharedPtr &preProcessReplyMsg) {
  const auto &senderId = preProcessReplyMsg->senderId();
  if (preProcessReplyMsg->status() == STATUS_GOOD) {
    numOfReceivedReplies_++;
    const auto &newHashArray = convertToArray(preProcessReplyMsg->resultsHash());
    // Counts equal hashes and saves the signatures with the replica ID. They will be used as a proof that the primary
    // is sending correct preexecution result to the rest of the replicas.
    preProcessingResultHashes_[newHashArray].emplace_back(preProcessReplyMsg->getResultHashSignature(),
                                                          preProcessReplyMsg->senderId());
    detectNonDeterministicPreProcessing(newHashArray, senderId, preProcessReplyMsg->reqRetryId());
  } else {
    SCOPED_MDC_CID(cid_);
    LOG_DEBUG(logger(), "Register rejected PreProcessReplyMsg" << KVLOG(senderId, reqSeqNum_, clientId_));
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
  // Calculate a maximum number of the same hashes received from non-primary replicas
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

  SCOPED_MDC_CID(cid_);
  LOG_DEBUG(logger(), KVLOG(preprocessingRightNow_));
  if (!preprocessingRightNow_) {
    // Check request timeout once an asynchronous pre-execution completed (to not abort the execution thread)
    auto reqProcessingTime = getMonotonicTimeMilli() - entryTime_;
    if (reqProcessingTime > clientPreProcessReqMsg_->requestTimeoutMilli()) {
      LOG_WARN(logger(),
               "Request timeout of " << clientPreProcessReqMsg_->requestTimeoutMilli() << " ms expired for"
                                     << KVLOG(reqSeqNum_, clientId_, reqProcessingTime));
      return true;
    }
  }
  return false;
}

std::pair<std::string, concord::util::SHA3_256::Digest> RequestProcessingState::detectFailureDueToBlockID(
    const concord::util::SHA3_256::Digest &other, uint64_t blockId) {
  // since this scenario is rare, a new string is allocated for safety.
  std::string modifiedResult(primaryPreProcessResult_, primaryPreProcessResultLen_);
  ConcordAssertGT(modifiedResult.size(), sizeof(uint64_t));
  memcpy(modifiedResult.data() + modifiedResult.size() - sizeof(uint64_t),
         reinterpret_cast<char *>(&blockId),
         sizeof(uint64_t));
  auto modifiedHash = convertToArray(SHA3_256().digest(modifiedResult.c_str(), modifiedResult.size()).data());
  if (other == modifiedHash) {
    LOG_INFO(logger(), "Primary hash is different from quorum due to mismatch in block id " << KVLOG(reqSeqNum_));
    return {modifiedResult, modifiedHash};
  }
  return {"", concord::util::SHA3_256::Digest{}};
}

void RequestProcessingState::modifyPrimaryResult(
    const std::pair<std::string, concord::util::SHA3_256::Digest> &result) {
  memcpy(const_cast<char *>(primaryPreProcessResult_), result.first.c_str(), primaryPreProcessResultLen_);
  primaryPreProcessResultHash_ = result.second;
  auto sm = SigManager::instance();
  std::vector<char> sig(sm->getMySigLength());
  sm->sign(reinterpret_cast<const char *>(primaryPreProcessResultHash_.data()),
           primaryPreProcessResultHash_.size(),
           sig.data(),
           sig.size());
  preProcessingResultHashes_[primaryPreProcessResultHash_].emplace_back(std::move(sig), myReplicaId_);
}

// The primary replica logic
PreProcessingResult RequestProcessingState::definePreProcessingConsensusResult() {
  SCOPED_MDC_CID(cid_);
  if (numOfReceivedReplies_ < numOfRequiredEqualReplies_) {
    LOG_DEBUG(logger(),
              "Not enough replies received, continue waiting"
                  << KVLOG(reqSeqNum_, numOfReceivedReplies_, numOfRequiredEqualReplies_));
    return CONTINUE;
  }

  uint16_t maxNumOfEqualHashes = 0;
  auto itOfChosenHash = calculateMaxNbrOfEqualHashes(maxNumOfEqualHashes);
  if (maxNumOfEqualHashes >= numOfRequiredEqualReplies_) {
    if (itOfChosenHash->first == primaryPreProcessResultHash_) return COMPLETE;  // Pre-execution consensus reached
    if (primaryPreProcessResultLen_ != 0 && !retrying_) {
      // A known scenario that can cause a mismatch, is due to rejection of the block id sent by the primary.
      // In this case the difference should be only the last 64 bits that encodes the `0` as the rejection value.
      if (auto modifiedResult = detectFailureDueToBlockID(itOfChosenHash->first, 0); modifiedResult.first.size() > 0) {
        modifyPrimaryResult(modifiedResult);
        return COMPLETE;
      }
      // Primary replica calculated hash is different from a hash that passed pre-execution consensus => we don't have
      // correct pre-processed results. Let's launch a pre-processing retry.
      const auto &primaryHash =
          Hash(SHA3_256().digest(primaryPreProcessResultHash_.data(), primaryPreProcessResultHash_.size())).toString();
      const auto &hashPassedConsensus =
          Hash(SHA3_256().digest(itOfChosenHash->first.data(), itOfChosenHash->first.size())).toString();
      LOG_WARN(logger(),
               "Primary replica pre-processing result hash: "
                   << primaryHash << " is different from one passed the consensus: " << hashPassedConsensus
                   << KVLOG(reqSeqNum_) << "; retry pre-processing on primary replica");
      retrying_ = true;
      return RETRY_PRIMARY;
    }
    LOG_DEBUG(logger(), "Primary replica did not complete pre-processing yet, continue waiting" << KVLOG(reqSeqNum_));
    return CONTINUE;
  } else
    LOG_DEBUG(
        logger(),
        "Not enough equal hashes collected yet" << KVLOG(reqSeqNum_, maxNumOfEqualHashes, numOfRequiredEqualReplies_));

  if (numOfReceivedReplies_ == numOfReplicas_) {
    // Replies from all replicas received, but not enough equal hashes collected => pre-execution consensus not
    // reached => cancel request.
    LOG_WARN(logger(), "Not enough equal hashes collected, cancel request" << KVLOG(reqSeqNum_));
    return CANCEL;
  }
  LOG_DEBUG(logger(),
            "Continue waiting for replies to arrive"
                << KVLOG(reqSeqNum_, numOfReceivedReplies_, maxNumOfEqualHashes, numOfRequiredEqualReplies_));
  return CONTINUE;
}

unique_ptr<MessageBase> RequestProcessingState::buildClientRequestMsg(bool emptyReq) {
  return clientPreProcessReqMsg_->convertToClientRequestMsg(emptyReq);
}

const std::list<PreProcessResultSignature> &RequestProcessingState::getPreProcessResultSignatures() {
  const auto &r = preProcessingResultHashes_.find(primaryPreProcessResultHash_);
  ConcordAssert(r != preProcessingResultHashes_.end());
  return r->second;
}

}  // namespace preprocessor
