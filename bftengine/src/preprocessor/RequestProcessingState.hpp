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

#pragma once

#include "PrimitiveTypes.hpp"
#include "messages/ClientPreProcessRequestMsg.hpp"
#include "messages/PreProcessRequestMsg.hpp"
#include "messages/PreProcessReplyMsg.hpp"
#include "PreProcessorRecorder.hpp"
#include <vector>
#include <map>

namespace preprocessor {

// This class collects and stores data relevant to the processing of one specific client request by all replicas.

typedef enum { NONE, CONTINUE, COMPLETE, CANCEL, RETRY_PRIMARY } PreProcessingResult;
typedef std::vector<ReplicaId> ReplicaIdsList;

class RequestProcessingState {
 public:
  RequestProcessingState(uint16_t numOfReplicas,
                         uint16_t clientId,
                         uint16_t reqOffsetInBatch,
                         const std::string& cid,
                         ReqId reqSeqNum,
                         ClientPreProcessReqMsgUniquePtr clientReqMsg,
                         PreProcessRequestMsgSharedPtr preProcessRequestMsg);
  ~RequestProcessingState() = default;

  void handlePrimaryPreProcessed(const char* preProcessResult, uint32_t preProcessResultLen);
  void handlePreProcessReplyMsg(const PreProcessReplyMsgSharedPtr& preProcessReplyMsg);
  std::unique_ptr<MessageBase> buildClientRequestMsg(bool resetPreProcessFlag);
  void setPreProcessRequest(PreProcessRequestMsgSharedPtr preProcessReqMsg);
  const PreProcessRequestMsgSharedPtr& getPreProcessRequest() const { return preProcessRequestMsg_; }
  const auto getClientId() const { return clientId_; }
  const auto getReqOffsetInBatch() const { return reqOffsetInBatch_; }
  const SeqNum getReqSeqNum() const { return reqSeqNum_; }
  PreProcessingResult definePreProcessingConsensusResult();
  const char* getPrimaryPreProcessedResult() const { return primaryPreProcessResult_; }
  uint32_t getPrimaryPreProcessedResultLen() const { return primaryPreProcessResultLen_; }
  bool isReqTimedOut() const;
  const uint64_t reqRetryId() const { return reqRetryId_; }
  uint64_t getReqTimeoutMilli() const {
    return clientPreProcessReqMsg_ ? clientPreProcessReqMsg_->requestTimeoutMilli() : 0;
  }
  std::string getReqCid() const { return clientPreProcessReqMsg_ ? clientPreProcessReqMsg_->getCid() : ""; }
  void detectNonDeterministicPreProcessing(const uint8_t* newHash, NodeIdType newSenderId, uint64_t reqRetryId) const;
  void releaseResources();
  ReplicaIdsList getRejectedReplicasList() { return rejectedReplicaIds_; }
  void resetRejectedReplicasList() { rejectedReplicaIds_.clear(); }
  void setPreprocessingRightNow(bool set) { preprocessingRightNow_ = set; }

  static void init(uint16_t numOfRequiredReplies, preprocessor::PreProcessorRecorder* histograms);

 private:
  static concord::util::SHA3_256::Digest convertToArray(
      const uint8_t resultsHash[concord::util::SHA3_256::SIZE_IN_BYTES]);
  static uint64_t getMonotonicTimeMilli();
  static logging::Logger& logger() {
    static logging::Logger logger_ = logging::getLogger("concord.preprocessor");
    return logger_;
  }
  auto calculateMaxNbrOfEqualHashes(uint16_t& maxNumOfEqualHashes) const;
  void detectNonDeterministicPreProcessing(const concord::util::SHA3_256::Digest& newHash,
                                           NodeIdType newSenderId,
                                           uint64_t reqRetryId) const;

 private:
  static uint16_t numOfRequiredEqualReplies_;
  static preprocessor::PreProcessorRecorder* preProcessorHistograms_;

  // The use of the class data members is thread-safe. The PreProcessor class uses a per-instance mutex lock for
  // the RequestProcessingState objects.
  const uint16_t numOfReplicas_;
  const uint16_t clientId_;
  const uint16_t reqOffsetInBatch_;
  const std::string cid_;
  const ReqId reqSeqNum_;
  const uint64_t entryTime_;
  ClientPreProcessReqMsgUniquePtr clientPreProcessReqMsg_;
  PreProcessRequestMsgSharedPtr preProcessRequestMsg_;
  uint16_t numOfReceivedReplies_ = 0;
  ReplicaIdsList rejectedReplicaIds_;
  const char* primaryPreProcessResult_ = nullptr;  // This memory is statically pre-allocated per client in PreProcessor
  uint32_t primaryPreProcessResultLen_ = 0;
  concord::util::SHA3_256::Digest primaryPreProcessResultHash_;
  // Maps result hash to the number of equal hashes
  std::map<concord::util::SHA3_256::Digest, int> preProcessingResultHashes_;
  bool retrying_ = false;
  bool preprocessingRightNow_ = false;
  uint64_t reqRetryId_ = 0;
};

typedef std::unique_ptr<RequestProcessingState> RequestProcessingStateUniquePtr;

}  // namespace preprocessor
