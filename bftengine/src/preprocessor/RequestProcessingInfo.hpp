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
#include "messages/ClientPreProcessRequestMsg.hpp"
#include "messages/PreProcessRequestMsg.hpp"
#include "messages/PreProcessReplyMsg.hpp"
#include <vector>
#include <map>

namespace preprocessor {

// This class collects and stores data relevant to the processing of one specific client request by all replicas.

typedef enum { CONTINUE, COMPLETE, CANCEL, RETRY_PRIMARY } PreProcessingResult;

class RequestProcessingInfo {
 public:
  RequestProcessingInfo(uint16_t numOfReplicas,
                        uint16_t numOfRequiredReplies,
                        ReqId reqSeqNum,
                        ClientPreProcessReqMsgUniquePtr clientReqMsg,
                        PreProcessRequestMsgSharedPtr preProcessRequestMsg);
  ~RequestProcessingInfo() = default;

  void handlePrimaryPreProcessed(const char* preProcessResult, uint32_t preProcessResultLen);
  void handlePreProcessReplyMsg(PreProcessReplyMsgSharedPtr preProcessReplyMsg);
  std::unique_ptr<MessageBase> convertClientPreProcessToClientMsg(bool resetPreProcessFlag);
  PreProcessRequestMsgSharedPtr getPreProcessRequest() const { return preProcessRequestMsg_; }
  const SeqNum getReqSeqNum() const { return reqSeqNum_; }
  PreProcessingResult getPreProcessingConsensusResult() const;
  const char* getPrimaryPreProcessedResult() const { return primaryPreProcessResult_; }
  uint32_t getPrimaryPreProcessedResultLen() const { return primaryPreProcessResultLen_; }
  bool isReqTimedOut() const;

 private:
  static concord::util::SHA3_256::Digest convertToArray(
      const uint8_t resultsHash[concord::util::SHA3_256::SIZE_IN_BYTES]);

  static uint64_t getMonotonicTimeMilli();
  auto calculateMaxNbrOfEqualHashes(uint16_t& maxNumOfEqualHashes) const;

 private:
  static uint16_t numOfRequiredEqualReplies_;

  const uint16_t numOfReplicas_;
  const ReqId reqSeqNum_;
  const uint64_t entryTime_;
  ClientPreProcessReqMsgUniquePtr clientPreProcessReqMsg_;
  PreProcessRequestMsgSharedPtr preProcessRequestMsg_;
  uint16_t numOfReceivedReplies_ = 0;
  const char* primaryPreProcessResult_ = nullptr;
  uint32_t primaryPreProcessResultLen_ = 0;
  concord::util::SHA3_256::Digest primaryPreProcessResultHash_;
  // Maps result hash to the number of equal hashes
  std::map<concord::util::SHA3_256::Digest, int> preProcessingResultHashes_;
};

typedef std::unique_ptr<RequestProcessingInfo> RequestProcessingInfoUniquePtr;

}  // namespace preprocessor
