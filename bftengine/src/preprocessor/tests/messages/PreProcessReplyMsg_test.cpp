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

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "PrimitiveTypes.hpp"
#include "messages/PreProcessReplyMsg.hpp"
#include "messages/PreProcessResultHashCreator.hpp"
#include "RequestProcessingState.hpp"
#include "helper.hpp"

namespace {
using namespace bftEngine::impl;
using namespace bftEngine;
using namespace preprocessor;

class PreProcessReplyMsgTestFixture : public testing::Test {
 protected:
  PreProcessReplyMsgTestFixture()
      : config{createReplicaConfig()},
        replicaInfo{config, false, false},
        sigManager(createSigManager(config.replicaId,
                                    config.replicaPrivateKey,
                                    concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat,
                                    config.publicKeysOfReplicas,
                                    replicaInfo)) {
    PreProcessReplyMsg::setPreProcessorHistograms(&preProcessorRecorder);
  }

  ReplicaConfig& config;
  ReplicasInfo replicaInfo;
  std::unique_ptr<SigManager> sigManager;
  PreProcessorRecorder preProcessorRecorder;
};

TEST_F(PreProcessReplyMsgTestFixture, getResultHashSignature) {
  ASSERT_TRUE(sigManager);
  const NodeIdType senderId = 1;
  const uint16_t clientId = 1;
  const uint16_t reqOffsetInBatch = 0;
  const uint64_t reqSeqNum = 100;
  const uint64_t reqRetryId = 0;
  const char preProcessResultBuf[] = "request body";
  const uint32_t preProcessResultBufLen = sizeof(preProcessResultBuf);
  const std::string& cid = "";
  const ReplyStatus status = STATUS_GOOD;
  const OperationResult opResult = SUCCESS;
  auto preProcessReplyMsg = PreProcessReplyMsg(senderId,
                                               clientId,
                                               reqOffsetInBatch,
                                               reqSeqNum,
                                               reqRetryId,
                                               preProcessResultBuf,
                                               preProcessResultBufLen,
                                               cid,
                                               status,
                                               opResult);
  const auto hash =
      PreProcessResultHashCreator::create(preProcessResultBuf, preProcessResultBufLen, opResult, clientId, reqSeqNum);
  auto expected_signature = std::vector<char>(sigManager->getMySigLength(), 0);
  sigManager->sign((char*)hash.data(), sizeof(hash), expected_signature.data(), expected_signature.size());
  EXPECT_THAT(expected_signature, testing::ContainerEq(preProcessReplyMsg.getResultHashSignature()));
}

}  // namespace
