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

void clearDiagnosticsHandlers() {
  auto& registrar = concord::diagnostics::RegistrarSingleton::getInstance();
  registrar.perf.clear();
  registrar.status.clear();
}

TEST_F(PreProcessReplyMsgTestFixture, verifyMessageParameters) {
  ASSERT_TRUE(sigManager);
  const NodeIdType senderId = 1;
  const uint16_t clientId = 1;
  const uint16_t reqOffsetInBatch = 2;
  const uint64_t reqSeqNum = 100;
  const uint64_t reqRetryId = 5;
  const char preProcessResultBuf[] = "Request body";
  const uint32_t preProcessResultBufLen = sizeof(preProcessResultBuf);
  const std::string& cid = "abcdef1";
  const ReplyStatus status = STATUS_FAILED;
  const OperationResult opResult = OperationResult::EXEC_DATA_TOO_LARGE;
  ViewNum viewNum = 1;
  auto preProcessReplyMsg = PreProcessReplyMsg(senderId,
                                               clientId,
                                               reqOffsetInBatch,
                                               reqSeqNum,
                                               reqRetryId,
                                               preProcessResultBuf,
                                               preProcessResultBufLen,
                                               cid,
                                               status,
                                               opResult,
                                               viewNum);
  EXPECT_EQ(senderId, preProcessReplyMsg.senderId());
  EXPECT_EQ(clientId, preProcessReplyMsg.clientId());
  EXPECT_EQ(reqOffsetInBatch, preProcessReplyMsg.reqOffsetInBatch());
  EXPECT_EQ(reqSeqNum, preProcessReplyMsg.reqSeqNum());
  EXPECT_EQ(reqRetryId, preProcessReplyMsg.reqRetryId());
  EXPECT_EQ(cid, preProcessReplyMsg.getCid());
  EXPECT_EQ(status, static_cast<uint32_t>(preProcessReplyMsg.status()));
  EXPECT_EQ(opResult, preProcessReplyMsg.preProcessResult());
  clearDiagnosticsHandlers();
}

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
  const OperationResult opResult = OperationResult::SUCCESS;
  ViewNum viewNum = 1;
  auto preProcessReplyMsg = PreProcessReplyMsg(senderId,
                                               clientId,
                                               reqOffsetInBatch,
                                               reqSeqNum,
                                               reqRetryId,
                                               preProcessResultBuf,
                                               preProcessResultBufLen,
                                               cid,
                                               status,
                                               opResult,
                                               viewNum);
  const auto hash =
      PreProcessResultHashCreator::create(preProcessResultBuf, preProcessResultBufLen, opResult, clientId, reqSeqNum);
  auto expected_signature = std::vector<char>(sigManager->getMySigLength(), 0);
  sigManager->sign((char*)hash.data(), sizeof(hash), expected_signature.data());
  EXPECT_THAT(expected_signature, testing::ContainerEq(preProcessReplyMsg.getResultHashSignature()));
  clearDiagnosticsHandlers();
}

}  // namespace
