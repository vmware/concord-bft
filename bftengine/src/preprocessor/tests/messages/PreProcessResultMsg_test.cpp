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
#include "messages/PreProcessResultMsg.hpp"
#include "messages/PreProcessResultHashCreator.hpp"
#include "helper.hpp"
#include "RequestProcessingState.hpp"
#include "Replica.hpp"

namespace {
using namespace bftEngine::impl;
using namespace bftEngine;
using namespace preprocessor;

bftEngine::ReplicaConfig& createReplicaConfigWithExtClient(uint16_t fVal,
                                                           uint16_t cVal,
                                                           bool transactionSigningEnabled) {
  auto& config = createReplicaConfig(fVal, cVal);
  config.numOfExternalClients = 1;
  config.numOfClientProxies = 1;

  auto clientPrincipalIds = std::set<uint16_t>{uint16_t(config.numReplicas + 1)};
  config.publicKeysOfClients.insert(make_pair(config.publicKeysOfReplicas.begin()->second, clientPrincipalIds));
  config.clientTransactionSigningEnabled = transactionSigningEnabled;
  return config;
}

bftEngine::impl::SigManager* createSigManagerWithSigning(
    size_t myId,
    std::string& myPrivateKey,
    concord::util::crypto::KeyFormat replicasKeysFormat,
    std::set<std::pair<uint16_t, const std::string>>& publicKeysOfReplicas,
    const std::set<std::pair<const std::string, std::set<uint16_t>>>* publicKeysOfClients,
    ReplicasInfo& replicasInfo) {
  return SigManager::init(myId,
                          myPrivateKey,
                          publicKeysOfReplicas,
                          replicasKeysFormat,
                          publicKeysOfClients,
                          concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat,
                          replicasInfo);
}

// Default message params used in the tests
struct MsgParams {
  NodeIdType senderId = 5u;
  const uint16_t clientId = 32;
  uint64_t reqSeqNum = 100u;
  const char result[12] = {"result body"};
  const uint64_t requestTimeoutMilli = 0;
  const std::string correlationId = "correlationId";
  const char rawSpanContext[14] = {"span_\0context"};  // make clang-tidy happy
  const std::string spanContext{rawSpanContext};
};

std::pair<std::string, std::vector<char>> getProcessResultSigBuff(const std::unique_ptr<SigManager>& sigManager,
                                                                  const MsgParams& p,
                                                                  const int sigCount) {
  auto msgSigSize = sigManager->getMySigLength();
  std::vector<char> msgSig(msgSigSize, 0);
  auto hash = PreProcessResultHashCreator::create(
      p.result, sizeof(p.result), OperationResult::SUCCESS, p.senderId, p.reqSeqNum);
  sigManager->sign(reinterpret_cast<const char*>(hash.data()), hash.size(), msgSig.data(), msgSigSize);

  // for simplicity, copy the same signatures
  std::set<PreProcessResultSignature> resultSigs;
  for (int i = 0; i < sigCount; i++) {
    resultSigs.emplace(std::vector<char>(msgSig), i, OperationResult::SUCCESS);
  }
  return std::make_pair(PreProcessResultSignature::serializeResultSignatures(resultSigs, sigCount), msgSig);
}

void checkMessageSanityCheck(std::unique_ptr<preprocessor::PreProcessResultMsg>& msg,
                             MsgParams& params,
                             const ReplicasInfo& repInfo,
                             bool transactionSigningEnabled) {
  EXPECT_EQ(msg->clientProxyId(), params.senderId);
  EXPECT_EQ(msg->flags(), MsgFlag::HAS_PRE_PROCESSED_FLAG);
  EXPECT_EQ(msg->requestSeqNum(), params.reqSeqNum);
  EXPECT_EQ(msg->requestLength(), sizeof(params.result));
  if (transactionSigningEnabled) {
    EXPECT_NE(msg->requestSignatureLength(), 0);
    EXPECT_NE(msg->requestSignature(), nullptr);
  } else {
    EXPECT_EQ(msg->requestSignatureLength(), 0);
    EXPECT_EQ(msg->requestSignature(), nullptr);
  }
  EXPECT_NE(msg->requestBuf(), params.result);
  EXPECT_TRUE(std::memcmp(msg->requestBuf(), params.result, sizeof(params.result)) == 0u);
  EXPECT_EQ(msg->getCid(), params.correlationId);
  EXPECT_EQ(msg->spanContext<ClientRequestMsg>().data(), params.spanContext);
  EXPECT_EQ(msg->requestTimeoutMilli(), params.requestTimeoutMilli);
  EXPECT_NO_THROW(msg->validate(repInfo));
}

class PreProcessResultMsgTestFixture : public testing::Test {
 protected:
  PreProcessResultMsgTestFixture()
      : config{createReplicaConfigWithExtClient(1, 0, true)},
        replicaInfo{config, false, false},
        sigManager(createSigManagerWithSigning(config.replicaId,
                                               config.replicaPrivateKey,
                                               concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat,
                                               config.publicKeysOfReplicas,
                                               &config.publicKeysOfClients,
                                               replicaInfo)) {}
  ReplicaConfig& config;
  ReplicasInfo replicaInfo;
  std::unique_ptr<SigManager> sigManager;

  std::unique_ptr<PreProcessResultMsg> createMessage(const MsgParams& p, const int sigCount) {
    auto resultSigsBuf = getProcessResultSigBuff(sigManager, p, sigCount);

    auto msgSigSize = sigManager->getMySigLength();
    std::vector<char> msgSig(msgSigSize, 0);

    auto hash = PreProcessResultHashCreator::create(
        p.result, sizeof(p.result), OperationResult::SUCCESS, p.senderId, p.reqSeqNum);
    sigManager->sign(reinterpret_cast<const char*>(hash.data()), hash.size(), msgSig.data(), msgSigSize);

    // For simplicity, copy the same signatures
    std::list<PreProcessResultSignature> resultSigs;
    for (int i = 0; i < sigCount; i++) {
      resultSigs.emplace_back(std::vector<char>(msgSig), i, OperationResult::SUCCESS);
    }

    return std::make_unique<PreProcessResultMsg>(p.senderId,
                                                 0,
                                                 p.reqSeqNum,
                                                 sizeof(p.result),
                                                 p.result,
                                                 p.requestTimeoutMilli,
                                                 p.correlationId,
                                                 concordUtils::SpanContext{p.spanContext},
                                                 resultSigsBuf.second.data(),
                                                 resultSigsBuf.second.size(),
                                                 resultSigsBuf.first);
  }

  void messageSanityCheck(std::unique_ptr<preprocessor::PreProcessResultMsg>& msg, MsgParams& params) {
    checkMessageSanityCheck(msg, params, replicaInfo, config.clientTransactionSigningEnabled);
  }
};

class PreProcessResultMsgTxSigningOffTestFixture : public testing::Test {
 protected:
  PreProcessResultMsgTxSigningOffTestFixture()
      : config{createReplicaConfigWithExtClient(1, 0, false)},
        replicaInfo{config, false, false},
        sigManager(createSigManagerWithSigning(config.replicaId,
                                               config.replicaPrivateKey,
                                               concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat,
                                               config.publicKeysOfReplicas,
                                               &config.publicKeysOfClients,
                                               replicaInfo)) {}

  ReplicaConfig& config;
  ReplicasInfo replicaInfo;
  std::unique_ptr<SigManager> sigManager;

  std::unique_ptr<PreProcessResultMsg> createMessage(const MsgParams& p, const int sigCount) {
    auto resultSigsBuf = getProcessResultSigBuff(sigManager, p, sigCount);

    return std::make_unique<PreProcessResultMsg>(p.senderId,
                                                 0,
                                                 p.reqSeqNum,
                                                 sizeof(p.result),
                                                 p.result,
                                                 p.requestTimeoutMilli,
                                                 p.correlationId,
                                                 concordUtils::SpanContext{p.spanContext},
                                                 nullptr,
                                                 0,
                                                 resultSigsBuf.first);
  }
  void messageSanityCheck(std::unique_ptr<preprocessor::PreProcessResultMsg>& msg, MsgParams& params) {
    checkMessageSanityCheck(msg, params, replicaInfo, config.clientTransactionSigningEnabled);
  }
};

TEST_F(PreProcessResultMsgTestFixture, ClientRequestMsgSanityChecks) {
  MsgParams params;
  auto msg = createMessage(params, replicaInfo.getNumberOfReplicas());
  messageSanityCheck(msg, params);

  // check message type
  MessageBase::Header* hdr = (MessageBase::Header*)msg->body();
  EXPECT_EQ(hdr->msgType, MsgCode::PreProcessResult);
}

TEST_F(PreProcessResultMsgTestFixture, SignatureDeserialization) {
  std::vector<char> msgSig{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14};

  std::set<PreProcessResultSignature> resultSigs;
  for (int i = 0; i < replicaInfo.getNumberOfReplicas(); i++) {
    resultSigs.emplace(std::vector<char>(msgSig), i, OperationResult::SUCCESS);
  }
  auto resultSigsBuf =
      PreProcessResultSignature::serializeResultSignatures(resultSigs, replicaInfo.getNumberOfReplicas());
  auto deserialized =
      PreProcessResultSignature::deserializeResultSignatures(resultSigsBuf.data(), resultSigsBuf.size());
  EXPECT_THAT(resultSigs, deserialized);
}

TEST_F(PreProcessResultMsgTestFixture, ShrinkSignaturesToSize) {
  std::vector<char> msgSig{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14};

  std::set<PreProcessResultSignature> resultSigs;
  auto numReplies = 7;
  auto numRepliesNeeded = 4;
  for (int i = 0; i < numReplies; i++) {
    resultSigs.emplace(std::vector<char>(msgSig), i, OperationResult::SUCCESS);
  }

  auto resultSigsBuf = PreProcessResultSignature::serializeResultSignatures(resultSigs, numRepliesNeeded);
  auto deserialized =
      PreProcessResultSignature::deserializeResultSignatures(resultSigsBuf.data(), resultSigsBuf.size());
  EXPECT_THAT(deserialized.size(), numRepliesNeeded);
}

TEST_F(PreProcessResultMsgTestFixture, MsgDeserialisation) {
  MsgParams params;
  auto serialised = createMessage(params, replicaInfo.getNumberOfReplicas());
  auto msg = std::make_unique<PreProcessResultMsg>((ClientRequestMsgHeader*)serialised->body());

  messageSanityCheck(msg, params);

  // check message type
  MessageBase::Header* hdr = (MessageBase::Header*)msg->body();
  EXPECT_EQ(hdr->msgType, MsgCode::PreProcessResult);

  // check the result is correct
  ASSERT_EQ(sizeof(params.result), msg->requestLength());
  for (int i = 0; i < msg->requestLength(); i++) {
    ASSERT_EQ(params.result[i], msg->requestBuf()[i]);
  }

  // Deserialise result signatures
  auto [sigBuf, sigBufLen] = msg->getResultSignaturesBuf();
  auto sigs = PreProcessResultSignature::deserializeResultSignatures(sigBuf, sigBufLen);

  // Verify the signatures - SigManager can't verify its own signature so first the result
  // from the message is signed, and then it is compared to each signature in the message
  auto msgSigSize = sigManager->getMySigLength();
  std::vector<char> msgSig(msgSigSize, 0);
  auto hash = PreProcessResultHashCreator::create(
      msg->requestBuf(), msg->requestLength(), OperationResult::SUCCESS, params.senderId, params.reqSeqNum);
  sigManager->sign(reinterpret_cast<char*>(hash.data()), hash.size(), msgSig.data(), msgSigSize);
  auto i = 0;
  for (const auto& s : sigs) {
    ASSERT_EQ(s.sender_replica, i++);
    EXPECT_THAT(msgSig, s.signature);
  }
}

TEST_F(PreProcessResultMsgTestFixture, MsgDeserialisationFromBase) {
  MsgParams params;
  auto serialised = createMessage(params, replicaInfo.getNumberOfReplicas());
  auto msg = std::make_unique<PreProcessResultMsg>((MessageBase*)serialised.get());
  messageSanityCheck(msg, params);

  // check message type
  MessageBase::Header* hdr = (MessageBase::Header*)msg->body();
  EXPECT_EQ(hdr->msgType, MsgCode::PreProcessResult);

  // check the result is correct
  ASSERT_EQ(sizeof(params.result), msg->requestLength());
  for (int i = 0; i < msg->requestLength(); i++) {
    ASSERT_EQ(params.result[i], msg->requestBuf()[i]);
  }

  // Deserialise result signatures
  auto [sigBuf, sigBufLen] = msg->getResultSignaturesBuf();
  auto sigs = PreProcessResultSignature::deserializeResultSignatures(sigBuf, sigBufLen);

  // Verify the signatures - SigManager can't verify its own signature so first the result
  // from the message is signed, and then it is compared to each signature in the message
  auto msgSigSize = sigManager->getMySigLength();
  std::vector<char> msgSig(msgSigSize, 0);
  auto hash = PreProcessResultHashCreator::create(
      msg->requestBuf(), msg->requestLength(), OperationResult::SUCCESS, params.senderId, params.reqSeqNum);
  sigManager->sign(reinterpret_cast<char*>(hash.data()), hash.size(), msgSig.data(), msgSigSize);
  auto i = 0;
  for (const auto& s : sigs) {
    ASSERT_EQ(s.sender_replica, i++);
    EXPECT_THAT(msgSig, s.signature);
  }
}

TEST_F(PreProcessResultMsgTestFixture, MsgWithTooMuchSigs) {
  MsgParams params;
  auto serialised = createMessage(params, config.fVal);

  auto m = std::make_unique<PreProcessResultMsg>((MessageBase*)serialised.get());
  auto res = m->validatePreProcessResultSignatures(config.replicaId, config.fVal);

  std::stringstream ss;
  ss << "expectedSignatureCount: " << config.fVal + 1 << ", sigs.size(): " << config.fVal;
  printf("%s\n", res->c_str());
  EXPECT_TRUE(res && res->find(ss.str()) != std::string::npos);
}

TEST_F(PreProcessResultMsgTxSigningOffTestFixture, ClientRequestMsgSanityChecks) {
  MsgParams params;
  auto msg = createMessage(params, replicaInfo.getNumberOfReplicas());
  messageSanityCheck(msg, params);

  // check message type
  MessageBase::Header* hdr = (MessageBase::Header*)msg->body();
  EXPECT_EQ(hdr->msgType, MsgCode::PreProcessResult);
}
}  // namespace