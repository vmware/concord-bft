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
#include "helper.hpp"
#include "RequestProcessingState.hpp"
#include "Replica.hpp"

namespace {
using namespace bftEngine::impl;
using namespace bftEngine;
using namespace preprocessor;

bftEngine::ReplicaConfig& createReplicaConfigWithExtClient(uint16_t fVal, uint16_t cVal) {
  auto& config = createReplicaConfig(fVal, cVal);
  config.numOfExternalClients = 1;
  config.numOfClientProxies = 1;

  auto clientPrincipalIds = std::set<uint16_t>{uint16_t(config.numReplicas + 1)};
  config.publicKeysOfClients.insert(make_pair(config.publicKeysOfReplicas.begin()->second, clientPrincipalIds));
  return config;
}

bftEngine::impl::SigManager* createSigManagerWithSigning(
    size_t myId,
    std::string& myPrivateKey,
    KeyFormat replicasKeysFormat,
    std::set<std::pair<uint16_t, const std::string>>& publicKeysOfReplicas,
    const std::set<std::pair<const std::string, std::set<uint16_t>>>* publicKeysOfClients,
    ReplicasInfo& replicasInfo) {
  return SigManager::init(myId,
                          myPrivateKey,
                          publicKeysOfReplicas,
                          replicasKeysFormat,
                          publicKeysOfClients,
                          KeyFormat::HexaDecimalStrippedFormat,
                          replicasInfo);
}

// Some default message params used through the test
struct MsgParams {
  NodeIdType senderId = 5u;
  uint64_t reqSeqNum = 100u;
  const char result[12] = {"result body"};
  const uint64_t requestTimeoutMilli = 0;
  const std::string correlationId = "correlationId";
  const char rawSpanContext[14] = {"span_\0context"};  // make clnag-tidy happy
  const std::string spanContext{rawSpanContext};
};

class PreProcessResultMsgTestFixture : public testing::Test {
 protected:
  PreProcessResultMsgTestFixture()
      : config{createReplicaConfigWithExtClient(1, 0)},
        replicaInfo{config, false, false},
        sigManager(createSigManagerWithSigning(config.replicaId,
                                               config.replicaPrivateKey,
                                               KeyFormat::HexaDecimalStrippedFormat,
                                               config.publicKeysOfReplicas,
                                               &config.publicKeysOfClients,
                                               replicaInfo)) {}

  ReplicaConfig& config;
  ReplicasInfo replicaInfo;
  std::unique_ptr<SigManager> sigManager;

  std::unique_ptr<PreProcessResultMsg> createMessage(const MsgParams& p, const int sigCount, bool duplicateSigs) {
    auto msgSigSize = sigManager->getMySigLength();
    std::vector<char> msgSig(msgSigSize, 0);
    auto hash = concord::util::SHA3_256().digest(p.result, sizeof(p.result));
    sigManager->sign(reinterpret_cast<const char*>(hash.data()), hash.size(), msgSig.data(), msgSigSize);

    // for simplicity, copy the same signatures
    std::list<PreProcessResultSignature> resultSigs;
    for (int i = 0; i < sigCount; i++) {
      resultSigs.emplace_back(std::vector<char>(msgSig), duplicateSigs ? 0 : i);
    }
    auto resultSigsBuf = PreProcessResultSignature::serializeResultSignatureList(resultSigs);

    return std::make_unique<PreProcessResultMsg>(p.senderId,
                                                 p.reqSeqNum,
                                                 sizeof(p.result),
                                                 p.result,
                                                 p.requestTimeoutMilli,
                                                 p.correlationId,
                                                 concordUtils::SpanContext{p.spanContext},
                                                 msgSig.data(),
                                                 msgSig.size(),
                                                 resultSigsBuf);
  }

  void messageSanityCheck(std::unique_ptr<preprocessor::PreProcessResultMsg>& m, MsgParams& p) {
    EXPECT_EQ(m->clientProxyId(), p.senderId);
    EXPECT_EQ(m->flags(), MsgFlag::HAS_PRE_PROCESSED_FLAG);
    EXPECT_EQ(m->requestSeqNum(), p.reqSeqNum);
    EXPECT_EQ(m->requestLength(), sizeof(p.result));
    EXPECT_NE(m->requestBuf(), p.result);
    EXPECT_TRUE(std::memcmp(m->requestBuf(), p.result, sizeof(p.result)) == 0u);
    EXPECT_EQ(m->getCid(), p.correlationId);
    EXPECT_EQ(m->spanContext<ClientRequestMsg>().data(), p.spanContext);
    EXPECT_EQ(m->requestTimeoutMilli(), p.requestTimeoutMilli);
    EXPECT_NO_THROW(m->validate(replicaInfo));
  }
};

TEST_F(PreProcessResultMsgTestFixture, ClientRequestMsgSanityChecks) {
  MsgParams p;
  auto m = createMessage(p, replicaInfo.getNumberOfReplicas(), false /* duplicateSigs */);
  messageSanityCheck(m, p);

  // check message type
  MessageBase::Header* hdr = (MessageBase::Header*)m->body();
  EXPECT_EQ(hdr->msgType, MsgCode::PreProcessResult);
}

TEST_F(PreProcessResultMsgTestFixture, SignatureDeserialisation) {
  std::vector<char> msgSig{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14};

  std::list<PreProcessResultSignature> resultSigs;
  for (int i = 0; i < replicaInfo.getNumberOfReplicas(); i++) {
    resultSigs.emplace_back(std::vector<char>(msgSig), 0);
  }
  auto resultSigsBuf = PreProcessResultSignature::serializeResultSignatureList(resultSigs);
  auto deserialized =
      PreProcessResultSignature::deserializeResultSignatureList(resultSigsBuf.data(), resultSigsBuf.size());
  EXPECT_THAT(resultSigs, deserialized);
}

TEST_F(PreProcessResultMsgTestFixture, MsgDeserialisation) {
  MsgParams p;
  auto serialised = createMessage(p, replicaInfo.getNumberOfReplicas(), true /* duplicateSigs */);
  auto m = std::make_unique<PreProcessResultMsg>((ClientRequestMsgHeader*)serialised->body());

  messageSanityCheck(m, p);

  // check message type
  MessageBase::Header* hdr = (MessageBase::Header*)m->body();
  EXPECT_EQ(hdr->msgType, MsgCode::PreProcessResult);

  // check the result is correct
  ASSERT_EQ(sizeof(p.result), m->requestLength());
  for (int i = 0; i < m->requestLength(); i++) {
    ASSERT_EQ(p.result[i], m->requestBuf()[i]);
  }

  // Deserialise result signatures
  auto [sigBuf, sigBufLen] = m->getResultSignaturesBuf();
  auto sigs = PreProcessResultSignature::deserializeResultSignatureList(sigBuf, sigBufLen);

  // Verify the signatures - SigManager can't veryfy its own signature so first the result
  // from the message is signed and then it is compared to each signature in the message
  auto msgSigSize = sigManager->getMySigLength();
  std::vector<char> msgSig(msgSigSize, 0);
  auto hash = concord::util::SHA3_256().digest(m->requestBuf(), m->requestLength());
  sigManager->sign(reinterpret_cast<char*>(hash.data()), hash.size(), msgSig.data(), msgSigSize);
  for (const auto& s : sigs) {
    ASSERT_EQ(s.sender_replica, 0);
    EXPECT_THAT(msgSig, s.signature);
  }
}

TEST_F(PreProcessResultMsgTestFixture, MsgDeserialisationFromBase) {
  MsgParams p;
  auto serialised = createMessage(p, replicaInfo.getNumberOfReplicas(), true /* duplicateSigs */);
  auto m = std::make_unique<PreProcessResultMsg>((MessageBase*)serialised.get());
  messageSanityCheck(m, p);

  // check message type
  MessageBase::Header* hdr = (MessageBase::Header*)m->body();
  EXPECT_EQ(hdr->msgType, MsgCode::PreProcessResult);

  // check the result is correct
  ASSERT_EQ(sizeof(p.result), m->requestLength());
  for (int i = 0; i < m->requestLength(); i++) {
    ASSERT_EQ(p.result[i], m->requestBuf()[i]);
  }

  // Deserialise result signatures
  auto [sigBuf, sigBufLen] = m->getResultSignaturesBuf();
  auto sigs = PreProcessResultSignature::deserializeResultSignatureList(sigBuf, sigBufLen);

  // Verify the signatures - SigManager can't veryfy its own signature so first the result
  // from the message is signed and then it is compared to each signature in the message
  auto msgSigSize = sigManager->getMySigLength();
  std::vector<char> msgSig(msgSigSize, 0);
  auto hash = concord::util::SHA3_256().digest(m->requestBuf(), m->requestLength());
  sigManager->sign(reinterpret_cast<char*>(hash.data()), hash.size(), msgSig.data(), msgSigSize);
  for (const auto& s : sigs) {
    ASSERT_EQ(s.sender_replica, 0);
    EXPECT_THAT(msgSig, s.signature);
  }
}

TEST_F(PreProcessResultMsgTestFixture, MsgWithTooMuchSigs) {
  MsgParams p;
  auto serialised = createMessage(p, config.fVal, false /* duplicateSigs */);

  auto m = std::make_unique<PreProcessResultMsg>((MessageBase*)serialised.get());
  auto res = m->validatePreProcessResultSignatures(config.replicaId, config.fVal);

  std::stringstream ss;
  ss << "unexpected number of signatures received. Expected " << config.fVal + 1 << " got " << config.fVal;
  EXPECT_TRUE(res && res->find(ss.str()) != std::string::npos);
}

TEST_F(PreProcessResultMsgTestFixture, MsgWithDuplicatedSigs) {
  MsgParams p;
  auto serialised = createMessage(p, config.fVal + 1, true /* duplicateSigs */);

  auto m = std::make_unique<PreProcessResultMsg>((MessageBase*)serialised.get());
  auto res = m->validatePreProcessResultSignatures(config.replicaId, config.fVal);

  std::stringstream ss;
  ss << "got more than one signatures with the same sender id";
  EXPECT_TRUE(res);
  EXPECT_TRUE(res->find(ss.str()) != std::string::npos);
}

}  // namespace