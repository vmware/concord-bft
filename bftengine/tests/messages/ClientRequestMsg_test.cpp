// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <cstring>
#include "helper.hpp"
#include "gtest/gtest.h"
#include "messages/ClientRequestMsg.hpp"
#include "bftengine/ClientMsgs.hpp"

using namespace bftEngine;
using namespace bftEngine::impl;

class ClientRequestMsgTestFixture : public ::testing::Test {
 public:
  ClientRequestMsgTestFixture()
      : config{createReplicaConfig()},
        replicaInfo(config, false, false),
        sigManager(createSigManager(config.replicaId,
                                    config.replicaPrivateKey,
                                    KeyFormat::HexaDecimalStrippedFormat,
                                    config.publicKeysOfReplicas)) {}

  ReplicaConfig& config;
  ReplicasInfo replicaInfo;
  std::unique_ptr<SigManager> sigManager;
};

TEST_F(ClientRequestMsgTestFixture, create_and_compare) {
  NodeIdType senderId = 1u;
  uint8_t flags = 'F';
  uint64_t reqSeqNum = 100u;
  const char request[] = {"request body"};
  const uint64_t requestTimeoutMilli = 0;
  const std::string correlationId = "correlationId";
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  ClientRequestMsg msg(senderId,
                       flags,
                       reqSeqNum,
                       sizeof(request),
                       request,
                       requestTimeoutMilli,
                       correlationId,
                       concordUtils::SpanContext{spanContext});

  EXPECT_EQ(msg.clientProxyId(), senderId);
  EXPECT_EQ(msg.flags(), flags);
  EXPECT_EQ(msg.requestSeqNum(), reqSeqNum);
  EXPECT_EQ(msg.requestLength(), sizeof(request));
  EXPECT_NE(msg.requestBuf(), request);
  EXPECT_TRUE(std::memcmp(msg.requestBuf(), request, sizeof(request)) == 0u);
  EXPECT_EQ(msg.getCid(), correlationId);
  EXPECT_EQ(msg.spanContext<ClientRequestMsg>().data(), spanContext);
  EXPECT_EQ(msg.requestTimeoutMilli(), requestTimeoutMilli);
  EXPECT_NO_THROW(msg.validate(replicaInfo));
}

TEST_F(ClientRequestMsgTestFixture, create_and_compare_with_empty_span) {
  NodeIdType senderId = 1u;
  uint8_t flags = 'F';
  uint64_t reqSeqNum = 100u;
  const char request[] = {"request body"};
  const uint64_t requestTimeoutMilli = 0;
  const std::string correlationId = "correlationId";
  const char rawSpanContext[] = {""};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  ClientRequestMsg msg(senderId,
                       flags,
                       reqSeqNum,
                       sizeof(request),
                       request,
                       requestTimeoutMilli,
                       correlationId,
                       concordUtils::SpanContext{spanContext});

  EXPECT_EQ(msg.clientProxyId(), senderId);
  EXPECT_EQ(msg.flags(), flags);
  EXPECT_EQ(msg.requestSeqNum(), reqSeqNum);
  EXPECT_EQ(msg.requestLength(), sizeof(request));
  EXPECT_NE(msg.requestBuf(), request);
  EXPECT_TRUE(std::memcmp(msg.requestBuf(), request, sizeof(request)) == 0u);
  EXPECT_EQ(msg.getCid(), correlationId);
  EXPECT_EQ(msg.spanContext<ClientRequestMsg>().data(), spanContext);
  EXPECT_NO_THROW(msg.validate(replicaInfo));
}

TEST_F(ClientRequestMsgTestFixture, create_and_compare_with_empty_cid) {
  NodeIdType senderId = 1u;
  uint8_t flags = 'F';
  uint64_t reqSeqNum = 100u;
  const char request[] = {"request body"};
  const uint64_t requestTimeoutMilli = 0;
  const std::string correlationId = "";
  const char rawSpanContext[] = {""};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  ClientRequestMsg msg(senderId,
                       flags,
                       reqSeqNum,
                       sizeof(request),
                       request,
                       requestTimeoutMilli,
                       correlationId,
                       concordUtils::SpanContext{spanContext});

  EXPECT_EQ(msg.clientProxyId(), senderId);
  EXPECT_EQ(msg.flags(), flags);
  EXPECT_EQ(msg.requestSeqNum(), reqSeqNum);
  EXPECT_EQ(msg.requestLength(), sizeof(request));
  EXPECT_NE(msg.requestBuf(), request);
  EXPECT_TRUE(std::memcmp(msg.requestBuf(), request, sizeof(request)) == 0u);
  EXPECT_EQ(msg.getCid(), correlationId);
  EXPECT_EQ(msg.spanContext<ClientRequestMsg>().data(), spanContext);
  EXPECT_EQ(msg.requestTimeoutMilli(), requestTimeoutMilli);
  EXPECT_NO_THROW(msg.validate(replicaInfo));
}

TEST_F(ClientRequestMsgTestFixture, create_from_buffer) {
  NodeIdType senderId = 1u;
  uint8_t flags = 'F';
  uint64_t reqSeqNum = 100u;
  const char request[] = {"request body"};
  const uint64_t requestTimeoutMilli = 0;
  const std::string correlationId = "correlationId";
  const char rawSpanContext[] = {""};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  ClientRequestMsg originalMsg(senderId,
                               flags,
                               reqSeqNum,
                               sizeof(request),
                               request,
                               requestTimeoutMilli,
                               correlationId,
                               concordUtils::SpanContext{spanContext});

  ClientRequestMsg copy_msg((ClientRequestMsgHeader*)originalMsg.body());

  EXPECT_EQ(originalMsg.clientProxyId(), copy_msg.clientProxyId());
  EXPECT_EQ(originalMsg.flags(), copy_msg.flags());
  EXPECT_EQ(originalMsg.requestSeqNum(), copy_msg.requestSeqNum());
  EXPECT_EQ(originalMsg.requestLength(), copy_msg.requestLength());
  EXPECT_EQ(originalMsg.requestBuf(), copy_msg.requestBuf());
  EXPECT_TRUE(std::memcmp(originalMsg.requestBuf(), copy_msg.requestBuf(), sizeof(request)) == 0u);
  EXPECT_EQ(originalMsg.getCid(), copy_msg.getCid());
  EXPECT_EQ(originalMsg.spanContext<ClientRequestMsg>().data(), copy_msg.spanContext<ClientRequestMsg>().data());
  EXPECT_EQ(originalMsg.requestTimeoutMilli(), requestTimeoutMilli);
  EXPECT_NO_THROW(originalMsg.validate(replicaInfo));
}

TEST_F(ClientRequestMsgTestFixture, base_methods) {
  NodeIdType senderId = 1u;
  uint8_t flags = 'F';
  uint64_t reqSeqNum = 100u;
  const char request[] = {"request body"};
  const uint64_t requestTimeoutMilli = 0;
  const std::string correlationId = "correlationId";
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  ClientRequestMsg msg(senderId,
                       flags,
                       reqSeqNum,
                       sizeof(request),
                       request,
                       requestTimeoutMilli,
                       correlationId,
                       concordUtils::SpanContext{spanContext});
  EXPECT_NO_THROW(msg.validate(replicaInfo));
  testMessageBaseMethods(msg, MsgCode::ClientRequest, senderId, spanContext);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
