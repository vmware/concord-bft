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

#include <cstdlib>
#include <cstring>
#include <chrono>
#include "Replica.hpp"
#include "helper.hpp"
#include "gtest/gtest.h"
#include "messages/ClientRequestMsg.hpp"
#include "bftengine/ClientMsgs.hpp"
#include "serialize.hpp"

using namespace bftEngine;
using namespace bftEngine::impl;

class ClientRequestMsgTestFixture : public ::testing::Test {
 public:
  ClientRequestMsgTestFixture()
      : config{createReplicaConfig()},
        replicaInfo(config, false, false),
        sigManager(createSigManager(config.replicaId,
                                    config.replicaPrivateKey,
                                    concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat,
                                    config.publicKeysOfReplicas,
                                    replicaInfo)) {}

  ReplicaConfig& config;
  ReplicasInfo replicaInfo;
  std::unique_ptr<SigManager> sigManager;
};

TEST_F(ClientRequestMsgTestFixture, create_and_compare) {
  NodeIdType senderId = 1u;
  uint64_t flags = 'F';
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
                       0,
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
  uint64_t flags = 'F';
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
                       0,
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
  uint64_t flags = 'F';
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
                       0,
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
  uint64_t flags = 'F';
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
                               0,
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

TEST_F(ClientRequestMsgTestFixture, test_with_timestamp) {
  NodeIdType senderId = 1u;
  uint64_t flags = MsgFlag::TIME_SERVICE_FLAG;
  uint64_t reqSeqNum = 100u;
  auto millis = std::chrono::system_clock::now().time_since_epoch();
  auto request = concord::util::serialize(millis.count());
  const uint64_t requestTimeoutMilli = 0;
  const std::string correlationId = "correlationId";
  const char rawSpanContext[] = {""};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  ClientRequestMsg originalMsg(senderId,
                               flags,
                               reqSeqNum,
                               request.size(),
                               request.data(),
                               requestTimeoutMilli,
                               correlationId,
                               0,
                               concordUtils::SpanContext{spanContext});

  ClientRequestMsg copy_msg((ClientRequestMsgHeader*)originalMsg.body());

  EXPECT_EQ(originalMsg.clientProxyId(), copy_msg.clientProxyId());
  EXPECT_EQ(originalMsg.flags(), copy_msg.flags());
  EXPECT_EQ(originalMsg.requestSeqNum(), copy_msg.requestSeqNum());
  EXPECT_EQ(originalMsg.requestLength(), copy_msg.requestLength());
  EXPECT_EQ(originalMsg.requestBuf(), copy_msg.requestBuf());
  EXPECT_TRUE(std::memcmp(originalMsg.requestBuf(), copy_msg.requestBuf(), request.size()) == 0u);
  EXPECT_EQ(originalMsg.getCid(), copy_msg.getCid());
  EXPECT_EQ(originalMsg.spanContext<ClientRequestMsg>().data(), copy_msg.spanContext<ClientRequestMsg>().data());
  EXPECT_EQ(originalMsg.requestTimeoutMilli(), requestTimeoutMilli);

  EXPECT_EQ(concord::util::deserialize<std::chrono::milliseconds::rep>(
                originalMsg.requestBuf(), originalMsg.requestBuf() + originalMsg.requestLength()),
            millis.count());
  EXPECT_NO_THROW(originalMsg.validate(replicaInfo));
}

TEST_F(ClientRequestMsgTestFixture, base_methods) {
  NodeIdType senderId = 1u;
  uint64_t flags = 'F';
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
                       0,
                       concordUtils::SpanContext{spanContext});
  EXPECT_NO_THROW(msg.validate(replicaInfo));
  testMessageBaseMethods(msg, MsgCode::ClientRequest, senderId, spanContext);
}

TEST_F(ClientRequestMsgTestFixture, extra_buffer) {
  // Inherit from ClientRequestMsg so that some protected methods can be exposed for the test
  struct TestClientRequestMsg : ClientRequestMsg {
    TestClientRequestMsg(NodeIdType sender,
                         uint64_t flags,
                         uint64_t reqSeqNum,
                         uint32_t requestLength,
                         const char* request,
                         uint64_t reqTimeoutMilli,
                         const std::string& cid,
                         const concordUtils::SpanContext& spanContext,
                         const char* requestSignature,
                         uint32_t requestSignatureLen,
                         const uint32_t extraBufSize)
        : ClientRequestMsg(sender,
                           flags,
                           reqSeqNum,
                           requestLength,
                           request,
                           reqTimeoutMilli,
                           cid,
                           0,
                           spanContext,
                           requestSignature,
                           requestSignatureLen,
                           extraBufSize) {}

    std::pair<char*, uint32_t> getExtraBufPtr() { return ClientRequestMsg::getExtraBufPtr(); }
  };

  NodeIdType senderId = 1u;
  uint64_t flags = 'F';
  uint64_t reqSeqNum = 100u;
  const char request[] = {"request body"};
  const uint64_t requestTimeoutMilli = 0;
  const std::string correlationId = "correlationId";
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  const size_t extraBufSize = 1024;
  TestClientRequestMsg msg(senderId,
                           flags,
                           reqSeqNum,
                           sizeof(request),
                           request,
                           requestTimeoutMilli,
                           correlationId,
                           concordUtils::SpanContext{spanContext},
                           nullptr,
                           0,
                           extraBufSize);
  EXPECT_NO_THROW(msg.validate(replicaInfo));
  testMessageBaseMethods<ClientRequestMsg>(msg, MsgCode::ClientRequest, senderId, spanContext);

  size_t mainMsgSize = sizeof(ClientRequestMsgHeader) + sizeof(request) + correlationId.size() + spanContext.size();
  ASSERT_EQ(msg.size(), mainMsgSize + extraBufSize);
  ASSERT_EQ(msg.getExtraBufPtr().first, msg.body() + mainMsgSize);
}

TEST_F(ClientRequestMsgTestFixture, validate_size) {
  // Inherit from ClientRequestMsg so that some protected methods can be exposed for the test
  struct TestClientRequestMsg : ClientRequestMsg {
    TestClientRequestMsg(NodeIdType sender,
                         uint64_t flags,
                         uint64_t reqSeqNum,
                         uint32_t requestLength,
                         const char* request,
                         uint64_t reqTimeoutMilli,
                         const std::string& cid,
                         const concordUtils::SpanContext& spanContext,
                         const char* requestSignature,
                         uint32_t requestSignatureLen,
                         const uint32_t extraBufSize)
        : ClientRequestMsg(sender,
                           flags,
                           reqSeqNum,
                           requestLength,
                           request,
                           reqTimeoutMilli,
                           cid,
                           0,
                           spanContext,
                           requestSignature,
                           requestSignatureLen,
                           extraBufSize) {}

    void setSize(uint32_t msgSize) { ClientRequestMsg::setMsgSize(msgSize); }
  };

  NodeIdType senderId = 1u;
  uint64_t flags = 'F';
  uint64_t reqSeqNum = 100u;
  const char request[] = {"request body"};
  const uint64_t requestTimeoutMilli = 0;
  const std::string correlationId = "correlationId";
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  const size_t extraBufSize = 1024;
  TestClientRequestMsg msg(senderId,
                           flags,
                           reqSeqNum,
                           sizeof(request),
                           request,
                           requestTimeoutMilli,
                           correlationId,
                           concordUtils::SpanContext{spanContext},
                           nullptr,
                           0,
                           extraBufSize);

  // Setting the msg size to zero so that it should throw an error. Validatng the size check
  msg.setSize(0);
  EXPECT_THROW(msg.validate(replicaInfo), std::runtime_error);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
