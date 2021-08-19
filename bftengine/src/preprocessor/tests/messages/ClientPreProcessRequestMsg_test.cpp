// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <cstring>
#include "gtest/gtest.h"
#include "helper.hpp"
#include "messages/ClientPreProcessRequestMsg.hpp"

using namespace bftEngine;
using namespace bftEngine::impl;
using namespace preprocessor;

class ClientPreprocessRequestMsgTestFixture : public ::testing::Test {
 public:
  ClientPreprocessRequestMsgTestFixture()
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

TEST_F(ClientPreprocessRequestMsgTestFixture, create_and_compare) {
  NodeIdType senderId = 1u;
  uint64_t reqSeqNum = 100u;
  const char request[] = {"request body"};
  const uint64_t requestTimeoutMilli = 0;
  const std::string correlationId = "correlationId";
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  ClientPreProcessRequestMsg msg(senderId,
                                 reqSeqNum,
                                 sizeof(request),
                                 request,
                                 requestTimeoutMilli,
                                 correlationId,
                                 concordUtils::SpanContext{spanContext});

  EXPECT_EQ(msg.clientProxyId(), senderId);
  EXPECT_EQ(msg.requestSeqNum(), reqSeqNum);
  EXPECT_EQ(msg.requestLength(), sizeof(request));
  EXPECT_NE(msg.requestBuf(), request);
  EXPECT_TRUE(std::memcmp(msg.requestBuf(), request, sizeof(request)) == 0u);
  EXPECT_EQ(msg.getCid(), correlationId);

  EXPECT_EQ(sizeOfHeader<ClientPreProcessRequestMsg>(), sizeOfHeader<ClientPreProcessRequestMsg>());

  EXPECT_EQ(msg.spanContext<ClientPreProcessRequestMsg>().data(), spanContext);
  EXPECT_EQ(msg.requestTimeoutMilli(), requestTimeoutMilli);
  EXPECT_NO_THROW(msg.validate(replicaInfo));
}

TEST_F(ClientPreprocessRequestMsgTestFixture, construct_from_msg_base_and_compare) {
  NodeIdType senderId = 1u;
  uint64_t reqSeqNum = 100u;
  const char request[] = {"request body"};
  const uint64_t requestTimeoutMilli = 0;
  const std::string correlationId = "correlationId";
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  ClientPreProcessRequestMsg msg(senderId,
                                 reqSeqNum,
                                 sizeof(request),
                                 request,
                                 requestTimeoutMilli,
                                 correlationId,
                                 concordUtils::SpanContext{spanContext});

  MessageBase* original_base = &msg;
  uint8_t* raw_msg = (uint8_t*)malloc(original_base->size());
  memcpy(raw_msg, original_base->body(), original_base->size());
  MessageBase m(senderId, (MessageBase::Header*)raw_msg, original_base->size(), true);

  ClientPreProcessRequestMsg recreated(&m);
  EXPECT_EQ(msg.clientProxyId(), recreated.clientProxyId());
  EXPECT_EQ(msg.requestSeqNum(), recreated.requestSeqNum());
  EXPECT_EQ(msg.requestLength(), recreated.requestLength());
  EXPECT_NE(msg.requestBuf(), recreated.requestBuf());
  EXPECT_TRUE(std::memcmp(msg.requestBuf(), recreated.requestBuf(), sizeof(request)) == 0u);
  EXPECT_EQ(msg.getCid(), recreated.getCid());
  EXPECT_EQ(msg.spanContext<ClientPreProcessRequestMsg>().data(),
            recreated.spanContext<ClientPreProcessRequestMsg>().data());
  EXPECT_EQ(msg.spanContext<ClientPreProcessRequestMsg>().data(), spanContext);
  EXPECT_EQ(msg.requestTimeoutMilli(), recreated.requestTimeoutMilli());
  EXPECT_NO_THROW(recreated.validate(replicaInfo));
}

TEST_F(ClientPreprocessRequestMsgTestFixture, check_after_convert_to_regular_client_request) {
  NodeIdType senderId = 1u;
  uint64_t reqSeqNum = 100u;
  const char request[] = {"request body"};
  const uint64_t requestTimeoutMilli = 0;
  const std::string correlationId = "correlationId";
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  ClientPreProcessRequestMsg msg(senderId,
                                 reqSeqNum,
                                 sizeof(request),
                                 request,
                                 requestTimeoutMilli,
                                 correlationId,
                                 concordUtils::SpanContext{spanContext});

  auto base_client_request = msg.convertToClientRequestMsg();
  auto client_request = static_cast<ClientRequestMsg*>(base_client_request.get());
  EXPECT_EQ(msg.clientProxyId(), client_request->clientProxyId());
  EXPECT_EQ(msg.requestSeqNum(), client_request->requestSeqNum());
  EXPECT_EQ(msg.requestLength(), client_request->requestLength());
  EXPECT_NE(msg.requestBuf(), client_request->requestBuf());
  EXPECT_TRUE(std::memcmp(msg.requestBuf(), client_request->requestBuf(), sizeof(request)) == 0u);
  EXPECT_EQ(msg.getCid(), client_request->getCid());
  EXPECT_EQ(msg.spanContext<ClientPreProcessRequestMsg>().data(),
            client_request->spanContext<ClientRequestMsg>().data());
  EXPECT_EQ(msg.spanContext<ClientPreProcessRequestMsg>().data(), spanContext);
  EXPECT_EQ(client_request->spanContext<ClientRequestMsg>().data(), spanContext);
  EXPECT_EQ(msg.requestTimeoutMilli(), client_request->requestTimeoutMilli());
  EXPECT_NO_THROW(client_request->validate(replicaInfo));
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
