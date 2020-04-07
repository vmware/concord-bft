#include <cstring>
#include <iostream>
#include <memory>
#include "gtest/gtest.h"
#include "messages/PrePrepareMsg.hpp"
#include "messages/ClientRequestMsg.hpp"
#include "bftengine/ClientMsgs.hpp"
#include "bftengine/ClientMsgs.hpp"
#include "bftengine/ReplicaConfig.hpp"
#include "helper.hpp"

using namespace bftEngine;
using namespace bftEngine::impl;

ReplicaConfig create_replica_config() {
  ReplicaConfig config;
  config.maxExternalMessageSize = 2048;
  config.fVal = 1;
  config.numReplicas = 4;
  return config;
}

ClientRequestMsg create_client_request() {
  uint64_t reqSeqNum = 100u;
  const char request[] = {"request body"};
  const uint64_t requestTimeoutMilli = 0;
  const std::string correlationId = "correlationId";
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};

  return ClientRequestMsg(
      1u, 'F', reqSeqNum, sizeof(request), request, requestTimeoutMilli, correlationId, spanContext);
}

TEST(PrePrepareMsg, create_and_compare) {
  auto config = createReplicaConfig();
  config.singletonFromThis();

  ReplicasInfo replicaInfo(config, false, false);

  ReplicaId senderId = 1u;
  ViewNum viewNum = 2u;
  SeqNum seqNum = 3u;
  CommitPath commitPath = CommitPath::OPTIMISTIC_FAST;
  bool is_null = false;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  PrePrepareMsg msg(senderId, viewNum, seqNum, commitPath, spanContext, is_null);
  EXPECT_EQ(msg.viewNumber(), viewNum);
  EXPECT_EQ(msg.seqNumber(), seqNum);
  EXPECT_EQ(msg.firstPath(), commitPath);
  EXPECT_EQ(msg.isNull(), is_null);
  EXPECT_EQ(msg.numberOfRequests(), 0u);

  ClientRequestMsg client_request = create_client_request();
  msg.addRequest(client_request.body(), client_request.size());
  msg.addRequest(client_request.body(), client_request.size());
  msg.finishAddingRequests();

  EXPECT_EQ(msg.numberOfRequests(), 2u);
  EXPECT_NO_THROW(msg.updateView(msg.viewNumber() + 1));
  EXPECT_EQ(msg.viewNumber(), 3u);
  EXPECT_EQ(msg.firstPath(), CommitPath::SLOW);
  EXPECT_NO_THROW(msg.validate(replicaInfo));

  RequestsIterator iterator(&msg);
  for (size_t i = 0; i < msg.numberOfRequests(); ++i) {
    char* request = nullptr;
    iterator.getAndGoToNext(request);
    ClientRequestMsg msg((ClientRequestMsgHeader*)request);
    EXPECT_EQ(memcmp(msg.body(), client_request.body(), msg.size()), 0);
  }
  iterator.restart();
}

TEST(PrePrepareMsg, create_null_message) {
  ReplicaId senderId = 1u;
  ViewNum viewNum = 2u;
  SeqNum seqNum = 3u;
  CommitPath commitPath = CommitPath::OPTIMISTIC_FAST;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  std::unique_ptr<PrePrepareMsg> null_msg{
      PrePrepareMsg::createNullPrePrepareMsg(senderId, viewNum, seqNum, commitPath, spanContext)};

  auto& msg = *null_msg;
  EXPECT_EQ(msg.viewNumber(), viewNum);
  EXPECT_EQ(msg.seqNumber(), seqNum);
  EXPECT_EQ(msg.firstPath(), commitPath);
  EXPECT_EQ(msg.isNull(), true);
  EXPECT_EQ(msg.numberOfRequests(), 0u);
}

TEST(PrePrepareMsg, base_methods) {
  auto config = createReplicaConfig();
  config.singletonFromThis();

  ReplicasInfo replicaInfo(config, false, false);
  ReplicaId senderId = 1u;
  ViewNum viewNum = 2u;
  SeqNum seqNum = 3u;
  CommitPath commitPath = CommitPath::OPTIMISTIC_FAST;
  bool is_null = false;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  PrePrepareMsg msg(senderId, viewNum, seqNum, commitPath, spanContext, is_null);

  ClientRequestMsg client_request = create_client_request();
  msg.addRequest(client_request.body(), client_request.size());
  msg.finishAddingRequests();
  EXPECT_NO_THROW(msg.validate(replicaInfo));
  testMessageBaseMethods(msg, MsgCode::PrePrepare, senderId, spanContext);
}
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
