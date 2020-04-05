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
  const uint64_t request_timeout_milli = 0;
  const std::string correlation_id = "correlation_id";
  const char raw_span_context[] = {"span_\0context"};
  const std::string span_context{raw_span_context, sizeof(raw_span_context)};

  return ClientRequestMsg(
      1u, 'F', reqSeqNum, sizeof(request), request, request_timeout_milli, correlation_id, span_context);
}

TEST(PrePrepareMsg, create_and_compare) {
  auto config = createReplicaConfig();
  config.singletonFromThis();

  ReplicasInfo replica_info(config, false, false);

  ReplicaId replica_id = 1u;
  ViewNum view_num = 2u;
  SeqNum seq_num = 3u;
  CommitPath commit_path = CommitPath::OPTIMISTIC_FAST;
  bool is_null = false;
  const char raw_span_context[] = {"span_\0context"};
  const std::string span_context{raw_span_context, sizeof(raw_span_context)};
  PrePrepareMsg msg(replica_id, view_num, seq_num, commit_path, span_context, is_null);
  EXPECT_EQ(msg.viewNumber(), view_num);
  EXPECT_EQ(msg.seqNumber(), seq_num);
  EXPECT_EQ(msg.firstPath(), commit_path);
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
  EXPECT_NO_THROW(msg.validate(replica_info));

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
  ReplicaId replica_id = 1u;
  ViewNum view_num = 2u;
  SeqNum seq_num = 3u;
  CommitPath commit_path = CommitPath::OPTIMISTIC_FAST;
  const char raw_span_context[] = {"span_\0context"};
  const std::string span_context{raw_span_context, sizeof(raw_span_context)};
  std::unique_ptr<PrePrepareMsg> null_msg{
      PrePrepareMsg::createNullPrePrepareMsg(replica_id, view_num, seq_num, commit_path, span_context)};

  auto& msg = *null_msg;
  EXPECT_EQ(msg.viewNumber(), view_num);
  EXPECT_EQ(msg.seqNumber(), seq_num);
  EXPECT_EQ(msg.firstPath(), commit_path);
  EXPECT_EQ(msg.isNull(), true);
  EXPECT_EQ(msg.numberOfRequests(), 0u);
}

TEST(PrePrepareMsg, base_methods) {
  ReplicaId replica_id = 1u;
  ViewNum view_num = 2u;
  SeqNum seq_num = 3u;
  CommitPath commit_path = CommitPath::OPTIMISTIC_FAST;
  bool is_null = false;
  const char raw_span_context[] = {"span_\0context"};
  const std::string span_context{raw_span_context, sizeof(raw_span_context)};
  PrePrepareMsg msg(replica_id, view_num, seq_num, commit_path, span_context, is_null);

  ClientRequestMsg client_request = create_client_request();
  msg.addRequest(client_request.body(), client_request.size());
  msg.finishAddingRequests();
  testMessageBaseMethods(msg, MsgCode::PrePrepare, replica_id, span_context);
}
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
