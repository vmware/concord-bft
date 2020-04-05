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

TEST(ClientRequestMsg, create_and_compare) {
  NodeIdType node_type = 1u;
  uint8_t flags = 'F';
  uint64_t reqSeqNum = 100u;
  const char request[] = {"request body"};
  const uint64_t request_timeout_milli = 0;
  const std::string correlation_id = "correlation_id";
  const char raw_span_context[] = {"span_\0context"};
  const std::string span_context{raw_span_context, sizeof(raw_span_context)};
  ClientRequestMsg msg(
      node_type, flags, reqSeqNum, sizeof(request), request, request_timeout_milli, correlation_id, span_context);

  EXPECT_EQ(msg.clientProxyId(), node_type);
  EXPECT_EQ(msg.flags(), flags);
  EXPECT_EQ(msg.requestSeqNum(), reqSeqNum);
  EXPECT_EQ(msg.requestLength(), sizeof(request));
  EXPECT_NE(msg.requestBuf(), request);
  EXPECT_TRUE(std::memcmp(msg.requestBuf(), request, sizeof(request)) == 0u);
  EXPECT_EQ(msg.getCid(), correlation_id);
  EXPECT_EQ(msg.spanContext(), span_context);
  EXPECT_EQ(msg.requestTimeoutMilli(), request_timeout_milli);
}

TEST(ClientRequestMsg, create_and_compare_with_empty_span) {
  NodeIdType node_type = 1u;
  uint8_t flags = 'F';
  uint64_t reqSeqNum = 100u;
  const char request[] = {"request body"};
  const uint64_t request_timeout_milli = 0;
  const std::string correlation_id = "correlation_id";
  const char raw_span_context[] = {""};
  const std::string span_context{raw_span_context, sizeof(raw_span_context)};
  ClientRequestMsg msg(
      node_type, flags, reqSeqNum, sizeof(request), request, request_timeout_milli, correlation_id, span_context);

  EXPECT_EQ(msg.clientProxyId(), node_type);
  EXPECT_EQ(msg.flags(), flags);
  EXPECT_EQ(msg.requestSeqNum(), reqSeqNum);
  EXPECT_EQ(msg.requestLength(), sizeof(request));
  EXPECT_NE(msg.requestBuf(), request);
  EXPECT_TRUE(std::memcmp(msg.requestBuf(), request, sizeof(request)) == 0u);
  EXPECT_EQ(msg.getCid(), correlation_id);
  EXPECT_EQ(msg.spanContext(), span_context);
}

TEST(ClientRequestMsg, create_and_compare_with_empty_cid) {
  NodeIdType node_type = 1u;
  uint8_t flags = 'F';
  uint64_t reqSeqNum = 100u;
  const char request[] = {"request body"};
  const uint64_t request_timeout_milli = 0;
  const std::string correlation_id = "";
  const char raw_span_context[] = {""};
  const std::string span_context{raw_span_context, sizeof(raw_span_context)};
  ClientRequestMsg msg(
      node_type, flags, reqSeqNum, sizeof(request), request, request_timeout_milli, correlation_id, span_context);

  EXPECT_EQ(msg.clientProxyId(), node_type);
  EXPECT_EQ(msg.flags(), flags);
  EXPECT_EQ(msg.requestSeqNum(), reqSeqNum);
  EXPECT_EQ(msg.requestLength(), sizeof(request));
  EXPECT_NE(msg.requestBuf(), request);
  EXPECT_TRUE(std::memcmp(msg.requestBuf(), request, sizeof(request)) == 0u);
  EXPECT_EQ(msg.getCid(), correlation_id);
  EXPECT_EQ(msg.spanContext(), span_context);
  EXPECT_EQ(msg.requestTimeoutMilli(), request_timeout_milli);
}

TEST(ClientRequestMsg, create_from_buffer) {
  NodeIdType node_type = 1u;
  uint8_t flags = 'F';
  uint64_t reqSeqNum = 100u;
  const char request[] = {"request body"};
  const uint64_t request_timeout_milli = 0;
  const std::string correlation_id = "correlation_id";
  const char raw_span_context[] = {""};
  const std::string span_context{raw_span_context, sizeof(raw_span_context)};
  ClientRequestMsg original_msg(
      node_type, flags, reqSeqNum, sizeof(request), request, request_timeout_milli, correlation_id, span_context);

  ClientRequestMsg copy_msg((ClientRequestMsgHeader*)original_msg.body());

  EXPECT_EQ(original_msg.clientProxyId(), copy_msg.clientProxyId());
  EXPECT_EQ(original_msg.flags(), copy_msg.flags());
  EXPECT_EQ(original_msg.requestSeqNum(), copy_msg.requestSeqNum());
  EXPECT_EQ(original_msg.requestLength(), copy_msg.requestLength());
  EXPECT_EQ(original_msg.requestBuf(), copy_msg.requestBuf());
  EXPECT_TRUE(std::memcmp(original_msg.requestBuf(), copy_msg.requestBuf(), sizeof(request)) == 0u);
  EXPECT_EQ(original_msg.getCid(), copy_msg.getCid());
  EXPECT_EQ(original_msg.spanContext(), copy_msg.spanContext());
  EXPECT_EQ(original_msg.requestTimeoutMilli(), request_timeout_milli);
}

TEST(ClientRequestMsg, base_methods) {
  createReplicaConfig().singletonFromThis();
  NodeIdType node_type = 1u;
  uint8_t flags = 'F';
  uint64_t reqSeqNum = 100u;
  const char request[] = {"request body"};
  const uint64_t request_timeout_milli = 0;
  const std::string correlation_id = "correlation_id";
  const char raw_span_context[] = {"span_\0context"};
  const std::string span_context{raw_span_context, sizeof(raw_span_context)};
  ClientRequestMsg msg(
      node_type, flags, reqSeqNum, sizeof(request), request, request_timeout_milli, correlation_id, span_context);
  testMessageBaseMethods(msg, MsgCode::ClientRequest, node_type, span_context);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
