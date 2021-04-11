// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <iostream>
#include <vector>
#include <cstring>
#include <iostream>
#include <memory>

#include "OpenTracing.hpp"
#include "gtest/gtest.h"

#include "Digest.hpp"
#include "messages/SignedShareMsgs.hpp"
#include "messages/MsgCode.hpp"
#include "bftengine/ClientMsgs.hpp"
#include "bftengine/ClientMsgs.hpp"
#include "bftengine/ReplicaConfig.hpp"
#include "helper.hpp"

using namespace bftEngine;
using namespace bftEngine::impl;

static void testSignedShareBaseMethods(const SignedShareBase& msg,
                                       ViewNum v,
                                       SeqNum s,
                                       const std::vector<char>& signature) {
  EXPECT_EQ(msg.viewNumber(), v);
  EXPECT_EQ(msg.seqNumber(), s);
  EXPECT_EQ(msg.signatureLen(), signature.size());
  EXPECT_EQ(std::memcmp(msg.signatureBody(), signature.data(), signature.size()), 0);
}

TEST(PreparePartialMsg, PreparePartialMsg_test) {
  ReplicasInfo replicaInfo(createReplicaConfig(), false, false);
  ReplicaId id = 1u;
  ViewNum v = 1u;
  SeqNum s = 100u;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  Digest digest;
  std::vector<char> signature(CryptoManager::instance().thresholdSignerForCommit(s)->requiredLengthForSignedData());
  CryptoManager::instance().thresholdSignerForOptimisticCommit(s)->signData(
      nullptr, 0, signature.data(), signature.size());
  std::unique_ptr<PreparePartialMsg> msg{PreparePartialMsg::create(
      v, s, id, digest, CryptoManager::instance().thresholdSignerForCommit(s), concordUtils::SpanContext{spanContext})};
  EXPECT_NO_THROW(msg->validate(replicaInfo));
  testSignedShareBaseMethods(*msg, v, s, signature);
  testMessageBaseMethods(*msg, MsgCode::PreparePartial, id, spanContext);
}

TEST(PrepareFullMsg, PrepareFullMsg_test) {
  ReplicasInfo replicaInfo(createReplicaConfig(), false, false);
  ReplicaId id = 1u;
  ViewNum v = 1u;
  SeqNum s = 100u;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  Digest digest;
  std::vector<char> signature(CryptoManager::instance().thresholdSignerForCommit(s)->requiredLengthForSignedData());
  CryptoManager::instance().thresholdSignerForOptimisticCommit(s)->signData(
      nullptr, 0, signature.data(), signature.size());
  std::unique_ptr<PrepareFullMsg> msg{
      PrepareFullMsg::create(v, s, id, signature.data(), signature.size(), concordUtils::SpanContext{spanContext})};
  EXPECT_NO_THROW(msg->validate(replicaInfo));
  testSignedShareBaseMethods(*msg, v, s, signature);
  testMessageBaseMethods(*msg, MsgCode::PrepareFull, id, spanContext);
}

TEST(CommitPartialMsg, CommitPartialMsg_test) {
  ReplicasInfo replicaInfo(createReplicaConfig(), false, false);
  ReplicaId id = 1u;
  ViewNum v = 1u;
  SeqNum s = 100u;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  Digest digest;
  std::vector<char> signature(CryptoManager::instance().thresholdSignerForCommit(s)->requiredLengthForSignedData());
  CryptoManager::instance().thresholdSignerForOptimisticCommit(s)->signData(
      nullptr, 0, signature.data(), signature.size());
  std::unique_ptr<CommitPartialMsg> msg{CommitPartialMsg::create(
      v, s, id, digest, CryptoManager::instance().thresholdSignerForCommit(s), concordUtils::SpanContext{spanContext})};
  EXPECT_NO_THROW(msg->validate(replicaInfo));
  testSignedShareBaseMethods(*msg, v, s, signature);
  testMessageBaseMethods(*msg, MsgCode::CommitPartial, id, spanContext);
}
TEST(CommitFullMsg, CommitFullMsg_test) {
  ReplicasInfo replicaInfo(createReplicaConfig(), false, false);
  ReplicaId id = 1u;
  ViewNum v = 1u;
  SeqNum s = 100u;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  Digest digest;
  std::vector<char> signature(CryptoManager::instance().thresholdSignerForCommit(s)->requiredLengthForSignedData());
  CryptoManager::instance().thresholdSignerForOptimisticCommit(s)->signData(
      nullptr, 0, signature.data(), signature.size());
  std::unique_ptr<CommitFullMsg> msg{
      CommitFullMsg::create(v, s, id, signature.data(), signature.size(), concordUtils::SpanContext{spanContext})};
  EXPECT_NO_THROW(msg->validate(replicaInfo));
  testSignedShareBaseMethods(*msg, v, s, signature);
  testMessageBaseMethods(*msg, MsgCode::CommitFull, id, spanContext);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
