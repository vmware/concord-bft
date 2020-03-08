#include <iostream>
#include <vector>
#include <cstring>
#include <iostream>
#include <memory>

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
                                       const std::string& spanContext,
                                       const std::vector<char>& signature) {
  EXPECT_EQ(msg.viewNumber(), v);
  EXPECT_EQ(msg.seqNumber(), s);
  EXPECT_EQ(msg.signatureLen(), signature.size());
  EXPECT_EQ(std::memcmp(msg.signatureBody(), signature.data(), signature.size()), 0);
}

TEST(PreparePartialMsg, PreparePartialMsg_test) {
  auto config = createReplicaConfig();
  ReplicaId id = 1u;
  ViewNum v = 1u;
  SeqNum s = 100u;
  const char raw_span_context[] = {"span_\0context"};
  const std::string span_context{raw_span_context, sizeof(raw_span_context)};
  Digest digest;
  std::vector<char> signature(config.thresholdSignerForCommit->requiredLengthForSignedData());
  config.thresholdSignerForOptimisticCommit->signData(nullptr, 0, signature.data(), signature.size());
  std::unique_ptr<PreparePartialMsg> msg{
      PreparePartialMsg::create(v, s, id, digest, config.thresholdSignerForCommit, span_context)};
  testSignedShareBaseMethods(*msg, v, s, span_context, signature);
  testMessageBaseMethods(*msg, MsgCode::PreparePartial, id, span_context);
}

TEST(PrepareFullMsg, PrepareFullMsg_test) {
  auto config = createReplicaConfig();
  ReplicaId id = 1u;
  ViewNum v = 1u;
  SeqNum s = 100u;
  const char raw_span_context[] = {"span_\0context"};
  const std::string span_context{raw_span_context, sizeof(raw_span_context)};
  Digest digest;
  std::vector<char> signature(config.thresholdSignerForCommit->requiredLengthForSignedData());
  config.thresholdSignerForOptimisticCommit->signData(nullptr, 0, signature.data(), signature.size());
  std::unique_ptr<PrepareFullMsg> msg{
      PrepareFullMsg::create(v, s, id, signature.data(), signature.size(), span_context)};
  testSignedShareBaseMethods(*msg, v, s, span_context, signature);
  testMessageBaseMethods(*msg, MsgCode::PrepareFull, id, span_context);
}

TEST(CommitPartialMsg, CommitPartialMsg_test) {
  auto config = createReplicaConfig();
  ReplicaId id = 1u;
  ViewNum v = 1u;
  SeqNum s = 100u;
  const char raw_span_context[] = {"span_\0context"};
  const std::string span_context{raw_span_context, sizeof(raw_span_context)};
  Digest digest;
  std::vector<char> signature(config.thresholdSignerForCommit->requiredLengthForSignedData());
  config.thresholdSignerForOptimisticCommit->signData(nullptr, 0, signature.data(), signature.size());
  std::unique_ptr<CommitPartialMsg> msg{
      CommitPartialMsg::create(v, s, id, digest, config.thresholdSignerForCommit, span_context)};
  testSignedShareBaseMethods(*msg, v, s, span_context, signature);
  testMessageBaseMethods(*msg, MsgCode::CommitPartial, id, span_context);
}
TEST(CommitFullMsg, CommitFullMsg_test) {
  auto config = createReplicaConfig();
  ReplicaId id = 1u;
  ViewNum v = 1u;
  SeqNum s = 100u;
  const char raw_span_context[] = {"span_\0context"};
  const std::string span_context{raw_span_context, sizeof(raw_span_context)};
  Digest digest;
  std::vector<char> signature(config.thresholdSignerForCommit->requiredLengthForSignedData());
  config.thresholdSignerForOptimisticCommit->signData(nullptr, 0, signature.data(), signature.size());
  std::unique_ptr<CommitFullMsg> msg{CommitFullMsg::create(v, s, id, signature.data(), signature.size(), span_context)};
  testSignedShareBaseMethods(*msg, v, s, span_context, signature);
  testMessageBaseMethods(*msg, MsgCode::CommitFull, id, span_context);
}
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
