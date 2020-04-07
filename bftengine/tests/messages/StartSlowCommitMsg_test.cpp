#include "gtest/gtest.h"

#include "helper.hpp"
#include "messages/MsgCode.hpp"
#include "messages/StartSlowCommitMsg.hpp"
#include "bftengine/ClientMsgs.hpp"
#include "bftengine/ReplicaConfig.hpp"

using namespace bftEngine;
using namespace bftEngine::impl;

TEST(StartSlowCommitMsg, base_methods) {
  auto config = createReplicaConfig();
  ReplicaId senderId = 1u;
  ViewNum viewNum = 2u;
  SeqNum seqNum = 3u;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  StartSlowCommitMsg msg(senderId, viewNum, seqNum, spanContext);
  EXPECT_EQ(msg.viewNumber(), viewNum);
  EXPECT_EQ(msg.seqNumber(), seqNum);
  testMessageBaseMethods(msg, MsgCode::StartSlowCommit, senderId, spanContext);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
