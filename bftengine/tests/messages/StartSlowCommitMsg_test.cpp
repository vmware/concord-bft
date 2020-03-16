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
  ReplicaId replica_id = 1u;
  ViewNum view_num = 2u;
  SeqNum seq_num = 3u;
  const char raw_span_context[] = {"span_\0context"};
  const std::string span_context{raw_span_context, sizeof(raw_span_context)};
  StartSlowCommitMsg msg(replica_id, view_num, seq_num, span_context);
  EXPECT_EQ(msg.viewNumber(), view_num);
  EXPECT_EQ(msg.seqNumber(), seq_num);
  testMessageBaseMethods(msg, MsgCode::StartSlowCommit, replica_id, span_context);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
