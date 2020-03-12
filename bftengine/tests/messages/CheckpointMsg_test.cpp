#include <cstring>
#include <iostream>
#include <memory>
#include "gtest/gtest.h"
#include "Digest.hpp"
#include "DigestType.h"
#include "messages/CheckpointMsg.hpp"
#include "bftengine/ClientMsgs.hpp"
#include "bftengine/ClientMsgs.hpp"
#include "bftengine/ReplicaConfig.hpp"
#include "helper.hpp"

TEST(CheckpointMsg, base_methods) {
  createReplicaConfig().singletonFromThis();
  NodeIdType node_type = 1u;
  uint64_t reqSeqNum = 100u;
  char digestContext[DIGEST_SIZE] = "digest_content";
  Digest digest(digestContext, sizeof(digestContext));
  bool isStable = false;
  const std::string correlation_id = "correlation_id";
  const char raw_span_context[] = {"span_\0context"};
  const std::string span_context{raw_span_context, sizeof(raw_span_context)};
  CheckpointMsg msg(node_type, reqSeqNum, digest, isStable, span_context);
  EXPECT_EQ(msg.seqNumber(), reqSeqNum);
  EXPECT_EQ(msg.isStableState(), isStable);
  msg.setStateAsStable();
  EXPECT_EQ(msg.isStableState(), !isStable);
  EXPECT_EQ(msg.digestOfState(), digest);
  testMessageBaseMethods(msg, MsgCode::Checkpoint, node_type, span_context);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
