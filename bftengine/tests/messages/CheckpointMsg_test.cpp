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
  auto config = createReplicaConfig();
  config.singletonFromThis();
  ReplicasInfo replicaInfo(config, false, false);
  NodeIdType senderId = 1u;
  uint64_t reqSeqNum = 150u;
  char digestContext[DIGEST_SIZE] = "digest_content";
  Digest digest(digestContext, sizeof(digestContext));
  bool isStable = false;
  const std::string correlationId = "correlationId";
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  CheckpointMsg msg(senderId, reqSeqNum, digest, isStable, spanContext);
  EXPECT_EQ(msg.seqNumber(), reqSeqNum);
  EXPECT_EQ(msg.isStableState(), isStable);
  msg.setStateAsStable();
  EXPECT_EQ(msg.isStableState(), !isStable);
  EXPECT_EQ(msg.digestOfState(), digest);
  EXPECT_NO_THROW(msg.validate(replicaInfo));
  testMessageBaseMethods(msg, MsgCode::Checkpoint, senderId, spanContext);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
