#include "gtest/gtest.h"

#include "helper.hpp"
#include "messages/MsgCode.hpp"
#include "messages/AskForCheckpointMsg.hpp"
#include "bftengine/ReplicaConfig.hpp"

using namespace bftEngine;
using namespace bftEngine::impl;

TEST(AskForCheckpointMsg, base_methods) {
  auto config = createReplicaConfig();
  ReplicasInfo replicaInfo(config, false, false);
  ReplicaId senderId = 1u;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  AskForCheckpointMsg msg(senderId, spanContext);
  EXPECT_NO_THROW(msg.validate(replicaInfo));
  testMessageBaseMethods(msg, MsgCode::AskForCheckpoint, senderId, spanContext);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
