#include "gtest/gtest.h"

#include "helper.hpp"
#include "messages/MsgCode.hpp"
#include "messages/AskForCheckpointMsg.hpp"
#include "bftengine/ReplicaConfig.hpp"

using namespace bftEngine;
using namespace bftEngine::impl;

TEST(AskForCheckpointMsg, base_methods) {
  auto config = createReplicaConfig();
  ReplicasInfo replica_info(config, false, false);
  ReplicaId replica_id = 1u;
  const char raw_span_context[] = {"span_\0context"};
  const std::string span_context{raw_span_context, sizeof(raw_span_context)};
  AskForCheckpointMsg msg(replica_id, span_context);
  msg.validate(replica_info);
  testMessageBaseMethods(msg, MsgCode::AskForCheckpoint, replica_id, span_context);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
