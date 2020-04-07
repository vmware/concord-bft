#include "gtest/gtest.h"

#include "helper.hpp"
#include "messages/MsgCode.hpp"
#include "messages/NewViewMsg.hpp"
#include "Digest.hpp"
#include "bftengine/ReplicaConfig.hpp"

using namespace bftEngine;
using namespace bftEngine::impl;

TEST(NewViewMsg, base_methods) {
  auto config = createReplicaConfig();
  ReplicasInfo replica_info(config, false, false);
  ReplicaId replica_id = 1u;
  ViewNum view_num = 1u;
  std::string commit_proof_signature{"commit_proof_signature"};
  const char raw_span_context[] = {"span_\0context"};
  const std::string span_context{raw_span_context, sizeof(raw_span_context)};
  NewViewMsg msg{replica_id, view_num, span_context};
  EXPECT_EQ(msg.newView(), view_num);
  EXPECT_EQ(msg.elementsCount(), 0);

  for (uint8_t i = 1; i <= config.fVal * 2 + config.cVal * 2 + 1; ++i) {
    Digest d{i};
    msg.addElement(i, d);
  }

  EXPECT_EQ(msg.elementsCount(), config.fVal * 2 + config.cVal * 2 + 1);
  msg.finalizeMessage(replica_info);
  msg.validate(replica_info);
  for (uint8_t i = 1; i <= config.fVal * 2 + config.cVal * 2 + 1; ++i) {
    Digest d{i};
    EXPECT_TRUE(msg.includesViewChangeFromReplica(i, d));
  }
  testMessageBaseMethods(msg, MsgCode::NewView, replica_id, span_context);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
