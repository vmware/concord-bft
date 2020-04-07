#include <iostream>
#include <vector>
#include <cstring>
#include <iostream>
#include <memory>
#include "gtest/gtest.h"
#include "messages/ReqMissingDataMsg.hpp"
#include "messages/MsgCode.hpp"
#include "bftengine/ClientMsgs.hpp"
#include "bftengine/ClientMsgs.hpp"
#include "bftengine/ReplicaConfig.hpp"
#include "Digest.hpp"
#include "helper.hpp"

using namespace bftEngine;
using namespace bftEngine::impl;

TEST(ReqMissingDataMsg, base_methods) {
  auto config = createReplicaConfig();
  ReplicaId senderId = 1u;
  ViewNum viewNum = 2u;
  SeqNum seqNum = 3u;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  Digest tmpDigest;
  ReqMissingDataMsg msg(senderId, viewNum, seqNum, spanContext);
  EXPECT_EQ(msg.viewNumber(), viewNum);
  EXPECT_EQ(msg.seqNumber(), seqNum);
  EXPECT_EQ(msg.getFlags(), 0);

  msg.setSlowPathHasStarted();
  EXPECT_EQ(msg.getSlowPathHasStarted(), true);
  msg.setFullCommitIsMissing();
  EXPECT_EQ(msg.getFullCommitIsMissing(), true);
  msg.setPrePrepareIsMissing();
  EXPECT_EQ(msg.getPrePrepareIsMissing(), true);
  msg.setFullPrepareIsMissing();
  EXPECT_EQ(msg.getFullPrepareIsMissing(), true);
  msg.setPartialProofIsMissing();
  EXPECT_EQ(msg.getPartialProofIsMissing(), true);
  msg.setPartialCommitIsMissing();
  EXPECT_EQ(msg.getPartialCommitIsMissing(), true);
  msg.setFullCommitProofIsMissing();
  EXPECT_EQ(msg.getFullCommitProofIsMissing(), true);
  msg.setPartialPrepareIsMissing();
  EXPECT_EQ(msg.getPartialPrepareIsMissing(), true);

  msg.resetFlags();
  EXPECT_EQ(msg.getFlags(), 0u);
  testMessageBaseMethods(msg, MsgCode::ReqMissingData, senderId, spanContext);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
