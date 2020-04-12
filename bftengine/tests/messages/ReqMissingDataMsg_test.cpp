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
  ReplicasInfo replicaInfo(config, false, false);
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

  EXPECT_EQ(msg.getSlowPathHasStarted(), false);
  msg.setSlowPathHasStarted();
  EXPECT_EQ(msg.getSlowPathHasStarted(), true);

  EXPECT_EQ(msg.getFullCommitIsMissing(), false);
  msg.setFullCommitIsMissing();
  EXPECT_EQ(msg.getFullCommitIsMissing(), true);

  EXPECT_EQ(msg.getPrePrepareIsMissing(), false);
  msg.setPrePrepareIsMissing();
  EXPECT_EQ(msg.getPrePrepareIsMissing(), true);

  EXPECT_EQ(msg.getFullPrepareIsMissing(), false);
  msg.setFullPrepareIsMissing();
  EXPECT_EQ(msg.getFullPrepareIsMissing(), true);

  EXPECT_EQ(msg.getPartialProofIsMissing(), false);
  msg.setPartialProofIsMissing();
  EXPECT_EQ(msg.getPartialProofIsMissing(), true);

  EXPECT_EQ(msg.getPartialCommitIsMissing(), false);
  msg.setPartialCommitIsMissing();
  EXPECT_EQ(msg.getPartialCommitIsMissing(), true);

  EXPECT_EQ(msg.getFullCommitProofIsMissing(), false);
  msg.setFullCommitProofIsMissing();
  EXPECT_EQ(msg.getFullCommitProofIsMissing(), true);

  EXPECT_EQ(msg.getPartialPrepareIsMissing(), false);
  msg.setPartialPrepareIsMissing();
  EXPECT_EQ(msg.getPartialPrepareIsMissing(), true);

  EXPECT_EQ(msg.getFlags(), 0x01FE);
  EXPECT_NO_THROW(msg.validate(replicaInfo));

  msg.resetFlags();
  EXPECT_EQ(msg.getFlags(), 0u);
  testMessageBaseMethods(msg, MsgCode::ReqMissingData, senderId, spanContext);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
