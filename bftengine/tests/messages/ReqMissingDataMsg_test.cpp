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
  ReplicaId replica_id = 1u;
  ViewNum view_num = 2u;
  SeqNum seq_num = 3u;
  const char raw_span_context[] = {"span_\0context"};
  const std::string span_context{raw_span_context, sizeof(raw_span_context)};
  Digest tmpDigest;
  ReqMissingDataMsg msg(replica_id, view_num, seq_num, span_context);
  EXPECT_EQ(msg.viewNumber(), view_num);
  EXPECT_EQ(msg.seqNumber(), seq_num);
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
  testMessageBaseMethods(msg, MsgCode::ReqMissingData, replica_id, span_context);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
