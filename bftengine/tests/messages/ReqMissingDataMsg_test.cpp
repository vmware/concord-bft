// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

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
#include "ReservedPagesMock.hpp"
#include "EpochManager.hpp"

using namespace bftEngine;
using namespace bftEngine::impl;
bftEngine::test::ReservedPagesMock<EpochManager> res_pages_mock_;

TEST(ReqMissingDataMsg, base_methods) {
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
  ReplicasInfo replicaInfo(createReplicaConfig(), false, false);
  ReplicaId senderId = 1u;
  ViewNum viewNum = 2u;
  SeqNum seqNum = 3u;
  EpochNum epochNum = 0u;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  Digest tmpDigest;
  ReqMissingDataMsg msg(senderId, viewNum, seqNum, epochNum, concordUtils::SpanContext{spanContext});
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
