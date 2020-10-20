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
  ReplicasInfo replicaInfo(createReplicaConfig(), false, false);
  NodeIdType senderId = 1u;
  uint64_t reqSeqNum = 150u;
  char digestContext[DIGEST_SIZE] = "digest_content";
  Digest digest(digestContext, sizeof(digestContext));
  bool isStable = false;
  const std::string correlationId = "correlationId";
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  CheckpointMsg msg(senderId, reqSeqNum, digest, isStable, concordUtils::SpanContext{spanContext});
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
