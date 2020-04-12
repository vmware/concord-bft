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
  ReplicasInfo replicaInfo(config, false, false);
  ReplicaId senderId = 1u;
  ViewNum viewNum = 1u;
  std::string commitProofSignature{"commitProofSignature"};
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  NewViewMsg msg{senderId, viewNum, spanContext};
  EXPECT_EQ(msg.newView(), viewNum);
  EXPECT_EQ(msg.elementsCount(), 0);

  for (uint8_t i = 1; i <= config.fVal * 2 + config.cVal * 2 + 1; ++i) {
    Digest d{i};
    msg.addElement(i, d);
  }

  EXPECT_EQ(msg.elementsCount(), config.fVal * 2 + config.cVal * 2 + 1);
  msg.finalizeMessage(replicaInfo);
  EXPECT_NO_THROW(msg.validate(replicaInfo));
  for (uint8_t i = 1; i <= config.fVal * 2 + config.cVal * 2 + 1; ++i) {
    Digest d{i};
    EXPECT_TRUE(msg.includesViewChangeFromReplica(i, d));
  }
  testMessageBaseMethods(msg, MsgCode::NewView, senderId, spanContext);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
