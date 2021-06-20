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

#include "OpenTracing.hpp"
#include "gtest/gtest.h"

#include "assertUtils.hpp"
#include "helper.hpp"
#include "messages/MsgCode.hpp"
#include "messages/AskForCheckpointMsg.hpp"
#include "bftengine/ReplicaConfig.hpp"

using namespace bftEngine;
using namespace bftEngine::impl;

TEST(AskForCheckpointMsg, base_methods) {
  ReplicasInfo replicaInfo(createReplicaConfig(), false, false);
  ReplicaId senderId = 1u;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  AskForCheckpointMsg msg(senderId, concordUtils::SpanContext{spanContext});
  EXPECT_NO_THROW(msg.validate(replicaInfo));
  testMessageBaseMethods(msg, MsgCode::AskForCheckpoint, senderId, spanContext);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
