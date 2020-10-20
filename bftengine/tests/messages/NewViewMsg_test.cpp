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

#include <limits>
#include <random>
#include "gtest/gtest.h"

#include "helper.hpp"
#include "messages/MsgCode.hpp"
#include "messages/NewViewMsg.hpp"
#include "Digest.hpp"
#include "bftengine/ReplicaConfig.hpp"

using namespace bftEngine;
using namespace bftEngine::impl;

TEST(NewViewMsg, base_methods) {
  ReplicasInfo replicaInfo(createReplicaConfig(), false, false);
  ReplicaId senderId = 1u;
  ViewNum viewNum = 1u;
  std::string commitProofSignature{"commitProofSignature"};
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  NewViewMsg msg{senderId, viewNum, concordUtils::SpanContext{spanContext}};
  EXPECT_EQ(msg.newView(), viewNum);
  EXPECT_EQ(msg.elementsCount(), 0);

  const size_t element_number = replicaInfo.fVal() * 2 + replicaInfo.cVal() * 2 + 1;
  std::vector<Digest> digests;
  digests.reserve(element_number);

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distrib(1, std::numeric_limits<uint8_t>::max());

  for (uint16_t i = 0; i < element_number; ++i) {
    Digest d{static_cast<uint8_t>(distrib(gen))};
    digests.push_back(d);
    msg.addElement(i, d);
  }

  EXPECT_EQ(msg.elementsCount(), replicaInfo.fVal() * 2 + replicaInfo.cVal() * 2 + 1);
  msg.finalizeMessage(replicaInfo);
  EXPECT_NO_THROW(msg.validate(replicaInfo));
  for (uint16_t i = 0; i < element_number; ++i) {
    EXPECT_TRUE(msg.includesViewChangeFromReplica(i, digests[i]));
  }
  testMessageBaseMethods(msg, MsgCode::NewView, senderId, spanContext);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
