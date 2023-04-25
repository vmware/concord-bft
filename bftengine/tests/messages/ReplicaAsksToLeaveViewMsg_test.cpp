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
#include "ViewsManager.hpp"
#include "ReplicasInfo.hpp"
#include "SigManager.hpp"
#include "messages/MsgCode.hpp"
#include "messages/ReplicaAsksToLeaveViewMsg.hpp"
#include "ReservedPagesMock.hpp"
#include "EpochManager.hpp"

using namespace bftEngine;
using namespace bftEngine::impl;
bftEngine::test::ReservedPagesMock<EpochManager> res_pages_mock_;
TEST(ReplicaAsksToLeaveViewMsg, base_methods) {
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
  auto& config = createReplicaConfig();
  ReplicaId senderId = 3u;
  ViewNum viewNum = 5u;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  ReplicasInfo replicaInfo(config, true, true);
  std::shared_ptr<SigManager> sigManager(createSigManager(config.replicaId,
                                                          config.replicaPrivateKey,
                                                          concord::crypto::KeyFormat::HexaDecimalStrippedFormat,
                                                          config.publicKeysOfReplicas,
                                                          replicaInfo));
  ViewsManager manager(&replicaInfo);
  std::unique_ptr<ReplicaAsksToLeaveViewMsg> msg(
      ReplicaAsksToLeaveViewMsg::create(senderId,
                                        viewNum,
                                        ReplicaAsksToLeaveViewMsg::Reason::ClientRequestTimeout,
                                        concordUtils::SpanContext{spanContext}));
  EXPECT_EQ(msg->idOfGeneratedReplica(), senderId);
  EXPECT_EQ(msg->viewNumber(), viewNum);
  EXPECT_EQ(msg->reason(), ReplicaAsksToLeaveViewMsg::Reason::ClientRequestTimeout);

  testMessageBaseMethods(*msg.get(), MsgCode::ReplicaAsksToLeaveView, senderId, spanContext);

  EXPECT_NO_THROW(msg->validate(replicaInfo));
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
