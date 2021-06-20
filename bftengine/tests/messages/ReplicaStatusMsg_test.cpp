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
#include "messages/ReplicaStatusMsg.hpp"
#include "messages/MsgCode.hpp"
#include "bftengine/ClientMsgs.hpp"
#include "bftengine/ClientMsgs.hpp"
#include "bftengine/ReplicaConfig.hpp"
#include "Digest.hpp"
#include "helper.hpp"

using namespace bftEngine;
using namespace bftEngine::impl;

TEST(ReplicaStatusMsg, viewActiveNoLists) {
  ReplicasInfo replicaInfo(createReplicaConfig(), false, false);
  ReplicaId senderId = 1u;
  ViewNum viewNum = 2u;
  SeqNum lastStable = 150u;
  SeqNum lastExecuted = 160u;
  bool viewIsActive = true;
  bool hasNewChangeMsg = true;
  bool listOfPrePrepareMsgsInActiveWindow = false;
  bool listOfMissingViewChangeMsgForViewChange = false;
  bool listOfMissingPrePrepareMsgForViewChange = false;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  Digest tmpDigest;
  ReplicaStatusMsg msg(senderId,
                       viewNum,
                       lastStable,
                       lastExecuted,
                       viewIsActive,
                       hasNewChangeMsg,
                       listOfPrePrepareMsgsInActiveWindow,
                       listOfMissingViewChangeMsgForViewChange,
                       listOfMissingPrePrepareMsgForViewChange,
                       concordUtils::SpanContext{spanContext});
  EXPECT_EQ(msg.getViewNumber(), viewNum);
  EXPECT_EQ(msg.getLastStableSeqNum(), lastStable);
  EXPECT_EQ(msg.getLastExecutedSeqNum(), lastExecuted);
  EXPECT_EQ(msg.currentViewIsActive(), viewIsActive);
  EXPECT_EQ(msg.currentViewHasNewViewMessage(), hasNewChangeMsg);
  EXPECT_EQ(msg.hasListOfPrePrepareMsgsInActiveWindow(), listOfPrePrepareMsgsInActiveWindow);
  EXPECT_EQ(msg.hasListOfMissingViewChangeMsgForViewChange(), listOfMissingViewChangeMsgForViewChange);
  EXPECT_EQ(msg.hasListOfMissingPrePrepareMsgForViewChange(), listOfMissingPrePrepareMsgForViewChange);

  for (auto& id : replicaInfo.idsOfPeerReplicas()) {
    EXPECT_FALSE(msg.hasComplaintFromReplica(id));
    msg.setComplaintFromReplica(id);
    EXPECT_TRUE(msg.hasComplaintFromReplica(id));
  }
  for (auto& id : replicaInfo.idsOfPeerReplicas()) {
    EXPECT_TRUE(msg.hasComplaintFromReplica(id));
  }

  EXPECT_NO_THROW(msg.validate(replicaInfo));
  /* the next methods crash the app with an assert ¯\_(ツ)_/¯
  EXPECT_FALSE(msg.isPrePrepareInActiveWindow(151));
  EXPECT_FALSE(msg.isMissingViewChangeMsgForViewChange(senderId));
  EXPECT_FALSE(msg.isMissingPrePrepareMsgForViewChange(151));
  */
  testMessageBaseMethods(msg, MsgCode::ReplicaStatus, senderId, spanContext);
}

TEST(ReplicaStatusMsg, haslistOfPrePrepareMsgsInActiveWindow) {
  ReplicasInfo replicaInfo(createReplicaConfig(), false, false);
  ReplicaId senderId = 1u;
  ViewNum viewNum = 2u;
  SeqNum lastStable = 150u;
  SeqNum lastExecuted = 160u;
  bool viewIsActive = true;
  bool hasNewChangeMsg = true;
  bool listOfPrePrepareMsgsInActiveWindow = true;
  bool listOfMissingViewChangeMsgForViewChange = false;
  bool listOfMissingPrePrepareMsgForViewChange = false;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  Digest tmpDigest;
  ReplicaStatusMsg msg(senderId,
                       viewNum,
                       lastStable,
                       lastExecuted,
                       viewIsActive,
                       hasNewChangeMsg,
                       listOfPrePrepareMsgsInActiveWindow,
                       listOfMissingViewChangeMsgForViewChange,
                       listOfMissingPrePrepareMsgForViewChange,
                       concordUtils::SpanContext{spanContext});
  EXPECT_EQ(msg.getViewNumber(), viewNum);
  EXPECT_EQ(msg.getLastStableSeqNum(), lastStable);
  EXPECT_EQ(msg.getLastExecutedSeqNum(), lastExecuted);
  EXPECT_EQ(msg.currentViewIsActive(), viewIsActive);
  EXPECT_EQ(msg.currentViewHasNewViewMessage(), hasNewChangeMsg);
  EXPECT_EQ(msg.hasListOfPrePrepareMsgsInActiveWindow(), listOfPrePrepareMsgsInActiveWindow);
  EXPECT_EQ(msg.hasListOfMissingViewChangeMsgForViewChange(), listOfMissingViewChangeMsgForViewChange);
  EXPECT_EQ(msg.hasListOfMissingPrePrepareMsgForViewChange(), listOfMissingPrePrepareMsgForViewChange);

  for (auto& id : replicaInfo.idsOfPeerReplicas()) {
    EXPECT_FALSE(msg.hasComplaintFromReplica(id));
    msg.setComplaintFromReplica(id);
    EXPECT_TRUE(msg.hasComplaintFromReplica(id));
  }
  for (auto& id : replicaInfo.idsOfPeerReplicas()) {
    EXPECT_TRUE(msg.hasComplaintFromReplica(id));
  }

  EXPECT_NO_THROW(msg.validate(replicaInfo));
  EXPECT_FALSE(msg.isPrePrepareInActiveWindow(151));
  msg.setPrePrepareInActiveWindow(151);
  EXPECT_TRUE(msg.isPrePrepareInActiveWindow(151));

  EXPECT_FALSE(msg.isPrePrepareInActiveWindow(152));
  msg.setPrePrepareInActiveWindow(152);
  EXPECT_TRUE(msg.isPrePrepareInActiveWindow(152));

  for (auto& id : replicaInfo.idsOfPeerReplicas()) {
    EXPECT_TRUE(msg.hasComplaintFromReplica(id));
  }

  EXPECT_NO_THROW(msg.validate(replicaInfo));
  /* the next methods crash the app with an assert ¯\_(ツ)_/¯
  EXPECT_FALSE(msg.isMissingViewChangeMsgForViewChange(senderId));
  EXPECT_FALSE(msg.isMissingPrePrepareMsgForViewChange(151));
  */
  testMessageBaseMethods(msg, MsgCode::ReplicaStatus, senderId, spanContext);
}

TEST(ReplicaStatusMsg, listOfMissingViewChangeMsgForViewChange) {
  ReplicasInfo replicaInfo(createReplicaConfig(), false, false);
  ReplicaId senderId = 1u;
  ViewNum viewNum = 2u;
  SeqNum lastStable = 150u;
  SeqNum lastExecuted = 160u;
  bool viewIsActive = false;
  bool hasNewChangeMsg = true;
  bool listOfPrePrepareMsgsInActiveWindow = false;
  bool listOfMissingViewChangeMsgForViewChange = true;
  bool listOfMissingPrePrepareMsgForViewChange = false;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  Digest tmpDigest;
  ReplicaStatusMsg msg(senderId,
                       viewNum,
                       lastStable,
                       lastExecuted,
                       viewIsActive,
                       hasNewChangeMsg,
                       listOfPrePrepareMsgsInActiveWindow,
                       listOfMissingViewChangeMsgForViewChange,
                       listOfMissingPrePrepareMsgForViewChange,
                       concordUtils::SpanContext{spanContext});
  EXPECT_EQ(msg.getViewNumber(), viewNum);
  EXPECT_EQ(msg.getLastStableSeqNum(), lastStable);
  EXPECT_EQ(msg.getLastExecutedSeqNum(), lastExecuted);
  EXPECT_EQ(msg.currentViewIsActive(), viewIsActive);
  EXPECT_EQ(msg.currentViewHasNewViewMessage(), hasNewChangeMsg);
  EXPECT_EQ(msg.hasListOfPrePrepareMsgsInActiveWindow(), listOfPrePrepareMsgsInActiveWindow);
  EXPECT_EQ(msg.hasListOfMissingViewChangeMsgForViewChange(), listOfMissingViewChangeMsgForViewChange);
  EXPECT_EQ(msg.hasListOfMissingPrePrepareMsgForViewChange(), listOfMissingPrePrepareMsgForViewChange);

  EXPECT_FALSE(msg.isMissingViewChangeMsgForViewChange(3));
  msg.setMissingViewChangeMsgForViewChange(3);
  EXPECT_TRUE(msg.isMissingViewChangeMsgForViewChange(3));
  EXPECT_FALSE(msg.isMissingViewChangeMsgForViewChange(4));
  msg.setMissingViewChangeMsgForViewChange(4);
  EXPECT_TRUE(msg.isMissingViewChangeMsgForViewChange(4));

  for (auto& id : replicaInfo.idsOfPeerReplicas()) {
    EXPECT_FALSE(msg.hasComplaintFromReplica(id));
    msg.setComplaintFromReplica(id);
    EXPECT_TRUE(msg.hasComplaintFromReplica(id));
  }
  for (auto& id : replicaInfo.idsOfPeerReplicas()) {
    EXPECT_TRUE(msg.hasComplaintFromReplica(id));
  }
  /* the next methods crash the app with an assert ¯\_(ツ)_/¯
  EXPECT_FALSE(msg.isPrePrepareInActiveWindow(151));
  EXPECT_FALSE(msg.isMissingPrePrepareMsgForViewChange(151));
  */
  testMessageBaseMethods(msg, MsgCode::ReplicaStatus, senderId, spanContext);
}

TEST(ReplicaStatusMsg, listOfMissingPrePrepareMsgForViewChange) {
  ReplicasInfo replicaInfo(createReplicaConfig(), false, false);
  ReplicaId senderId = 1u;
  ViewNum viewNum = 2u;
  SeqNum lastStable = 150u;
  SeqNum lastExecuted = 160u;
  bool viewIsActive = false;
  bool hasNewChangeMsg = true;
  bool listOfPrePrepareMsgsInActiveWindow = false;
  bool listOfMissingViewChangeMsgForViewChange = false;
  bool listOfMissingPrePrepareMsgForViewChange = true;
  const char rawSpanContext[] = {"span_\0context"};
  const std::string spanContext{rawSpanContext, sizeof(rawSpanContext)};
  Digest tmpDigest;
  ReplicaStatusMsg msg(senderId,
                       viewNum,
                       lastStable,
                       lastExecuted,
                       viewIsActive,
                       hasNewChangeMsg,
                       listOfPrePrepareMsgsInActiveWindow,
                       listOfMissingViewChangeMsgForViewChange,
                       listOfMissingPrePrepareMsgForViewChange,
                       concordUtils::SpanContext{spanContext});
  EXPECT_EQ(msg.getViewNumber(), viewNum);
  EXPECT_EQ(msg.getLastStableSeqNum(), lastStable);
  EXPECT_EQ(msg.getLastExecutedSeqNum(), lastExecuted);
  EXPECT_EQ(msg.currentViewIsActive(), viewIsActive);
  EXPECT_EQ(msg.currentViewHasNewViewMessage(), hasNewChangeMsg);
  EXPECT_EQ(msg.hasListOfPrePrepareMsgsInActiveWindow(), listOfPrePrepareMsgsInActiveWindow);
  EXPECT_EQ(msg.hasListOfMissingViewChangeMsgForViewChange(), listOfMissingViewChangeMsgForViewChange);
  EXPECT_EQ(msg.hasListOfMissingPrePrepareMsgForViewChange(), listOfMissingPrePrepareMsgForViewChange);

  EXPECT_FALSE(msg.isMissingPrePrepareMsgForViewChange(151));
  msg.setMissingPrePrepareMsgForViewChange(151);
  EXPECT_TRUE(msg.isMissingPrePrepareMsgForViewChange(151));
  EXPECT_FALSE(msg.isMissingPrePrepareMsgForViewChange(152));
  msg.setMissingPrePrepareMsgForViewChange(152);
  EXPECT_TRUE(msg.isMissingPrePrepareMsgForViewChange(152));

  for (auto& id : replicaInfo.idsOfPeerReplicas()) {
    EXPECT_FALSE(msg.hasComplaintFromReplica(id));
    msg.setComplaintFromReplica(id);
    EXPECT_TRUE(msg.hasComplaintFromReplica(id));
  }
  for (auto& id : replicaInfo.idsOfPeerReplicas()) {
    EXPECT_TRUE(msg.hasComplaintFromReplica(id));
  }

  testMessageBaseMethods(msg, MsgCode::ReplicaStatus, senderId, spanContext);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
