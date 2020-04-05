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
  auto config = createReplicaConfig();
  ReplicasInfo replica_info(config, false, false);
  ReplicaId replica_id = 1u;
  ViewNum view_num = 2u;
  SeqNum lastStable = 150u;
  SeqNum lastExecuted = 160u;
  bool viewIsActive = true;
  bool hasNewChangeMsg = true;
  bool listOfPrePrepareMsgsInActiveWindow = false;
  bool listOfMissingViewChangeMsgForViewChange = false;
  bool listOfMissingPrePrepareMsgForViewChange = false;
  const char raw_span_context[] = {"span_\0context"};
  const std::string span_context{raw_span_context, sizeof(raw_span_context)};
  Digest tmpDigest;
  ReplicaStatusMsg msg(replica_id,
                       view_num,
                       lastStable,
                       lastExecuted,
                       viewIsActive,
                       hasNewChangeMsg,
                       listOfPrePrepareMsgsInActiveWindow,
                       listOfMissingViewChangeMsgForViewChange,
                       listOfMissingPrePrepareMsgForViewChange,
                       span_context);
  EXPECT_EQ(msg.getViewNumber(), view_num);
  EXPECT_EQ(msg.getLastStableSeqNum(), lastStable);
  EXPECT_EQ(msg.getLastExecutedSeqNum(), lastExecuted);
  EXPECT_EQ(msg.currentViewIsActive(), viewIsActive);
  EXPECT_EQ(msg.currentViewHasNewViewMessage(), hasNewChangeMsg);
  EXPECT_EQ(msg.hasListOfPrePrepareMsgsInActiveWindow(), listOfPrePrepareMsgsInActiveWindow);
  EXPECT_EQ(msg.hasListOfMissingViewChangeMsgForViewChange(), listOfMissingViewChangeMsgForViewChange);
  EXPECT_EQ(msg.hasListOfMissingPrePrepareMsgForViewChange(), listOfMissingPrePrepareMsgForViewChange);

  msg.validate(replica_info);
  /* the next methods crash the app with an assert ¯\_(ツ)_/¯
  EXPECT_FALSE(msg.isPrePrepareInActiveWindow(151));
  EXPECT_FALSE(msg.isMissingViewChangeMsgForViewChange(replica_id));
  EXPECT_FALSE(msg.isMissingPrePrepareMsgForViewChange(151));
  */
  testMessageBaseMethods(msg, MsgCode::ReplicaStatus, replica_id, span_context);
}

TEST(ReplicaStatusMsg, haslistOfPrePrepareMsgsInActiveWindow) {
  auto config = createReplicaConfig();
  ReplicasInfo replica_info(config, false, false);
  ReplicaId replica_id = 1u;
  ViewNum view_num = 2u;
  SeqNum lastStable = 150u;
  SeqNum lastExecuted = 160u;
  bool viewIsActive = true;
  bool hasNewChangeMsg = true;
  bool listOfPrePrepareMsgsInActiveWindow = true;
  bool listOfMissingViewChangeMsgForViewChange = false;
  bool listOfMissingPrePrepareMsgForViewChange = false;
  const char raw_span_context[] = {"span_\0context"};
  const std::string span_context{raw_span_context, sizeof(raw_span_context)};
  Digest tmpDigest;
  ReplicaStatusMsg msg(replica_id,
                       view_num,
                       lastStable,
                       lastExecuted,
                       viewIsActive,
                       hasNewChangeMsg,
                       listOfPrePrepareMsgsInActiveWindow,
                       listOfMissingViewChangeMsgForViewChange,
                       listOfMissingPrePrepareMsgForViewChange,
                       span_context);
  EXPECT_EQ(msg.getViewNumber(), view_num);
  EXPECT_EQ(msg.getLastStableSeqNum(), lastStable);
  EXPECT_EQ(msg.getLastExecutedSeqNum(), lastExecuted);
  EXPECT_EQ(msg.currentViewIsActive(), viewIsActive);
  EXPECT_EQ(msg.currentViewHasNewViewMessage(), hasNewChangeMsg);
  EXPECT_EQ(msg.hasListOfPrePrepareMsgsInActiveWindow(), listOfPrePrepareMsgsInActiveWindow);
  EXPECT_EQ(msg.hasListOfMissingViewChangeMsgForViewChange(), listOfMissingViewChangeMsgForViewChange);
  EXPECT_EQ(msg.hasListOfMissingPrePrepareMsgForViewChange(), listOfMissingPrePrepareMsgForViewChange);

  msg.validate(replica_info);
  EXPECT_FALSE(msg.isPrePrepareInActiveWindow(151));
  msg.setPrePrepareInActiveWindow(151);
  EXPECT_TRUE(msg.isPrePrepareInActiveWindow(151));

  EXPECT_FALSE(msg.isPrePrepareInActiveWindow(152));
  msg.setPrePrepareInActiveWindow(152);
  EXPECT_TRUE(msg.isPrePrepareInActiveWindow(152));

  msg.validate(replica_info);
  /* the next methods crash the app with an assert ¯\_(ツ)_/¯
  EXPECT_FALSE(msg.isMissingViewChangeMsgForViewChange(replica_id));
  EXPECT_FALSE(msg.isMissingPrePrepareMsgForViewChange(151));
  */
  testMessageBaseMethods(msg, MsgCode::ReplicaStatus, replica_id, span_context);
}

TEST(ReplicaStatusMsg, listOfMissingViewChangeMsgForViewChange) {
  auto config = createReplicaConfig();
  ReplicasInfo replica_info(config, false, false);
  ReplicaId replica_id = 1u;
  ViewNum view_num = 2u;
  SeqNum lastStable = 150u;
  SeqNum lastExecuted = 160u;
  bool viewIsActive = false;
  bool hasNewChangeMsg = true;
  bool listOfPrePrepareMsgsInActiveWindow = false;
  bool listOfMissingViewChangeMsgForViewChange = true;
  bool listOfMissingPrePrepareMsgForViewChange = false;
  const char raw_span_context[] = {"span_\0context"};
  const std::string span_context{raw_span_context, sizeof(raw_span_context)};
  Digest tmpDigest;
  ReplicaStatusMsg msg(replica_id,
                       view_num,
                       lastStable,
                       lastExecuted,
                       viewIsActive,
                       hasNewChangeMsg,
                       listOfPrePrepareMsgsInActiveWindow,
                       listOfMissingViewChangeMsgForViewChange,
                       listOfMissingPrePrepareMsgForViewChange,
                       span_context);
  EXPECT_EQ(msg.getViewNumber(), view_num);
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
  /* the next methods crash the app with an assert ¯\_(ツ)_/¯
  EXPECT_FALSE(msg.isPrePrepareInActiveWindow(151));
  EXPECT_FALSE(msg.isMissingPrePrepareMsgForViewChange(151));
  */
  testMessageBaseMethods(msg, MsgCode::ReplicaStatus, replica_id, span_context);
}

TEST(ReplicaStatusMsg, listOfMissingPrePrepareMsgForViewChange) {
  auto config = createReplicaConfig();
  ReplicaId replica_id = 1u;
  ViewNum view_num = 2u;
  SeqNum lastStable = 150u;
  SeqNum lastExecuted = 160u;
  bool viewIsActive = false;
  bool hasNewChangeMsg = true;
  bool listOfPrePrepareMsgsInActiveWindow = false;
  bool listOfMissingViewChangeMsgForViewChange = false;
  bool listOfMissingPrePrepareMsgForViewChange = true;
  const char raw_span_context[] = {"span_\0context"};
  const std::string span_context{raw_span_context, sizeof(raw_span_context)};
  Digest tmpDigest;
  ReplicaStatusMsg msg(replica_id,
                       view_num,
                       lastStable,
                       lastExecuted,
                       viewIsActive,
                       hasNewChangeMsg,
                       listOfPrePrepareMsgsInActiveWindow,
                       listOfMissingViewChangeMsgForViewChange,
                       listOfMissingPrePrepareMsgForViewChange,
                       span_context);
  EXPECT_EQ(msg.getViewNumber(), view_num);
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
  testMessageBaseMethods(msg, MsgCode::ReplicaStatus, replica_id, span_context);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
