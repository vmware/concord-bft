// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include "gtest/gtest.h"
#include "helper.hpp"
#include "Logger.hpp"
#include "ReplicaConfig.hpp"
#include "SigManager.hpp"
#include "ViewsManager.hpp"

namespace {

using namespace bftEngine;
using namespace bftEngine::impl;

constexpr int numberOfFaultyReplicas = 2;
constexpr bool dynamicCollectorForPartialProofs = true, dynamicCollectorForExecutionProofs = false;
constexpr int initialView = 0;
constexpr bftEngine::impl::SeqNum lastStableSeqNum = 150;

std::function<bool(MessageBase*)> mockedMessageValidator() {
  return [](MessageBase* message) { return true; };
}

class ViewsManagerTest : public ::testing::Test {
 protected:
  ReplicaConfig& rc;
  ReplicasInfo replicasInfo;
  std::unique_ptr<SigManager> sigManager;
  std::unique_ptr<ViewsManager> viewsManager;

  ViewsManagerTest()
      : rc(createReplicaConfig(numberOfFaultyReplicas)),
        replicasInfo(rc, dynamicCollectorForPartialProofs, dynamicCollectorForExecutionProofs),
        sigManager(createSigManager(rc.replicaId,
                                    rc.replicaPrivateKey,
                                    KeyFormat::HexaDecimalStrippedFormat,
                                    rc.publicKeysOfReplicas,
                                    replicasInfo)),
        viewsManager(std::make_unique<ViewsManager>(&replicasInfo)) {}
};

TEST_F(ViewsManagerTest, moving_to_higher_view) {
  const auto currentView = viewsManager->getCurrentView();
  LOG_INFO(GL, KVLOG(currentView));
  viewsManager->setHigherView(currentView + 1);
  LOG_INFO(GL, KVLOG(viewsManager->getCurrentView()));
  ASSERT_EQ(viewsManager->getCurrentView(), currentView + 1);
}

TEST_F(ViewsManagerTest, store_complaint) {
  std::unique_ptr<ReplicaAsksToLeaveViewMsg> complaint(ReplicaAsksToLeaveViewMsg::create(
      rc.replicaId, initialView, ReplicaAsksToLeaveViewMsg::Reason::ClientRequestTimeout));

  if (complaint->viewNumber() == viewsManager->getCurrentView()) {
    viewsManager->storeComplaint(std::move(complaint));
  }

  ASSERT_EQ(viewsManager->getAllMsgsFromComplainedReplicas().size(), 1);
}

TEST_F(ViewsManagerTest, form_a_quorum_of_complaints) {
  for (int replicaId = 0; replicaId < rc.numReplicas; ++replicaId) {
    if (replicaId == rc.replicaId) continue;

    std::unique_ptr<ReplicaAsksToLeaveViewMsg> complaint(ReplicaAsksToLeaveViewMsg::create(
        replicaId, initialView, ReplicaAsksToLeaveViewMsg::Reason::ClientRequestTimeout));

    if (complaint->viewNumber() == viewsManager->getCurrentView()) {
      viewsManager->storeComplaint(std::move(complaint));
    }
  }

  ASSERT_EQ(viewsManager->hasQuorumToLeaveView(), true);
}

TEST_F(ViewsManagerTest, status_message_with_complaints) {
  bftEngine::impl::SeqNum lastExecutedSeqNum = 200;
  const bool viewIsActive = true;
  const bool hasNewChangeMsg = true;
  const bool listOfPPInActiveWindow = false, listOfMissingVCMsg = false, listOfMissingPPMsg = false;

  for (int replicaId = 0; replicaId < rc.numReplicas; ++replicaId) {
    if (replicaId == rc.replicaId) continue;

    std::unique_ptr<ReplicaAsksToLeaveViewMsg> complaint(ReplicaAsksToLeaveViewMsg::create(
        replicaId, initialView, ReplicaAsksToLeaveViewMsg::Reason::ClientRequestTimeout));

    if (complaint->viewNumber() == viewsManager->getCurrentView()) {
      viewsManager->storeComplaint(std::move(complaint));
    }
  }

  bftEngine::impl::ReplicaStatusMsg replicaStatusMessage(rc.getreplicaId(),
                                                         initialView,
                                                         lastStableSeqNum,
                                                         lastExecutedSeqNum,
                                                         viewIsActive,
                                                         hasNewChangeMsg,
                                                         listOfPPInActiveWindow,
                                                         listOfMissingVCMsg,
                                                         listOfMissingPPMsg);

  viewsManager->addComplaintsToStatusMessage(replicaStatusMessage);

  for (const auto& i : viewsManager->getAllMsgsFromComplainedReplicas()) {
    ASSERT_TRUE(replicaStatusMessage.hasComplaintFromReplica(i.first));
  }
}

TEST_F(ViewsManagerTest, get_quorum_for_next_view_on_view_change_message_with_enough_complaints) {
  bftEngine::impl::ReplicaId sourceReplicaId = (rc.replicaId + 1) % rc.numReplicas;
  std::set<bftEngine::impl::ReplicaId> otherReplicas;
  const int nextView = initialView + 1;
  const int viewToComplainAbout = initialView;

  ViewChangeMsg viewChangeMsg = ViewChangeMsg(sourceReplicaId, nextView, lastStableSeqNum);

  for (int i = 0; i < rc.numReplicas; ++i) {
    if (i == rc.replicaId || i == sourceReplicaId) continue;
    otherReplicas.insert(i);
  }

  ASSERT_LE(rc.fVal + 1, otherReplicas.size());

  for (int complaintNumber = 0; complaintNumber <= rc.fVal; ++complaintNumber) {
    viewChangeMsg.addComplaint(ReplicaAsksToLeaveViewMsg::create(
        *std::next(otherReplicas.begin(), complaintNumber),  // Add F + 1 Different complaints
        viewToComplainAbout,
        ReplicaAsksToLeaveViewMsg::Reason::ClientRequestTimeout));
  }

  viewsManager->processComplaintsFromViewChangeMessage(&viewChangeMsg, mockedMessageValidator());

  ASSERT_EQ(viewsManager->hasQuorumToLeaveView(), true);
}

TEST_F(ViewsManagerTest, get_quorum_for_higher_view_on_view_change_message_with_enough_complaints) {
  bftEngine::impl::ReplicaId sourceReplicaId = (rc.replicaId + 1) % rc.numReplicas;
  std::set<bftEngine::impl::ReplicaId> otherReplicas;
  const int higherView = initialView + 2;
  const int viewToComplainAbout = higherView - 1;

  ViewChangeMsg viewChangeMsg = ViewChangeMsg(sourceReplicaId, higherView, lastStableSeqNum);

  for (int i = 0; i < rc.numReplicas; ++i) {
    if (i == rc.replicaId || i == sourceReplicaId) continue;
    otherReplicas.insert(i);
  }

  ASSERT_LE(rc.fVal + 1, otherReplicas.size());

  for (int complaintNumber = 0; complaintNumber <= rc.fVal; ++complaintNumber) {
    viewChangeMsg.addComplaint(ReplicaAsksToLeaveViewMsg::create(
        *std::next(otherReplicas.begin(), complaintNumber),  // Add F + 1 Different complaints
        viewToComplainAbout,
        ReplicaAsksToLeaveViewMsg::Reason::ClientRequestTimeout));
  }

  viewsManager->processComplaintsFromViewChangeMessage(&viewChangeMsg, mockedMessageValidator());

  ASSERT_EQ(viewsManager->tryToJumpToHigherViewAndMoveComplaintsOnQuorum(&viewChangeMsg), true);
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}