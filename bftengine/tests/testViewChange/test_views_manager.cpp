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

#include "EpochManager.hpp"
#include "gtest/gtest.h"
#include "helper.hpp"
#include "Logger.hpp"
#include "ReplicaConfig.hpp"
#include "ReservedPagesMock.hpp"
#include "SigManager.hpp"
#include "SimpleClient.hpp"
#include "ViewsManager.hpp"

#include "messages/ClientRequestMsg.hpp"
#include "messages/NewViewMsg.hpp"
#include "messages/PrePrepareMsg.hpp"
#include "messages/SignedShareMsgs.hpp"

namespace {

using namespace bftEngine;
using namespace bftEngine::impl;

constexpr int numberOfFaultyReplicas = 2;
constexpr bool dynamicCollectorForPartialProofs = true, dynamicCollectorForExecutionProofs = false;
constexpr int initialView = 0;
constexpr bftEngine::impl::SeqNum lastStableSeqNum = 150, lastExecutedSeqNum = lastStableSeqNum + 1;
bftEngine::test::ReservedPagesMock<EpochManager> res_pages_mock_;

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
                                    concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat,
                                    rc.publicKeysOfReplicas,
                                    replicasInfo)),
        viewsManager(std::make_unique<ViewsManager>(&replicasInfo)) {}
};

TEST_F(ViewsManagerTest, moving_to_higher_view) {
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
  const auto currentView = viewsManager->getCurrentView();
  LOG_INFO(GL, KVLOG(currentView));
  viewsManager->setHigherView(currentView + 1);
  LOG_INFO(GL, KVLOG(viewsManager->getCurrentView()));
  ASSERT_EQ(viewsManager->getCurrentView(), currentView + 1);
}

TEST_F(ViewsManagerTest, store_complaint) {
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
  std::unique_ptr<ReplicaAsksToLeaveViewMsg> complaint(ReplicaAsksToLeaveViewMsg::create(
      rc.replicaId, initialView, ReplicaAsksToLeaveViewMsg::Reason::ClientRequestTimeout));

  if (complaint->viewNumber() == viewsManager->getCurrentView()) {
    viewsManager->storeComplaint(std::move(complaint));
  }

  ASSERT_EQ(viewsManager->getAllMsgsFromComplainedReplicas().size(), 1);
}

TEST_F(ViewsManagerTest, form_a_quorum_of_complaints) {
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
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
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
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
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
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
    auto ptr = ReplicaAsksToLeaveViewMsg::create(
        *std::next(otherReplicas.begin(), complaintNumber),  // Add F + 1 Different complaints
        viewToComplainAbout,
        ReplicaAsksToLeaveViewMsg::Reason::ClientRequestTimeout);
    viewChangeMsg.addComplaint(ptr);
    delete ptr;
  }

  viewsManager->processComplaintsFromViewChangeMessage(&viewChangeMsg, mockedMessageValidator());

  ASSERT_EQ(viewsManager->hasQuorumToLeaveView(), true);
}

TEST_F(ViewsManagerTest, get_quorum_for_higher_view_on_view_change_message_with_enough_complaints) {
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
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
    auto ptr = ReplicaAsksToLeaveViewMsg::create(
        *std::next(otherReplicas.begin(), complaintNumber),  // Add F + 1 Different complaints
        viewToComplainAbout,
        ReplicaAsksToLeaveViewMsg::Reason::ClientRequestTimeout);
    viewChangeMsg.addComplaint(ptr);
    delete ptr;
  }

  viewsManager->processComplaintsFromViewChangeMessage(&viewChangeMsg, mockedMessageValidator());

  ASSERT_EQ(viewsManager->tryToJumpToHigherViewAndMoveComplaintsOnQuorum(&viewChangeMsg), true);
}

TEST_F(ViewsManagerTest, adding_view_change_messages_to_status_message) {
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
  const bftEngine::impl::ReplicaId firstMessageSourceReplicaId = (rc.replicaId + 1) % rc.numReplicas,
                                   secondMessageSourceReplicaId = (rc.replicaId + 2) % rc.numReplicas;
  const int nextView = initialView + 1;
  std::set<bftEngine::impl::ReplicaId> replicasWithNoViewChangeMsgSent;
  for (auto replicaId = 0; replicaId < rc.numReplicas; ++replicaId) {
    replicasWithNoViewChangeMsgSent.insert(replicasWithNoViewChangeMsgSent.end(), replicaId);
  }

  replicasWithNoViewChangeMsgSent.erase(rc.replicaId);  // Remove myself
  replicasWithNoViewChangeMsgSent.erase(firstMessageSourceReplicaId);
  replicasWithNoViewChangeMsgSent.erase(secondMessageSourceReplicaId);

  std::vector<bftEngine::impl::ViewsManager::PrevViewInfo> prevView;

  viewsManager->exitFromCurrentView(lastStableSeqNum, lastExecutedSeqNum, prevView);
  viewsManager->setHigherView(nextView);

  ASSERT_NE(rc.replicaId, firstMessageSourceReplicaId);
  ASSERT_NE(rc.replicaId, secondMessageSourceReplicaId);
  ASSERT_NE(firstMessageSourceReplicaId, secondMessageSourceReplicaId);

  ViewChangeMsg *viewChangeMsg1 = new ViewChangeMsg(firstMessageSourceReplicaId, nextView, lastStableSeqNum),
                *viewChangeMsg2 = new ViewChangeMsg(secondMessageSourceReplicaId, nextView, lastStableSeqNum);

  ASSERT_TRUE(viewsManager->add(viewChangeMsg1));
  ASSERT_TRUE(viewsManager->add(viewChangeMsg2));

  bftEngine::impl::ReplicaStatusMsg replicaStatusMessage(
      rc.getreplicaId(), initialView, lastStableSeqNum, lastExecutedSeqNum, false, false, false, true, false);

  viewsManager->fillPropertiesOfStatusMessage(replicaStatusMessage, &replicasInfo, lastStableSeqNum);

  ASSERT_FALSE(replicaStatusMessage.isMissingViewChangeMsgForViewChange(firstMessageSourceReplicaId));
  ASSERT_FALSE(replicaStatusMessage.isMissingViewChangeMsgForViewChange(secondMessageSourceReplicaId));

  for (auto replicaId : replicasWithNoViewChangeMsgSent) {
    ASSERT_TRUE(replicaStatusMessage.isMissingViewChangeMsgForViewChange(replicaId));
  }
}

TEST_F(ViewsManagerTest, adding_pre_prepare_messages_to_status_message) {
  // Two separate status messages are needed to validate the state of the views manager.
  // The first status message verifies that there is a missing Pre-Prepare message.
  bftEngine::impl::ReplicaStatusMsg replicaStatusMessage(
      rc.getreplicaId(), initialView, lastStableSeqNum, lastExecutedSeqNum, false, false, false, false, true);
  // The second status message verifies that there is no longer a missing Pre-Prepare message after it has been added.
  bftEngine::impl::ReplicaStatusMsg replicaStatusMessage2(
      rc.getreplicaId(), initialView, lastStableSeqNum, lastExecutedSeqNum, false, false, false, false, true);

  // Create a sample client request.
  const uint32_t kRequestLength = 2;
  uint64_t expectedLastValue = 12345;
  const uint64_t requestBuffer[kRequestLength] = {(uint64_t)200, expectedLastValue};

  auto* clientRequest = new ClientRequestMsg((uint16_t)1,
                                             bftEngine::ClientMsgFlag::EMPTY_FLAGS_REQ,
                                             (uint64_t)1234567,
                                             kRequestLength,
                                             (const char*)requestBuffer,
                                             (uint64_t)1000000);

  // Create a Pre-Prepare message and add the client request to it.
  PrePrepareMsg* prePrepareMsg =
      new PrePrepareMsg(rc.getreplicaId(), initialView, lastExecutedSeqNum, CommitPath::SLOW, clientRequest->size());

  prePrepareMsg->addRequest(clientRequest->body(), clientRequest->size());
  prePrepareMsg->finishAddingRequests();

  const auto primary = replicasInfo.primaryOfView(initialView);
  char buff[32]{};

  // Create a PrepareFullMessage. When this message is added as an element to a view change message,
  // it will be used to create a PreparedCertificate.
  PrepareFullMsg* prepareFullMsg = PrepareFullMsg::create(initialView, lastExecutedSeqNum, primary, buff, sizeof(buff));

  const int N = rc.numReplicas;
  ViewChangeMsg** viewChangeMsgs = new ViewChangeMsg*[N];
  const auto nextView = initialView + 1;
  uint16_t numberOfNeededMessages = 2 * rc.fVal + 1;

  // Create view change messages and add them to the views manager.
  for (int i = 0; i < numberOfNeededMessages; ++i) {
    viewChangeMsgs[i] = new ViewChangeMsg(i + 1, nextView, lastStableSeqNum);
    viewsManager->add(viewChangeMsgs[i]);
  }

  // Add the Pre-Prepare message's digest and the PrepareFullMessage to one of the view change messages.
  viewChangeMsgs[0]->addElement(lastExecutedSeqNum,
                                prePrepareMsg->digestOfRequests(),
                                prePrepareMsg->viewNumber(),
                                true,
                                prePrepareMsg->viewNumber(),
                                prepareFullMsg->signatureLen(),
                                prepareFullMsg->signatureBody());

  for (int i = numberOfNeededMessages; i < N; ++i) {
    viewChangeMsgs[i] = nullptr;
  }

  NewViewMsg* newViewMsg = new NewViewMsg(1, nextView);
  Digest digest;

  // Add the view messages' digests to the new view message.
  for (size_t i = 0; i < numberOfNeededMessages; ++i) {
    viewChangeMsgs[i]->getMsgDigest(digest);
    ConcordAssert(!digest.isZero());
    newViewMsg->addElement(i + 1, digest);
  }

  // Add the new view message to the views manager.
  viewsManager->add(newViewMsg);

  std::vector<bftEngine::impl::ViewsManager::PrevViewInfo> prevView;
  // In order to change the view manager's status from Stat::IN_VIEW, exit should be called beforehand.
  // This permits calls of the "tryToEnterView" function to be made.
  viewsManager->exitFromCurrentView(lastStableSeqNum, lastExecutedSeqNum, prevView);

  vector<PrePrepareMsg*> outPrePrepareMsgs;
  // Change the views manager's status to Stat::PENDING_WITH_RESTRICTIONS by attempting to enter the next view.
  // This permits the call of "addPotentiallyMissingPP", which is needed in order for the Pre-Prepare message to be
  // added to the views manager.
  // This attempt to enter the next view should fail due to the missing Pre-Prepare message.
  ASSERT_FALSE(viewsManager->tryToEnterView(nextView, lastStableSeqNum, lastExecutedSeqNum, &outPrePrepareMsgs));

  // Change the view so that the current view becomes pending.
  // While the current view is pending, the function "fillPropertiesOfStatusMessage" reflects missing Pre-Prepare
  // messages in the status request message.
  viewsManager->setHigherView(nextView);

  // Fill the necessary information in the first status message so that it designates that there is a missing
  // Pre-Prepare message.
  viewsManager->fillPropertiesOfStatusMessage(replicaStatusMessage, &replicasInfo, lastStableSeqNum);
  // Observe that there is a missing Pre-Prepare message indeed.
  ASSERT_TRUE(replicaStatusMessage.isMissingPrePrepareMsgForViewChange(lastExecutedSeqNum));

  // Add the missing Pre-Prepare message.
  viewsManager->addPotentiallyMissingPP(prePrepareMsg, lastStableSeqNum);

  // After the missing Pre-Prepare message has been added there is no longer any obstacle that prevents from entering
  // the next view.
  ASSERT_TRUE(viewsManager->tryToEnterView(nextView, lastStableSeqNum, lastExecutedSeqNum, &outPrePrepareMsgs));

  // Fill the necessary information in the second status message so that it designates that there are no longer missing
  // Pre-Prepare messages.
  viewsManager->fillPropertiesOfStatusMessage(replicaStatusMessage2, &replicasInfo, lastStableSeqNum);
  // Observe that there are no missing Pre-Prepare messages.
  ASSERT_FALSE(replicaStatusMessage2.isMissingPrePrepareMsgForViewChange(lastExecutedSeqNum));
}

TEST_F(ViewsManagerTest, trigger_view_change) {
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
  ASSERT_EQ(rc.numReplicas, 3 * numberOfFaultyReplicas + 1);
  ASSERT_EQ(rc.fVal, numberOfFaultyReplicas);
  ASSERT_EQ(rc.cVal, 0);

  const bftEngine::impl::ReplicaId firstMessageSourceReplicaId = (rc.replicaId + 1) % rc.numReplicas,
                                   secondMessageSourceReplicaId = (rc.replicaId + 2) % rc.numReplicas,
                                   thirdMessageSourceReplicaId = (rc.replicaId + 3) % rc.numReplicas,
                                   fourthMessageSourceReplicaId = (rc.replicaId + 4) % rc.numReplicas,
                                   fifthMessageSourceReplicaId = (rc.replicaId + 5) % rc.numReplicas;
  vector<ReplicaId> sourceReplicaIds = {firstMessageSourceReplicaId,
                                        secondMessageSourceReplicaId,
                                        thirdMessageSourceReplicaId,
                                        fourthMessageSourceReplicaId,
                                        fifthMessageSourceReplicaId};

  ASSERT_NE(rc.replicaId, firstMessageSourceReplicaId);
  ASSERT_NE(rc.replicaId, secondMessageSourceReplicaId);
  ASSERT_NE(rc.replicaId, thirdMessageSourceReplicaId);
  ASSERT_NE(rc.replicaId, fourthMessageSourceReplicaId);
  ASSERT_NE(rc.replicaId, fifthMessageSourceReplicaId);

  const int nextView = initialView + 1;

  std::vector<ViewChangeMsg*> viewChangeMsgs;
  viewChangeMsgs.push_back(new ViewChangeMsg(firstMessageSourceReplicaId, nextView, lastStableSeqNum));
  viewChangeMsgs.push_back(new ViewChangeMsg(secondMessageSourceReplicaId, nextView, lastStableSeqNum));
  viewChangeMsgs.push_back(new ViewChangeMsg(thirdMessageSourceReplicaId, nextView, lastStableSeqNum));
  viewChangeMsgs.push_back(new ViewChangeMsg(fourthMessageSourceReplicaId, nextView, lastStableSeqNum));
  viewChangeMsgs.push_back(new ViewChangeMsg(fifthMessageSourceReplicaId, nextView, lastStableSeqNum));

  for (auto viewChangeMsg : viewChangeMsgs) {
    ASSERT_TRUE(viewsManager->add(viewChangeMsg));
  }

  ASSERT_EQ(sourceReplicaIds.size(), viewChangeMsgs.size());

  NewViewMsg* newViewMsg = new NewViewMsg(firstMessageSourceReplicaId, nextView);
  Digest digest;

  for (size_t i = 0; i < viewChangeMsgs.size(); ++i) {
    viewChangeMsgs[i]->getMsgDigest(digest);
    ConcordAssert(!digest.isZero());
    newViewMsg->addElement(sourceReplicaIds[i], digest);
  }

  viewsManager->add(newViewMsg);

  std::vector<bftEngine::impl::ViewsManager::PrevViewInfo> prevView;
  // In order to change the view manager's status from Stat::IN_VIEW, exit should be called beforehand.
  viewsManager->exitFromCurrentView(lastStableSeqNum, lastExecutedSeqNum, prevView);

  vector<PrePrepareMsg*> outPrePrepareMsgs;
  viewsManager->tryToEnterView(nextView, lastStableSeqNum, lastExecutedSeqNum, &outPrePrepareMsgs);

  ASSERT_EQ(viewsManager->latestActiveView(), nextView);
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
