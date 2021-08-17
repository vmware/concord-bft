// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "messages/FullCommitProofMsg.hpp"
#include "InternalReplicaApi.hpp"
#include "communication/CommFactory.hpp"
#include "Logger.hpp"
#include "ReplicaConfig.hpp"
#include "SimpleThreadPool.hpp"
#include "ViewChangeSafetyLogic.hpp"
#include "messages/PrePrepareMsg.hpp"
#include "KeyfileIOUtils.hpp"
#include "messages/SignedShareMsgs.hpp"
#include "messages/ClientRequestMsg.hpp"
#include "SimpleClient.hpp"
#include "SequenceWithActiveWindow.hpp"
#include "SeqNumInfo.hpp"
#include "SysConsts.hpp"
#include "MsgHandlersRegistrator.hpp"
#include "IncomingMsgsStorageImp.hpp"
#include "ViewsManager.hpp"
#include "helper.hpp"
#include "SigManager.hpp"
#include "ReservedPagesMock.hpp"
#include "EpochManager.hpp"

#include "gtest/gtest.h"

#include <memory>

using namespace std;
using namespace bftEngine;
bftEngine::test::ReservedPagesMock<EpochManager> res_pages_mock_;
namespace {
// Since ReplicaConfig is a singleton and the test requires several instances,
// we need to define a replica config for tests.
class TestReplicaConfig : public bftEngine::ReplicaConfig {};

TestReplicaConfig replicaConfig[4] = {};
static const int N = 4;
static const int F = 1;
static const int C = 0;
ViewChangeSafetyLogic::Restriction restrictions[kWorkWindowSize];
std::unique_ptr<bftEngine::impl::ReplicasInfo> pRepInfo;
std::unique_ptr<SigManager> sigManager_;

class DummyShareSecretKey : public IShareSecretKey {
 public:
  virtual std::string toString() const override { return std::string("123"); }
};

class DummyShareVerificationKey : public IShareVerificationKey {
 public:
  virtual std::string toString() const override { return std::string("123"); }
};

class DummySigner : public IThresholdSigner {
  DummyShareSecretKey dummyShareSecretKey_;
  DummyShareVerificationKey dummyShareVerificationKey_;

 public:
  virtual int requiredLengthForSignedData() const override { return 3; }
  virtual void signData(const char* hash, int hashLen, char* outSig, int outSigLen) override {}

  virtual const IShareSecretKey& getShareSecretKey() const override { return dummyShareSecretKey_; }
  virtual const IShareVerificationKey& getShareVerificationKey() const override { return dummyShareVerificationKey_; }

} dummySigner_;

void setUpConfiguration_4() {
  for (int i = 0; i < N; i++) {
    replicaConfig[i].numReplicas = N;
    replicaConfig[i].fVal = F;
    replicaConfig[i].cVal = C;
    replicaConfig[i].replicaId = i;
  }
  loadPrivateAndPublicKeys(replicaConfig[0].replicaPrivateKey, replicaConfig[0].publicKeysOfReplicas, 0, N);
  pRepInfo = std::make_unique<bftEngine::impl::ReplicasInfo>(replicaConfig[0], true, true);

  sigManager_.reset(createSigManager(0,
                                     replicaConfig[0].replicaPrivateKey,
                                     KeyFormat::HexaDecimalStrippedFormat,
                                     replicaConfig[0].publicKeysOfReplicas,
                                     *pRepInfo));
}

TEST(testViewchangeSafetyLogic_test, computeRestrictions) {
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
  bftEngine::impl::SeqNum lastStableSeqNum = 150;
  const uint32_t kRequestLength = 2;

  uint64_t expectedLastValue = 12345;
  const uint64_t requestBuffer[kRequestLength] = {(uint64_t)200, expectedLastValue};
  ViewNum view = 0;
  bftEngine::impl::SeqNum assignedSeqNum = lastStableSeqNum + 1;

  auto* clientRequest = new ClientRequestMsg((uint16_t)1,
                                             bftEngine::ClientMsgFlag::EMPTY_FLAGS_REQ,
                                             (uint64_t)1234567,
                                             kRequestLength,
                                             (const char*)requestBuffer,
                                             (uint64_t)1000000);

  auto primary = pRepInfo->primaryOfView(view);
  // Generate the PrePrepare from the primary for the clientRequest
  auto* ppMsg =
      new PrePrepareMsg(primary, view, assignedSeqNum, bftEngine::impl::CommitPath::SLOW, clientRequest->size());
  ppMsg->addRequest(clientRequest->body(), clientRequest->size());
  ppMsg->finishAddingRequests();

  char buff[32]{};
  PrepareFullMsg* pfMsg = PrepareFullMsg::create(view, assignedSeqNum, primary, buff, sizeof(buff));

  ViewChangeMsg** viewChangeMsgs = new ViewChangeMsg*[N];
  auto futureView = view + 1;

  viewChangeMsgs[0] = new ViewChangeMsg(0, futureView, lastStableSeqNum);
  viewChangeMsgs[1] = nullptr;
  viewChangeMsgs[2] = new ViewChangeMsg(2, futureView, lastStableSeqNum);
  viewChangeMsgs[3] = new ViewChangeMsg(3, futureView, lastStableSeqNum);

  viewChangeMsgs[2]->addElement(assignedSeqNum,
                                ppMsg->digestOfRequests(),
                                ppMsg->viewNumber(),
                                true,
                                ppMsg->viewNumber(),
                                pfMsg->signatureLen(),
                                pfMsg->signatureBody());

  auto VCS = ViewChangeSafetyLogic(N, F, C, PrePrepareMsg::digestOfNullPrePrepareMsg());

  SeqNum min{}, max{};
  VCS.computeRestrictions(viewChangeMsgs, VCS.calcLBStableForView(viewChangeMsgs), min, max, restrictions);

  for (int i = 0; i < kWorkWindowSize; i++) {
    if (i == assignedSeqNum - min) {
      ConcordAssert(!restrictions[assignedSeqNum - min].isNull);
      ConcordAssert(ppMsg->digestOfRequests().toString() == restrictions[assignedSeqNum - min].digest.toString());
    } else {
      ConcordAssert(restrictions[i].isNull);
    }
  }

  for (int i = 0; i < N; i++) {
    delete viewChangeMsgs[i];
    viewChangeMsgs[i] = nullptr;
  }
  delete[] viewChangeMsgs;
  delete clientRequest;
  delete pfMsg;
  delete ppMsg;
}
// The purpose of this test is to verify that when a Replica receives a set of
// view change messages enough for it to confirm a view change it will consider
// the Safe Prepare Certificate for a sequence number that has two Prepare
// Certificates. The safe Prepare Certificate for a given sequence is the one with
// the highest View number, so after ViewchangeSafetyLogic::computeRestrictions
// is called we verify that for this sequence number we are going to redo the
// protocol with the right PrePrepare, matching the Prepare Certificate with
// the highest View number.
TEST(testViewchangeSafetyLogic_test, computeRestrictions_two_prepare_certs_for_same_seq_no) {
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
  char buff[32]{};
  bftEngine::impl::SeqNum lastStableSeqNum = 100;
  const uint32_t kRequestLength = 2;
  uint64_t expectedLastValue1 = 12345;
  const uint64_t requestBuffer1[kRequestLength] = {(uint64_t)200, expectedLastValue1};
  ViewNum view1 = 0;
  bftEngine::impl::SeqNum assignedSeqNum = lastStableSeqNum + 1;

  auto* clientRequest1 = new ClientRequestMsg((uint16_t)1,
                                              bftEngine::ClientMsgFlag::EMPTY_FLAGS_REQ,
                                              (uint64_t)1234567,
                                              kRequestLength,
                                              (const char*)requestBuffer1,
                                              (uint64_t)1000000);

  // Generate the PrePrepare from the primary for the clientRequest1
  auto* ppMsg1 = new PrePrepareMsg(
      pRepInfo->primaryOfView(view1), view1, assignedSeqNum, bftEngine::impl::CommitPath::SLOW, clientRequest1->size());
  ppMsg1->addRequest(clientRequest1->body(), clientRequest1->size());
  ppMsg1->finishAddingRequests();

  // Here we generate a valid Prepare Certificate for clientRequest1 with
  // View=0, this is going to be the Prepare Certificate that has to be
  // discarded for the sequence number -> assignedSeqNum, because we are
  // going to generate a Prepare Certificate for this sequence number with
  // a higher view.
  PrepareFullMsg* pfMsg1 =
      PrepareFullMsg::create(view1, assignedSeqNum, pRepInfo->primaryOfView(view1), buff, sizeof(buff));

  uint64_t expectedLastValue2 = 1234567;
  const uint64_t requestBuffer2[kRequestLength] = {(uint64_t)200, expectedLastValue2};
  ViewNum view2 = 1;

  auto* clientRequest2 = new ClientRequestMsg((uint16_t)2,
                                              bftEngine::ClientMsgFlag::EMPTY_FLAGS_REQ,
                                              (uint64_t)1234567,
                                              kRequestLength,
                                              (const char*)requestBuffer2,
                                              (uint64_t)1000000);

  // Generate the PrePrepare from the primary for the clientRequest2
  auto* ppMsg2 = new PrePrepareMsg(
      pRepInfo->primaryOfView(view2), view2, assignedSeqNum, bftEngine::impl::CommitPath::SLOW, clientRequest2->size());
  ppMsg2->addRequest(clientRequest2->body(), clientRequest2->size());
  ppMsg2->finishAddingRequests();
  // Here we generate a valid Prepare Certificate for clientRequest2 with
  // View=1, this is going to be the Prepare Certificate that has to be
  // considered for the sequence number -> assignedSeqNum, because it will
  // be the one with the highest View for assignedSeqNum.
  PrepareFullMsg* pfMsg2 =
      PrepareFullMsg::create(view2, assignedSeqNum, pRepInfo->primaryOfView(view2), buff, sizeof(buff));

  ViewChangeMsg** viewChangeMsgs = new ViewChangeMsg*[N];
  auto futureView = view2 + 1;

  viewChangeMsgs[0] = new ViewChangeMsg(0, futureView, lastStableSeqNum);
  viewChangeMsgs[1] = nullptr;
  viewChangeMsgs[2] = new ViewChangeMsg(2, futureView, lastStableSeqNum);
  viewChangeMsgs[3] = new ViewChangeMsg(3, futureView, lastStableSeqNum);

  // Add the first Prepare Certificate which we expect to be ignored
  // to the View Change Msg from Replica 2
  viewChangeMsgs[2]->addElement(assignedSeqNum,
                                ppMsg1->digestOfRequests(),
                                ppMsg1->viewNumber(),
                                true,
                                ppMsg1->viewNumber(),
                                pfMsg1->signatureLen(),
                                pfMsg1->signatureBody());

  // Add the second Prepare Certificate which we expect to be considered in the next view
  // to the View Change Msg from Replica 3
  viewChangeMsgs[3]->addElement(assignedSeqNum,
                                ppMsg2->digestOfRequests(),
                                ppMsg2->viewNumber(),
                                true,
                                ppMsg2->viewNumber(),
                                pfMsg2->signatureLen(),
                                pfMsg2->signatureBody());

  auto VCS = ViewChangeSafetyLogic(N, F, C, PrePrepareMsg::digestOfNullPrePrepareMsg());

  SeqNum min{}, max{};
  VCS.computeRestrictions(viewChangeMsgs, VCS.calcLBStableForView(viewChangeMsgs), min, max, restrictions);

  for (int i = 0; i < kWorkWindowSize; i++) {
    if (i == assignedSeqNum - min) {
      ConcordAssert(!restrictions[assignedSeqNum - min].isNull);
      ConcordAssert(ppMsg2->digestOfRequests().toString() != ppMsg1->digestOfRequests().toString());
      // Assert the prepare certificate with higher view number is selected for assignedSeqNum
      ConcordAssert(ppMsg2->digestOfRequests().toString() == restrictions[assignedSeqNum - min].digest.toString());
    } else {
      ConcordAssert(restrictions[i].isNull);
    }
  }

  for (int i = 0; i < N; i++) {
    delete viewChangeMsgs[i];
    viewChangeMsgs[i] = nullptr;
  }
  delete[] viewChangeMsgs;
  delete clientRequest1;
  delete pfMsg1;
  delete ppMsg1;
  delete clientRequest2;
  delete pfMsg2;
  delete ppMsg2;
}
// The purpose of this test is to verify that a Prepare Certificate
// whose Sequence number is below the Last Stable Sequence number
// will be ignored in the View Change and its associated Preprepare's
// digest will not be present in the restrictions array generated by
// ViewChangeSafetyLogic::computeRestrictions.
TEST(testViewchangeSafetyLogic_test, computeRestrictions_two_prepare_certs_one_ignored) {
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
  char buff[32]{};
  bftEngine::impl::SeqNum lastStableSeqNum = 300;
  const uint32_t kRequestLength = 2;
  uint64_t expectedLastValue1 = 12345;
  const uint64_t requestBuffer1[kRequestLength] = {(uint64_t)200, expectedLastValue1};
  ViewNum view1 = 0;
  // sequence number that is going to be above the last stable and has to be
  // considered during the View Change.
  bftEngine::impl::SeqNum assignedSeqNum = lastStableSeqNum + (kWorkWindowSize / 3);
  // sequence number that is going to be below the last stable and has to be
  // ignored during the View Change.
  bftEngine::impl::SeqNum assignedSeqNumIgnored = lastStableSeqNum - (kWorkWindowSize / 2);

  auto* clientRequest1 = new ClientRequestMsg((uint16_t)1,
                                              bftEngine::ClientMsgFlag::EMPTY_FLAGS_REQ,
                                              (uint64_t)1234567,
                                              kRequestLength,
                                              (const char*)requestBuffer1,
                                              (uint64_t)1000000);

  // Generate the PrePrepare from the primary for the clientRequest1
  auto* ppMsg1 = new PrePrepareMsg(
      pRepInfo->primaryOfView(view1), view1, assignedSeqNum, bftEngine::impl::CommitPath::SLOW, clientRequest1->size());
  ppMsg1->addRequest(clientRequest1->body(), clientRequest1->size());
  ppMsg1->finishAddingRequests();

  // Here we generate a valid Prepare Certificate for clientRequest1 with
  // View=0 and a sequence number that is above the Last Stable Sequence
  // Number. This Prepare certificate has to be considered during a View
  // change and it's associated PrePrepare message has to be in the propper
  // position in the restrictions array generated by ViewChangeSafetyLogic::computeRestrictions
  PrepareFullMsg* pfMsg1 =
      PrepareFullMsg::create(view1, assignedSeqNum, pRepInfo->primaryOfView(view1), buff, sizeof(buff));

  uint64_t expectedLastValue2 = 1234567;
  const uint64_t requestBuffer2[kRequestLength] = {(uint64_t)200, expectedLastValue2};
  ViewNum view2 = 1;

  auto* clientRequest2 = new ClientRequestMsg((uint16_t)2,
                                              bftEngine::ClientMsgFlag::EMPTY_FLAGS_REQ,
                                              (uint64_t)1234567,
                                              kRequestLength,
                                              (const char*)requestBuffer2,
                                              (uint64_t)1000000);

  // Generate the PrePrepare from the primary for the clientRequest2
  auto* ppMsg2 = new PrePrepareMsg(pRepInfo->primaryOfView(view2),
                                   view2,
                                   assignedSeqNumIgnored,
                                   bftEngine::impl::CommitPath::SLOW,
                                   clientRequest2->size());
  ppMsg2->addRequest(clientRequest2->body(), clientRequest2->size());
  ppMsg2->finishAddingRequests();

  // Here we generate a valid Prepare Certificate for clientRequest2 with
  // View=1, but with Sequence number below the last Stable Sequence.
  // This Prepare Certificate's associated PrePrepare must not be present
  // in the restrictions array generated by ViewChangeSafetyLogic::computeRestrictions
  PrepareFullMsg* pfMsg2 =
      PrepareFullMsg::create(view2, assignedSeqNumIgnored, pRepInfo->primaryOfView(view2), buff, sizeof(buff));

  ViewChangeMsg** viewChangeMsgs = new ViewChangeMsg*[N];
  auto futureView = view2 + 1;

  viewChangeMsgs[0] = new ViewChangeMsg(0, futureView, lastStableSeqNum);
  viewChangeMsgs[1] = nullptr;
  viewChangeMsgs[2] = new ViewChangeMsg(2, futureView, lastStableSeqNum);
  viewChangeMsgs[3] = new ViewChangeMsg(3, futureView, 0);

  // Add the first Prepare Certificate
  // to the View Change Msg from Replica 1
  viewChangeMsgs[0]->addElement(assignedSeqNum,
                                ppMsg1->digestOfRequests(),
                                ppMsg1->viewNumber(),
                                true,
                                ppMsg1->viewNumber(),
                                pfMsg1->signatureLen(),
                                pfMsg1->signatureBody());

  // Add the second Prepare Certificate
  // to the View Change Msg from Replica 3
  viewChangeMsgs[3]->addElement(assignedSeqNumIgnored,
                                ppMsg2->digestOfRequests(),
                                ppMsg2->viewNumber(),
                                true,
                                ppMsg2->viewNumber(),
                                pfMsg2->signatureLen(),
                                pfMsg2->signatureBody());

  auto VCS = ViewChangeSafetyLogic(N, F, C, PrePrepareMsg::digestOfNullPrePrepareMsg());

  SeqNum min{}, max{};
  VCS.computeRestrictions(viewChangeMsgs, VCS.calcLBStableForView(viewChangeMsgs), min, max, restrictions);

  // Check that the reported lowest meaningful sequence number for the
  // View Change (min) is above the one that is below the last stable
  // (assignedSeqNumIgnored) used to generate pfMsg2.
  ConcordAssert(min > assignedSeqNumIgnored);

  for (int i = 0; i < kWorkWindowSize; i++) {
    if (i == assignedSeqNum - min) {
      // Check that the sequence number above the last stable is considered
      ConcordAssert(!restrictions[assignedSeqNum - min].isNull);
      ConcordAssert(ppMsg1->digestOfRequests().toString() == restrictions[assignedSeqNum - min].digest.toString());
    } else {
      // Check that all others are NULL, and thus the Prepare Certificate
      // for the sequence number below the last stable is ignored.
      ConcordAssert(restrictions[i].isNull);
    }
  }

  for (int i = 0; i < N; i++) {
    delete viewChangeMsgs[i];
    viewChangeMsgs[i] = nullptr;
  }
  delete[] viewChangeMsgs;
  delete clientRequest1;
  delete pfMsg1;
  delete ppMsg1;
  delete clientRequest2;
  delete pfMsg2;
  delete ppMsg2;
}

TEST(testViewchangeSafetyLogic_test, one_different_new_view_in_VC_msgs) {
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
  ViewChangeMsg** viewChangeMsgs = new ViewChangeMsg*[N];

  for (int i = 0; i < N; i++) {
    viewChangeMsgs[i] = nullptr;
  }

  viewChangeMsgs[0] = new ViewChangeMsg(0, 10, 0);
  viewChangeMsgs[2] = new ViewChangeMsg(2, 10, 0);
  viewChangeMsgs[3] = new ViewChangeMsg(3, 11, 0);

  auto VCS = ViewChangeSafetyLogic(N, F, C, PrePrepareMsg::digestOfNullPrePrepareMsg());

  ASSERT_DEATH(VCS.calcLBStableForView(viewChangeMsgs), "");

  for (int i = 0; i < N; i++) {
    delete viewChangeMsgs[i];
    viewChangeMsgs[i] = nullptr;
  }
  delete[] viewChangeMsgs;
}

TEST(testViewchangeSafetyLogic_test, different_new_views_in_VC_msgs) {
  ViewChangeMsg** viewChangeMsgs = new ViewChangeMsg*[N];

  for (int i = 0; i < N; i++) {
    if (i == 1) {
      viewChangeMsgs[i] = nullptr;
    } else {
      viewChangeMsgs[i] = new ViewChangeMsg(i, i, 0);
    }
  }

  auto VCS = ViewChangeSafetyLogic(N, F, C, PrePrepareMsg::digestOfNullPrePrepareMsg());

  ASSERT_DEATH(VCS.calcLBStableForView(viewChangeMsgs), "");

  for (int i = 0; i < N; i++) {
    delete viewChangeMsgs[i];
    viewChangeMsgs[i] = nullptr;
  }
  delete[] viewChangeMsgs;
}

TEST(testViewchangeSafetyLogic_test, empty_correct_VC_msgs) {
  bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
  ViewChangeMsg** viewChangeMsgs = new ViewChangeMsg*[N];
  bftEngine::impl::SeqNum lastStableSeqNum = 150;

  for (int i = 0; i < N; i++) {
    if (i == 2) {
      viewChangeMsgs[i] = nullptr;
    } else {
      viewChangeMsgs[i] = new ViewChangeMsg(i, 1, lastStableSeqNum);
    }
  }

  auto VCS = ViewChangeSafetyLogic(N, F, C, PrePrepareMsg::digestOfNullPrePrepareMsg());

  auto seqNum = VCS.calcLBStableForView(viewChangeMsgs);
  ConcordAssert(seqNum == lastStableSeqNum);

  for (int i = 0; i < N; i++) {
    delete viewChangeMsgs[i];
    viewChangeMsgs[i] = nullptr;
  }
  delete[] viewChangeMsgs;
}

}  // end namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  setUpConfiguration_4();
  bftEngine::CryptoManager::instance(std::make_unique<TestCryptoSystem>());
  int res = RUN_ALL_TESTS();
  // TODO cleanup the generated certificates
  return res;
}
