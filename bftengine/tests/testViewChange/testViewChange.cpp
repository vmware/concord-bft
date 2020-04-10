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
#include "TimersSingleton.hpp"
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

#include "gtest/gtest.h"

using namespace std;
using namespace bftEngine;

namespace {

ReplicaConfig replicaConfig[4] = {};
static const int N = 4;
static const int F = 1;
static const int C = 0;
ViewChangeSafetyLogic::Restriction restrictions[kWorkWindowSize];
std::unique_ptr<bftEngine::impl::ReplicasInfo> pRepInfo;
SigManager* sigManager_;

class myViewsManager : public ViewsManager {
 public:
  static void setSigMgr(SigManager* ptr) { ViewsManager::sigManager_ = ptr; }
};

class DummyShareSecretKey : public IShareSecretKey {
 public:
  virtual std::string toString() const override { return std::string("123"); }
};

class DummyShareVerificationKey : public IShareVerificationKey {
 public:
  virtual std::string toString() const override { return std::string("123"); }
};

class DummyThresholdAccumulator : public IThresholdAccumulator {
 public:
  virtual int add(const char* sigShareWithId, int len) override { return 0; }
  virtual void setExpectedDigest(const unsigned char* msg, int len) override {}
  virtual bool hasShareVerificationEnabled() const override { return true; }
  virtual int getNumValidShares() const override { return 0; }
  virtual void getFullSignedData(char* outThreshSig, int threshSigLen) override {}
  virtual IThresholdAccumulator* clone() override { return this; }
};

class DummySigner : public IThresholdSigner {
  DummyShareSecretKey dummyShareSecretKey_;
  DummyShareVerificationKey dummyShareVerificationKey_;

 public:
  virtual int requiredLengthForSignedData() const override { return 3; }
  virtual void signData(const char* hash, int hashLen, char* outSig, int outSigLen) override {}

  virtual const IShareSecretKey& getShareSecretKey() const override { return dummyShareSecretKey_; }
  virtual const IShareVerificationKey& getShareVerificationKey() const override { return dummyShareVerificationKey_; }

  virtual const std::string getVersion() const override { return std::string("123"); }
  virtual void serializeDataMembers(std::ostream&) const override {}
  virtual void deserializeDataMembers(std::istream&) override {}
} dummySigner_;

class DummyVerifier : public IThresholdVerifier {
  DummyShareVerificationKey dummyShareVerificationKey_;
  mutable DummyThresholdAccumulator dummyThresholdAccumulator_;

 public:
  virtual IThresholdAccumulator* newAccumulator(bool withShareVerification) const override {
    return &dummyThresholdAccumulator_;
  }
  virtual void release(IThresholdAccumulator* acc) override {}

  virtual bool verify(const char* msg, int msgLen, const char* sig, int sigLen) const override { return true; }
  virtual int requiredLengthForSignedData() const override { return 3; }

  virtual const IPublicKey& getPublicKey() const override { return dummyShareVerificationKey_; }
  virtual const IShareVerificationKey& getShareVerificationKey(ShareID signer) const override {
    return dummyShareVerificationKey_;
  }
  virtual const std::string getVersion() const override { return std::string("123"); }
  virtual void serializeDataMembers(std::ostream&) const override {}
  virtual void deserializeDataMembers(std::istream&) override {}
} dummyVerifier_;

void setUpConfiguration_4() {
  inputReplicaKeyfile("replica_keys_0", replicaConfig[0]);
  inputReplicaKeyfile("replica_keys_1", replicaConfig[1]);
  inputReplicaKeyfile("replica_keys_2", replicaConfig[2]);
  inputReplicaKeyfile("replica_keys_3", replicaConfig[3]);

  pRepInfo = std::make_unique<bftEngine::impl::ReplicasInfo>(replicaConfig[0], true, true);

  replicaConfig[0].singletonFromThis();

  sigManager_ = new SigManager(
      0, replicaConfig[0].numReplicas, replicaConfig[0].replicaPrivateKey, replicaConfig[0].publicKeysOfReplicas);

  myViewsManager::setSigMgr(sigManager_);
}

TEST(testViewchangeSafetyLogic_test, computeRestrictions) {
  bftEngine::impl::SeqNum lastStableSeqNum = 150;
  const uint32_t kRequestLength = 2;

  uint64_t expectedLastValue = 12345;
  const uint64_t requestBuffer[kRequestLength] = {(uint64_t)200, expectedLastValue};
  ViewNum curView = 0;
  bftEngine::impl::SeqNum assignedSeqNum = lastStableSeqNum + 1;

  auto* clientRequest = new ClientRequestMsg((uint16_t)1,
                                             bftEngine::ClientMsgFlag::EMPTY_FLAGS_REQ,
                                             (uint64_t)1234567,
                                             kRequestLength,
                                             (const char*)requestBuffer,
                                             (uint64_t)1000000);

  auto primary = pRepInfo->primaryOfView(curView);
  // Generate the PrePrepare from the primary for the clientRequest
  auto* ppMsg = new PrePrepareMsg(primary, curView, assignedSeqNum, bftEngine::impl::CommitPath::SLOW, false);
  ppMsg->addRequest(clientRequest->body(), clientRequest->size());
  ppMsg->finishAddingRequests();

  char buff[32]{};
  PrepareFullMsg* pfMsg = PrepareFullMsg::create(curView, assignedSeqNum, primary, buff, sizeof(buff));

  ViewChangeMsg** viewChangeMsgs = new ViewChangeMsg*[N];

  viewChangeMsgs[0] = new ViewChangeMsg(0, curView, lastStableSeqNum);
  viewChangeMsgs[1] = nullptr;
  viewChangeMsgs[2] = new ViewChangeMsg(2, curView, lastStableSeqNum);
  viewChangeMsgs[3] = new ViewChangeMsg(3, curView, lastStableSeqNum);

  viewChangeMsgs[2]->addElement(*pRepInfo,
                                assignedSeqNum,
                                ppMsg->digestOfRequests(),
                                ppMsg->viewNumber(),
                                true,
                                ppMsg->viewNumber(),
                                pfMsg->signatureLen(),
                                pfMsg->signatureBody());

  auto VCS = ViewChangeSafetyLogic(N, F, C, &dummyVerifier_, PrePrepareMsg::digestOfNullPrePrepareMsg());

  SeqNum min{}, max{};
  VCS.computeRestrictions(viewChangeMsgs, 0, min, max, restrictions);

  for (int i = 0; i < kWorkWindowSize; i++) {
    if (i == assignedSeqNum - 1) {
      Assert(!restrictions[i].isNull);
      Assert(ppMsg->digestOfRequests().toString() == restrictions[i].digest.toString());
    } else {
      Assert(restrictions[i].isNull);
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
  char buff[32]{};
  bftEngine::impl::SeqNum lastStableSeqNum = 100;
  const uint32_t kRequestLength = 2;

  uint64_t expectedLastValue1 = 12345;
  const uint64_t requestBuffer1[kRequestLength] = {(uint64_t)200, expectedLastValue1};
  ViewNum curView1 = 0;
  bftEngine::impl::SeqNum assignedSeqNum = lastStableSeqNum + 1;

  auto* clientRequest1 = new ClientRequestMsg((uint16_t)1,
                                              bftEngine::ClientMsgFlag::EMPTY_FLAGS_REQ,
                                              (uint64_t)1234567,
                                              kRequestLength,
                                              (const char*)requestBuffer1,
                                              (uint64_t)1000000);

  // Generate the PrePrepare from the primary for the clientRequest1
  auto* ppMsg1 = new PrePrepareMsg(
      pRepInfo->primaryOfView(curView1), curView1, assignedSeqNum, bftEngine::impl::CommitPath::SLOW, false);
  ppMsg1->addRequest(clientRequest1->body(), clientRequest1->size());
  ppMsg1->finishAddingRequests();

  // Here we generate a valid Prepare Certificate for clientRequest1 with
  // View=0, this is going to be the Prepare Certificate that has to be
  // discarded for the sequence number -> assignedSeqNum, because we are
  // going to generate a Prepare Certificate for this sequence number with
  // a higher view.
  PrepareFullMsg* pfMsg1 =
      PrepareFullMsg::create(curView1, assignedSeqNum, pRepInfo->primaryOfView(curView1), buff, sizeof(buff));

  uint64_t expectedLastValue2 = 1234567;
  const uint64_t requestBuffer2[kRequestLength] = {(uint64_t)200, expectedLastValue2};
  ViewNum curView2 = 1;

  auto* clientRequest2 = new ClientRequestMsg((uint16_t)2,
                                              bftEngine::ClientMsgFlag::EMPTY_FLAGS_REQ,
                                              (uint64_t)1234567,
                                              kRequestLength,
                                              (const char*)requestBuffer2,
                                              (uint64_t)1000000);

  // Generate the PrePrepare from the primary for the clientRequest2
  auto* ppMsg2 = new PrePrepareMsg(
      pRepInfo->primaryOfView(curView2), curView2, assignedSeqNum, bftEngine::impl::CommitPath::SLOW, false);
  ppMsg2->addRequest(clientRequest2->body(), clientRequest2->size());
  ppMsg2->finishAddingRequests();
  // Here we generate a valid Prepare Certificate for clientRequest2 with
  // View=1, this is going to be the Prepare Certificate that has to be
  // considered for the sequence number -> assignedSeqNum, because it will
  // be the one with the highest View for assignedSeqNum.
  PrepareFullMsg* pfMsg2 =
      PrepareFullMsg::create(curView2, assignedSeqNum, pRepInfo->primaryOfView(curView2), buff, sizeof(buff));

  ViewChangeMsg** viewChangeMsgs = new ViewChangeMsg*[N];

  viewChangeMsgs[0] = new ViewChangeMsg(0, curView2, lastStableSeqNum);
  viewChangeMsgs[1] = nullptr;
  viewChangeMsgs[2] = new ViewChangeMsg(2, curView2, lastStableSeqNum);
  viewChangeMsgs[3] = new ViewChangeMsg(3, curView2, lastStableSeqNum);

  // Add the first Prepare Certificate which we expect to be ignored
  // to the View Change Msg from Replica 2
  viewChangeMsgs[2]->addElement(*pRepInfo,
                                assignedSeqNum,
                                ppMsg1->digestOfRequests(),
                                ppMsg1->viewNumber(),
                                true,
                                ppMsg1->viewNumber(),
                                pfMsg1->signatureLen(),
                                pfMsg1->signatureBody());

  // Add the second Prepare Certificate which we expect to be considered in the next view
  // to the View Change Msg from Replica 3
  viewChangeMsgs[3]->addElement(*pRepInfo,
                                assignedSeqNum,
                                ppMsg2->digestOfRequests(),
                                ppMsg2->viewNumber(),
                                true,
                                ppMsg2->viewNumber(),
                                pfMsg2->signatureLen(),
                                pfMsg2->signatureBody());

  auto VCS = ViewChangeSafetyLogic(N, F, C, &dummyVerifier_, PrePrepareMsg::digestOfNullPrePrepareMsg());

  SeqNum min{}, max{};
  VCS.computeRestrictions(viewChangeMsgs, 0, min, max, restrictions);

  for (int i = 0; i < kWorkWindowSize; i++) {
    if (i == assignedSeqNum - 1) {
      Assert(!restrictions[i].isNull);
      // Assert the prepare certificate with higher view number is selected for assignedSeqNum
      Assert(ppMsg2->digestOfRequests().toString() == restrictions[i].digest.toString());
      Assert(ppMsg2->digestOfRequests().toString() != ppMsg1->digestOfRequests().toString());
    } else {
      Assert(restrictions[i].isNull);
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
  ViewChangeMsg** viewChangeMsgs = new ViewChangeMsg*[N];

  for (int i = 0; i < N; i++) {
    viewChangeMsgs[i] = nullptr;
  }

  viewChangeMsgs[0] = new ViewChangeMsg(0, 10, 0);
  viewChangeMsgs[2] = new ViewChangeMsg(2, 10, 0);
  viewChangeMsgs[3] = new ViewChangeMsg(3, 11, 0);

  auto VCS = ViewChangeSafetyLogic(
      N, F, C, replicaConfig[0].thresholdVerifierForSlowPathCommit, PrePrepareMsg::digestOfNullPrePrepareMsg());

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

  auto VCS = ViewChangeSafetyLogic(
      N, F, C, replicaConfig[0].thresholdVerifierForSlowPathCommit, PrePrepareMsg::digestOfNullPrePrepareMsg());

  ASSERT_DEATH(VCS.calcLBStableForView(viewChangeMsgs), "");

  for (int i = 0; i < N; i++) {
    delete viewChangeMsgs[i];
    viewChangeMsgs[i] = nullptr;
  }
  delete[] viewChangeMsgs;
}

TEST(testViewchangeSafetyLogic_test, empty_correct_VC_msgs) {
  ViewChangeMsg** viewChangeMsgs = new ViewChangeMsg*[N];
  bftEngine::impl::SeqNum lastStableSeqNum = 150;

  for (int i = 0; i < N; i++) {
    if (i == 2) {
      viewChangeMsgs[i] = nullptr;
    } else {
      viewChangeMsgs[i] = new ViewChangeMsg(i, 1, lastStableSeqNum);
    }
  }

  auto VCS = ViewChangeSafetyLogic(
      N, F, C, replicaConfig[0].thresholdVerifierForSlowPathCommit, PrePrepareMsg::digestOfNullPrePrepareMsg());

  auto seqNum = VCS.calcLBStableForView(viewChangeMsgs);
  Assert(seqNum == lastStableSeqNum);

  for (int i = 0; i < N; i++) {
    delete viewChangeMsgs[i];
    viewChangeMsgs[i] = nullptr;
  }
  delete[] viewChangeMsgs;
}

}  // end namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  Assert(system("../../../tools/GenerateConcordKeys -n 4 -f 1 -o replica_keys_") != -1);
  setUpConfiguration_4();
  int res = RUN_ALL_TESTS();
  // TODO cleanup the generated certificates
  return res;
}
