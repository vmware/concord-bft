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

#include "gtest/gtest.h"

using namespace std;
using namespace bftEngine;

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
SigManager* sigManager_;
string privateKey_0 =
    "308204BA020100300D06092A864886F70D0101010500048204A4308204A00201000282010100C55B8F7979BF24B335017082BF33EE2960E3A0"
    "68DCDB45CA3017214BFB3F32649400A2484E2108C7CD07AA7616290667AF7C7A1922C82B51CA01867EED9B60A57F5B6EE33783EC258B234748"
    "8B0FA3F99B05CFFBB45F80960669594B58C993D07B94D9A89ED8266D9931EAE70BB5E9063DEA9EFAF744393DCD92F2F5054624AA048C7EE50B"
    "EF374FCDCE1C8CEBCA1EF12AF492402A6F56DC9338834162F3773B119145BF4B72672E0CF2C7009EBC3D593DFE3715D942CA8749771B484F72"
    "A2BC8C89F86DB52ECC40763B6298879DE686C9A2A78604A503609BA34B779C4F55E3BEB0C26B1F84D8FC4EB3C79693B25A77A158EF88292D4C"
    "01F99EFE3CC912D09B020111028201001D05EF73BF149474B4F8AEA9D0D2EE5161126A69C6203EF8162184E586D4967833E1F9BF56C89F68AD"
    "35D54D99D8DB4B7BB06C4EFD95E840BBD30C3FD7A5E890CEF6DB99E284576EEED07B6C8CEBB63B4B80DAD2311D1A706A5AC95DE768F017213B"
    "896B9EE38D2E3C2CFCE5BDF51ABD27391761245CDB3DCB686F05EA2FF654FA91F89DA699F14ACFA7F0D8030F74DBFEC28D55C902A27E9C03AB"
    "1CA2770EFC5BE541560D86FA376B1A688D92124496BB3E7A3B78A86EBF1B694683CDB32BC49431990A18B570C104E47AC6B0DE5616851F4309"
    "CFE7D0E20B17C154A3D85F33C7791451FFF73BFC4CDC8C16387D184F42AD2A31FCF545C3F9A498FAAC6E94E902818100F40CF9152ED4854E1B"
    "BF67C5EA185C52EBEA0C11875563AEE95037C2E61C8D988DDF71588A0B45C23979C5FBFD2C45F9416775E0A644CAD46792296FDC68A98148F7"
    "BD3164D9A5E0D6A0C2DF0141D82D610D56CB7C53F3C674771ED9ED77C0B5BF3C936498218176DC9933F1215BC831E0D41285611F512F68327E"
    "4FBD9E5C5902818100CF05519FD69D7C6B61324F0A201574C647792B80E5D4D56A51CF5988927A1D54DF9AE4EA656AE25961923A0EC046F1C5"
    "69BAB53A64EB0E9F5AB2ABF1C9146935BA40F75E0EB68E0BE4BC29A5A0742B59DF5A55AB028F1CCC42243D2AEE4B74344CA33E72879EF2D1CD"
    "D874A7F237202AC7EB57AEDCBD539DEFDA094476EAE613028180396C76D7CEC897D624A581D43714CA6DDD2802D6F2AAAE0B09B885974533E5"
    "14D6167505C620C51EA41CA70E1D73D43AA5FA39DA81799922EB3173296109914B98B2C31AAE515434E734E28ED31E8D37DA99BA11C2E693B6"
    "398570ABBF6778A33C0E40CC6007E23A15C9B1DE6233B6A25304B91053166D7490FCD26D1D8EAC5102818079C6E4B86020674E392CA6F6E5B2"
    "44B0DEBFBF3CC36E232F7B6AE95F6538C5F5B0B57798F05CFD9DFD28D6DB8029BB6511046A9AD1F3AE3F9EC37433DFB1A74CC7E9FAEC08A79E"
    "D9D1D8187F8B8FA107B08F7DAFE3633E1DCC8DC9A0C8689EB55A41E87F9B12347B6A06DB359D89D6AFC0E4CA2A9FF6E5E46EF8BA2845F39665"
    "0281802A89B2BD4A665A0F07DCAFA6D9DB7669B1D1276FC3365173A53F0E0D5F9CB9C3E08E68503C62EA73EB8E0DA42CCF6B136BF4A85B0AC4"
    "24730B4F3CAD8C31D34DD75EF2A39B6BCFE3985CCECC470CF479CF0E9B9D6C7CE1C6C70D853728925326A22352DF73B502D4D3CBC2A770DE27"
    "6E1C5953DF7A9614C970C94D194CAE9188";

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

class DummyVerifier : public IThresholdVerifier {
  DummyShareVerificationKey dummyShareVerificationKey_;
  mutable DummyThresholdAccumulator dummyThresholdAccumulator_;

 public:
  IThresholdAccumulator* newAccumulator(bool withShareVerification) const override {
    return new DummyThresholdAccumulator;
  }
  virtual bool verify(const char* msg, int msgLen, const char* sig, int sigLen) const override { return true; }
  virtual int requiredLengthForSignedData() const override { return 3; }

  virtual const IPublicKey& getPublicKey() const override { return dummyShareVerificationKey_; }
  virtual const IShareVerificationKey& getShareVerificationKey(ShareID signer) const override {
    return dummyShareVerificationKey_;
  }
};

void setUpConfiguration_4() {
  for (int i = 0; i < N; i++) {
    replicaConfig[i].numReplicas = N;
    replicaConfig[i].fVal = F;
    replicaConfig[i].cVal = C;
    replicaConfig[i].replicaId = i;
  }
  replicaConfig[0].replicaPrivateKey = privateKey_0;

  pRepInfo = std::make_unique<bftEngine::impl::ReplicasInfo>(replicaConfig[0], true, true);

  sigManager_ = new SigManager(
      0, replicaConfig[0].numReplicas, replicaConfig[0].replicaPrivateKey, replicaConfig[0].publicKeysOfReplicas);

  myViewsManager::setSigMgr(sigManager_);
}

TEST(testViewchangeSafetyLogic_test, computeRestrictions) {
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

  auto VCS =
      ViewChangeSafetyLogic(N, F, C, std::make_shared<DummyVerifier>(), PrePrepareMsg::digestOfNullPrePrepareMsg());

  SeqNum min{}, max{};
  std::shared_ptr<IThresholdVerifier> ver{new DummyVerifier()};
  VCS.computeRestrictions(viewChangeMsgs, VCS.calcLBStableForView(viewChangeMsgs), min, max, restrictions, ver);

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

  auto VCS =
      ViewChangeSafetyLogic(N, F, C, std::make_shared<DummyVerifier>(), PrePrepareMsg::digestOfNullPrePrepareMsg());

  SeqNum min{}, max{};
  std::shared_ptr<IThresholdVerifier> ver{new DummyVerifier()};
  VCS.computeRestrictions(viewChangeMsgs, VCS.calcLBStableForView(viewChangeMsgs), min, max, restrictions, ver);

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

  auto VCS =
      ViewChangeSafetyLogic(N, F, C, std::make_shared<DummyVerifier>(), PrePrepareMsg::digestOfNullPrePrepareMsg());

  SeqNum min{}, max{};
  std::shared_ptr<IThresholdVerifier> ver{new DummyVerifier()};
  VCS.computeRestrictions(viewChangeMsgs, VCS.calcLBStableForView(viewChangeMsgs), min, max, restrictions, ver);

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
  ViewChangeMsg** viewChangeMsgs = new ViewChangeMsg*[N];

  for (int i = 0; i < N; i++) {
    viewChangeMsgs[i] = nullptr;
  }

  viewChangeMsgs[0] = new ViewChangeMsg(0, 10, 0);
  viewChangeMsgs[2] = new ViewChangeMsg(2, 10, 0);
  viewChangeMsgs[3] = new ViewChangeMsg(3, 11, 0);

  auto VCS = ViewChangeSafetyLogic(N, F, C, nullptr, PrePrepareMsg::digestOfNullPrePrepareMsg());

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

  auto VCS = ViewChangeSafetyLogic(N, F, C, nullptr, PrePrepareMsg::digestOfNullPrePrepareMsg());

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

  auto VCS = ViewChangeSafetyLogic(N, F, C, nullptr, PrePrepareMsg::digestOfNullPrePrepareMsg());

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
  int res = RUN_ALL_TESTS();
  // TODO cleanup the generated certificates
  return res;
}
