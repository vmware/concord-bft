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

class DummyReplica : public InternalReplicaApi {
 public:
  DummyReplica(const bftEngine::impl::ReplicasInfo& replicasInfo, const ReplicaConfig& replicaConfig)
      : operationStatus_(PrepareCombinedSigOperationStatus::OPERATION_PENDING),
        replicasInfo_(replicasInfo),
        replicaConfig_(replicaConfig),
        msgHandlersPtr_(new MsgHandlersRegistrator()),
        incomingMsgsStorage_(new IncomingMsgsStorageImp(msgHandlersPtr_, timersResolution, replicaConfig_.replicaId)) {
    SeqNumInfo::init(seqNumInfo_, static_cast<void*>(this));
    dynamic_cast<IncomingMsgsStorageImp*>(incomingMsgsStorage_)->start();
  }
  ~DummyReplica() {
    dynamic_cast<IncomingMsgsStorageImp*>(incomingMsgsStorage_)->stop();
    delete incomingMsgsStorage_;
  }

  void onPrepareCombinedSigFailed(SeqNum seqNumber, ViewNum view, const set<uint16_t>& replicasWithBadSigs) {
    std::unique_lock<std::mutex> lock(m_);
    operationStatus_ = PrepareCombinedSigOperationStatus::OPERATION_FAILED;
    cv_.notify_one();
  }
  void onPrepareCombinedSigSucceeded(SeqNum seqNumber, ViewNum view, const char* combinedSig, uint16_t combinedSigLen) {
    seqNumInfo_.onCompletionOfPrepareSignaturesProcessing(seqNumber, view, combinedSig, combinedSigLen);

    PrepareFullMsg* preFull = seqNumInfo_.getValidPrepareFullMsg();

    std::unique_lock<std::mutex> lock(m_);
    if (preFull) {
      operationStatus_ = PrepareCombinedSigOperationStatus::OPERATION_SUCCEEDED;
    } else {
      operationStatus_ = PrepareCombinedSigOperationStatus::OPERATION_FAILED;
    }
    cv_.notify_one();
  }
  void onPrepareVerifyCombinedSigResult(SeqNum seqNumber, ViewNum view, bool isValid) {}

  void onCommitCombinedSigFailed(SeqNum seqNumber, ViewNum view, const set<uint16_t>& replicasWithBadSigs) {}
  void onCommitCombinedSigSucceeded(SeqNum seqNumber, ViewNum view, const char* combinedSig, uint16_t combinedSigLen) {}
  void onCommitVerifyCombinedSigResult(SeqNum seqNumber, ViewNum view, bool isValid) {}

  void onInternalMsg(FullCommitProofMsg* m) {}
  void onMerkleExecSignature(ViewNum view, SeqNum seqNum, uint16_t signatureLength, const char* signature) {}

  void onRetransmissionsProcessingResults(SeqNum relatedLastStableSeqNum,
                                          const ViewNum relatedViewNumber,
                                          const forward_list<RetSuggestion>* const suggestedRetransmissions) {}

  const bftEngine::impl::ReplicasInfo& getReplicasInfo() const { return replicasInfo_; }
  bool isValidClient(NodeIdType client) const { return true; }
  bool isIdOfReplica(NodeIdType id) const { return true; }
  const set<ReplicaId>& getIdsOfPeerReplicas() const { return replicaIds_; }
  ViewNum getCurrentView() const { return 0; }
  ReplicaId currentPrimary() const { return 0; }
  bool isCurrentPrimary() const { return true; }
  bool currentViewIsActive() const { return true; }
  ReqId seqNumberOfLastReplyToClient(NodeIdType client) const { return 1; }

  IncomingMsgsStorage& getIncomingMsgsStorage() { return *incomingMsgsStorage_; }
  util::SimpleThreadPool& getInternalThreadPool() { return pool_; }

  void updateMetricsForInternalMessage() {}
  bool isCollectingState() const { return false; }

  const ReplicaConfig& getReplicaConfig() const { return replicaConfig_; }

  IThresholdVerifier* getThresholdVerifierForExecution() { return nullptr; }
  IThresholdVerifier* getThresholdVerifierForSlowPathCommit() {
    return replicaConfig_.thresholdVerifierForSlowPathCommit;
  }
  IThresholdVerifier* getThresholdVerifierForCommit() { return nullptr; }
  IThresholdVerifier* getThresholdVerifierForOptimisticCommit() { return nullptr; }

  SeqNumInfo& GetSeqNumInfo() { return seqNumInfo_; }

  enum class PrepareCombinedSigOperationStatus : uint8_t { OPERATION_PENDING, OPERATION_SUCCEEDED, OPERATION_FAILED };

  PrepareCombinedSigOperationStatus GetPrepareFullMsgAndAssociatedPrePrepare(ClientRequestMsg* clientRequest,
                                                                             ViewNum view,
                                                                             bftEngine::impl::SeqNum assignedSeqNum,
                                                                             PrePrepareMsg*& ppMsg,
                                                                             PrepareFullMsg*& pfMsg) {
    auto primary = getReplicasInfo().primaryOfView(view);
    // Generate the PrePrepare from the primary for the clientRequest
    auto* pp = new PrePrepareMsg(primary, view, assignedSeqNum, bftEngine::impl::CommitPath::SLOW, false);
    pp->addRequest(clientRequest->body(), clientRequest->size());
    pp->finishAddingRequests();

    if (primary == replicasInfo_.myId()) {
      seqNumInfo_.addSelfMsg(pp, true);
    } else {
      seqNumInfo_.addMsg(pp, true);
    }

    // Generate the 2*f+1 Prepare messages from different replicas
    int generatedPrepares = 0;
    for (int i = 0; i < replicasInfo_.numberOfReplicas(); i++) {
      PreparePartialMsg* p = PreparePartialMsg::create(view,
                                                       pp->seqNumber(),
                                                       replicaConfig[i].replicaId,
                                                       pp->digestOfRequests(),
                                                       replicaConfig[i].thresholdSignerForSlowPathCommit);
      // After the last Prepare message of the quorum is added to the seqNumInfo_
      // object, its member prepareSigCollector will add a SignaturesProcessingJob
      // to the replica's thread pool to verify the signature shares. After verification
      // is done, an internal message will be generated of either CombinedSigSucceededInternalMsg
      // or CombinedSigFailedInternalMsg, and this internal message will be added to
      // the replica's incomingMsgsStorage_. This incomingMsgsStorage_ has a dispatching thread
      // that will process one of those internal messages by signaling to the replica's
      // onPrepareCombinedSigSucceeded or onPrepareCombinedSigFailed callback.
      // In case of CombinedSigSucceededInternalMsg the onPrepareCombinedSigSucceeded callback
      // calls seqNumInfo_.onCompletionOfPrepareSignaturesProcessing, thus finally generating the
      // PrepareFullMsg.
      if (i == replicasInfo_.myId()) {
        seqNumInfo_.addSelfMsg(p);
      } else {
        seqNumInfo_.addMsg(p);
      }
      generatedPrepares++;
      if (generatedPrepares == 2 * replicasInfo_.fVal() + 1) {
        break;  // We have reached the required quorum of Prepare messages
      }
    }

    // Here we have to wait for either onPrepareCombinedSigSucceeded, or
    // onPrepareCombinedSigFailed callback to be called by the
    // incomingMsgsStorage_'s dispatching thread
    std::unique_lock<std::mutex> lock(m_);
    cv_.wait(lock, [this] { return this->operationStatus_ != PrepareCombinedSigOperationStatus::OPERATION_PENDING; });

    seqNumInfo_.getAndReset(ppMsg, pfMsg);

    auto retStatus = operationStatus_;
    operationStatus_ = PrepareCombinedSigOperationStatus::OPERATION_PENDING;
    return retStatus;
  }

 private:
  std::condition_variable cv_;
  std::mutex m_;
  PrepareCombinedSigOperationStatus operationStatus_;
  bftEngine::impl::ReplicasInfo replicasInfo_;
  ReplicaConfig replicaConfig_;
  shared_ptr<MsgHandlersRegistrator> msgHandlersPtr_;
  IncomingMsgsStorage* incomingMsgsStorage_;
  util::SimpleThreadPool pool_;
  set<ReplicaId> replicaIds_;
  SeqNumInfo seqNumInfo_;
};

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

void cleanupConfiguration_4() { delete sigManager_; }

// The purpose of this test is to verify that when a Replica receives a set of
// view change messages enough for it to confirm a view change it will consider
// the Safe Prepare Certificate for a sequence number that has two Prepare
// Certificates. The safe Prepare Certificate for a given sequence is the one with
// the highest View number, so after ViewchangeSafetyLogic::computeRestrictions
// is called we verify that for this sequence number we are going to redo the
// protocol with the right PrePrepare, matching the Prepare Certificate with
// the highest View number.
TEST(testViewchangeSafetyLogic_test, computeRestrictions_two_prepare_certs_for_same_seq_no) {
  bftEngine::impl::ReplicasInfo replicasInfo(replicaConfig[0], false, false);
  DummyReplica replica(replicasInfo, replicaConfig[0]);
  replica.getInternalThreadPool().start(1);

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

  PrepareFullMsg* pfMsg1{};
  PrePrepareMsg* ppMsg1{};

  // Here we generate a valid Prepare Certificate for clientRequest1 with
  // View=0, this is going to be the Prepare Certificate that has to be
  // discarded for the sequence number -> assignedSeqNum, because we are
  // going to generate a Prepare Certificate for this sequence number with
  // a higher view.
  auto operation_status1 =
      replica.GetPrepareFullMsgAndAssociatedPrePrepare(clientRequest1, curView1, assignedSeqNum, ppMsg1, pfMsg1);
  Assert(operation_status1 == DummyReplica::PrepareCombinedSigOperationStatus::OPERATION_SUCCEEDED);

  uint64_t expectedLastValue2 = 1234567;
  const uint64_t requestBuffer2[kRequestLength] = {(uint64_t)200, expectedLastValue2};
  ViewNum curView2 = 1;

  auto* clientRequest2 = new ClientRequestMsg((uint16_t)1,
                                              bftEngine::ClientMsgFlag::EMPTY_FLAGS_REQ,
                                              (uint64_t)1234567,
                                              kRequestLength,
                                              (const char*)requestBuffer2,
                                              (uint64_t)1000000);
  PrepareFullMsg* pfMsg2{};
  PrePrepareMsg* ppMsg2{};

  // Here we generate a valid Prepare Certificate for clientRequest2 with
  // View=1, this is going to be the Prepare Certificate that has to be
  // considered for the sequence number -> assignedSeqNum, because it will
  // be the one with the highest View for assignedSeqNum.
  auto operation_status2 =
      replica.GetPrepareFullMsgAndAssociatedPrePrepare(clientRequest2, curView2, assignedSeqNum, ppMsg2, pfMsg2);
  Assert(operation_status2 == DummyReplica::PrepareCombinedSigOperationStatus::OPERATION_SUCCEEDED);

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

  auto VCS = ViewChangeSafetyLogic(
      N, F, C, replicaConfig[0].thresholdVerifierForSlowPathCommit, PrePrepareMsg::digestOfNullPrePrepareMsg());

  SeqNum min{}, max{};
  VCS.computeRestrictions(viewChangeMsgs, 0, min, max, restrictions);

  for (int i = 0; i < kWorkWindowSize; i++) {
    if (i == assignedSeqNum - 1) {
      Assert(!restrictions[i].isNull);
      // Assert the prepare certificate with higher view number is selected for assignedSeqNum
      Assert(ppMsg2->digestOfRequests().toString() == restrictions[i].digest.toString())
    } else {
      Assert(restrictions[i].isNull);
    }
  }

  replica.getInternalThreadPool().stop(true);
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

TEST(testViewchangeSafetyLogic_test, computeRestrictions) {
  bftEngine::impl::ReplicasInfo replicasInfo(replicaConfig[0], false, false);
  DummyReplica replica(replicasInfo, replicaConfig[0]);
  replica.getInternalThreadPool().start(1);

  bftEngine::impl::SeqNum lastStableSeqNum = 150;
  uint64_t expectedLastValue = 12345;
  const uint32_t kRequestLength = 2;
  const uint64_t requestBuffer[kRequestLength] = {(uint64_t)200, expectedLastValue};
  ViewNum curView = 0;
  bftEngine::impl::SeqNum assignedSeqNum = lastStableSeqNum + 1;

  auto* clientRequest = new ClientRequestMsg((uint16_t)1,
                                             bftEngine::ClientMsgFlag::EMPTY_FLAGS_REQ,
                                             (uint64_t)1234567,
                                             kRequestLength,
                                             (const char*)requestBuffer,
                                             (uint64_t)1000000);

  PrepareFullMsg* pfMsg{};
  PrePrepareMsg* ppMsg{};

  auto operation_status =
      replica.GetPrepareFullMsgAndAssociatedPrePrepare(clientRequest, curView, assignedSeqNum, ppMsg, pfMsg);
  Assert(operation_status == DummyReplica::PrepareCombinedSigOperationStatus::OPERATION_SUCCEEDED);

  ViewChangeMsg** viewChangeMsgs = new ViewChangeMsg*[N];
  for (int i = 0; i < N; i++) {
    if (i == 1) {
      viewChangeMsgs[i] = nullptr;
    } else {
      viewChangeMsgs[i] = new ViewChangeMsg(i, 1, lastStableSeqNum);
    }
  }

  viewChangeMsgs[2]->addElement(
      *pRepInfo, assignedSeqNum, ppMsg->digestOfRequests(), 0, true, 0, pfMsg->signatureLen(), pfMsg->signatureBody());

  auto VCS = ViewChangeSafetyLogic(
      N, F, C, replicaConfig[0].thresholdVerifierForSlowPathCommit, PrePrepareMsg::digestOfNullPrePrepareMsg());

  SeqNum min{}, max{};
  VCS.computeRestrictions(viewChangeMsgs, 0, min, max, restrictions);

  for (int i = 0; i < kWorkWindowSize; i++) {
    if (i == assignedSeqNum - 1) {
      Assert(!restrictions[i].isNull);
      Assert(ppMsg->digestOfRequests().toString() == restrictions[i].digest.toString())
    } else {
      Assert(restrictions[i].isNull);
    }
  }

  replica.getInternalThreadPool().stop(true);
  for (int i = 0; i < N; i++) {
    delete viewChangeMsgs[i];
    viewChangeMsgs[i] = nullptr;
  }
  delete[] viewChangeMsgs;
  delete clientRequest;
  delete pfMsg;
  delete ppMsg;
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
  cleanupConfiguration_4();
  // TODO cleanup the generated certificates
  return res;
}
