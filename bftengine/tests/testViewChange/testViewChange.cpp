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
      : replicasInfo_(replicasInfo),
        replicaConfig_(replicaConfig),
        msgHandlersPtr_(new MsgHandlersRegistrator()),
        incomingMsgsStorage_(new IncomingMsgsStorageImp(msgHandlersPtr_, timersResolution, replicaConfig_.replicaId)) {
    combine_operation_future = combine_operation_promise.get_future();
    SeqNumInfo::init(seqNumInfo_, static_cast<void*>(this));
  }
  ~DummyReplica() { delete incomingMsgsStorage_; }

  void onPrepareCombinedSigFailed(SeqNum seqNumber, ViewNum view, const set<uint16_t>& replicasWithBadSigs) {
    combine_operation_promise.set_value(PrepareCombinedSigOperationStatus::OPERATION_FAILED);
  }
  void onPrepareCombinedSigSucceeded(SeqNum seqNumber, ViewNum view, const char* combinedSig, uint16_t combinedSigLen) {
    seqNumInfo_.onCompletionOfPrepareSignaturesProcessing(seqNumber, view, combinedSig, combinedSigLen);

    PrepareFullMsg* preFull = seqNumInfo_.getValidPrepareFullMsg();

    if (preFull) {
      combine_operation_promise.set_value(PrepareCombinedSigOperationStatus::OPERATION_SUCCEEDED);
    } else {
      combine_operation_promise.set_value(PrepareCombinedSigOperationStatus::OPERATION_FAILED);
    }
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

  enum class PrepareCombinedSigOperationStatus : uint8_t { OPERATION_SUCCEEDED, OPERATION_FAILED };
  std::future<PrepareCombinedSigOperationStatus> combine_operation_future;

 private:
  std::promise<PrepareCombinedSigOperationStatus> combine_operation_promise;
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
  auto* pp = new PrePrepareMsg(0, curView, assignedSeqNum, bftEngine::impl::CommitPath::SLOW, false);
  pp->addRequest(clientRequest->body(), clientRequest->size());
  pp->finishAddingRequests();

  PreparePartialMsg* p1 = PreparePartialMsg::create(curView,
                                                    pp->seqNumber(),
                                                    replicaConfig[1].replicaId,
                                                    pp->digestOfRequests(),
                                                    replicaConfig[1].thresholdSignerForSlowPathCommit);
  PreparePartialMsg* p2 = PreparePartialMsg::create(curView,
                                                    pp->seqNumber(),
                                                    replicaConfig[2].replicaId,
                                                    pp->digestOfRequests(),
                                                    replicaConfig[2].thresholdSignerForSlowPathCommit);
  PreparePartialMsg* p3 = PreparePartialMsg::create(curView,
                                                    pp->seqNumber(),
                                                    replicaConfig[3].replicaId,
                                                    pp->digestOfRequests(),
                                                    replicaConfig[3].thresholdSignerForSlowPathCommit);

  replica.GetSeqNumInfo().addSelfMsg(pp, true);
  replica.GetSeqNumInfo().addMsg(p1);
  replica.GetSeqNumInfo().addMsg(p2);
  replica.GetSeqNumInfo().addMsg(p3);

  auto* theMsgStore = dynamic_cast<IncomingMsgsStorageImp*>(&(replica.getIncomingMsgsStorage()));
  theMsgStore->start();
  auto operation_status = replica.combine_operation_future.get();
  assert(operation_status == DummyReplica::PrepareCombinedSigOperationStatus::OPERATION_SUCCEEDED);
  auto pfMsg = replica.GetSeqNumInfo().getValidPrepareFullMsg();

  ViewChangeMsg** viewChangeMsgs = new ViewChangeMsg*[N];
  for (int i = 0; i < N; i++) {
    if (i == 1) {
      viewChangeMsgs[i] = nullptr;
    } else {
      viewChangeMsgs[i] = new ViewChangeMsg(i, 1, lastStableSeqNum);
    }
  }

  viewChangeMsgs[2]->addElement(
      *pRepInfo, assignedSeqNum, pp->digestOfRequests(), 0, true, 0, pfMsg->signatureLen(), pfMsg->signatureBody());

  auto VCS = ViewChangeSafetyLogic(
      N, F, C, replicaConfig[0].thresholdVerifierForSlowPathCommit, PrePrepareMsg::digestOfNullPrePrepareMsg());

  SeqNum min{}, max{};
  VCS.computeRestrictions(viewChangeMsgs, 0, min, max, restrictions);

  for (int i = 0; i < kWorkWindowSize; i++) {
    if (i == assignedSeqNum - 1) {
      Assert(!restrictions[i].isNull);
      Assert(pp->digestOfRequests().toString() == restrictions[i].digest.toString())
    } else {
      Assert(restrictions[i].isNull);
    }
  }

  replica.getInternalThreadPool().stop(true);
  theMsgStore->stop();
  for (int i = 0; i < N; i++) {
    delete viewChangeMsgs[i];
    viewChangeMsgs[i] = nullptr;
  }
  delete[] viewChangeMsgs;
  delete sigManager_;
  delete clientRequest;
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
  assert(system("../../../tools/GenerateConcordKeys -n 4 -f 1 -o replica_keys_") != -1);
  setUpConfiguration_4();
  int res = RUN_ALL_TESTS();
  // TODO cleanup the generated certificates
  return res;
}
