// Concord
//
// Copyright (c) 2018-2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include "Replica.hpp"
#include "ReplicaForStateTransfer.hpp"
#include "CollectorOfThresholdSignatures.hpp"
#include "SeqNumInfo.hpp"
#include "Digest.hpp"
#include "Crypto.hpp"
#include "SimpleThreadPool.hpp"
#include "ControllerBase.hpp"
#include "RetransmissionsManager.hpp"
#include "DynamicUpperLimitWithSimpleFilter.hpp"
#include "ViewsManager.hpp"
#include "InternalReplicaApi.hpp"
#include "ClientsManager.hpp"
#include "CheckpointInfo.hpp"
#include "SimpleThreadPool.hpp"
#include "Bitmap.hpp"

namespace bftEngine::impl {

class ClientRequestMsg;
class ClientReplyMsg;
class PrePrepareMsg;
class CheckpointMsg;
class ViewChangeMsg;
class NewViewMsg;
class ClientReplyMsg;
class StartSlowCommitMsg;
class ReqMissingDataMsg;
class PreparePartialMsg;
class PrepareFullMsg;
class SimpleAckMsg;
class ReplicaStatusMsg;
class ReplicaImp;
struct LoadedReplicaData;
class PersistentStorage;

using bftEngine::ReplicaConfig;
using std::shared_ptr;

class ReplicaImp : public InternalReplicaApi, public ReplicaForStateTransfer {
 protected:
  const bool viewChangeProtocolEnabled;
  const bool autoPrimaryRotationEnabled;
  const bool supportDirectProofs = false;  // TODO(GG): add support

  // If this replica was restarted and loaded data from persistent storage.
  bool restarted_ = false;

  // thread pool of this replica
  util::SimpleThreadPool internalThreadPool;  // TODO(GG): !!!! rename

  // retransmissions manager (can be disabled)
  RetransmissionsManager* retransmissionsManager = nullptr;

  // controller
  ControllerBase* controller = nullptr;

  // digital signatures
  SigManager* sigManager = nullptr;

  // view change logic
  ViewsManager* viewsManager = nullptr;

  // the current view of this replica
  ViewNum curView = 0;

  // the last SeqNum used to generate a new SeqNum (valid when this replica is the primary)
  SeqNum primaryLastUsedSeqNum = 0;

  // the latest stable SeqNum known to this replica
  SeqNum lastStableSeqNum = 0;

  //
  SeqNum strictLowerBoundOfSeqNums = 0;

  //
  SeqNum maxSeqNumTransferredFromPrevViews = 0;

  // requests queue (used by the primary)
  std::queue<ClientRequestMsg*> requestsQueueOfPrimary;  // only used by the primary

  // bounded log used to store information about SeqNums in the range (lastStableSeqNum,lastStableSeqNum +
  // kWorkWindowSize]
  SequenceWithActiveWindow<kWorkWindowSize, 1, SeqNum, SeqNumInfo, SeqNumInfo>* mainLog = nullptr;

  // bounded log used to store information about checkpoints in the range [lastStableSeqNum,lastStableSeqNum +
  // kWorkWindowSize]
  SequenceWithActiveWindow<kWorkWindowSize + checkpointWindowSize,
                           checkpointWindowSize,
                           SeqNum,
                           CheckpointInfo,
                           CheckpointInfo>* checkpointsLog = nullptr;

  // last known stable checkpoint of each peer replica.
  // We sometimes delete checkpoints before lastExecutedSeqNum
  std::map<ReplicaId, CheckpointMsg*> tableOfStableCheckpoints;

  // managing information about the clients
  ClientsManager* clientsManager = nullptr;

  // buffer used to store replies
  char* replyBuffer = nullptr;

  // variables that are used to heuristically compute the 'optimal' batch size
  size_t maxNumberOfPendingRequestsInRecentHistory = 0;
  size_t batchingFactor = 1;

  IRequestsHandler* const userRequestsHandler = nullptr;

  // used to dynamically estimate a upper bound for consensus rounds
  DynamicUpperLimitWithSimpleFilter<int64_t>* dynamicUpperLimitOfRounds = nullptr;

  //
  ViewNum lastViewThatTransferredSeqNumbersFullyExecuted = 0;

  //
  Time lastTimeThisReplicaSentStatusReportMsgToAllPeerReplicas = MinTime;
  Time timeOfLastStateSynch;    // last time the replica received a new state (via the state transfer mechanism)
  Time timeOfLastViewEntrance;  // last time the replica entered to a new view

  //
  ViewNum lastAgreedView = 0;  // latest view number v such that the replica received 2f+2c+1 ViewChangeMsg messages
                               // with view >= v
  Time timeOfLastAgreedView;   // last time we changed lastAgreedView

  // timers
  concordUtil::Timers::Handle retranTimer_;
  concordUtil::Timers::Handle slowPathTimer_;
  concordUtil::Timers::Handle infoReqTimer_;
  concordUtil::Timers::Handle statusReportTimer_;
  concordUtil::Timers::Handle viewChangeTimer_;

  int viewChangeTimerMilli = 0;
  int autoPrimaryRotationTimerMilli = 0;

  shared_ptr<PersistentStorage> ps_;

  bool recoveringFromExecutionOfRequests = false;
  Bitmap mapOfRequestsThatAreBeingRecovered;

  //******** METRICS ************************************
  GaugeHandle metric_view_;
  GaugeHandle metric_last_stable_seq_num_;
  GaugeHandle metric_last_executed_seq_num_;
  GaugeHandle metric_last_agreed_view_;
  GaugeHandle metric_current_active_view_;

  // The first commit path being attempted for a new request.
  StatusHandle metric_first_commit_path_;

  CounterHandle metric_slow_path_count_;
  CounterHandle metric_received_internal_msgs_;
  CounterHandle metric_received_client_requests_;
  CounterHandle metric_received_pre_prepares_;
  CounterHandle metric_received_start_slow_commits_;
  CounterHandle metric_received_partial_commit_proofs_;
  CounterHandle metric_received_full_commit_proofs_;
  CounterHandle metric_received_prepare_partials_;
  CounterHandle metric_received_commit_partials_;
  CounterHandle metric_received_prepare_fulls_;
  CounterHandle metric_received_commit_fulls_;
  CounterHandle metric_received_checkpoints_;
  CounterHandle metric_received_replica_statuses_;
  CounterHandle metric_received_view_changes_;
  CounterHandle metric_received_new_views_;
  CounterHandle metric_received_req_missing_datas_;
  CounterHandle metric_received_simple_acks_;
  //*****************************************************
 public:
  ReplicaImp(const ReplicaConfig&,
             IRequestsHandler* requestsHandler,
             IStateTransfer* stateTransfer,
             shared_ptr<MsgsCommunicator> msgsCommunicator,
             shared_ptr<PersistentStorage> persistentStorage,
             shared_ptr<MsgHandlersRegistrator> msgHandlers);

  ReplicaImp(const LoadedReplicaData&,
             IRequestsHandler* requestsHandler,
             IStateTransfer* stateTransfer,
             shared_ptr<MsgsCommunicator> msgsCommunicator,
             shared_ptr<PersistentStorage> persistentStorage,
             shared_ptr<MsgHandlersRegistrator> msgHandlers);

  virtual ~ReplicaImp();

  void start() override;
  void stop() override;

  virtual bool isReadOnly() const override { return false; }

  shared_ptr<PersistentStorage> getPersistentStorage() const { return ps_; }
  IRequestsHandler* getRequestsHandler() const { return userRequestsHandler; }

  void processMessages();

  // InternalReplicaApi
  virtual void onInternalMsg(FullCommitProofMsg* m) override;
  virtual void onMerkleExecSignature(ViewNum v, SeqNum s, uint16_t signatureLength, const char* signature) override;
  void updateMetricsForInternalMessage() override { metric_received_internal_msgs_.Get().Inc(); }
  bool isCollectingState() const override { return stateTransfer->isCollectingState(); }
  bool isValidClient(NodeIdType clientId) const override { return clientsManager->isValidClient(clientId); }
  bool isIdOfReplica(NodeIdType id) const override { return repsInfo->isIdOfReplica(id); }
  const std::set<ReplicaId>& getIdsOfPeerReplicas() const override { return repsInfo->idsOfPeerReplicas(); }
  ViewNum getCurrentView() const override { return curView; }
  ReplicaId currentPrimary() const override { return repsInfo->primaryOfView(curView); }
  bool isCurrentPrimary() const override { return (currentPrimary() == config_.replicaId); }
  bool currentViewIsActive() const override { return (viewsManager->viewIsActive(curView)); }
  ReqId seqNumberOfLastReplyToClient(NodeIdType clientId) const override {
    return clientsManager->seqNumberOfLastReplyToClient(clientId);
  }

 protected:
  ReplicaImp(bool firstTime,
             const ReplicaConfig&,
             IRequestsHandler*,
             IStateTransfer*,
             SigManager*,
             ReplicasInfo*,
             ViewsManager*,
             shared_ptr<MsgsCommunicator>,
             shared_ptr<MsgHandlersRegistrator>);

  void registerMsgHandlers();

  template <typename T>
  void messageHandler(MessageBase* msg);

  void send(MessageBase*, NodeIdType) override;

  bool tryToEnterView();
  void onNewView(const std::vector<PrePrepareMsg*>& prePreparesForNewView);
  void MoveToHigherView(ViewNum nextView);  // also sends the ViewChangeMsg message
  void GotoNextView();

  void tryToSendStatusReport();
  void tryToSendReqMissingDataMsg(SeqNum seqNumber,
                                  bool slowPathOnly = false,
                                  uint16_t destReplicaId = ALL_OTHER_REPLICAS);

  friend class DebugStatistics;
  friend class PreProcessor;

  template <typename T>
  void onMessage(T* msg);

  bool handledByRetransmissionsManager(const ReplicaId sourceReplica,
                                       const ReplicaId destReplica,
                                       const ReplicaId primaryReplica,
                                       const SeqNum seqNum,
                                       const uint16_t msgType);

  void sendRetransmittableMsgToReplica(MessageBase* m,
                                       ReplicaId destReplica,
                                       SeqNum s,
                                       bool ignorePreviousAcks = false);
  void sendAckIfNeeded(MessageBase* msg, const NodeIdType sourceNode, const SeqNum seqNum);

  void tryToSendPrePrepareMsg(bool batchingLogic = false);

  void sendPartialProof(SeqNumInfo&);

  void tryToStartSlowPaths();

  void tryToAskForMissingInfo();

  void sendPreparePartial(SeqNumInfo&);
  void sendCommitPartial(SeqNum);  // TODO(GG): the argument should be a ref to SeqNumInfo

  void executeReadOnlyRequest(ClientRequestMsg* m);

  void executeNextCommittedRequests(const bool requestMissingInfo = false);

  void executeRequestsInPrePrepareMsg(PrePrepareMsg* pp, bool recoverFromErrorInRequestsExecution = false);

  void onSeqNumIsStable(
      SeqNum newStableSeqNum,
      bool hasStateInformation = true,  // true IFF we have checkpoint Or digest in the state transfer
      bool oldSeqNum = false  // true IFF sequence number newStableSeqNum+kWorkWindowSize has already been executed
  );

  void onTransferringCompleteImp(SeqNum) override;

  template <typename T>
  bool relevantMsgForActiveView(const T* msg);

  void onReportAboutAdvancedReplica(ReplicaId reportedReplica, SeqNum seqNum, ViewNum viewNum);
  void onReportAboutLateReplica(ReplicaId reportedReplica, SeqNum seqNum, ViewNum viewNum);
  void onReportAboutAdvancedReplica(ReplicaId reportedReplica, SeqNum seqNum);
  void onReportAboutLateReplica(ReplicaId reportedReplica, SeqNum seqNum);

  void onReportAboutInvalidMessage(MessageBase* msg, const char* reason) override;

  void sendCheckpointIfNeeded();

  IncomingMsgsStorage& getIncomingMsgsStorage() override;

  virtual util::SimpleThreadPool& getInternalThreadPool() override { return internalThreadPool; }

  virtual IThresholdVerifier* getThresholdVerifierForExecution() override {
    return config_.thresholdVerifierForExecution;
  }

  virtual IThresholdVerifier* getThresholdVerifierForSlowPathCommit() override {
    return config_.thresholdVerifierForSlowPathCommit;
  }

  virtual IThresholdVerifier* getThresholdVerifierForCommit() override { return config_.thresholdVerifierForCommit; }

  virtual IThresholdVerifier* getThresholdVerifierForOptimisticCommit() override {
    return config_.thresholdVerifierForOptimisticCommit;
  }

  virtual const ReplicasInfo& getReplicasInfo() override { return (*repsInfo); }

  void onViewsChangeTimer(concordUtil::Timers::Handle);
  void onRetransmissionsTimer(concordUtil::Timers::Handle);
  void onStatusReportTimer(concordUtil::Timers::Handle);
  void onSlowPathTimer(concordUtil::Timers::Handle);
  void onInfoRequestTimer(concordUtil::Timers::Handle);

  // handlers for internal messages

  virtual void onPrepareCombinedSigFailed(SeqNum seqNumber,
                                          ViewNum view,
                                          const std::set<uint16_t>& replicasWithBadSigs) override;
  virtual void onPrepareCombinedSigSucceeded(SeqNum seqNumber,
                                             ViewNum view,
                                             const char* combinedSig,
                                             uint16_t combinedSigLen) override;
  virtual void onPrepareVerifyCombinedSigResult(SeqNum seqNumber, ViewNum view, bool isValid) override;

  virtual void onCommitCombinedSigFailed(SeqNum seqNumber,
                                         ViewNum view,
                                         const std::set<uint16_t>& replicasWithBadSigs) override;
  virtual void onCommitCombinedSigSucceeded(SeqNum seqNumber,
                                            ViewNum view,
                                            const char* combinedSig,
                                            uint16_t combinedSigLen) override;
  virtual void onCommitVerifyCombinedSigResult(SeqNum seqNumber, ViewNum view, bool isValid) override;

  virtual void onRetransmissionsProcessingResults(
      SeqNum relatedLastStableSeqNum,
      const ViewNum relatedViewNumber,
      const std::forward_list<RetSuggestion>* const suggestedRetransmissions)
      override;  // TODO(GG): use generic iterators

 private:
  void addTimers();
};

}  // namespace bftEngine::impl
