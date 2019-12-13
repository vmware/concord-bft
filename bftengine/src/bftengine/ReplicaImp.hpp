// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "PrimitiveTypes.hpp"
#include "ReplicaConfig.hpp"
#include "CollectorOfThresholdSignatures.hpp"
#include "SeqNumInfo.hpp"
#include "Digest.hpp"
#include "Crypto.hpp"
#include "DebugStatistics.hpp"
#include "SimpleThreadPool.hpp"
#include "SimpleAutoResetEvent.hpp"
#include "ControllerBase.hpp"
#include "messages/CheckpointMsg.hpp"
#include "RetransmissionsManager.hpp"
#include "DynamicUpperLimitWithSimpleFilter.hpp"
#include "ViewsManager.hpp"
#include "ReplicasInfo.hpp"
#include "messages/MsgsCertificate.hpp"
#include "InternalReplicaApi.hpp"
#include "IStateTransfer.hpp"
#include "ClientsManager.hpp"
#include "CheckpointInfo.hpp"
#include "ICommunication.hpp"
#include "Replica.hpp"
#include "SimpleThreadPool.hpp"
#include "PersistentStorage.hpp"
#include "ReplicaLoader.hpp"
#include "Metrics.hpp"

#include "Timers.hpp"

#include <thread>

namespace bftEngine {
namespace impl {
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
class StateTransferMsg;
class ReplicaStatusMsg;
class ReplicaImp;

using bftEngine::ReplicaConfig;
using PtrToMetaMsgHandler = void (ReplicaImp::*)(MessageBase* msg);

class ReplicaImp : public InternalReplicaApi, public IReplicaForStateTransfer {
 protected:
  ReplicaConfig config_;

  const uint16_t numOfReplicas;
  const bool viewChangeProtocolEnabled;
  const bool supportDirectProofs = false;  // TODO(GG): add support

  // pointers to message handlers
  const std::unordered_map<uint16_t, PtrToMetaMsgHandler> metaMsgHandlers;

  // communication
  class MsgReceiver;                                // forward declaration
  IncomingMsgsStorage incomingMsgsStorage = 20000;  // TODO(GG): use configuration
  MsgReceiver* msgReceiver = nullptr;
  ICommunication* communication;

  // main thread of the this replica
  std::thread mainThread;
  bool mainThreadStarted = false;
  bool mainThreadShouldStop = false;
  bool mainThreadShouldStopWhenStateIsNotCollected = false;

  // If this replica was restarted and loaded data from
  // persistent storage.
  bool restarted_;

  // thread pool of this replica
  util::SimpleThreadPool internalThreadPool;  // TODO(GG): !!!! rename

  // retransmissions manager (can be disabled)
  RetransmissionsManager* retransmissionsManager = nullptr;

  // controller
  ControllerBase* controller = nullptr;

  // general information about the replicas
  ReplicasInfo* repsInfo = nullptr;

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

  // last SeqNum executed  by this replica (or its affect was transferred to this replica)
  SeqNum lastExecutedSeqNum = 0;

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
  char* replyBuffer;

  // pointer to a state transfer module
  bftEngine::IStateTransfer* stateTransfer = nullptr;

  // variables that are used to heuristically compute the 'optimal' batch size
  size_t maxNumberOfPendingRequestsInRecentHistory = 0;
  size_t batchingFactor = 1;

  RequestsHandler* const userRequestsHandler;

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

  // Timer manager/container
  concordUtil::Timers timers_;

  // timers
  concordUtil::Timers::Handle stateTranTimer_;
  concordUtil::Timers::Handle retranTimer_;
  concordUtil::Timers::Handle slowPathTimer_;
  concordUtil::Timers::Handle infoReqTimer_;
  concordUtil::Timers::Handle statusReportTimer_;
  concordUtil::Timers::Handle viewChangeTimer_;
  concordUtil::Timers::Handle debugStatTimer_;
  concordUtil::Timers::Handle metricsTimer_;

  int viewChangeTimerMilli = 0;

  std::shared_ptr<PersistentStorage> ps_;

  bool recoveringFromExecutionOfRequests = false;
  Bitmap mapOfRequestsThatAreBeingRecovered;

  class MsgReceiver : public IReceiver {
   public:
    MsgReceiver(IncomingMsgsStorage& queue);

    virtual ~MsgReceiver(){};

    virtual void onNewMessage(const NodeNum sourceNode, const char* const message, const size_t messageLength) override;

    virtual void onConnectionStatusChanged(const NodeNum node, const ConnectionStatus newStatus) override;

   private:
    IncomingMsgsStorage& incomingMsgs;
  };

  class StopInternalMsg : public InternalMessage {
   public:
    StopInternalMsg(ReplicaImp* myReplica);
    virtual void handle();

   protected:
    ReplicaImp* replica;
  };

  friend class StopInternalMsg;

  class StopWhenStateIsNotCollectedInternalMsg : public InternalMessage {
   public:
    StopWhenStateIsNotCollectedInternalMsg(ReplicaImp* myReplica);
    virtual void handle();

   protected:
    ReplicaImp* replica;
  };

  friend class StopWhenStateIsNotCollectedInternalMsg;

  // this event is signalled iff the start() method has completed
  // and the process_message() method has not start working yet
  SimpleAutoResetEvent startSyncEvent;

  //******** METRICS ************************************
  concordMetrics::Component metrics_;

  typedef concordMetrics::Component::Handle<concordMetrics::Gauge> GaugeHandle;
  typedef concordMetrics::Component::Handle<concordMetrics::Status> StatusHandle;
  typedef concordMetrics::Component::Handle<concordMetrics::Counter> CounterHandle;

  GaugeHandle metric_view_;
  GaugeHandle metric_last_stable_seq_num__;
  GaugeHandle metric_last_executed_seq_num_;
  GaugeHandle metric_last_agreed_view_;

  // The first commit path being attempted for a new
  // request.
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
  CounterHandle metric_received_state_transfers_;

  //*****************************************************
 public:
  ReplicaImp(const ReplicaConfig&,
             RequestsHandler* requestsHandler,
             IStateTransfer* stateTransfer,
             ICommunication* communication,
             shared_ptr<PersistentStorage>& persistentStorage);

  ReplicaImp(const LoadedReplicaData&,
             RequestsHandler* requestsHandler,
             IStateTransfer* stateTransfer,
             ICommunication* communication,
             shared_ptr<PersistentStorage>& persistentStorage);

  virtual ~ReplicaImp();

  void start();
  void stop();
  void stopWhenStateIsNotCollected();
  bool isRunning() const;
  SeqNum getLastExecutedSequenceNum() const;
  bool isRecoveringFromExecutionOfRequests() const { return recoveringFromExecutionOfRequests; }

  shared_ptr<PersistentStorage> getPersistentStorage() const { return ps_; }
  RequestsHandler* getRequestsHandler() const { return userRequestsHandler; }
  IStateTransfer* getStateTransfer() const { return stateTransfer; }
  ICommunication* getCommunication() const { return communication; }

  IncomingMsg recvMsg();

  void processMessages();

  // IReplicaForStateTransfer
  virtual void freeStateTransferMsg(char* m) override;
  virtual void sendStateTransferMessage(char* m, uint32_t size, uint16_t replicaId) override;
  virtual void onTransferringComplete(int64_t checkpointNumberOfNewState) override;
  virtual void changeStateTransferTimerPeriod(uint32_t timerPeriodMilli) override;

  // InternalReplicaApi
  virtual void onInternalMsg(FullCommitProofMsg* m) override;
  virtual void onMerkleExecSignature(ViewNum v, SeqNum s, uint16_t signatureLength, const char* signature) override;

  void SetAggregator(std::shared_ptr<concordMetrics::Aggregator> a);

 protected:
  ReplicaImp(bool firstTime,
             const ReplicaConfig&,
             RequestsHandler* requestsHandler,
             IStateTransfer* stateTransfer,
             SigManager* sigMgr,
             ReplicasInfo* replicasInfo,
             ViewsManager* viewsMgr);

  static std::unordered_map<uint16_t, PtrToMetaMsgHandler> createMapOfMetaMsgHandlers();

  template <typename T>
  void metaMessageHandler(MessageBase* msg);
  template <typename T>
  void metaMessageHandler_IgnoreWhenCollectingState(MessageBase* msg);  // TODO(GG): rename

  ReplicaId currentPrimary() const { return repsInfo->primaryOfView(curView); }
  bool isCurrentPrimary() const { return (currentPrimary() == config_.replicaId); }

  static const uint16_t ALL_OTHER_REPLICAS = UINT16_MAX;
  void send(MessageBase* m, NodeIdType dest);
  void sendToAllOtherReplicas(MessageBase* m);
  void sendRaw(char* m, NodeIdType dest, uint16_t type, MsgSize size);

  bool tryToEnterView();
  void onNewView(const std::vector<PrePrepareMsg*>& prePreparesForNewView);
  void MoveToHigherView(ViewNum nextView);  // also sends the ViewChangeMsg message
  void GotoNextView();

  void tryToSendStatusReport();
  void tryToSendReqMissingDataMsg(SeqNum seqNumber,
                                  bool slowPathOnly = false,
                                  uint16_t destReplicaId = ALL_OTHER_REPLICAS);

  friend class DebugStatistics;

  void onMessage(ClientRequestMsg*);
  void onMessage(PrePrepareMsg*);
  void onMessage(StartSlowCommitMsg*);
  void onMessage(PartialCommitProofMsg*);
  void onMessage(FullCommitProofMsg*);
  void onMessage(PartialExecProofMsg*);
  void onMessage(PreparePartialMsg*);
  void onMessage(CommitPartialMsg*);
  void onMessage(PrepareFullMsg*);
  void onMessage(CommitFullMsg*);
  void onMessage(CheckpointMsg*);
  void onMessage(ViewChangeMsg*);
  void onMessage(NewViewMsg*);
  void onMessage(ReqMissingDataMsg*);
  void onMessage(SimpleAckMsg*);
  void onMessage(ReplicaStatusMsg*);
  void onMessage(StateTransferMsg*);

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

  void onTransferringCompleteImp(SeqNum);

  bool currentViewIsActive() const { return (viewsManager->viewIsActive(curView)); }

  template <typename T>
  bool relevantMsgForActiveView(const T* msg);

  void onReportAboutAdvancedReplica(ReplicaId reportedReplica, SeqNum seqNum, ViewNum viewNum);
  void onReportAboutLateReplica(ReplicaId reportedReplica, SeqNum seqNum, ViewNum viewNum);
  void onReportAboutAdvancedReplica(ReplicaId reportedReplica, SeqNum seqNum);
  void onReportAboutLateReplica(ReplicaId reportedReplica, SeqNum seqNum);

  void onReportAboutInvalidMessage(MessageBase* msg);

  void sendCheckpointIfNeeded();

  virtual IncomingMsgsStorage& getIncomingMsgsStorage() override { return incomingMsgsStorage; }

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
  void onStateTranTimer(concordUtil::Timers::Handle);
  void onRetransmissionsTimer(concordUtil::Timers::Handle);
  void onStatusReportTimer(concordUtil::Timers::Handle);
  void onSlowPathTimer(concordUtil::Timers::Handle);
  void onInfoRequestTimer(concordUtil::Timers::Handle);
  void onDebugStatTimer(concordUtil::Timers::Handle);
  void onMetricsTimer(concordUtil::Timers::Handle);

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
};
}  // namespace impl
}  // namespace bftEngine
