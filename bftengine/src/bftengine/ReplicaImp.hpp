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
#include "RetransmissionsManager.hpp"
#include "DynamicUpperLimitWithSimpleFilter.hpp"
#include "ViewsManager.hpp"
#include "ReplicasInfo.hpp"
#include "InternalReplicaApi.hpp"
#include "IStateTransfer.hpp"
#include "ClientsManager.hpp"
#include "CheckpointInfo.hpp"
#include "MsgsCommunicator.hpp"
#include "Replica.hpp"
#include "SimpleThreadPool.hpp"
#include "PersistentStorage.hpp"
#include "ReplicaLoader.hpp"
#include "Metrics.hpp"
#include "MsgHandlersRegistrator.hpp"
#include "TimersSingleton.hpp"

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

class ReplicaImp : public InternalReplicaApi, public IReplicaForStateTransfer {
 protected:
  ReplicaConfig config_;

  const uint16_t numOfReplicas;
  const bool viewChangeProtocolEnabled;
  const bool autoPrimaryRotationEnabled;
  const bool supportDirectProofs = false;  // TODO(GG): add support

  shared_ptr<MsgHandlersRegistrator> msgHandlers_;
  shared_ptr<MsgsCommunicator> msgsCommunicator_;

  // If this replica was restarted and loaded data from persistent storage.
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

  // timers
  concordUtil::Timers::Handle stateTranTimer_;
  concordUtil::Timers::Handle retranTimer_;
  concordUtil::Timers::Handle slowPathTimer_;
  concordUtil::Timers::Handle infoReqTimer_;
  concordUtil::Timers::Handle statusReportTimer_;
  concordUtil::Timers::Handle viewChangeTimer_;
  concordUtil::Timers::Handle debugStatTimer_;

  int viewChangeTimerMilli = 0;
  int autoPrimaryRotationTimerMilli = 0;

  std::shared_ptr<PersistentStorage> ps_;

  bool recoveringFromExecutionOfRequests = false;
  Bitmap mapOfRequestsThatAreBeingRecovered;

 public:
  ReplicaImp(const ReplicaConfig&,
             RequestsHandler* requestsHandler,
             IStateTransfer* stateTransfer,
             shared_ptr<MsgsCommunicator>& msgsCommunicator,
             shared_ptr<PersistentStorage>& persistentStorage,
             shared_ptr<MsgHandlersRegistrator>& msgHandlers);

  ReplicaImp(const LoadedReplicaData&,
             RequestsHandler* requestsHandler,
             IStateTransfer* stateTransfer,
             shared_ptr<MsgsCommunicator>& msgsCommunicator,
             shared_ptr<PersistentStorage>& persistentStorage,
             shared_ptr<MsgHandlersRegistrator>& msgHandlers);

  virtual ~ReplicaImp();

  void start();
  void stop();
  bool isRunning() const { return msgsCommunicator_->isMsgsProcessingRunning(); }
  SeqNum getLastExecutedSequenceNum() const { return lastExecutedSeqNum; }
  bool isRecoveringFromExecutionOfRequests() const { return recoveringFromExecutionOfRequests; }

  shared_ptr<PersistentStorage> getPersistentStorage() const { return ps_; }
  RequestsHandler* getRequestsHandler() const { return userRequestsHandler; }
  IStateTransfer* getStateTransfer() const { return stateTransfer; }
  std::shared_ptr<MsgsCommunicator>& getMsgsCommunicator() { return msgsCommunicator_; }
  std::shared_ptr<MsgHandlersRegistrator>& getMsgHandlersRegistrator() { return msgHandlers_; }

  // IReplicaForStateTransfer
  virtual void freeStateTransferMsg(char* m) override;
  virtual void sendStateTransferMessage(char* m, uint32_t size, uint16_t replicaId) override;
  virtual void onTransferringComplete(int64_t checkpointNumberOfNewState) override;
  virtual void changeStateTransferTimerPeriod(uint32_t timerPeriodMilli) override;

  // InternalReplicaApi
  virtual void onInternalMsg(FullCommitProofMsg* m) override;
  virtual void onMerkleExecSignature(ViewNum v, SeqNum s, uint16_t signatureLength, const char* signature) override;
  void updateMetricsForInternalMessage() override {
    concordMetrics::MetricsCollector::instance(config_.replicaId)
        .takeMetric(concordMetrics::MetricType::REPLICA_RECEIVED_INTERNAL_MSGS);
  }
  bool isCollectingState() override { return stateTransfer->isCollectingState(); }

 protected:
  ReplicaImp(bool firstTime,
             const ReplicaConfig&,
             RequestsHandler* requestsHandler,
             IStateTransfer* stateTransfer,
             SigManager* sigMgr,
             ReplicasInfo* replicasInfo,
             ViewsManager* viewsMgr,
             shared_ptr<MsgsCommunicator>& msgsCommunicator,
             shared_ptr<MsgHandlersRegistrator>& msgHandlers);

  void registerMsgHandlers();

  template <typename T>
  void messageHandler(MessageBase* msg);

  template <typename T>
  void messageHandlerWithIgnoreLogic(MessageBase* msg);

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

  IncomingMsgsStorage& getIncomingMsgsStorage() override { return *msgsCommunicator_->getIncomingMsgsStorage(); }

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
}  // namespace impl
}  // namespace bftEngine
