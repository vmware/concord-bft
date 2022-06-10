// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include <string>
#include <utility>

#include "ReplicaForStateTransfer.hpp"
#include "CollectorOfThresholdSignatures.hpp"
#include "SeqNumInfo.hpp"
#include "Digest.hpp"
#include "SimpleThreadPool.hpp"
#include "ControllerBase.hpp"
#include "RetransmissionsManager.hpp"
#include "DynamicUpperLimitWithSimpleFilter.hpp"
#include "Timers.hpp"
#include "ViewsManager.hpp"
#include "InternalReplicaApi.hpp"
#include "ClientsManager.hpp"
#include "CheckpointInfo.hpp"
#include "SimpleThreadPool.hpp"
#include "Bitmap.hpp"
#include "OpenTracing.hpp"
#include "RequestHandler.h"
#include "InternalBFTClient.hpp"
#include "diagnostics.h"
#include "performance_handler.h"
#include "RequestsBatchingLogic.hpp"
#include "ReplicaStatusHandlers.hpp"
#include "PerformanceManager.hpp"
#include "secrets_manager_impl.h"
#include "SigManager.hpp"
#include "TimeServiceManager.hpp"
#include "FakeClock.hpp"
#include <ccron/ticks_generator.hpp>
#include "EpochManager.hpp"
#include "PerfMetrics.hpp"
#include "ControlStateManager.hpp"

namespace preprocessor {
class PreProcessResultMsg;
}
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
class ReplicaRestartReadyMsg;
class ReplicasRestartReadyProofMsg;

using bftEngine::ReplicaConfig;
using std::shared_ptr;
using concordMetrics::GaugeHandle;
using concordMetrics::CounterHandle;
using concordMetrics::StatusHandle;

class ReplicaImp : public InternalReplicaApi, public ReplicaForStateTransfer {
 protected:
  const bool viewChangeProtocolEnabled;
  const bool autoPrimaryRotationEnabled;

  // If this replica was restarted and loaded data from persistent storage.
  bool restarted_ = false;

  // thread pool of this replica
  concord::util::SimpleThreadPool internalThreadPool;  // TODO(GG): !!!! rename

  // retransmissions manager (can be disabled)
  RetransmissionsManager* retransmissionsManager = nullptr;

  // controller
  ControllerBase* controller = nullptr;

  // digital signatures
  std::unique_ptr<SigManager> sigManager_;

  // view change logic
  ViewsManager* viewsManager = nullptr;

  // the last SeqNum used to generate a new SeqNum (valid when this replica is the primary)
  SeqNum primaryLastUsedSeqNum = 0;

  // the latest stable SeqNum known to this replica
  SeqNum lastStableSeqNum = 0;

  SeqNum lastSuperStableSeqNum = 0;

  //
  SeqNum strictLowerBoundOfSeqNums = 0;

  //
  SeqNum maxSeqNumTransferredFromPrevViews = 0;

  // requests queue (used by the primary)
  std::queue<ClientRequestMsg*> requestsQueueOfPrimary;  // only used by the primary
  size_t primaryCombinedReqSize = 0;                     // only used by the primary

  std::map<uint64_t, std::pair<Time, ClientRequestMsg*>>
      requestsOfNonPrimary;  // used to retransmit client requests by a non primary replica
  size_t NonPrimaryCombinedReqSize = 1000;
  //
  const std::thread::id MAIN_THREAD_ID;
  // A preprepare message which is still building is called transient preprepare.
  // activeTransientPreprepare_ represents the number of preprepare messages that are
  // currently getting build in a separate thread.
  // This should not be thread safe because its used only in the main thread
  // This should start with 0, since by default we will assume that a preprepare is taken
  // to be added to main log. And this will solely represent those preprepares which are
  // to be added to main log, but are not added as yet.
  uint16_t numOfTransientPreprepareMsgs_ = 0;

  // represents the number of running executions at this moment.
  // This should not be thread safe because its been changed in 3 places:
  // 1) startPrePrepareMsgExecution - when we are getting empty PP its symbolize that we should push
  // FinishPrePrepareExecutionInternalMsg which can happen only on the main thread. 2) executeAllPrePreparedRequests -
  // just before we insert the job to execution thread which means we are in the main thread. 3)
  // finishExecutePrePrepareMsg   - which can be called only when we fetch FinishPrePrepareExecutionInternalMsg from the
  // internal messages queue which can happen only on the main thread.
  uint16_t activeExecutions_ = 0;
  // if we have active executions and the main thread fetches RO request we will defer the message and store it in this
  // queue, we will handle these requests after the post execution finishes.
  // This queue should not be thread safe because we add requests only in onMessage<ClientRequestMsg> which can be
  // called only in the main thread.
  // fetching of requests from the queue happens when we are getting to it as part of finishExecutePrePrepareMsg which
  // can happen only in main thread since its being called when FinishPrePrepareExecutionInternalMsg fetched m the
  // internal msgs queue.
  std::deque<ClientRequestMsg*> deferredRORequests_;
  // if we have active executions and the main thread fetches request we will defer the message and store it in this
  // queue, we will handle these requests after the post execution finishes
  // This queue should not be thread safe because we add requests only in several onMessages which can be
  // called only in the main thread.
  // fetching of requests from the queue happens when we are getting to it as part of finishExecutePrePrepareMsg which
  // can happen only in main thread since its being called when FinishPrePrepareExecutionInternalMsg fetched m the
  // internal msgs queue.
  std::deque<MessageBase*> deferredMessages_;
  uint16_t maxQueueSize_;
  bool shouldTryToGoToNextView_ = false;
  bool shouldGoToNextView_ = false;
  bool isSendCheckpointIfNeeded_ = false;
  bool isStartCollectingState_ = false;
  bool startedExecution = false;
  concord::util::SimpleThreadPool postExecThread_;

  // bounded log used to store information about SeqNums in the range (lastStableSeqNum,lastStableSeqNum +
  // kWorkWindowSize]
  typedef SequenceWithActiveWindow<kWorkWindowSize, 1, SeqNum, SeqNumInfo, SeqNumInfo, 1, false> WindowOfSeqNumInfo;
  std::shared_ptr<WindowOfSeqNumInfo> mainLog;

  // bounded log used to store information about checkpoints in the range [lastStableSeqNum,lastStableSeqNum +
  // kWorkWindowSize]
  SequenceWithActiveWindow<kWorkWindowSize + 2 * checkpointWindowSize,
                           checkpointWindowSize,
                           SeqNum,
                           CheckpointInfo<>,
                           CheckpointInfo<>>* checkpointsLog = nullptr;

  // last known stable checkpoint of each peer replica.
  // We sometimes delete checkpoints before lastExecutedSeqNum
  std::map<ReplicaId, std::shared_ptr<CheckpointMsg>> tableOfStableCheckpoints;

  // managing information about the clients
  std::shared_ptr<ClientsManager> clientsManager;
  std::shared_ptr<InternalBFTClient> internalBFTClient_;

  size_t numInvalidClients = 0;
  size_t numValidNoOps = 0;

  // buffer used to store replies
  char* replyBuffer = nullptr;

  // used to dynamically estimate a upper bound for consensus rounds
  DynamicUpperLimitWithSimpleFilter<int64_t>* dynamicUpperLimitOfRounds = nullptr;

  //
  ViewNum lastViewThatTransferredSeqNumbersFullyExecuted = 0;

  //
  Time lastTimeThisReplicaSentStatusReportMsgToAllPeerReplicas = MinTime;
  Time timeOfLastStateSynch;    // last time the replica received a new state (via the state transfer mechanism)
  Time timeOfLastViewEntrance;  // last time the replica entered to a new view

  // latest view number v such that the replica received 2f+2c+1 ViewChangeMsg messages
  // with view >= v
  ViewNum lastAgreedView = 0;
  // last time we changed lastAgreedView
  Time timeOfLastAgreedView;

  // timers
  concordUtil::Timers::Handle retranTimer_;
  concordUtil::Timers::Handle slowPathTimer_;
  concordUtil::Timers::Handle infoReqTimer_;
  concordUtil::Timers::Handle statusReportTimer_;
  concordUtil::Timers::Handle viewChangeTimer_;
  concordUtil::Timers::Handle clientRequestsRetransmissionTimer_;

  int viewChangeTimerMilli = 0;
  int autoPrimaryRotationTimerMilli = 0;

  shared_ptr<PersistentStorage> ps_;

  bool recoveringFromExecutionOfRequests = false;
  Bitmap mapOfRecoveredRequests;
  ConsensusTickRep recoveredTime = 0;

  shared_ptr<concord::performance::PerformanceManager> pm_;

  // sm_ MUST be initialized. If nullptr sm provided in constructor
  // it will be initialized to a SecretsManagerPlain
  shared_ptr<concord::secretsmanager::ISecretsManagerImpl> sm_;

  std::shared_ptr<concord::cron::TicksGenerator> ticks_gen_;

  std::unordered_map<uint8_t, std::map<ReplicaId, std::unique_ptr<ReplicaRestartReadyMsg>>> restart_ready_msgs_;

  //******** METRICS ************************************
  GaugeHandle metric_view_;
  GaugeHandle metric_last_stable_seq_num_;
  GaugeHandle metric_last_executed_seq_num_;
  GaugeHandle metric_last_agreed_view_;
  GaugeHandle metric_current_active_view_;
  GaugeHandle metric_viewchange_timer_;
  GaugeHandle metric_retransmissions_timer_;
  GaugeHandle metric_status_report_timer_;
  GaugeHandle metric_slow_path_timer_;
  GaugeHandle metric_info_request_timer_;
  GaugeHandle metric_current_primary_;
  GaugeHandle metric_concurrency_level_;
  GaugeHandle metric_primary_last_used_seq_num_;
  GaugeHandle metric_on_call_back_of_super_stable_cp_;
  GaugeHandle metric_sent_replica_asks_to_leave_view_msg_;
  GaugeHandle metric_bft_batch_size_;
  GaugeHandle my_id;
  GaugeHandle primary_queue_size_;
  GaugeHandle consensus_avg_time_;
  GaugeHandle accumulating_batch_avg_time_;
  GaugeHandle deferredRORequestsMetric_;
  GaugeHandle deferredMessagesMetric_;
  // The first commit path being attempted for a new request.
  StatusHandle metric_first_commit_path_;
  CounterHandle batch_closed_on_logic_off_;
  CounterHandle batch_closed_on_logic_on_;
  CounterHandle metric_indicator_of_non_determinism_;
  CounterHandle metric_total_committed_sn_;
  /// Executed slow consensuses
  CounterHandle metric_total_slowPath_;
  /// Inner requests in slow executions
  CounterHandle metric_total_slowPath_requests_;
  /// StartSlowCommitMsg received
  CounterHandle metric_received_start_slow_commits_;
  /// Executed fast consensuses
  CounterHandle metric_total_fastPath_;
  /// Committed slow consensuses
  CounterHandle metric_committed_slow_;
  /// Committed FAST_WITH_THRESHOLD consensuses
  CounterHandle metric_committed_fast_threshold_;
  /// Committed fast consensuses
  CounterHandle metric_committed_fast_;
  /// Inner requests in fast executions
  CounterHandle metric_total_fastPath_requests_;
  CounterHandle metric_received_internal_msgs_;
  CounterHandle metric_received_client_requests_;
  CounterHandle metric_received_pre_prepares_;
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
  CounterHandle metric_sent_status_msgs_not_due_timer_;
  CounterHandle metric_sent_req_for_missing_data_;
  CounterHandle metric_sent_checkpoint_msg_due_to_status_;
  CounterHandle metric_sent_viewchange_msg_due_to_status_;
  CounterHandle metric_sent_newview_msg_due_to_status_;
  CounterHandle metric_sent_preprepare_msg_due_to_status_;
  CounterHandle metric_sent_replica_asks_to_leave_view_msg_due_to_status_;
  CounterHandle metric_sent_preprepare_msg_due_to_reqMissingData_;
  CounterHandle metric_sent_startSlowPath_msg_due_to_reqMissingData_;
  CounterHandle metric_sent_partialCommitProof_msg_due_to_reqMissingData_;
  CounterHandle metric_sent_preparePartial_msg_due_to_reqMissingData_;
  CounterHandle metric_sent_prepareFull_msg_due_to_reqMissingData_;
  CounterHandle metric_sent_commitPartial_msg_due_to_reqMissingData_;
  CounterHandle metric_sent_commitFull_msg_due_to_reqMissingData_;
  CounterHandle metric_sent_fullCommitProof_msg_due_to_reqMissingData_;
  CounterHandle metric_total_finished_consensuses_;
  CounterHandle metric_total_preexec_requests_executed_;
  CounterHandle metric_received_restart_ready_;
  CounterHandle metric_received_restart_proof_;
  PerfMetric<uint64_t> metric_consensus_duration_;
  PerfMetric<uint64_t> metric_post_exe_duration_;
  PerfMetric<uint64_t> metric_core_exe_func_duration_;
  PerfMetric<uint64_t> metric_consensus_end_to_core_exe_duration_;
  PerfMetric<uint64_t> metric_post_exe_thread_idle_time_;
  PerfMetric<uint64_t> metric_post_exe_thread_active_time_;
  PerfMetric<std::string> metric_primary_batching_duration_;
  //*****************************************************
  RollingAvgAndVar consensus_time_;
  RollingAvgAndVar accumulating_batch_time_;
  Time time_to_collect_batch_ = MinTime;

 public:
  ReplicaImp(const ReplicaConfig&,
             shared_ptr<IRequestsHandler>,
             IStateTransfer* stateTransfer,
             shared_ptr<MsgsCommunicator> msgsCommunicator,
             shared_ptr<PersistentStorage> persistentStorage,
             shared_ptr<MsgHandlersRegistrator> msgHandlers,
             concordUtil::Timers& timers,
             shared_ptr<concord::performance::PerformanceManager> pm,
             shared_ptr<concord::secretsmanager::ISecretsManagerImpl> sm,
             const std::function<void(bool)>& viewChangeCallBack);

  ReplicaImp(const LoadedReplicaData&,
             shared_ptr<IRequestsHandler>,
             IStateTransfer* stateTransfer,
             shared_ptr<MsgsCommunicator> msgsCommunicator,
             shared_ptr<PersistentStorage> persistentStorage,
             shared_ptr<MsgHandlersRegistrator> msgHandlers,
             concordUtil::Timers& timers,
             shared_ptr<concord::performance::PerformanceManager> pm,
             shared_ptr<concord::secretsmanager::ISecretsManagerImpl> sm,
             const std::function<void(bool)>& viewChangeCallBack);

  virtual ~ReplicaImp();

  void start() override;
  void stop() override;

  std::shared_ptr<IInternalBFTClient> internalClient() const { return internalBFTClient_; }

  std::shared_ptr<concord::cron::TicksGenerator> ticksGenerator() const { return ticks_gen_; }

  virtual bool isReadOnly() const override { return false; }

  shared_ptr<PersistentStorage> getPersistentStorage() const { return ps_; }
  std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> getSecretsManager() { return sm_; }

  void recoverRequests();

  bool validateMessage(MessageBase* msg);
  std::function<bool(MessageBase*)> getMessageValidator();

  // InternalReplicaApi
  bool isCollectingState() const override { return stateTransfer->isCollectingState(); }
  bool isValidClient(NodeIdType clientId) const override { return clientsManager->isValidClient(clientId); }
  bool isIdOfReplica(NodeIdType id) const override { return repsInfo->isIdOfReplica(id); }
  const std::set<ReplicaId>& getIdsOfPeerReplicas() const override { return repsInfo->idsOfPeerReplicas(); }
  ViewNum getCurrentView() const override { return viewsManager->getCurrentView(); }
  ReplicaId currentPrimary() const override { return repsInfo->primaryOfView(getCurrentView()); }
  bool isCurrentPrimary() const override { return (currentPrimary() == config_.replicaId); }
  bool currentViewIsActive() const override { return (viewsManager->viewIsActive(getCurrentView())); }
  bool isReplyAlreadySentToClient(NodeIdType clientId, ReqId reqSeqNum) const override {
    return clientsManager->hasReply(clientId, reqSeqNum);
  }
  bool isClientRequestInProcess(NodeIdType clientId, ReqId reqSeqNum) const override {
    return clientsManager->isClientRequestInProcess(clientId, reqSeqNum);
  }
  SeqNum getPrimaryLastUsedSeqNum() const override { return primaryLastUsedSeqNum; }
  uint64_t getRequestsInQueue() const override { return requestsQueueOfPrimary.size(); }
  SeqNum getLastExecutedSeqNum() const override { return lastExecutedSeqNum; }
  std::pair<PrePrepareMsg*, bool> buildPrePrepareMessage() override;
  bool tryToSendPrePrepareMsg(bool batchingLogic = false) override;
  std::pair<PrePrepareMsg*, bool> buildPrePrepareMsgBatchByRequestsNum(uint32_t requiredRequestsNum) override;
  std::pair<PrePrepareMsg*, bool> buildPrePrepareMsgBatchByOverallSize(uint32_t requiredBatchSizeInBytes) override;
  void handleDeferredRequests();
  void onExecutionFinish();
  void finalizeExecution();
  void updateLimitsAndMetrics(PrePrepareMsg* ppMsg);
  void updateCommitMetrics(const CommitPath& commitPath);
  void finishExecutePrePrepareMsg(PrePrepareMsg* pp, IRequestsHandler::ExecutionRequestsQueue* pAccumulatedRequests);
  /// This function is mostly called through the separate thread execution flow
  void executeRequests(PrePrepareMsg* ppMsg, Bitmap& requestSet, Timestamp time);
  void executeSpecialRequests(PrePrepareMsg* ppMsg,
                              uint16_t numOfSpecialReqs,
                              bool recoverFromErrorInRequestsExecution,
                              Timestamp& outTimestamp);
  void executeAllPrePreparedRequests(bool allowParallelExecution,
                                     bool shouldRunRequestsInParallel,
                                     uint16_t numOfSpecialReqs,
                                     PrePrepareMsg* ppMsg,
                                     Bitmap& requestSet,
                                     bool recoverFromErrorInRequestsExecution);
  void markSpecialRequests(RequestsIterator& reqIter,
                           char* requestBody,
                           uint16_t& numOfSpecialReqs,
                           size_t& reqIdx,
                           Bitmap& requestSet,
                           bool allowParallelExecution,
                           bool& shouldRunRequestsInParallel);
  void startPrePrepareMsgExecution(PrePrepareMsg* ppMsg,
                                   bool allowParallelExecution,
                                   bool recoverFromErrorInRequestsExecution);
  void tryToStartOrFinishExecution(bool requestMissingInfo = false);
  void startExecution(SeqNum seqNumber, concordUtils::SpanWrapper& parent_span, bool requestMissingInfo);
  void pushDeferredMessage(MessageBase*);

 protected:
  ReplicaImp(bool firstTime,
             const ReplicaConfig&,
             shared_ptr<IRequestsHandler>,
             IStateTransfer*,
             SigManager*,
             ReplicasInfo*,
             ViewsManager*,
             shared_ptr<MsgsCommunicator>,
             shared_ptr<MsgHandlersRegistrator>,
             concordUtil::Timers& timers,
             shared_ptr<concord::performance::PerformanceManager> pm,
             shared_ptr<concord::secretsmanager::ISecretsManagerImpl> sm,
             shared_ptr<PersistentStorage> ps,
             const std::function<void(bool)>& viewChangeCallBack);

  void registerMsgHandlers();

  template <typename T>
  void messageHandler(MessageBase* msg);

  template <typename T>
  void validatedMessageHandler(CarrierMesssage* msg);

  void send(MessageBase*, NodeIdType) override;
  void sendAndIncrementMetric(MessageBase*, NodeIdType, CounterHandle&);

  bool tryToEnterView();
  void onNewView(const std::vector<PrePrepareMsg*>& prePreparesForNewView);
  void MoveToHigherView(ViewNum nextView);  // also sends the ViewChangeMsg message
  void goToNextView();

  void tryToSendStatusReport(bool onTimer = false);
  void tryToSendReqMissingDataMsg(SeqNum seqNumber,
                                  bool slowPathOnly = false,
                                  uint16_t destReplicaId = ALL_OTHER_REPLICAS);

  friend class DebugStatistics;
  friend class PreProcessor;

  // Generate diagnostics status replies
  std::string getReplicaState() const;
  std::string getReplicaLastStableSeqNum() const;
  template <typename T>
  void onMessage(T* msg);

  void onInternalMsg(InternalMessage&& msg);
  void onInternalMsg(GetStatus& msg) const;

  std::pair<PrePrepareMsg*, bool> finishAddingRequestsToPrePrepareMsg(PrePrepareMsg*& prePrepareMsg,
                                                                      uint32_t maxSpaceForReqs,
                                                                      uint32_t requiredRequestsSize,
                                                                      uint32_t requiredRequestsNum);

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

  bool checkSendPrePrepareMsgPrerequisites();

  void sendPartialProof(SeqNumInfo&);

  ClientRequestMsg* addRequestToPrePrepareMessage(ClientRequestMsg*& nextRequest,
                                                  PrePrepareMsg& prePrepareMsg,
                                                  uint32_t maxStorageForRequests);

  PrePrepareMsg* createPrePrepareMessage();

  std::pair<PrePrepareMsg*, bool> buildPrePrepareMessageByRequestsNum(uint32_t requiredRequestsNum);

  std::pair<PrePrepareMsg*, bool> buildPrePrepareMessageByBatchSize(uint32_t requiredBatchSizeInBytes);

  void validatePrePrepareMsg(PrePrepareMsg*& ppm);

  template <typename MSG>
  void asyncValidateMessage(MSG* msg);

  void removeDuplicatedRequestsFromRequestsQueue();

  void tryToStartSlowPaths();

  void tryToAskForMissingInfo();

  void tryToRemovePendingRequestsForSeqNum(SeqNum seqNum);

  void sendPreparePartial(SeqNumInfo&);

  void sendCommitPartial(SeqNum);  // TODO(GG): the argument should be a ref to SeqNumInfo

  void executeReadOnlyRequest(concordUtils::SpanWrapper& parent_span, ClientRequestMsg* m);

  /// Single threaded execution only
  void executeNextCommittedRequests(concordUtils::SpanWrapper& parent_span, bool requestMissingInfo = false);

  void executeRequestsInPrePrepareMsg(concordUtils::SpanWrapper& parent_span,
                                      PrePrepareMsg* pp,
                                      bool recoverFromErrorInRequestsExecution = false);

  void executeRequestsAndSendResponses(PrePrepareMsg* pp, Bitmap& requestSet, concordUtils::SpanWrapper& span);
  void sendResponses(PrePrepareMsg* ppMsg, IRequestsHandler::ExecutionRequestsQueue& accumulatedRequests);

  void onSeqNumIsStable(
      SeqNum newStableSeqNum,
      bool hasStateInformation = true,  // true IFF we have checkpoint Or digest in the state transfer
      bool oldSeqNum = false  // true IFF sequence number newStableSeqNum+kWorkWindowSize has already been executed
  );

  void onSeqNumIsSuperStable(SeqNum superStableSeqNum);
  void onTransferringCompleteImp(uint64_t) override;

  template <typename T>
  bool relevantMsgForActiveView(const T* msg);

  void onReportAboutAdvancedReplica(ReplicaId reportedReplica, SeqNum seqNum, ViewNum viewNum);
  void onReportAboutLateReplica(ReplicaId reportedReplica, SeqNum seqNum, ViewNum viewNum);
  void onReportAboutAdvancedReplica(ReplicaId reportedReplica, SeqNum seqNum);
  void onReportAboutLateReplica(ReplicaId reportedReplica, SeqNum seqNum);

  void onReportAboutInvalidMessage(MessageBase* msg, const char* reason) override;

  void sendCheckpointIfNeeded();
  void tryToMarkCheckpointStableForFastPath(const SeqNum& lastCheckpointNumber,
                                            CheckpointInfo<>& checkInfo,
                                            CheckpointMsg* checkpointMessage);

  void tryToGoToNextView();

  IncomingMsgsStorage& getIncomingMsgsStorage() override;

  virtual concord::util::SimpleThreadPool& getInternalThreadPool() override { return internalThreadPool; }

  const ReplicaConfig& getReplicaConfig() const override { return config_; }

  virtual const ReplicasInfo& getReplicasInfo() const override { return (*repsInfo); }

  void askToLeaveView(ReplicaAsksToLeaveViewMsg::Reason reasonToLeave);
  void onViewsChangeTimer(concordUtil::Timers::Handle);
  void onRetransmissionsTimer(concordUtil::Timers::Handle);
  void onStatusReportTimer(concordUtil::Timers::Handle);
  void onSlowPathTimer(concordUtil::Timers::Handle);
  void onInfoRequestTimer(concordUtil::Timers::Handle);
  void onSuperStableCheckpointTimer(concordUtil::Timers::Handle);
  void sendRepilcaRestartReady(uint8_t, const std::string&);
  void sendReplicasRestartReadyProof(uint8_t reason);

  // handlers for internal messages

  void onPrepareCombinedSigFailed(SeqNum seqNumber, ViewNum view, const std::set<uint16_t>& replicasWithBadSigs);
  void onPrepareCombinedSigSucceeded(SeqNum seqNumber,
                                     ViewNum view,
                                     const char* combinedSig,
                                     uint16_t combinedSigLen,
                                     const concordUtils::SpanContext& span_context);
  void onPrepareVerifyCombinedSigResult(SeqNum seqNumber, ViewNum view, bool isValid);

  void onCommitCombinedSigFailed(SeqNum seqNumber,
                                 ViewNum view,
                                 CommitPath cPath,
                                 const std::set<uint16_t>& replicasWithBadSigs);
  void onCommitCombinedSigSucceeded(SeqNum seqNumber,
                                    ViewNum view,
                                    const char* combinedSig,
                                    uint16_t combinedSigLen,
                                    const concordUtils::SpanContext& span_context);
  void onCommitVerifyCombinedSigResult(SeqNum seqNumber, ViewNum view, bool isValid);

  void onFastPathCommitCombinedSigSucceeded(SeqNum seqNumber,
                                            ViewNum view,
                                            CommitPath cPath,
                                            const char* combinedSig,
                                            uint16_t combinedSigLen,
                                            const concordUtils::SpanContext& span_context);
  void onFastPathCommitVerifyCombinedSigResult(SeqNum seqNumber, ViewNum view, CommitPath cPath, bool isValid);

  void onRetransmissionsProcessingResults(SeqNum relatedLastStableSeqNum,
                                          const ViewNum relatedViewNumber,
                                          const std::forward_list<RetSuggestion>& suggestedRetransmissions);

  void onCarrierMessage(CarrierMesssage* msg);

 private:
  void addTimers();
  void startConsensusProcess(PrePrepareMsg* pp, bool isCreatedEarlier);
  void startConsensusProcess(PrePrepareMsg* pp);
  /**
   * Updates both seqNumInfo and slow_path metric
   * @param seqNumInfo
   */
  void startSlowPath(SeqNumInfo& seqNumInfo);

  bool isSeqNumToStopAt(SeqNum seq_num);

  bool validatePreProcessedResults(const PrePrepareMsg* msg, const ViewNum registeredView) const;
  EpochNum getSelfEpochNumber() { return static_cast<EpochNum>(EpochManager::instance().getSelfEpochNumber()); }

  void setConflictDetectionBlockId(const ClientRequestMsg&, IRequestsHandler::ExecutionRequest&);
  /**
   * Updates execution related metrics
   * @param isSlow - The consensus path type
   * @param numOfRequests - The number of executed requests
   */
  void updateExecutedPathMetrics(const bool isSlow, uint16_t numOfRequests);

  class PostExecJob : public concord::util::SimpleThreadPool::Job {
   private:
    PrePrepareMsg* ppMsg_;
    Bitmap requestSet_;
    Timestamp time_;
    ReplicaImp& parent_;

   public:
    PostExecJob(PrePrepareMsg* ppMsg, Bitmap requestSet, Timestamp time, ReplicaImp& p)
        : ppMsg_{ppMsg}, requestSet_{std::move(requestSet)}, time_{time}, parent_{p} {}

    virtual ~PostExecJob() {}

    virtual void release() override { delete this; }

    virtual void execute() override {
      bftEngine::ControlStateManager::instance().waitForPruningIfNeeded();
      MDC_PUT(MDC_REPLICA_ID_KEY, std::to_string(parent_.config_.replicaId));
      MDC_PUT(MDC_THREAD_KEY, "post-execution-thread");
      SCOPED_MDC_SEQ_NUM(std::to_string(ppMsg_->seqNumber()));
      LOG_INFO(CNSUS, "Starting post-execution for seqNumber:" << ppMsg_->seqNumber());
      parent_.executeRequests(ppMsg_, requestSet_, time_);
    }
  };

  // 5 years
  static constexpr int64_t MAX_VALUE_SECONDS = 60 * 60 * 24 * 365 * 5;
  // 5 Minutes
  static constexpr int64_t MAX_VALUE_MICROSECONDS = 1000 * 1000 * 60 * 5l;
  // 60 seconds
  static constexpr int64_t MAX_VALUE_NANOSECONDS = 1000 * 1000 * 1000 * 60l;

  using Recorder = concord::diagnostics::Recorder;
  using Unit = concord::diagnostics::Unit;

  struct Recorders {
    Recorders() {
      auto& registrar = concord::diagnostics::RegistrarSingleton::getInstance();
      registrar.perf.registerComponent("replica",
                                       {send,
                                        executeReadOnlyRequest,
                                        executeWriteRequest,
                                        executeRequestsInPrePrepareMsg,
                                        executeRequestsAndSendResponses,
                                        prepareAndSendResponses,
                                        advanceActiveWindowMainLog,
                                        numRequestsInPrePrepareMsg,
                                        requestsQueueOfPrimarySize,
                                        onSeqNumIsStable,
                                        onTransferringCompleteImp,
                                        consensus,
                                        timeInActiveView,
                                        timeInStateTransfer,
                                        checkpointFromCreationToStable,
                                        removeDuplicatedRequestsFromQueue,
                                        buildPrePrepareMessage,
                                        startConsensusProcess,
                                        finishAddingRequestsToPrePrepareMsg,
                                        addAllRequestsToPrePrepare,
                                        addSelfMsgPrePrepare,
                                        prePrepareWriteTransaction,
                                        broadcastPrePrepare,
                                        sendPreparePartialToSelf,
                                        sendPartialProofToSelf});
    }

    DEFINE_SHARED_RECORDER(send, 1, MAX_VALUE_NANOSECONDS, 3, Unit::NANOSECONDS);
    DEFINE_SHARED_RECORDER(executeReadOnlyRequest, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(executeWriteRequest, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(executeRequestsInPrePrepareMsg, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(executeRequestsAndSendResponses, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(prepareAndSendResponses, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(advanceActiveWindowMainLog, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(numRequestsInPrePrepareMsg, 1, 2500, 3, Unit::COUNT);
    DEFINE_SHARED_RECORDER(requestsQueueOfPrimarySize,
                           1,
                           // Currently, hardcoded to 700 in ReplicaImp.cpp
                           701,
                           3,
                           Unit::COUNT);
    DEFINE_SHARED_RECORDER(onSeqNumIsStable, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(onTransferringCompleteImp, 1, MAX_VALUE_NANOSECONDS, 3, Unit::NANOSECONDS);

    // Only updated by the primary
    DEFINE_SHARED_RECORDER(consensus, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);

    DEFINE_SHARED_RECORDER(timeInActiveView, 1, MAX_VALUE_SECONDS, 3, Unit::SECONDS);
    DEFINE_SHARED_RECORDER(timeInStateTransfer, 1, MAX_VALUE_SECONDS, 3, Unit::SECONDS);
    DEFINE_SHARED_RECORDER(checkpointFromCreationToStable, 1, MAX_VALUE_SECONDS, 3, Unit::SECONDS);

    // Measurements related to PrePrepare creation and sending
    DEFINE_SHARED_RECORDER(removeDuplicatedRequestsFromQueue, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(buildPrePrepareMessage, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(startConsensusProcess, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(finishAddingRequestsToPrePrepareMsg, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(addAllRequestsToPrePrepare, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(addSelfMsgPrePrepare, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(prePrepareWriteTransaction, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(broadcastPrePrepare, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(sendPreparePartialToSelf, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(sendPartialProofToSelf, 1, MAX_VALUE_MICROSECONDS, 3, Unit::MICROSECONDS);
  };

  Recorders histograms_;

  // Used to measure the time for each consensus slot to go from pre-prepare to commit at the primary.
  // Time is recorded in histograms_.consensus
  concord::diagnostics::AsyncTimeRecorderMap<SeqNum> consensus_times_;
  concord::diagnostics::AsyncTimeRecorderMap<SeqNum> checkpoint_times_;

  concord::diagnostics::AsyncTimeRecorder<false> time_in_active_view_;
  concord::diagnostics::AsyncTimeRecorder<false> time_in_state_transfer_;
  batchingLogic::RequestsBatchingLogic reqBatchingLogic_;
  ReplicaStatusHandlers replStatusHandlers_;
  concord::util::CallbackRegistry<uint64_t> onViewNumCallbacks_;
  concord::util::CallbackRegistry<SeqNum> onSeqNumIsStableCallbacks_;

#ifdef USE_FAKE_CLOCK_IN_TS
  std::optional<TimeServiceManager<concord::util::FakeClock>> time_service_manager_;
#else
  std::optional<TimeServiceManager<std::chrono::system_clock>> time_service_manager_;
#endif
};  // namespace bftEngine::impl

}  // namespace bftEngine::impl
