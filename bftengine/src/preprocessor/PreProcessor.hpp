// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include "OpenTracing.hpp"
#include "messages/ClientBatchRequestMsg.hpp"
#include "MsgsCommunicator.hpp"
#include "MsgHandlersRegistrator.hpp"
#include "SimpleThreadPool.hpp"
#include "IRequestHandler.hpp"
#include "Replica.hpp"
#include "RequestProcessingState.hpp"
#include "sliver.hpp"
#include "SigManager.hpp"
#include "Metrics.hpp"
#include "Timers.hpp"
#include "InternalReplicaApi.hpp"
#include "PreProcessorRecorder.hpp"
#include "diagnostics.h"
#include "PerformanceManager.hpp"

#include <mutex>

namespace preprocessor {

struct RequestState {
  std::mutex mutex;  // Define a mutex per request to avoid contentions
  RequestProcessingStateUniquePtr reqProcessingStatePtr;
  // A list of requests passed a pre-processing consensus used for a non-determinism detection at later stage.
  static uint8_t reqProcessingHistoryHeight;
  std::deque<RequestProcessingStateUniquePtr> reqProcessingHistory;
  uint64_t reqRetryId = 1;
};

// Pre-allocated (clientId * dataSize) buffers
typedef std::vector<concordUtils::Sliver> PreProcessResultBuffers;
typedef std::shared_ptr<RequestState> RequestStateSharedPtr;
// (clientId * dataSize + reqOffsetInBatch) -> RequestStateSharedPtr
typedef std::unordered_map<uint16_t, RequestStateSharedPtr> OngoingReqMap;

using TimeRecorder = concord::diagnostics::TimeRecorder<true>;  // use atomic recorder
//**************** Class PreProcessor ****************//

// This class is responsible for the coordination of pre-execution activities on both - primary and non-primary
// replica types. It handles client pre-execution requests, pre-processing requests, and replies.
// On primary replica - it collects pre-execution result hashes from other replicas and decides whether to continue
// or to fail request processing.

class PreProcessor {
 public:
  PreProcessor(std::shared_ptr<MsgsCommunicator> &msgsCommunicator,
               std::shared_ptr<IncomingMsgsStorage> &incomingMsgsStorage,
               std::shared_ptr<MsgHandlersRegistrator> &msgHandlersRegistrator,
               bftEngine::IRequestsHandler &requestsHandler,
               const InternalReplicaApi &replica,
               concordUtil::Timers &timers,
               std::shared_ptr<concord::performance::PerformanceManager> &sdm);

  ~PreProcessor();

  static void addNewPreProcessor(std::shared_ptr<MsgsCommunicator> &msgsCommunicator,
                                 std::shared_ptr<IncomingMsgsStorage> &incomingMsgsStorage,
                                 std::shared_ptr<MsgHandlersRegistrator> &msgHandlersRegistrator,
                                 bftEngine::IRequestsHandler &requestsHandler,
                                 InternalReplicaApi &myReplica,
                                 concordUtil::Timers &timers,
                                 std::shared_ptr<concord::performance::PerformanceManager> &pm);

  static void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator);

  ReqId getOngoingReqIdForClient(uint16_t clientId, uint16_t reqOffsetInBatch);

 private:
  friend class AsyncPreProcessJob;

  uint16_t numOfRequiredReplies();

  template <typename T>
  void messageHandler(MessageBase *msg);

  template <typename T>
  void onMessage(T *msg);

  bool registerRequestOnPrimaryReplica(ClientPreProcessReqMsgUniquePtr clientReqMsg,
                                       PreProcessRequestMsgSharedPtr &preProcessRequestMsg,
                                       uint16_t reqOffsetInBatch,
                                       uint64_t reqRetryId);
  bool registerRequest(ClientPreProcessReqMsgUniquePtr clientReqMsg,
                       PreProcessRequestMsgSharedPtr preProcessRequestMsg,
                       uint16_t reqOffsetInBatch);
  void releaseClientPreProcessRequestSafe(uint16_t clientId, uint16_t reqOffsetInBatch, PreProcessingResult result);
  void releaseClientPreProcessRequest(const RequestStateSharedPtr &reqEntry, PreProcessingResult result);
  bool validateMessage(MessageBase *msg) const;
  void registerMsgHandlers();
  bool checkClientMsgCorrectness(
      uint64_t reqSeqNum, const std::string &cid, bool isReadOnly, uint16_t clientId, NodeIdType senderId) const;
  bool checkClientBatchMsgCorrectness(const ClientBatchRequestMsgUniquePtr &clientBatchReqMsg);
  void handleClientPreProcessRequestByPrimary(PreProcessRequestMsgSharedPtr preProcessRequestMsg);
  void registerAndHandleClientPreProcessReqOnNonPrimary(ClientPreProcessReqMsgUniquePtr clientReqMsg,
                                                        bool arrivedInBatch,
                                                        uint16_t reqOffsetInBatch);
  void sendMsg(char *msg, NodeIdType dest, uint16_t msgType, MsgSize msgSize);
  void sendPreProcessRequestToAllReplicas(const PreProcessRequestMsgSharedPtr &preProcessReqMsg);
  void resendPreProcessRequest(const RequestProcessingStateUniquePtr &reqStatePtr);
  void sendRejectPreProcessReplyMsg(NodeIdType clientId,
                                    uint16_t reqOffsetInBatch,
                                    NodeIdType senderId,
                                    SeqNum reqSeqNum,
                                    SeqNum ongoingReqSeqNum,
                                    uint64_t reqRetryId,
                                    const std::string &cid,
                                    const std::string &ongoingCid);
  const char *getPreProcessResultBuffer(uint16_t clientId, ReqId reqSeqNum, uint16_t reqOffsetInBatch) const;
  const uint16_t getOngoingReqIndex(uint16_t clientId, uint16_t reqOffsetInBatch) const;
  void launchAsyncReqPreProcessingJob(const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                                      bool isPrimary,
                                      bool isRetry,
                                      TimeRecorder &&time_recorder = TimeRecorder());
  uint32_t launchReqPreProcessing(uint16_t clientId,
                                  uint16_t reqOffsetInBatch,
                                  const std::string &cid,
                                  ReqId reqSeqNum,
                                  uint32_t reqLength,
                                  char *reqBuf,
                                  const concordUtils::SpanContext &span_context);
  void handleReqPreProcessingJob(const PreProcessRequestMsgSharedPtr &preProcessReqMsg, bool isPrimary, bool isRetry);
  void handlePreProcessedReqByNonPrimary(uint16_t clientId,
                                         uint16_t reqOffsetInBatch,
                                         ReqId reqSeqNum,
                                         uint64_t reqRetryId,
                                         uint32_t resBufLen,
                                         const std::string &cid);
  void handlePreProcessedReqByPrimary(const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                                      uint16_t clientId,
                                      uint32_t resultBufLen);
  void handlePreProcessedReqPrimaryRetry(NodeIdType clientId, uint16_t reqOffsetInBatch, uint32_t resultBufLen);
  void finalizePreProcessing(NodeIdType clientId, uint16_t reqOffsetInBatch);
  void cancelPreProcessing(NodeIdType clientId, uint16_t reqOffsetInBatch);
  void setPreprocessingRightNow(uint16_t clientId, uint16_t reqOffsetInBatch, bool set);
  PreProcessingResult handlePreProcessedReqByPrimaryAndGetConsensusResult(uint16_t clientId,
                                                                          uint16_t reqOffsetInBatch,
                                                                          uint32_t resultBufLen);
  void handlePreProcessReplyMsg(const std::string &cid,
                                PreProcessingResult result,
                                NodeIdType clientId,
                                uint16_t reqOffsetInBatch,
                                SeqNum reqSeqNum);
  void updateAggregatorAndDumpMetrics();
  void addTimers();
  void cancelTimers();
  void onRequestsStatusCheckTimer();
  void handleSingleClientRequestMessage(ClientPreProcessReqMsgUniquePtr clientMsg,
                                        bool arrivedInBatch,
                                        uint16_t msgOffsetInBatch);
  bool isRequestPreProcessingRightNow(const RequestStateSharedPtr &reqEntry,
                                      ReqId reqSeqNum,
                                      NodeIdType clientId,
                                      NodeIdType senderId);
  bool isRequestAlreadyExecuted(ReqId reqSeqNum, NodeIdType clientId, const std::string &cid);
  bool isRequestPreProcessedBefore(const RequestStateSharedPtr &reqEntry,
                                   SeqNum reqSeqNum,
                                   NodeIdType clientId,
                                   const std::string &cid);

  bool isRequestPassingConsensusOrPostExec(SeqNum reqSeqNum,
                                           NodeIdType clientId,
                                           NodeIdType senderId,
                                           const std::string &cid);
  static logging::Logger &logger() {
    static logging::Logger logger_ = logging::getLogger("concord.preprocessor");
    return logger_;
  }

 private:
  static std::vector<std::shared_ptr<PreProcessor>> preProcessors_;  // The place holder for PreProcessor objects

  std::shared_ptr<MsgsCommunicator> msgsCommunicator_;
  std::shared_ptr<IncomingMsgsStorage> incomingMsgsStorage_;
  std::shared_ptr<MsgHandlersRegistrator> msgHandlersRegistrator_;
  bftEngine::IRequestsHandler &requestsHandler_;
  const InternalReplicaApi &myReplica_;
  const ReplicaId myReplicaId_;
  const uint32_t maxPreExecResultSize_;
  const std::set<ReplicaId> &idsOfPeerReplicas_;
  const uint16_t numOfReplicas_;
  const uint16_t numOfInternalClients_;
  const bool clientBatchingEnabled_;
  const uint16_t clientMaxBatchSize_;
  util::SimpleThreadPool threadPool_;
  // One-time allocated buffers (one per client) for the pre-execution results storage
  PreProcessResultBuffers preProcessResultBuffers_;
  OngoingReqMap ongoingRequests_;  // clientId + reqOffsetInBatch -> RequestStateSharedPtr
  concordMetrics::Component metricsComponent_;
  std::chrono::seconds metricsLastDumpTime_;
  std::chrono::seconds metricsDumpIntervalInSec_;
  struct PreProcessingMetrics {
    concordMetrics::CounterHandle preProcReqReceived;
    concordMetrics::CounterHandle preProcReqInvalid;
    concordMetrics::CounterHandle preProcReqIgnored;
    concordMetrics::CounterHandle preProcConsensusNotReached;
    concordMetrics::CounterHandle preProcessRequestTimedout;
    concordMetrics::CounterHandle preProcReqSentForFurtherProcessing;
    concordMetrics::CounterHandle preProcPossiblePrimaryFaultDetected;
    concordMetrics::CounterHandle preProcReqForwardedByNonPrimaryNotIgnored;
    concordMetrics::GaugeHandle preProcInFlyRequestsNum;
  } preProcessorMetrics_;
  concordUtil::Timers::Handle requestsStatusCheckTimer_;
  concordUtil::Timers::Handle metricsTimer_;
  const uint64_t preExecReqStatusCheckPeriodMilli_;
  concordUtil::Timers &timers_;
  PreProcessorRecorder histograms_;
  std::shared_ptr<concord::diagnostics::Recorder> recorder_;
  ViewNum lastViewNum_;
  std::shared_ptr<concord::performance::PerformanceManager> pm_ = nullptr;
};

//**************** Class AsyncPreProcessJob ****************//

// This class is used to send messages to other replicas in parallel

class AsyncPreProcessJob : public util::SimpleThreadPool::Job {
 public:
  AsyncPreProcessJob(PreProcessor &preProcessor,
                     const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                     bool isPrimary,
                     bool isRetry,
                     TimeRecorder &&time_recorder);
  virtual ~AsyncPreProcessJob() = default;

  void execute() override;
  void release() override;

 private:
  PreProcessor &preProcessor_;
  PreProcessRequestMsgSharedPtr preProcessReqMsg_;
  bool isPrimary_ = false;
  bool isRetry_ = false;
  TimeRecorder time_recorder_;
};

}  // namespace preprocessor
