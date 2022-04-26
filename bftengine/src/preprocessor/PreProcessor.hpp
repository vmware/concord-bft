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
#include "messages/PreProcessBatchRequestMsg.hpp"
#include "messages/PreProcessBatchReplyMsg.hpp"
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
#include "RollingAvgAndVar.hpp"
#include "SharedTypes.hpp"
#include "RawMemoryPool.hpp"
#include "GlobalData.hpp"
#include "PerfMetrics.hpp"

// TODO[TK] till boost upgrade
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <boost/lockfree/spsc_queue.hpp>
#pragma GCC diagnostic pop

#include <mutex>
#include <condition_variable>
#include <functional>

namespace preprocessor {

struct RequestState {
  std::mutex mutex;  // Define a mutex per request to avoid contentions
  RequestProcessingStateUniquePtr reqProcessingStatePtr;
  // A list of requests passed a pre-processing consensus used for a non-determinism detection at later stage.
  static uint8_t reqProcessingHistoryHeight;
  std::deque<RequestProcessingStateUniquePtr> reqProcessingHistory;
  // Identity for matching request and reply
  uint64_t reqRetryId = 1;
};

struct SafeResultBuffer {
  std::mutex mutex;
  char *buffer = nullptr;
};

using SafeResultBufferSharedPtr = std::shared_ptr<SafeResultBuffer>;
// Memory pool allocated (clientId * dataSize) buffers
using PreProcessResultBuffers = std::deque<SafeResultBufferSharedPtr>;
using TimeRecorder = concord::diagnostics::TimeRecorder<true>;  // use atomic recorder
using RequestStateSharedPtr = std::shared_ptr<RequestState>;

//**************** Class RequestsBatch ****************//
class PreProcessor;

class RequestsBatch {
 public:
  RequestsBatch(PreProcessor &preProcessor, uint16_t clientId) : preProcessor_(preProcessor), clientId_(clientId) {}
  void init();
  void registerBatch(NodeIdType senderId, const std::string &batchCid, uint32_t batchSize);
  void startBatch(NodeIdType senderId, const std::string &batchCid, uint32_t batchSize);
  bool isBatchRegistered(std::string &batchCid) const;
  bool isBatchInProcess() const;
  bool isBatchInProcess(std::string &batchCid) const;
  void increaseNumOfCompletedReqs(uint32_t count) { numOfCompletedReqs_ += count; }
  RequestStateSharedPtr &getRequestState(uint16_t reqOffsetInBatch);
  const std::string getBatchCid() const;
  void cancelBatchAndReleaseRequests(const std::string &batchCid, PreProcessingResult status);
  void cancelRequestAndBatchIfCompleted(const std::string &reqBatchCid,
                                        uint16_t reqOffsetInBatch,
                                        PreProcessingResult status);
  void releaseReqsAndSendBatchedReplyIfCompleted(PreProcessReplyMsgSharedPtr replyMsg);
  void finalizeBatchIfCompletedSafe();
  void handlePossiblyExpiredRequests();
  void sendCancelBatchedPreProcessingMsgToNonPrimaries(const ClientMsgsList &clientMsgs, NodeIdType destId);
  uint64_t getBlockId() const;

 private:
  void setBatchParameters(const std::string &batchCid, uint32_t batchSize);
  void resetBatchParams();
  void finalizeBatchIfCompleted();

 private:
  PreProcessor &preProcessor_;
  const uint16_t clientId_;
  std::string batchCid_;
  std::pair<std::string, uint64_t> cidToBlockId_;
  uint32_t batchSize_ = 0;
  bool batchRegistered_ = false;
  bool batchInProcess_ = false;
  std::atomic_uint32_t numOfCompletedReqs_ = 0;
  // Pre-allocated map: request offset in batch -> request state
  std::map<uint16_t, RequestStateSharedPtr> requestsMap_;
  mutable std::mutex batchMutex_;
  PreProcessReplyMsgsList repliesList_;
};

using RequestsBatchSharedPtr = std::shared_ptr<RequestsBatch>;
// clientId -> RequestsBatch map
using OngoingReqBatchesMap = std::map<uint16_t, RequestsBatchSharedPtr>;

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

 protected:
  bool checkPreProcessRequestMsgCorrectness(const PreProcessRequestMsgSharedPtr &requestMsg);
  bool checkPreProcessReplyMsgCorrectness(const PreProcessReplyMsgSharedPtr &replyMsg);
  bool checkPreProcessBatchReqMsgCorrectness(const PreProcessBatchReqMsgSharedPtr &batchReq);
  bool checkPreProcessBatchReplyMsgCorrectness(const PreProcessBatchReplyMsgSharedPtr &batchReply);

 private:
  friend class AsyncPreProcessJob;
  friend class RequestsBatch;

  uint16_t numOfRequiredReplies();

  template <typename T>
  void messageHandler(MessageBase *msg);

  template <typename T>
  void onMessage(T *msg);

  bool registerRequestOnPrimaryReplica(const std::string &batchCid,
                                       uint32_t batchSize,
                                       ClientPreProcessReqMsgUniquePtr clientReqMsg,
                                       PreProcessRequestMsgSharedPtr &preProcessRequestMsg,
                                       uint16_t reqOffsetInBatch,
                                       RequestStateSharedPtr reqEntry);
  void countRetriedRequests(const ClientPreProcessReqMsgUniquePtr &clientReqMsg, const RequestStateSharedPtr &reqEntry);
  bool registerRequest(const std::string &batchCid,
                       uint32_t batchSize,
                       ClientPreProcessReqMsgUniquePtr clientReqMsg,
                       PreProcessRequestMsgSharedPtr preProcessRequestMsg,
                       uint16_t reqOffsetInBatch);
  void releaseClientPreProcessRequestSafe(uint16_t clientId, uint16_t reqOffsetInBatch, PreProcessingResult result);
  void releaseClientPreProcessRequestSafe(uint16_t clientId,
                                          const RequestStateSharedPtr &reqEntry,
                                          PreProcessingResult result);
  void releaseClientPreProcessRequest(const RequestStateSharedPtr &reqEntry, PreProcessingResult result);
  bool validateMessage(MessageBase *msg) const;
  void registerMsgHandlers();
  bool checkClientMsgCorrectness(uint64_t reqSeqNum,
                                 const std::string &reqCid,
                                 bool isReadOnly,
                                 uint16_t clientId,
                                 NodeIdType senderId,
                                 const std::string &batchCid) const;
  bool checkClientBatchMsgCorrectness(const ClientBatchRequestMsgUniquePtr &clientBatchReqMsg);
  bool checkPreProcessReqPrerequisites(SeqNum reqSeqNum,
                                       const std::string &reqCid,
                                       NodeIdType senderId,
                                       NodeIdType clientId,
                                       const std::string &batchCid,
                                       uint16_t reqOffsetInBatch);
  void handleClientPreProcessRequestByPrimary(PreProcessRequestMsgSharedPtr preProcessRequestMsg,
                                              const std::string &batchCid,
                                              bool arrivedInBatch);
  bool checkPreProcessReplyPrerequisites(SeqNum reqSeqNum,
                                         const std::string &reqCid,
                                         NodeIdType senderId,
                                         const std::string &batchCid,
                                         uint16_t offsetInBatch);
  void registerAndHandleClientPreProcessReqOnNonPrimary(const std::string &batchCid,
                                                        uint32_t batchSize,
                                                        ClientPreProcessReqMsgUniquePtr clientReqMsg,
                                                        bool arrivedInBatch,
                                                        uint16_t reqOffsetInBatch);
  void sendMsg(char *msg, NodeIdType dest, uint16_t msgType, MsgSize msgSize);
  void sendPreProcessBatchReqToAllReplicas(ClientBatchRequestMsgUniquePtr clientBatchMsg,
                                           const PreProcessReqMsgsList &preProcessReqMsgList,
                                           uint32_t requestsSize);
  void sendPreProcessRequestToAllReplicas(const PreProcessRequestMsgSharedPtr &preProcessReqMsg);
  void resendPreProcessRequest(const RequestProcessingStateUniquePtr &reqStatePtr);
  void sendRejectPreProcessReplyMsg(NodeIdType clientId,
                                    uint16_t reqOffsetInBatch,
                                    NodeIdType senderId,
                                    SeqNum reqSeqNum,
                                    SeqNum ongoingReqSeqNum,
                                    uint64_t reqRetryId,
                                    const std::string &reqCid,
                                    const std::string &ongoingCid);
  void cancelPreProcessingOnNonPrimary(const ClientPreProcessReqMsgUniquePtr &clientReqMsg,
                                       NodeIdType destId,
                                       uint16_t reqOffsetInBatch,
                                       uint64_t reqRetryId,
                                       const std::string &batchCid);
  uint32_t getBufferOffset(uint16_t clientId, ReqId reqSeqNum, uint16_t reqOffsetInBatch) const;
  const char *getPreProcessResultBuffer(uint16_t clientId, ReqId reqSeqNum, uint16_t reqOffsetInBatch);
  void releasePreProcessResultBuffer(uint16_t clientId, ReqId reqSeqNum, uint16_t reqOffsetInBatch);
  void launchAsyncReqPreProcessingJob(const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                                      const std::string &batchCid,
                                      bool isPrimary,
                                      bool isRetry,
                                      TimeRecorder &&totalPreExecDurationRecorder = TimeRecorder());
  bftEngine::OperationResult launchReqPreProcessing(const std::string &batchCid,
                                                    const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                                                    uint32_t &resultLen);
  void handleReqPreProcessingJob(const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                                 const std::string &batchCid,
                                 bool isPrimary,
                                 bool isRetry);
  void handleReqPreProcessedByNonPrimary(uint16_t clientId,
                                         uint16_t reqOffsetInBatch,
                                         ReqId reqSeqNum,
                                         uint64_t reqRetryId,
                                         uint32_t resBufLen,
                                         const std::string &reqCid,
                                         bftEngine::OperationResult preProcessResult);
  void handleReqPreProcessedByPrimary(const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                                      const std::string &batchCid,
                                      uint16_t clientId,
                                      uint32_t resultBufLen,
                                      bftEngine::OperationResult preProcessResult);
  void handlePreProcessedReqPrimaryRetry(NodeIdType clientId,
                                         uint16_t reqOffsetInBatch,
                                         uint32_t resultBufLen,
                                         const std::string &batchCid,
                                         bftEngine::OperationResult preProcessResult);
  void finalizePreProcessing(NodeIdType clientId, uint16_t reqOffsetInBatch, const std::string &batchCid);
  void cancelPreProcessing(NodeIdType clientId, const std::string &batchCid, uint16_t reqOffsetInBatch);
  void setPreprocessingRightNow(uint16_t clientId, uint16_t reqOffsetInBatch, bool set);
  PreProcessingResult handlePreProcessedReqByPrimaryAndGetConsensusResult(uint16_t clientId,
                                                                          uint16_t reqOffsetInBatch,
                                                                          uint32_t resultBufLen,
                                                                          bftEngine::OperationResult preProcessResult);
  void handlePreProcessReplyMsg(const std::string &reqCid,
                                PreProcessingResult result,
                                NodeIdType clientId,
                                uint16_t reqOffsetInBatch,
                                SeqNum reqSeqNum,
                                const std::string &batchCid);
  void updateAggregatorAndDumpMetrics();
  void addTimers();
  void cancelTimers();
  void onRequestsStatusCheckTimer();
  bool handleSingleClientRequestMessage(ClientPreProcessReqMsgUniquePtr clientMsg,
                                        NodeIdType senderId,
                                        bool arrivedInBatch,
                                        uint16_t msgOffsetInBatch,
                                        PreProcessRequestMsgSharedPtr &preProcessRequestMsg,
                                        const std::string &batchCid,
                                        uint32_t batchSize);
  bool handleSinglePreProcessRequestMsg(PreProcessRequestMsgSharedPtr preProcessReqMsg,
                                        const std::string &batchCid,
                                        uint32_t batchSize);
  void handleSinglePreProcessReplyMsg(PreProcessReplyMsgSharedPtr preProcessReplyMsg, const std::string &batchCid);
  bool isRequestPreProcessingRightNow(const RequestStateSharedPtr &reqEntry,
                                      ReqId reqSeqNum,
                                      NodeIdType clientId,
                                      const std::string &batchCid,
                                      NodeIdType senderId);
  bool isRequestPreProcessedBefore(const RequestStateSharedPtr &reqEntry,
                                   SeqNum reqSeqNum,
                                   NodeIdType clientId,
                                   const std::string &batchCid,
                                   const std::string &reqCid);
  bool isRequestPassingConsensusOrPostExec(SeqNum reqSeqNum,
                                           NodeIdType senderId,
                                           NodeIdType clientId,
                                           const std::string &batchCid,
                                           const std::string &reqCid);
  void releaseReqAndSendReplyMsg(PreProcessReplyMsgSharedPtr replyMsg);
  bool handlePossiblyExpiredRequest(const RequestStateSharedPtr &reqStateEntry);

  static logging::Logger &logger() {
    static logging::Logger logger_ = logging::getLogger("concord.preprocessor");
    return logger_;
  }

 private:
  const uint32_t MAX_MSGS = 10000;
  const uint32_t WAIT_TIMEOUT_MILLI = 100;
  void msgProcessingLoop();

  boost::lockfree::spsc_queue<MessageBase *> msgs_{MAX_MSGS};
  std::thread msgLoopThread_;
  std::mutex msgLock_;
  std::condition_variable msgLoopSignal_;
  std::atomic_bool msgLoopDone_{false};

  static std::vector<std::shared_ptr<PreProcessor>> preProcessors_;  // The place-holder for PreProcessor objects

  std::shared_ptr<MsgsCommunicator> msgsCommunicator_;
  std::shared_ptr<IncomingMsgsStorage> incomingMsgsStorage_;
  std::shared_ptr<MsgHandlersRegistrator> msgHandlersRegistrator_;
  bftEngine::IRequestsHandler &requestsHandler_;
  const InternalReplicaApi &myReplica_;
  const ReplicaId myReplicaId_;
  const uint32_t maxExternalMsgSize_;
  const uint32_t maxPreExecResultSize_;
  const uint16_t numOfReplicas_;
  const uint16_t numOfClientProxies_;
  const bool clientBatchingEnabled_;
  inline static uint16_t clientMaxBatchSize_ = 0;
  concord::util::SimpleThreadPool threadPool_;
  // One-time allocated buffers (one per client) for the pre-execution results storage
  PreProcessResultBuffers preProcessResultBuffers_;
  OngoingReqBatchesMap ongoingReqBatches_;  // clientId -> RequestsBatch
  concordUtil::RawMemoryPool memoryPool_;

  concordMetrics::Component metricsComponent_;
  std::chrono::seconds metricsLastDumpTime_;
  std::chrono::seconds metricsDumpIntervalInSec_;
  struct PreProcessingMetrics {
    concordMetrics::AtomicCounterHandle preProcReqReceived;
    concordMetrics::CounterHandle preProcBatchReqReceived;
    concordMetrics::CounterHandle preProcReqInvalid;
    concordMetrics::AtomicCounterHandle preProcReqIgnored;
    concordMetrics::AtomicCounterHandle preProcReqRejected;
    concordMetrics::CounterHandle preProcConsensusNotReached;
    concordMetrics::CounterHandle preProcessRequestTimedOut;
    concordMetrics::CounterHandle preProcPossiblePrimaryFaultDetected;
    concordMetrics::CounterHandle preProcReqCompleted;
    concordMetrics::CounterHandle preProcReqRetried;
    concordMetrics::AtomicGaugeHandle preProcessingTimeAvg;
    concordMetrics::AtomicGaugeHandle launchAsyncPreProcessJobTimeAvg;
    concordMetrics::AtomicGaugeHandle preProcInFlyRequestsNum;
  } preProcessorMetrics_;

  PerfMetric<std::string> metric_pre_exe_duration_;

  bftEngine::impl::RollingAvgAndVar totalPreProcessingTime_;
  bftEngine::impl::RollingAvgAndVar launchAsyncJobTimeAvg_;
  concordUtil::Timers::Handle requestsStatusCheckTimer_;
  concordUtil::Timers::Handle metricsTimer_;
  const uint64_t preExecReqStatusCheckPeriodMilli_;
  concordUtil::Timers &timers_;
  PreProcessorRecorder histograms_;
  std::shared_ptr<concord::diagnostics::Recorder> totalPreExecDurationRecorder_;
  std::shared_ptr<concord::diagnostics::Recorder> launchAsyncPreProcessJobRecorder_;
  std::shared_ptr<concord::performance::PerformanceManager> pm_ = nullptr;
  bool batchedPreProcessEnabled_;
  bool memoryPoolEnabled_;
};

//**************** Class AsyncPreProcessJob ****************//

// This class is used to send messages to other replicas in parallel

class AsyncPreProcessJob : public concord::util::SimpleThreadPool::Job {
 public:
  AsyncPreProcessJob(PreProcessor &preProcessor,
                     const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                     const std::string &batchCid,
                     bool isPrimary,
                     bool isRetry,
                     TimeRecorder &&totalPreExecDurationRecorder,
                     TimeRecorder &&launchAsyncPreProcessJobRecorder);
  virtual ~AsyncPreProcessJob() = default;

  void execute() override;
  void release() override;

 private:
  const int32_t resetFrequency_ = 5000;
  PreProcessor &preProcessor_;
  PreProcessRequestMsgSharedPtr preProcessReqMsg_;
  const std::string batchCid_;
  bool isPrimary_ = false;
  bool isRetry_ = false;
  TimeRecorder totalJobDurationRecorder_;
  TimeRecorder launchAsyncPreProcessJobRecorder_;
};

}  // namespace preprocessor
