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

#include "util/OpenTracing.hpp"
#include "messages/ClientBatchRequestMsg.hpp"
#include "messages/PreProcessBatchRequestMsg.hpp"
#include "messages/PreProcessBatchReplyMsg.hpp"
#include "util/SimpleThreadPool.hpp"
#include "IRequestHandler.hpp"
#include "RequestProcessingState.hpp"
#include "util/sliver.hpp"
#include "SigManager.hpp"
#include "util/Metrics.hpp"
#include "util/Timers.hpp"
#include "InternalReplicaApi.hpp"
#include "PreProcessorRecorder.hpp"
#include "diagnostics/diagnostics.hpp"
#include "PerformanceManager.hpp"
#include "util/RollingAvgAndVar.hpp"
#include "SharedTypes.hpp"
#include "util/MultiSizeBufferPool.hpp"
#include "util/RawMemoryPool.hpp"
#include "GlobalData.hpp"
#include "util/PerfMetrics.hpp"
#include "Replica.hpp"
#include <boost/lockfree/spsc_queue.hpp>

#include <mutex>
#include <condition_variable>
#include <functional>

namespace bftEngine::impl {
class MsgsCommunicator;
class MsgHandlersRegistrator;
}  // namespace bftEngine::impl

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
  uint32_t size{};
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
  void increaseNumOfCompletedReqs(uint32_t count);
  RequestStateSharedPtr &getRequestState(uint16_t reqOffsetInBatch);
  const std::string getBatchCid() const;
  void cancelBatchAndReleaseRequests(const std::string &batchCid, PreProcessingResult status);
  void cancelRequestAndBatchIfCompleted(const std::string &reqBatchCid,
                                        uint16_t reqOffsetInBatch,
                                        PreProcessingResult status);
  void releaseReqsAndSendBatchedReplyIfCompleted(PreProcessReplyMsgSharedPtr replyMsg);
  void finalizeBatchIfCompletedSafe(const std::string &batchCid);
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

class PreProcessor : public bftEngine::IExternalObject {
 public:
  PreProcessor(std::shared_ptr<bftEngine::impl::MsgsCommunicator> &msgsCommunicator,
               std::shared_ptr<IncomingMsgsStorage> &incomingMsgsStorage,
               std::shared_ptr<bftEngine::impl::MsgHandlersRegistrator> &msgHandlersRegistrator,
               bftEngine::IRequestsHandler &requestsHandler,
               InternalReplicaApi &replica,
               concordUtil::Timers &timers,
               std::shared_ptr<concord::performance::PerformanceManager> &sdm);

  ~PreProcessor();
  void stop();

  // For testing purposes
  ReqId getOngoingReqIdForClient(uint16_t clientId, uint16_t reqOffsetInBatch);
  RequestsBatchSharedPtr getOngoingBatchForClient(uint16_t clientId);

  void setAggregator(const std::shared_ptr<concordMetrics::Aggregator> &a) override {
    metricsComponent_.SetAggregator(a);
    if (multiSizeBufferPool_)
      multiSizeBufferPool_->setAggregator(a);
    else if (rawMemoryPool_)
      rawMemoryPool_->setAggregator(a);
  }

 protected:
  bool checkPreProcessRequestMsgCorrectness(const PreProcessRequestMsgSharedPtr &requestMsg);
  bool checkPreProcessReplyMsgCorrectness(const PreProcessReplyMsgSharedPtr &replyMsg);
  bool checkPreProcessBatchReqMsgCorrectness(const PreProcessBatchReqMsgUniquePtr &batchReq);
  bool checkPreProcessBatchReplyMsgCorrectness(const PreProcessBatchReplyMsgUniquePtr &batchReply);

 private:
  friend class AsyncPreProcessJob;
  friend class RequestsBatch;

  uint16_t numOfRequiredReplies();

  template <typename T>
  void messageHandler(std::unique_ptr<MessageBase> msg);

  template <typename T>
  void onMessage(std::unique_ptr<T> msg);

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
  uint32_t getBufferOffset(uint16_t clientId, ReqId reqSeqNum, uint16_t reqOffsetInBatch) const;
  // If minBufferSize is 0, default (minimal) buffer size is returned. Else, a buffer size of at least minBufferSize
  // bytes is returned. If no such buffer size exist - function throws an exception.
  // Returns: an entry to a buffer entry which holds the allocated result buffer and its size.
  SafeResultBuffer &getPreProcessResultBufferEntry(uint16_t clientId,
                                                   ReqId reqSeqNum,
                                                   uint16_t reqOffsetInBatch,
                                                   uint32_t minBufferSize = 0);
  void releasePreProcessResultBuffer(uint16_t clientId, ReqId reqSeqNum, uint16_t reqOffsetInBatch);
  void launchAsyncReqPreProcessingJob(const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                                      const std::string &batchCid,
                                      bool isPrimary,
                                      TimeRecorder &&totalPreExecDurationRecorder = TimeRecorder());
  bftEngine::OperationResult launchReqPreProcessing(const std::string &batchCid,
                                                    const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                                                    uint32_t &resultLen);
  void handleReqPreProcessingJob(const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                                 const std::string &batchCid,
                                 bool isPrimary);
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
  void finalizePreProcessing(NodeIdType clientId, uint16_t reqOffsetInBatch, const std::string &batchCid);
  void cancelPreProcessing(NodeIdType clientId, const std::string &batchCid, uint16_t reqOffsetInBatch);
  void setPreprocessingRightNow(uint16_t clientId, uint16_t reqOffsetInBatch, bool set);
  void holdCompleteOrCancelRequest(const std::string &reqCid,
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
  bool validateSubpoolsConfig();
  static logging::Logger &logger() {
    static logging::Logger logger_ = logging::getLogger("concord.preprocessor");
    return logger_;
  }

  concordUtil::MultiSizeBufferPool::SubpoolsConfig calcSubpoolsConfig();
  std::unique_ptr<concordUtil::MultiSizeBufferPool> createMultiSizeBufferPool();

 private:
  const uint32_t MAX_MSGS = 10000;
  const uint32_t WAIT_TIMEOUT_MILLI = 100;
  void msgProcessingLoop();

  boost::lockfree::spsc_queue<MessageBase *> msgs_{MAX_MSGS};
  std::thread msgLoopThread_;
  std::mutex msgLock_;
  std::condition_variable msgLoopSignal_;
  std::atomic_bool msgLoopDone_{false};

  std::shared_ptr<MsgsCommunicator> msgsCommunicator_;
  std::shared_ptr<IncomingMsgsStorage> incomingMsgsStorage_;
  std::shared_ptr<MsgHandlersRegistrator> msgHandlersRegistrator_;
  bftEngine::IRequestsHandler &requestsHandler_;
  InternalReplicaApi &myReplica_;
  const ReplicaId myReplicaId_;
  const uint32_t maxExternalMsgSize_;
  // Add overheads that should be reduced from the net space given for execution engine
  const uint32_t responseSizeInternalOverhead_;
  const uint16_t numOfReplicas_;
  const uint16_t numOfClientProxies_;
  const bool clientBatchingEnabled_;
  inline static uint16_t clientMaxBatchSize_ = 0;
  concord::util::SimpleThreadPool threadPool_;
  concordUtil::Timers &timers_;
  // One-time allocated buffers (one per client) for the pre-execution results storage
  PreProcessResultBuffers preProcessResultBuffers_;
  OngoingReqBatchesMap ongoingReqBatches_;  // clientId -> RequestsBatch
  // Memory pool parameters:
  // Maximal subpool size should support up to 32MB (including responseSizeInternalOverhead_). So the maximal execution
  // engine supported size is kMaxSubpoolBuffersize_ - responseSizeInternalOverhead_
  static constexpr uint32_t kMaxSubpoolBuffersize_{1UL << 25};

  // Execution engine results can be stored in 3 different modes:
  // 1) Without any pool, so the result buffers are managed by the pre-processor itself.
  // 2) With MultiSizeBufferPool (default case)- the least amount of memory is consumed.
  // 3) With RawMemoryPool - legacy method. We keep this method to ensure stability - no abstract layer/interface
  //  or pool polymorphism is implemented since we hope to remove this code at some point soon.
  enum class PreProcessorMemoryPoolMode { NO_POOL, MULTI_SIZE_BUFFER_POOL, RAW_MEMORY_POOL };
  PreProcessorMemoryPoolMode initMemoryPoolMode();
  PreProcessorMemoryPoolMode memoryPoolMode_;
  // SubpoolsConfig must be sorted in ascending order, by bufferSize
  const concordUtil::MultiSizeBufferPool::SubpoolsConfig subpoolsConfig_;
  std::unique_ptr<concordUtil::MultiSizeBufferPool> multiSizeBufferPool_;
  std::unique_ptr<concordUtil::RawMemoryPool> rawMemoryPool_;

  concordMetrics::Component metricsComponent_;
  std::chrono::seconds metricsLastDumpTime_;
  std::chrono::seconds metricsDumpIntervalInSec_;
  struct PreProcessingMetrics {
    concordMetrics::AtomicCounterHandle preProcReqReceived;
    concordMetrics::CounterHandle preProcBatchReqReceived;
    concordMetrics::CounterHandle preProcReqInvalid;
    concordMetrics::AtomicCounterHandle preProcReqIgnored;
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
  PreProcessorRecorder histograms_;
  std::shared_ptr<concord::diagnostics::Recorder> totalPreExecDurationRecorder_;
  std::shared_ptr<concord::diagnostics::Recorder> launchAsyncPreProcessJobRecorder_;
  std::shared_ptr<concord::performance::PerformanceManager> pm_ = nullptr;
};

//**************** Class AsyncPreProcessJob ****************//

// This class is used to send messages to other replicas in parallel

class AsyncPreProcessJob : public concord::util::SimpleThreadPool::Job {
 public:
  AsyncPreProcessJob(PreProcessor &preProcessor,
                     const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                     const std::string &batchCid,
                     bool isPrimary,
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
  TimeRecorder totalJobDurationRecorder_;
  TimeRecorder launchAsyncPreProcessJobRecorder_;
};

}  // namespace preprocessor
