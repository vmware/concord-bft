// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <set>
#include <map>
#include <chrono>
#include <random>
#include <cassert>
#include <iostream>
#include <string>
#include <array>
#include <cstdint>
#include <optional>

#include "Logger.hpp"
#include "SimpleBCStateTransfer.hpp"
#include "IStateTransfer.hpp"
#include "DataStore.hpp"
#include "MsgsCertificate.hpp"
#include "Messages.hpp"
#include "Metrics.hpp"
#include "SourceSelector.hpp"
#include "callback_registry.hpp"
#include "Handoff.hpp"
#include "SysConsts.hpp"
#include "throughput.hpp"
#include "diagnostics.h"
#include "performance_handler.h"
#include "Timers.hpp"
#include "TimeUtils.hpp"
#include "SimpleMemoryPool.hpp"
#include "messages/MessageBase.hpp"

using std::set;
using std::map;
using std::string;
using concordMetrics::StatusHandle;
using concordMetrics::GaugeHandle;
using concordMetrics::CounterHandle;
using concordMetrics::AtomicGaugeHandle;
using concordMetrics::AtomicCounterHandle;
using concord::util::Throughput;
using concord::diagnostics::Recorder;
using concord::diagnostics::AsyncTimeRecorder;
using concord::util::DurationTracker;
using concord::client::reconfiguration::ClientReconfigurationEngine;

namespace bftEngine::bcst::impl {

class RVBManager;

class BCStateTran : public IStateTransfer {
  // The next friend declerations are used strictly for testing
  friend class BcStTestDelegator;

 public:
  //////////////////////////////////////////////////////////////////////////////
  // Ctor & Dtor (Initialization)
  //////////////////////////////////////////////////////////////////////////////
  BCStateTran(const Config& config, IAppState* const stateApi, DataStore* ds = nullptr);
  ~BCStateTran() override;

  ///////////////////////////////////////////////////////////////////////////
  // IStateTransfer methods
  ///////////////////////////////////////////////////////////////////////////
  void init(uint64_t maxNumOfRequiredStoredCheckpoints,
            uint32_t numberOfRequiredReservedPages,
            uint32_t sizeOfReservedPage) override {
    initHandler_(maxNumOfRequiredStoredCheckpoints, numberOfRequiredReservedPages, sizeOfReservedPage);
  }
  void startRunning(IReplicaForStateTransfer* r) override { startRunningHandler_(r); }
  void stopRunning() override;
  bool isRunning() const override { return running_; }
  void createCheckpointOfCurrentState(uint64_t checkpointNumber) override {
    createCheckpointOfCurrentStateHandler_(checkpointNumber);
  }
  void getDigestOfCheckpoint(uint64_t checkpointNumber,
                             uint16_t sizeOfDigestBuffer,
                             uint64_t& outBlockId,
                             char* outStateDigest,
                             char* outResPagesDigest,
                             char* outRVBDataDigest) override {
    getDigestOfCheckpointHandler_(checkpointNumber,
                                  sizeOfDigestBuffer,
                                  std::ref(outBlockId),
                                  outStateDigest,
                                  outResPagesDigest,
                                  outRVBDataDigest);
  }
  void startCollectingState() override { startCollectingStateHandler_(); }
  bool isCollectingState() const override { return psd_->getIsFetchingState(); }
  void onTimer() override { onTimerHandler_(); };

  // Handle ST incoming messages
  using LocalTimePoint = time_point<steady_clock>;
  static constexpr auto UNDEFINED_LOCAL_TIME_POINT = LocalTimePoint::max();
  void handleStateTransferMessage(char* msg, uint32_t msgLen, uint16_t senderId) override {
    handleStateTransferMessageHandler_(msg, msgLen, senderId, steady_clock::now());
  }

  void handleIncomingConsensusMessage(const ConsensusMsg msg) override { handleIncomingConsensusMessageHandler_(msg); }
  std::string getStatus() override;
  void addOnTransferringCompleteCallback(
      const std::function<void(uint64_t)>& cb,
      StateTransferCallBacksPriorities priority = StateTransferCallBacksPriorities::DEFAULT) override {
    addOnTransferringCompleteCallbackHandler_(cb, priority);
  };

  // TODO - this one should be removed - BC-15921
  void addOnFetchingStateChangeCallback(const std::function<void(uint64_t)>& cb) override {
    addOnFetchingStateChangeCallbackHandler_(cb);
  }

  // Calls into RVB manager on an external thread context. Reports of the last agreed prunable block. Relevant only for
  // replica in consensus.
  void reportLastAgreedPrunableBlockId(uint64_t lastAgreedPrunableBlockId) override {
    reportLastAgreedPrunableBlockIdHandler_(lastAgreedPrunableBlockId);
  }

  ///////////////////////////////////////////////////////////////////////////
  // IReservedPages methods
  ///////////////////////////////////////////////////////////////////////////
  uint32_t numberOfReservedPages() const override { return static_cast<uint32_t>(numberOfReservedPages_); }
  uint32_t sizeOfReservedPage() const override { return config_.sizeOfReservedPage; }
  bool loadReservedPage(uint32_t reservedPageId, uint32_t copyLength, char* outReservedPage) const override;
  void saveReservedPage(uint32_t reservedPageId, uint32_t copyLength, const char* inReservedPage) override;
  void zeroReservedPage(uint32_t reservedPageId) override;

  ///////////////////////////////////////////////////////////////////////////
  // Reconfiguration engine methods
  ///////////////////////////////////////////////////////////////////////////
  // TODO - the next function should be removed or refactored into special dedicated class
  void setEraseMetadataFlag() override { setEraseMetadataFlagHandler_(); }
  // TODO - the next function should be removed or refactored into special dedicated class
  void setReconfigurationEngine(std::shared_ptr<ClientReconfigurationEngine> cre) override {
    setReconfigurationEngineHandler_(cre);
  }

 protected:
  ///////////////////////////////////////////////////////////////////////////
  // IStateTransfer Implementation methods
  ///////////////////////////////////////////////////////////////////////////
  void initImpl(uint64_t maxNumOfRequiredStoredCheckpoints,
                uint32_t numberOfRequiredReservedPages,
                uint32_t sizeOfReservedPage);
  void startRunningImpl(IReplicaForStateTransfer* r);
  void stopRunningImpl();
  void createCheckpointOfCurrentStateImpl(uint64_t checkpointNumber);
  void getDigestOfCheckpointImpl(uint64_t checkpointNumber,
                                 uint16_t sizeOfDigestBuffer,
                                 uint64_t& outBlockId,
                                 char* outStateDigest,
                                 char* outResPagesDigest,
                                 char* outRVBDataDigest);
  void startCollectingStateImpl();
  void onTimerImpl();
  void handleStateTransferMessageImpl(char* msg,
                                      uint32_t msgLen,
                                      uint16_t senderI,
                                      LocalTimePoint incomingEventsQPushTime);
  void handleIncomingConsensusMessageImpl(ConsensusMsg msg);
  void getStatusImpl(std::string& statusOut);
  void addOnTransferringCompleteCallbackImpl(const std::function<void(uint64_t)>& cb,
                                             StateTransferCallBacksPriorities priority);
  void reportLastAgreedPrunableBlockIdImpl(uint64_t lastAgreedPrunableBlockId);
  // TODO - this one should be removed - BC-15921
  void addOnFetchingStateChangeCallbackImpl(const std::function<void(uint64_t)>& cb);

  // Reconfiguration engine functions
  // TODO - the next function should be removed or refactored into special dedicated class
  void setEraseMetadataFlagImpl();
  // TODO - the next function should be removed or refactored into special dedicated class
  void setReconfigurationEngineImpl(std::shared_ptr<ClientReconfigurationEngine> cre);

  ///////////////////////////////////////////////////////////////////////////
 public:
  static uint32_t calcMaxItemSize(uint32_t maxBlockSize, uint32_t maxNumberOfPages, uint32_t pageSize);
  static uint32_t calcMaxNumOfChunksInBlock(uint32_t maxItemSize,
                                            uint32_t maxBlockSize,
                                            uint32_t maxChunkSize,
                                            bool isVBlock);
  static set<uint16_t> generateSetOfReplicas(const int16_t numberOfReplicas);

  static void computeDigestOfBlockImpl(const uint64_t blockNum,
                                       const char* block,
                                       const uint32_t blockSize,
                                       char* outDigest);

  ///////////////////////////////////////////////////////////////////////////
  // Interface Handlers
  ///////////////////////////////////////////////////////////////////////////
  // A handler calls an Imp function directly or indirectly, as a function of implementation complexity, thread safety
  // and configuration (e.g runInSeparateThread=true/false).
  // An indirect call is done via the handoff priority queue incomingEventsQ_: blocked calls take priority over
  // non-block calls.

  // Some Interface functions are not implemented using handlers, as they are getter function and protects for thread
  // safety using atomic members.
 protected:
  // Initialization function to initialize all handlers according to runtime configuration.
  void bindInterfaceHandlers();

  // IStateTransfer handlers
  std::function<void()> startCollectingStateHandler_;
  std::function<void(IReplicaForStateTransfer*)> startRunningHandler_;
  std::function<void()> stopRunningHandler_;
  std::function<void(uint64_t)> createCheckpointOfCurrentStateHandler_;
  std::function<void(uint64_t, uint32_t, uint32_t)> initHandler_;
  std::function<void(char*, uint32_t, uint16_t, LocalTimePoint)> handleStateTransferMessageHandler_;
  std::function<void(ConsensusMsg msg)> handleIncomingConsensusMessageHandler_;
  std::function<void()> onTimerHandler_;
  std::function<void(uint64_t, uint16_t, uint64_t&, char*, char*, char*)> getDigestOfCheckpointHandler_;
  std::function<void(std::string&)> getStatusHandler_;
  std::function<void(uint64_t)> reportLastAgreedPrunableBlockIdHandler_;

  // CRE handlers
  // TODO - refactor/move out from ST
  std::function<void()> setEraseMetadataFlagHandler_;
  // TODO - refactor/move out from ST
  std::function<void(std::shared_ptr<ClientReconfigurationEngine>)> setReconfigurationEngineHandler_;
  // TODO - refactor/move out from ST
  std::function<void(const std::function<void(uint64_t)>&)> addOnFetchingStateChangeCallbackHandler_;
  std::function<void(const std::function<void(uint64_t)>&, StateTransferCallBacksPriorities)>
      addOnTransferringCompleteCallbackHandler_;

  logging::Logger& logger_;
  ///////////////////////////////////////////////////////////////////////////
  // Constants
  ///////////////////////////////////////////////////////////////////////////
  static constexpr uint64_t kMaxNumOfStoredCheckpoints = 10;
  static constexpr uint16_t kMaxVBlocksInCache = 28;                    // TBD
  static constexpr uint32_t kResetCount_AskForCheckpointSummaries = 4;  // TBD

  ///////////////////////////////////////////////////////////////////////////
  // External interfaces
  ///////////////////////////////////////////////////////////////////////////

  IAppState* const as_;
  std::shared_ptr<DataStore> psd_;

  ///////////////////////////////////////////////////////////////////////////
  // Event queues and handlers
  ///////////////////////////////////////////////////////////////////////////

  // Incoming Events queue - ST main thread is a consumer of timeouts and messages arriving from an external context
  std::unique_ptr<concord::util::Handoff> incomingEventsQ_;

  // Post processing Queue - ST main thread is a producer and post processing thread is a consumer
  std::unique_ptr<concord::util::Handoff> postProcessingQ_;
  uint64_t postProcessingUpperBoundBlockId_;
  std::atomic<uint64_t> maxPostprocessedBlockId_;
  void postProcessNextBatch(uint64_t upperBoundBlockId);
  void triggerPostProcessing();

  ///////////////////////////////////////////////////////////////////////////
  // Management and general data
  ///////////////////////////////////////////////////////////////////////////
  const Config config_;
  const set<uint16_t> replicas_;
  const uint32_t maxVBlockSize_;
  const uint32_t maxItemSize_;
  const uint32_t maxNumOfChunksInAppBlock_;
  const uint32_t maxNumOfChunksInVBlock_;

  uint64_t maxNumOfStoredCheckpoints_;
  std::atomic_uint64_t numberOfReservedPages_;
  uint32_t cycleCounter_;

  std::atomic<bool> running_;
  IReplicaForStateTransfer* replicaForStateTransfer_;

  std::unique_ptr<char[]> buffer_;  // general use buffer

  // random generator
  std::random_device randomDevice_;
  std::mt19937 randomGen_;

  ///////////////////////////////////////////////////////////////////////////
  // Unique message IDs
  ///////////////////////////////////////////////////////////////////////////

  uint64_t uniqueMsgSeqNum();
  bool checkValidityAndSaveMsgSeqNum(uint16_t replicaId, uint64_t msgSeqNum);

  // used to computed my last msg sequence number
  uint64_t lastMilliOfUniqueFetchID_ = 0;
  uint32_t lastCountOfUniqueFetchID_ = 0;
  uint64_t lastMsgSeqNum_ = 0;

  // msg sequence number from other replicas
  // map from replica id to its last MsgSeqNum
  map<uint16_t, uint64_t> lastMsgSeqNumOfReplicas_;

  ///////////////////////////////////////////////////////////////////////////
  // State
  ///////////////////////////////////////////////////////////////////////////
 public:
  enum class FetchingState {
    // Common
    NotFetching,

    // Destination
    GettingCheckpointSummaries,
    GettingMissingBlocks,
    GettingMissingResPages,
    FinalizingCycle,

    // Source
    SendingBatch,
  };

 protected:
  friend std::ostream& operator<<(std::ostream& os, const BCStateTran::FetchingState fs);
  static string stateName(FetchingState fs);
  static inline bool isActiveDestination(FetchingState fs);
  static inline bool isActiveSource(FetchingState fs);

  // TODO - should be renamed to evaluateFetchingState
  FetchingState getFetchingState();

  ///////////////////////////////////////////////////////////////////////////
  // Send messages
  ///////////////////////////////////////////////////////////////////////////
 protected:
  void sendToAllOtherReplicas(char* msg, uint32_t msgSize);

  void sendAskForCheckpointSummariesMsg();

  void trySendFetchBlocksMsg(int16_t lastKnownChunkInLastRequiredBlock, string&& reason);

  void sendFetchResPagesMsg(int16_t lastKnownChunkInLastRequiredBlock);

  ///////////////////////////////////////////////////////////////////////////
  // Message handlers
  ///////////////////////////////////////////////////////////////////////////

  inline std::string getScopedMdcStr(uint16_t replicaId, uint64_t seqNum, uint16_t = 0, uint64_t = 0);
  bool onMessage(const AskForCheckpointSummariesMsg* m, uint32_t msgLen, uint16_t replicaId);
  bool onMessage(const CheckpointSummaryMsg* m, uint32_t msgLen, uint16_t replicaId);
  bool onMessage(const FetchBlocksMsg* m, uint32_t msgLen, uint16_t replicaId);
  bool onMessage(const FetchResPagesMsg* m, uint32_t msgLen, uint16_t replicaId);
  bool onMessage(const RejectFetchingMsg* m, uint32_t msgLen, uint16_t replicaId);
  bool onMessage(const ItemDataMsg* m, uint32_t msgLen, uint16_t replicaId, LocalTimePoint incomingEventsQPushTime);

  ///////////////////////////////////////////////////////////////////////////
  // cache that holds virtual blocks
  ///////////////////////////////////////////////////////////////////////////

  struct DescOfVBlockForResPages {
    uint64_t checkpointNum;
    uint64_t lastCheckpointKnownToRequester;

    // TOOD(GG): TBD
    bool operator<(const DescOfVBlockForResPages& rhs) const {
      if (checkpointNum != rhs.checkpointNum)
        return (checkpointNum < rhs.checkpointNum);
      else
        return (lastCheckpointKnownToRequester < rhs.lastCheckpointKnownToRequester);
    }
  };

  // map from DescOfVBlockForResPages to the virtual block
  map<DescOfVBlockForResPages, char*> cacheOfVirtualBlockForResPages;

  char* getVBlockFromCache(const DescOfVBlockForResPages& desc) const;
  void setVBlockInCache(const DescOfVBlockForResPages& desc, char* vBlock);
  char* createVBlock(const DescOfVBlockForResPages& desc);

  ///////////////////////////////////////////////////////////////////////////
  // The following is only used when the state is
  // FetchingState::GettingCheckpointSummaries
  ///////////////////////////////////////////////////////////////////////////

  uint64_t lastTimeSentAskForCheckpointSummariesMsg = 0;
  uint16_t retransmissionNumberOfAskForCheckpointSummariesMsg = 0;

  typedef MsgsCertificate<CheckpointSummaryMsg, false, false, true, CheckpointSummaryMsg> CheckpointSummaryMsgCert;

  // map from checkpointNum to CheckpointSummaryMsgCert
  map<uint64_t, CheckpointSummaryMsgCert*> summariesCerts;

  // map from replica Id to number of accepted CheckpointSummaryMsg messages
  map<uint16_t, uint16_t> numOfSummariesFromOtherReplicas;

  void clearInfoAboutGettingCheckpointSummary();

  void verifyEmptyInfoAboutGettingCheckpointSummary();

  ///////////////////////////////////////////////////////////////////////////
  // The following is only used when the state is GettingMissingBlocks
  // or GettingMissingResPages
  ///////////////////////////////////////////////////////////////////////////

  SourceSelector sourceSelector_;

  static const uint64_t ID_OF_VBLOCK_RES_PAGES = UINT64_MAX;

  struct BlocksBatchDesc {
    uint64_t minBlockId = 0;
    uint64_t maxBlockId = 0;
    uint64_t nextBlockId = 0;
    uint64_t upperBoundBlockId = 0;  // Dynamic upper limit to the next batch

    bool operator==(const BlocksBatchDesc& rhs) const;
    bool operator!=(const BlocksBatchDesc& rhs) const { return !(this->operator==(rhs)); }
    bool operator<=(const BlocksBatchDesc& rhs) const { return (*this < rhs) || (*this == rhs); }
    bool operator<(const BlocksBatchDesc& rhs) const;

    void reset();
    bool isValid() const;
    bool isMinBlockId(uint64_t blockId) const { return blockId == minBlockId; };
    bool isMaxBlockId(uint64_t blockId) const { return blockId == maxBlockId; };
    std::string toString() const;
  };
  friend std::ostream& operator<<(std::ostream&, const BCStateTran::BlocksBatchDesc&);

  BlocksBatchDesc fetchState_;
  BlocksBatchDesc commitState_;

  DataStore::CheckpointDesc targetCheckpointDesc_;
  Digest digestOfNextRequiredBlock_;
  bool postponedSendFetchBlocksMsg_;

  inline bool isMinBlockIdInFetchRange(uint64_t blockId) const;
  inline bool isMaxBlockIdInFetchRange(uint64_t blockId) const;
  inline bool isLastFetchedBlockIdInCycle(uint64_t blockId) const;
  inline bool isMaxFetchedBlockIdInCycle(uint64_t blockId) const;
  inline bool isRvbBlockId(uint64_t blockId) const;
  inline uint64_t prevRvbBlockId(uint64_t block_id) const;
  inline uint64_t nextRvbBlockId(uint64_t block_id) const;

  struct compareItemDataMsg {
    bool operator()(const ItemDataMsg* l, const ItemDataMsg* r) const {
      if (l->blockNumber != r->blockNumber)
        return (l->blockNumber > r->blockNumber);
      else
        return (l->chunkNumber < r->chunkNumber);
    }
  };

  set<ItemDataMsg*, compareItemDataMsg> pendingItemDataMsgs;
  uint32_t totalSizeOfPendingItemDataMsgs = 0;

  void stReset(DataStoreTransaction* txn,
               bool resetRvbm = false,
               bool resetStoredCp = false,
               bool resetDataStore = false);
  void clearAllPendingItemsData();
  void clearPendingItemsData(uint64_t fromBlock, uint64_t untilBlock);
  bool getNextFullBlock(uint64_t requiredBlock,
                        bool& outBadDataDetected,
                        int16_t& outLastChunkInRequiredBlock,
                        char* outBlock,
                        uint32_t& outBlockSize,
                        bool isVBLock);

  // enter a new cycle internally
  void startCollectingStateInternal();

  BlocksBatchDesc computeNextBatchToFetch(uint64_t minRequiredBlockId);
  bool checkBlock(uint64_t blockNum, char* block, uint32_t blockSize) const;

  bool checkVirtualBlockOfResPages(const Digest& expectedDigestOfResPagesDescriptor,
                                   char* vblock,
                                   uint32_t vblockSize) const;

  void processData(bool lastInBatch = false, uint32_t rvbDigestsSize = 0);
  void cycleEndSummary();
  void onGettingMissingBlocksEnd(DataStoreTransaction* txn);
  set<uint16_t> allOtherReplicas();
  void setAllReplicasAsPreferred();

  ///////////////////////////////////////////////////////////////////////////
  // Helper methods
  ///////////////////////////////////////////////////////////////////////////

  DataStore::CheckpointDesc createCheckpointDesc(uint64_t checkpointNumber, const Digest& digestOfResPagesDescriptor);
  Digest checkpointReservedPages(uint64_t checkpointNumber, DataStoreTransaction* txn);
  void deleteOldCheckpoints(uint64_t checkpointNumber, DataStoreTransaction* txn);
  const Digest& computeDefaultRvbDataDigest() const;

  ///////////////////////////////////////////////////////////////////////////
  // Consistency
  ///////////////////////////////////////////////////////////////////////////

  void checkConsistency(bool checkAllBlocks, bool duringInit = false);
  void checkConfig();
  void checkFirstAndLastCheckpoint(uint64_t firstStoredCheckpoint, uint64_t lastStoredCheckpoint);
  void checkReachableBlocks(uint64_t genesisBlockNum, uint64_t lastReachableBlockNum);
  void checkUnreachableBlocks(uint64_t lastReachableBlockNum, uint64_t lastBlockNum, bool duringInit);
  void checkBlocksBeingFetchedNow(bool checkAllBlocks, uint64_t lastReachableBlockNum, uint64_t lastBlockNum);
  void checkStoredCheckpoints(uint64_t firstStoredCheckpoint, uint64_t lastStoredCheckpoint);

 public:
  ///////////////////////////////////////////////////////////////////////////
  // Compute digests
  ///////////////////////////////////////////////////////////////////////////

  static void computeDigestOfPage(
      const uint32_t pageId, const uint64_t checkpointNumber, const char* page, uint32_t pageSize, Digest& outDigest);

  static void computeDigestOfPagesDescriptor(const DataStore::ResPagesDescriptor* pagesDesc, Digest& outDigest);

  static void computeDigestOfBlock(const uint64_t blockNum,
                                   const char* block,
                                   const uint32_t blockSize,
                                   Digest* outDigest);

  static BlockDigest computeDigestOfBlock(const uint64_t blockNum, const char* block, const uint32_t blockSize);

 protected:
  // A wrapper function to get a block from the IAppState and compute its digest.
  Digest getBlockAndComputeDigest(uint64_t currBlock);

  ///////////////////////////////////////////////////////////////////////////
  // Asynchronous Operations - Blocks IO
  ///////////////////////////////////////////////////////////////////////////

  struct BlockIOContext {
    static size_t sizeOfBlockData;
    BlockIOContext() {
      ConcordAssert(sizeOfBlockData != 0);
      blockData.reset(new char[sizeOfBlockData]);
    }
    uint64_t blockId = 0;
    uint32_t actualBlockSize = 0;
    std::unique_ptr<char[]> blockData;
    std::future<bool> future;
  };

  using BlockIOContextPtr = std::shared_ptr<BlockIOContext>;
  // Must be less than config_.refreshTimerMs
  static constexpr uint32_t finalizePutblockTimeoutMilli_ = 5;
  concord::util::SimpleMemoryPool<BlockIOContext> ioPool_;
  std::deque<BlockIOContextPtr> ioContexts_;
  // used to control the trigger of oneShotTimer self requests
  bool oneShotTimerFlag_;

  // Trigger onTimer in timeoutMilli. Only a single trigger is allowed at any time. In general, we shouldn't use this
  // call for any timeout greater than config_.refreshTimerMs.
  void addOneShotTimer(uint32_t timeoutMilli, std::string&& reason = "");

  // Fetch (Async) numBlocks from storage, starting from block maxBlockId, and not crossing minBlockId.
  // Returns number of jobs pushed to queue
  uint16_t getBlocksConcurrentAsync(uint64_t maxBlockId, uint64_t minBlockId, uint16_t numBlocks);
  void sourcePrepareBatch(uint64_t numBlocksRequested);
  void clearIoContexts();

  // lastBlock: is true if we put the oldest block (firstRequiredBlock)
  //
  // waitPolicy:
  // NO_WAIT: if caller would like to exit immediately if the next future is not ready
  // (job not ended yet).
  // WAIT_SINGLE_JOB: if caller would like to wait for a single job to finish and exit immediately if the next job is
  // not ready. WAIT_ALL_JOBS - wait for all jobs to finalize.
  //
  // In any case of an early exit (before all jobs are finalized), a ONESHOT timer is invoked to check the future again
  // soon. return: true if done procesing all futures, and false if the front one was not std::future_status::ready
  enum class PutBlockWaitPolicy { NO_WAIT, WAIT_SINGLE_JOB, WAIT_ALL_JOBS };

  bool finalizePutblockAsync(PutBlockWaitPolicy waitPolicy, DataStoreTransaction* txn);
  //////////////////////////////////////////////////////////////////////////
  // Range Validation
  //////////////////////////////////////////////////////////////////////////
  struct rvbm_deleter {
    void operator()(RVBManager*) const;
  };
  std::unique_ptr<RVBManager, rvbm_deleter> rvbm_;

  //////////////////////////////////////////////////////////////////////////
  // Metrics
  ///////////////////////////////////////////////////////////////////////////
 public:
  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator);

 protected:
  void loadMetrics();
  std::chrono::seconds metrics_dump_interval_in_sec_;
  concordMetrics::Component metrics_component_;
  struct Metrics {
    StatusHandle fetching_state_;

    GaugeHandle is_fetching_;
    GaugeHandle checkpoint_being_fetched_;
    GaugeHandle last_stored_checkpoint_;
    GaugeHandle number_of_reserved_pages_;
    GaugeHandle size_of_reserved_page_;
    GaugeHandle last_msg_seq_num_;
    GaugeHandle next_required_block_;
    GaugeHandle next_commited_block_id_;
    GaugeHandle num_pending_item_data_msgs_;
    GaugeHandle total_size_of_pending_item_data_msgs_;
    AtomicGaugeHandle last_block_;
    GaugeHandle last_reachable_block_;

    CounterHandle sent_ask_for_checkpoint_summaries_msg_;
    CounterHandle sent_checkpoint_summary_msg_;

    CounterHandle sent_fetch_blocks_msg_;
    CounterHandle sent_fetch_res_pages_msg_;
    CounterHandle sent_reject_fetch_msg_;
    CounterHandle sent_item_data_msg_;

    CounterHandle received_ask_for_checkpoint_summaries_msg_;
    CounterHandle received_checkpoint_summary_msg_;
    CounterHandle received_fetch_blocks_msg_;
    CounterHandle received_fetch_res_pages_msg_;
    CounterHandle received_reject_fetching_msg_;
    CounterHandle received_item_data_msg_;
    CounterHandle received_illegal_msg_;

    CounterHandle invalid_ask_for_checkpoint_summaries_msg_;
    CounterHandle irrelevant_ask_for_checkpoint_summaries_msg_;
    CounterHandle invalid_checkpoint_summary_msg_;
    CounterHandle irrelevant_checkpoint_summary_msg_;
    CounterHandle invalid_fetch_blocks_msg_;
    CounterHandle irrelevant_fetch_blocks_msg_;
    CounterHandle invalid_fetch_res_pages_msg_;
    CounterHandle irrelevant_fetch_res_pages_msg_;
    CounterHandle invalid_reject_fetching_msg_;
    CounterHandle irrelevant_reject_fetching_msg_;
    CounterHandle invalid_item_data_msg_;
    CounterHandle irrelevant_item_data_msg_;

    AtomicCounterHandle create_checkpoint_;
    CounterHandle mark_checkpoint_as_stable_;
    AtomicCounterHandle load_reserved_page_;
    AtomicCounterHandle load_reserved_page_from_pending_;
    AtomicCounterHandle load_reserved_page_from_checkpoint_;
    AtomicCounterHandle save_reserved_page_;
    CounterHandle zero_reserved_page_;
    CounterHandle start_collecting_state_;
    CounterHandle on_timer_;
    CounterHandle one_shot_timer_;

    CounterHandle on_transferring_complete_;
    CounterHandle internal_cycle_counter;

    CounterHandle handle_AskForCheckpointSummaries_msg_;
    CounterHandle handle_CheckpointsSummary_msg_;
    CounterHandle handle_FetchBlocks_msg_;
    CounterHandle handle_FetchResPages_msg_;
    CounterHandle handle_RejectFetching_msg_;
    CounterHandle handle_ItemData_msg_;

    GaugeHandle overall_blocks_collected_;
    GaugeHandle overall_blocks_throughput_;
    GaugeHandle overall_bytes_collected_;
    GaugeHandle overall_bytes_throughput_;
    GaugeHandle prev_win_blocks_collected_;
    GaugeHandle prev_win_blocks_throughput_;
    GaugeHandle prev_win_bytes_collected_;
    GaugeHandle prev_win_bytes_throughput_;

    // TODO - consider moving into RVB Manager + add more metrics as needed.
    CounterHandle overall_rvb_digests_validated_;
    CounterHandle overall_rvb_digest_groups_validated_;
    CounterHandle overall_rvb_digests_validation_failed_;
    CounterHandle overall_rvb_digest_groups_validation_failed_;
    StatusHandle current_rvb_data_state_;

    CounterHandle src_overall_batches_sent_;
    CounterHandle src_overall_prefetched_batches_sent_;
    CounterHandle src_overall_on_spot_batches_sent_;

    GaugeHandle src_num_io_contexts_dropped_;
    GaugeHandle src_num_io_contexts_invoked_;
    CounterHandle src_num_io_contexts_consumed_;
  };
  mutable Metrics metrics_;
  Metrics createRegisterMetrics();

  void finalizeCycle();
  static constexpr uint32_t onTransferringCompleteTimeoutMilli_ = 10;
  bool on_transferring_complete_ongoing_;
  std::future<void> on_transferring_complete_future_;
  std::map<uint64_t, concord::util::CallbackRegistry<uint64_t>> on_transferring_complete_cb_registry_;
  // TODO - on_fetching_state_change_cb_registry_ should be removed
  // All callbacks should be integrated as a callback into on_transferring_complete_cb_registry_.
  concord::util::CallbackRegistry<uint64_t> on_fetching_state_change_cb_registry_;

 protected:
  //////////////////////////////////////////////////////////////////////////////
  // Virtual Blocks that are used to pass the reserved pages
  // (private to the file)
  //////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 1)
  struct HeaderOfVirtualBlock {
    uint32_t numberOfUpdatedPages;
    uint64_t lastCheckpointKnownToRequester;
  };

  struct ElementOfVirtualBlock {
    uint32_t pageId;
    uint64_t checkpointNumber;
    Digest pageDigest;
    char page[1];  // the actual size is sizeOfReservedPage_ bytes
  };
#pragma pack(pop)

  static uint32_t calcMaxVBlockSize(uint32_t maxNumberOfPages, uint32_t pageSize);
  static uint32_t getNumberOfElements(char* virtualBlock);
  static uint32_t getSizeOfVirtualBlock(char* virtualBlock, uint32_t pageSize);
  static ElementOfVirtualBlock* getVirtualElement(uint32_t index, uint32_t pageSize, char* virtualBlock);
  static bool checkStructureOfVirtualBlock(char* virtualBlock,
                                           uint32_t virtualBlockSize,
                                           uint32_t pageSize,
                                           logging::Logger& logger);

  ///////////////////////////////////////////////////////////////////////////
  // Internal Statistics (debuging, logging)
  ///////////////////////////////////////////////////////////////////////////
 protected:
  // Three stages : Fetch / Commit / Post-Process
  // Commit is less interesting for statistic, we track only 1st and 3rd
  Throughput blocksFetched_;
  Throughput bytesFetched_;
  Throughput blocksPostProcessed_;
  static constexpr size_t blocksPostProcessedReportWindow = 3;

  uint64_t minBlockIdToCollectInCycle_;
  uint64_t maxBlockIdToCollectInCycle_;
  uint64_t totalBlocksLeftToCollectInCycle_;
  mutable uint64_t totalRvbsValidatedInCycle_;

  DurationTracker<std::chrono::milliseconds> cycleDT_;
  DurationTracker<std::chrono::milliseconds> postProcessingDT_;
  DurationTracker<std::chrono::milliseconds> gettingCheckpointSummariesDT_;
  DurationTracker<std::chrono::milliseconds> gettingMissingBlocksDT_;
  DurationTracker<std::chrono::milliseconds> gettingMissingResPagesDT_;

  FetchingState lastFetchingState_;

  void onFetchingStateChange(FetchingState newFetchingState);

  // used to print periodic summary of recent checkpoints, and collected date while in state GettingMissingBlocks
  std::string logsForCollectingStatus();
  void reportCollectingStatus(const uint32_t actualBlockSize, bool toLog = false);
  void startCollectingStats();
  std::string convertUInt64ToReadableStr(uint64_t num, std::string&& trailer = "") const;
  std::string convertMillisecToReadableStr(uint64_t ms) const;

  ///////////////////////////////////////////////////////////////////////////
  // Source session/batch management
  ///////////////////////////////////////////////////////////////////////////

  // Currently, source supports a single session at any given time
  struct SourceSession {
   public:
    SourceSession(logging::Logger& logger, uint64_t sourceSessionExpiryDurationMs)
        : logger_(logger), expiryDurationMs_{sourceSessionExpiryDurationMs}, replicaId_{0}, startTime_{0} {}
    SourceSession() = delete;
    void close();
    // returns a pair of booleans:
    // First bool: true if session was already openned by replicaId, or if a new one was opened
    // Second bool: true if another session was closed. Relevant only if the first value is true.
    // It might be that session was already open, or opened during the call.
    std::pair<bool, bool> tryOpen(uint16_t replicaId);
    // returns true if session exist
    bool isOpen() const { return startTime_ != 0; }
    uint16_t ownerDestReplicaId() const { return replicaId_; };
    uint64_t activeDuration() const { return getMonotonicTimeMilli() - startTime_; }
    void refresh(uint64_t startTime = 0);
    // A session can be expired only if it's open, when expiryDurationMs_ - session always expire.
    bool expired() const { return isOpen() && ((expiryDurationMs_ == 0) || (activeDuration() > expiryDurationMs_)); }

   protected:
    void open(uint16_t replicaId);

    logging::Logger& logger_;
    const uint64_t expiryDurationMs_;
    uint16_t replicaId_;
    uint64_t startTime_;
  };

  struct SourceBatch {
    SourceBatch()
        : active{false},
          batchNumber{0},
          numSentBytes{0},
          numSentChunks{0},
          nextBlockId{0},
          nextChunk{0},
          preFetchBlockId{0},
          getNextBlock{false},
          rvbGroupDigestsExpectedSize{0},
          destReplicaId{0},
          prefetched{false} {}
    std::string toString() const;
    void init(uint64_t batchNumber,
              uint64_t maxBlockId,
              uint64_t nextChunk,
              uint64_t maxBlockIdInCycle,
              bool getNextBlock,
              const Config& config,
              size_t rvbGroupDigestsExpectedSize,
              const FetchBlocksMsg* msg,
              uint16_t destReplicaId);

    bool active;
    uint64_t batchNumber;
    uint64_t numSentBytes;
    uint64_t numSentChunks;
    uint64_t nextBlockId;
    uint16_t nextChunk;
    uint64_t preFetchBlockId;
    bool getNextBlock;
    size_t rvbGroupDigestsExpectedSize;
    FetchBlocksMsg destRequest;
    uint16_t destReplicaId;
    bool prefetched;  // true if this batch succeed with pre-fetch prediction
  };

  SourceBatch sourceBatch_;
  SourceSession sourceSession_;

  friend std::ostream& operator<<(std::ostream& os, const BCStateTran::SourceBatch& batch);
  void continueSendBatch();
  void sendRejectFetchingMsg(const uint16_t rejectionCode,
                             uint64_t msgSeqNum,
                             uint16_t destReplicaId,
                             std::string_view additionalInfo = "");
  ///////////////////////////////////////////////////////////////////////////
  // Latency Historgrams (snapshots)
  ///////////////////////////////////////////////////////////////////////////
 protected:
  struct Recorders {
    static constexpr uint64_t MAX_VALUE_MICROSECONDS = 60ULL * 1000ULL * 1000ULL;          // 60 seconds
    static constexpr uint64_t MAX_BLOCK_SIZE = 100ULL * 1024ULL * 1024ULL;                 // 100MB
    static constexpr uint64_t MAX_BATCH_SIZE_BYTES = 10ULL * 1024ULL * 1024ULL * 1024ULL;  // 10GB
    static constexpr uint64_t MAX_BATCH_SIZE_BLOCKS = 1000ULL;
    static constexpr uint64_t MAX_INCOMING_EVENTS_QUEUE_SIZE = 10000ULL;
    static constexpr uint64_t MAX_PENDING_BLOCKS_SIZE = 1000ULL;

    Recorders() {
      auto& registrar = concord::diagnostics::RegistrarSingleton::getInstance();
      // common component
      registrar.perf.registerComponent("state_transfer",
                                       {on_timer,
                                        time_in_incoming_events_queue,
                                        incoming_events_queue_size,
                                        compute_block_digest_duration,
                                        compute_block_digest_size,
                                        time_to_clear_io_contexts});
      // destination component
      registrar.perf.registerComponent("state_transfer_dest",
                                       {dst_handle_ItemData_msg,
                                        dst_time_between_sendFetchBlocksMsg,
                                        dst_num_pending_blocks_to_commit,
                                        dst_digest_calc_duration,
                                        dst_time_ItemData_msg_in_incoming_events_queue,
                                        time_in_post_processing_events_queue});
      // source component
      registrar.perf.registerComponent("state_transfer_src",
                                       {src_handle_FetchBlocks_msg_duration,
                                        src_handle_FetchResPages_msg_duration,
                                        src_get_block_size_bytes,
                                        src_send_batch_duration,
                                        src_send_prefetched_batch_duration,
                                        src_send_on_spot_batch_duration,
                                        src_send_batch_size_bytes,
                                        src_send_batch_num_of_chunks,
                                        src_next_block_wait_duration});
    }
    ~Recorders() {
      auto& registrar = concord::diagnostics::RegistrarSingleton::getInstance();
      registrar.perf.unRegisterComponent("state_transfer");
      registrar.perf.unRegisterComponent("state_transfer_dest");
      registrar.perf.unRegisterComponent("state_transfer_src");
    }
    //////////////////////////////////////////////////////////
    // Shared Recorders - match the above registered recorders
    //////////////////////////////////////////////////////////
    // common
    DEFINE_SHARED_RECORDER(on_timer, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(
        time_in_incoming_events_queue, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(
        incoming_events_queue_size, 1, MAX_INCOMING_EVENTS_QUEUE_SIZE, 3, concord::diagnostics::Unit::COUNT);
    DEFINE_SHARED_RECORDER(
        compute_block_digest_duration, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(compute_block_digest_size, 1, MAX_BLOCK_SIZE, 3, concord::diagnostics::Unit::COUNT);
    DEFINE_SHARED_RECORDER(
        time_to_clear_io_contexts, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    // destination
    DEFINE_SHARED_RECORDER(
        dst_handle_ItemData_msg, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(
        dst_time_between_sendFetchBlocksMsg, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(
        dst_num_pending_blocks_to_commit, 1, MAX_PENDING_BLOCKS_SIZE, 3, concord::diagnostics::Unit::COUNT);
    DEFINE_SHARED_RECORDER(
        dst_digest_calc_duration, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(dst_time_ItemData_msg_in_incoming_events_queue,
                           1,
                           MAX_VALUE_MICROSECONDS,
                           3,
                           concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(
        time_in_post_processing_events_queue, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    // source
    DEFINE_SHARED_RECORDER(
        src_handle_FetchBlocks_msg_duration, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(
        src_handle_FetchResPages_msg_duration, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(src_get_block_size_bytes, 1, MAX_BLOCK_SIZE, 3, concord::diagnostics::Unit::BYTES);
    DEFINE_SHARED_RECORDER(
        src_send_batch_duration, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(
        src_send_prefetched_batch_duration, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(
        src_send_on_spot_batch_duration, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(src_send_batch_size_bytes, 1, MAX_BATCH_SIZE_BYTES, 3, concord::diagnostics::Unit::BYTES);
    DEFINE_SHARED_RECORDER(
        src_send_batch_num_of_chunks, 1, MAX_BATCH_SIZE_BLOCKS, 3, concord::diagnostics::Unit::COUNT);
    DEFINE_SHARED_RECORDER(
        src_next_block_wait_duration, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
  };
  Recorders histograms_;

  // Async time recorders - wrap the above shared recorders with the same name and prefix _rec_
  AsyncTimeRecorder<false> src_send_batch_duration_rec_;
  AsyncTimeRecorder<false> src_send_prefetched_batch_duration_rec_;
  AsyncTimeRecorder<false> src_send_on_spot_batch_duration_rec_;
  AsyncTimeRecorder<false> dst_time_between_sendFetchBlocksMsg_rec_;
  AsyncTimeRecorder<false> time_in_incoming_events_queue_rec_;
  AsyncTimeRecorder<true> time_in_post_processing_events_queue_rec_;
  AsyncTimeRecorder<false> src_next_block_wait_duration_rec_;

  // TODO - This member do not belong here (CRE) - move outside
  std::shared_ptr<ClientReconfigurationEngine> cre_;
};  // class BCStateTran

}  // namespace bftEngine::bcst::impl
