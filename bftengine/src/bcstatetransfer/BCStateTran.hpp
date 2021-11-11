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
#include "STDigest.hpp"
#include "Metrics.hpp"
#include "SourceSelector.hpp"
#include "callback_registry.hpp"
#include "Handoff.hpp"
#include "SysConsts.hpp"
#include "throughput.hpp"
#include "diagnostics.h"
#include "performance_handler.h"
#include "Timers.hpp"
#include "SimpleMemoryPool.hpp"

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

namespace bftEngine::bcst::impl {

class BCStateTran : public IStateTransfer {
  // The next friend declerations are used strictly for testing
  friend class BcStTest;
  friend class MockedSources;

 public:
  //////////////////////////////////////////////////////////////////////////////
  // Ctor & Dtor (Initialization)
  //////////////////////////////////////////////////////////////////////////////
  BCStateTran(const Config& config, IAppState* const stateApi, DataStore* ds = nullptr);
  ~BCStateTran() override;
  static uint32_t calcMaxItemSize(uint32_t maxBlockSize, uint32_t maxNumberOfPages, uint32_t pageSize);
  static uint32_t calcMaxNumOfChunksInBlock(uint32_t maxItemSize,
                                            uint32_t maxBlockSize,
                                            uint32_t maxChunkSize,
                                            bool isVBlock);
  static set<uint16_t> generateSetOfReplicas(const int16_t numberOfReplicas);

  ///////////////////////////////////////////////////////////////////////////
  // IStateTransfer methods
  ///////////////////////////////////////////////////////////////////////////

  void init(uint64_t maxNumOfRequiredStoredCheckpoints,
            uint32_t numberOfRequiredReservedPages,
            uint32_t sizeOfReservedPage) override;
  void startRunning(IReplicaForStateTransfer* r) override;
  void stopRunning() override;
  bool isRunning() const override;

  void createCheckpointOfCurrentState(uint64_t checkpointNumber) override;

  void markCheckpointAsStable(uint64_t checkpointNumber) override;

  void getDigestOfCheckpoint(uint64_t checkpointNumber,
                             uint16_t sizeOfDigestBuffer,
                             uint64_t& outBlockId,
                             char* outStateDigest,
                             char* outFullStateDigest) override;

  static void computeDigestOfBlockImpl(const uint64_t blockNum,
                                       const char* block,
                                       const uint32_t blockSize,
                                       char* outDigest);
  void startCollectingState() override;

  bool isCollectingState() const override;

  uint32_t numberOfReservedPages() const override;

  uint32_t sizeOfReservedPage() const override;

  bool loadReservedPage(uint32_t reservedPageId, uint32_t copyLength, char* outReservedPage) const override;

  void saveReservedPage(uint32_t reservedPageId, uint32_t copyLength, const char* inReservedPage) override;
  void zeroReservedPage(uint32_t reservedPageId) override;

  void onTimer() override { timerHandler_(); };

  using LocalTimePoint = std::chrono::time_point<std::chrono::steady_clock>;
  static constexpr auto UNDEFINED_LOCAL_TIME_POINT = LocalTimePoint::max();
  void handleStateTransferMessage(char* msg, uint32_t msgLen, uint16_t senderId) override {
    messageHandler_(msg, msgLen, senderId, UNDEFINED_LOCAL_TIME_POINT);
  };

  std::string getStatus() override;

  void addOnTransferringCompleteCallback(
      std::function<void(uint64_t)>,
      StateTransferCallBacksPriorities priority = StateTransferCallBacksPriorities::DEFAULT) override;

  void addOnFetchingStateChangeCallback(std::function<void(uint64_t)>) override;

  void setEraseMetadataFlag() override { psd_->setEraseDataStoreFlag(); }
  void setReconfigurationEngine(
      std::shared_ptr<concord::client::reconfiguration::ClientReconfigurationEngine> cre) override {
    cre_ = cre;
  }

  std::shared_ptr<concord::client::reconfiguration::ClientReconfigurationEngine> getReconfigurationEngine() override {
    return cre_;
  }

 protected:
  // handling messages from other context
  std::function<void(char*, uint32_t, uint16_t, LocalTimePoint)> messageHandler_;
  void handleStateTransferMessageImp(char* msg,
                                     uint32_t msgLen,
                                     uint16_t senderId,
                                     LocalTimePoint msgArrivalTime = UNDEFINED_LOCAL_TIME_POINT);
  void handoffMsg(char* msg, uint32_t msgLen, uint16_t senderId) {
    handoff_->push(std::bind(
        &BCStateTran::handleStateTransferMessageImp, this, msg, msgLen, senderId, std::chrono::steady_clock::now()));
  }

  // handling timer from other context
  std::function<void()> timerHandler_;
  void onTimerImp();
  void handoffTimer() { handoff_->push(std::bind(&BCStateTran::onTimerImp, this)); }

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
  // Management and general data
  ///////////////////////////////////////////////////////////////////////////
  const Config config_;
  const set<uint16_t> replicas_;
  const uint32_t maxVBlockSize_;
  const uint32_t maxItemSize_;
  const uint32_t maxNumOfChunksInAppBlock_;
  const uint32_t maxNumOfChunksInVBlock_;

  uint64_t maxNumOfStoredCheckpoints_;
  uint64_t numberOfReservedPages_;
  uint32_t cycleCounter_;

  std::atomic<bool> running_ = false;
  std::unique_ptr<concord::util::Handoff> handoff_;
  IReplicaForStateTransfer* replicaForStateTransfer_ = nullptr;

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

  // my last msg sequence number
  uint64_t lastMsgSeqNum_ = 0;

  // msg sequence number from other replicas
  // map from replica id to its last MsgSeqNum
  map<uint16_t, uint64_t> lastMsgSeqNumOfReplicas_;

  ///////////////////////////////////////////////////////////////////////////
  // State
  ///////////////////////////////////////////////////////////////////////////

  // Public for testing and status
 public:
  enum class FetchingState { NotFetching, GettingCheckpointSummaries, GettingMissingBlocks, GettingMissingResPages };

  static string stateName(FetchingState fs);

  FetchingState getFetchingState();
  bool isFetching() const;

  inline std::string getSequenceNumber(uint16_t replicaId, uint64_t seqNum, uint16_t = 0, uint64_t = 0);

  ///////////////////////////////////////////////////////////////////////////
  // Time
  ///////////////////////////////////////////////////////////////////////////
  static uint64_t getMonotonicTimeMilli();

  ///////////////////////////////////////////////////////////////////////////
  // Send messages
  ///////////////////////////////////////////////////////////////////////////
 protected:
  void sendToAllOtherReplicas(char* msg, uint32_t msgSize);

  void sendAskForCheckpointSummariesMsg();

  void trySendFetchBlocksMsg(uint64_t firstRequiredBlock,
                             uint64_t lastRequiredBlock,
                             int16_t lastKnownChunkInLastRequiredBlock,
                             string&& reason);

  void sendFetchResPagesMsg(int16_t lastKnownChunkInLastRequiredBlock);

  ///////////////////////////////////////////////////////////////////////////
  // Message handlers
  ///////////////////////////////////////////////////////////////////////////

  bool onMessage(const AskForCheckpointSummariesMsg* m, uint32_t msgLen, uint16_t replicaId);
  bool onMessage(const CheckpointSummaryMsg* m, uint32_t msgLen, uint16_t replicaId);
  bool onMessage(const FetchBlocksMsg* m, uint32_t msgLen, uint16_t replicaId);
  bool onMessage(const FetchResPagesMsg* m, uint32_t msgLen, uint16_t replicaId);
  bool onMessage(const RejectFetchingMsg* m, uint32_t msgLen, uint16_t replicaId);
  bool onMessage(const ItemDataMsg* m, uint32_t msgLen, uint16_t replicaId, LocalTimePoint msgArrivalTime);

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

  // map from checkpintNum to CheckpointSummaryMsgCert
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

  uint64_t nextRequiredBlock_ = 0;
  uint64_t nextCommittedBlockId_ = 0;
  STDigest digestOfNextRequiredBlock;
  bool posponedSendFetchBlocksMsg_;

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

  string preferredReplicasToString();
  void clearAllPendingItemsData();
  void clearPendingItemsData(uint64_t untilBlock);
  bool getNextFullBlock(uint64_t requiredBlock,
                        bool& outBadDataDetected,
                        int16_t& outLastChunkInRequiredBlock,
                        char* outBlock,
                        uint32_t& outBlockSize,
                        bool isVBLock);

  bool checkBlock(uint64_t blockNum, const STDigest& expectedBlockDigest, char* block, uint32_t blockSize) const;

  bool checkVirtualBlockOfResPages(const STDigest& expectedDigestOfResPagesDescriptor,
                                   char* vblock,
                                   uint32_t vblockSize) const;

  void processData(bool lastInBatch = false);
  void cycleEndSummary();

  void EnterGettingCheckpointSummariesState();
  set<uint16_t> allOtherReplicas();
  void SetAllReplicasAsPreferred();

  ///////////////////////////////////////////////////////////////////////////
  // Helper methods
  ///////////////////////////////////////////////////////////////////////////

  DataStore::CheckpointDesc createCheckpointDesc(uint64_t checkpointNumber, const STDigest& digestOfResPagesDescriptor);

  STDigest checkpointReservedPages(uint64_t checkpointNumber, DataStoreTransaction* txn);

  void deleteOldCheckpoints(uint64_t checkpointNumber, DataStoreTransaction* txn);
  void srcInitialize();

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
      const uint32_t pageId, const uint64_t checkpointNumber, const char* page, uint32_t pageSize, STDigest& outDigest);

  static void computeDigestOfPagesDescriptor(const DataStore::ResPagesDescriptor* pagesDesc, STDigest& outDigest);

  static void computeDigestOfBlock(const uint64_t blockNum,
                                   const char* block,
                                   const uint32_t blockSize,
                                   STDigest* outDigest);

  static std::array<std::uint8_t, BLOCK_DIGEST_SIZE> computeDigestOfBlock(const uint64_t blockNum,
                                                                          const char* block,
                                                                          const uint32_t blockSize);

  // A wrapper function to get a block from the IAppState and compute its digest.
  //
  // SIDE EFFECT: This function mutates buffer_ and resets it to 0 after the fact.
  STDigest getBlockAndComputeDigest(uint64_t currBlock);

 protected:
  ///////////////////////////////////////////////////////////////////////////
  // Asynchronous Operations - Blocks IO
  ///////////////////////////////////////////////////////////////////////////

  class BlockIOContext {
   public:
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
  const uint32_t finalizePutblockTimeoutMilli_ = 5;
  concord::util::SimpleMemoryPool<BlockIOContext> ioPool_;
  std::deque<BlockIOContextPtr> ioContexts_;
  // used to control the trigger of oneShotTimer self requests
  bool oneShotTimerFlag_;

  // returns number of jobs pushed to queue
  uint16_t getBlocksConcurrentAsync(uint64_t nextBlockId, uint64_t firstRequiredBlock, uint16_t numBlocks);

  void clearIoContexts() {
    for (auto& ctx : ioContexts_) ioPool_.free(ctx);
    ioContexts_.clear();
  }

  // lastBlock: is true if we put the oldest block (firstRequiredBlock)
  //
  // waitPolicy:
  // NO_WAIT: if caller would like to exit immidiately if the next future is not ready
  // (job not ended yet).
  // WAIT_SINGLE_JOB: if caller would like to wait for a single job to finish and exit immidiately if the next job is
  // not ready. WAIT_ALL_JOBS - wait for all jobs to finalize.
  //
  // In any case of an early exit (before all jobs are finalized), a ONESHOT timer is invoked to check the future again
  // soon. return: true if done procesing all futures, and false if the front one was not std::future_status::ready
  enum class PutBlockWaitPolicy { NO_WAIT, WAIT_SINGLE_JOB, WAIT_ALL_JOBS };

  bool finalizePutblockAsync(bool lastBlock, PutBlockWaitPolicy waitPolicy);
  ///////////////////////////////////////////////////////////////////////////
  // Metrics
  ///////////////////////////////////////////////////////////////////////////
 public:
  void SetAggregator(std::shared_ptr<concordMetrics::Aggregator> a);

 private:
  void loadMetrics();
  std::chrono::seconds last_metrics_dump_time_;
  std::chrono::seconds metrics_dump_interval_in_sec_;
  concordMetrics::Component metrics_component_;
  struct Metrics {
    StatusHandle fetching_state_;
    StatusHandle preferred_replicas_;

    GaugeHandle current_source_replica_;
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
    CounterHandle load_reserved_page_;
    CounterHandle load_reserved_page_from_pending_;
    AtomicCounterHandle load_reserved_page_from_checkpoint_;
    AtomicCounterHandle save_reserved_page_;
    CounterHandle zero_reserved_page_;
    CounterHandle start_collecting_state_;
    CounterHandle on_timer_;
    CounterHandle one_shot_timer_;

    CounterHandle on_transferring_complete_;

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
  };

  mutable Metrics metrics_;

  std::map<uint64_t, concord::util::CallbackRegistry<uint64_t>> on_transferring_complete_cb_registry_;
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
    STDigest pageDigest;
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
  Throughput blocks_collected_;
  Throughput bytes_collected_;
  std::optional<uint64_t> firstCollectedBlockId_;
  std::optional<uint64_t> lastCollectedBlockId_;

  // Duration Trackers
  DurationTracker<std::chrono::milliseconds> cycleDT_;
  DurationTracker<std::chrono::milliseconds> commitToChainDT_;
  DurationTracker<std::chrono::milliseconds> gettingCheckpointSummariesDT_;
  DurationTracker<std::chrono::milliseconds> gettingMissingBlocksDT_;
  DurationTracker<std::chrono::milliseconds> gettingMissingResPagesDT_;

  FetchingState lastFetchingState_;
  logging::Logger& logger_;

  void onFetchingStateChange(FetchingState newFetchingState);

  // When true: log historgrams, zero source flag and counter, and then unconditionally clear the iOcontexts
  void finalizeSource(bool logSrcHistograms);

  // used to print periodic summary of recent checkpoints, and collected date while in state GettingMissingBlocks
  std::string logsForCollectingStatus(const uint64_t firstRequiredBlock);
  void reportCollectingStatus(const uint64_t firstRequiredBlock, const uint32_t actualBlockSize, bool toLog = false);
  void startCollectingStats();

  // These 2 variables are used to snapshot source historgrams for GettingMissingBlocks state
  bool sourceFlag_;
  uint8_t sourceSnapshotCounter_;

  uint64_t sourceBatchCounter_ = 0;

  ///////////////////////////////////////////////////////////////////////////
  // Latency Historgrams
  ///////////////////////////////////////////////////////////////////////////
 private:
  struct Recorders {
    static constexpr uint64_t MAX_VALUE_MICROSECONDS = 60ULL * 1000ULL * 1000ULL;          // 60 seconds
    static constexpr uint64_t MAX_BLOCK_SIZE = 100ULL * 1024ULL * 1024ULL;                 // 100MB
    static constexpr uint64_t MAX_BATCH_SIZE_BYTES = 10ULL * 1024ULL * 1024ULL * 1024ULL;  // 10GB
    static constexpr uint64_t MAX_BATCH_SIZE_BLOCKS = 1000ULL;
    static constexpr uint64_t MAX_HANDOFF_QUEUE_SIZE = 10000ULL;
    static constexpr uint64_t MAX_PENDING_BLOCKS_SIZE = 1000ULL;

    Recorders() {
      auto& registrar = concord::diagnostics::RegistrarSingleton::getInstance();
      // common component
      registrar.perf.registerComponent("state_transfer", {on_timer, time_in_handoff_queue, handoff_queue_size});
      // destination component
      registrar.perf.registerComponent("state_transfer_dest",
                                       {
                                           dst_handle_ItemData_msg,
                                           dst_time_between_sendFetchBlocksMsg,
                                           dst_num_pending_blocks_to_commit,
                                           dst_digest_calc_duration,
                                       });
      // source component
      registrar.perf.registerComponent("state_transfer_src",
                                       {src_handle_FetchBlocks_msg,
                                        src_get_block_size_bytes,
                                        src_send_batch_duration,
                                        src_send_batch_size_bytes,
                                        src_send_batch_size_chunks});
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
        time_in_handoff_queue, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(handoff_queue_size, 1, MAX_HANDOFF_QUEUE_SIZE, 3, concord::diagnostics::Unit::COUNT);
    // destination
    DEFINE_SHARED_RECORDER(
        dst_handle_ItemData_msg, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(
        dst_time_between_sendFetchBlocksMsg, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(
        dst_num_pending_blocks_to_commit, 1, MAX_PENDING_BLOCKS_SIZE, 3, concord::diagnostics::Unit::COUNT);
    DEFINE_SHARED_RECORDER(
        dst_digest_calc_duration, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    // source
    DEFINE_SHARED_RECORDER(
        src_handle_FetchBlocks_msg, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(src_get_block_size_bytes, 1, MAX_BLOCK_SIZE, 3, concord::diagnostics::Unit::BYTES);
    DEFINE_SHARED_RECORDER(
        src_send_batch_duration, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(src_send_batch_size_bytes, 1, MAX_BATCH_SIZE_BYTES, 3, concord::diagnostics::Unit::BYTES);
    DEFINE_SHARED_RECORDER(src_send_batch_size_chunks, 1, MAX_BATCH_SIZE_BLOCKS, 3, concord::diagnostics::Unit::COUNT);
  };
  Recorders histograms_;

  // Async time recorders - wrap the above shared recorders with the same name and prefix _rec_
  AsyncTimeRecorder<false> src_send_batch_duration_rec_;
  AsyncTimeRecorder<false> dst_time_between_sendFetchBlocksMsg_rec_;
  AsyncTimeRecorder<false> time_in_handoff_queue_rec_;
  std::shared_ptr<concord::client::reconfiguration::ClientReconfigurationEngine> cre_ = nullptr;
};  // class BCStateTran

}  // namespace bftEngine::bcst::impl
