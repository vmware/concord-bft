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

#include <algorithm>
#include <chrono>
#include <exception>
#include <list>
#include <set>
#include <string>
#include <sstream>
#include <functional>
#include <utility>
#include <iterator>

#include "assertUtils.hpp"
#include "hex_tools.h"
#include "BCStateTran.hpp"
#include "STDigest.hpp"
#include "InMemoryDataStore.hpp"
#include "json_output.hpp"
#include "ReservedPagesClient.hpp"
#include "DBDataStore.hpp"
#include "storage/db_interface.h"
#include "storage/key_manipulator_interface.h"
#include "memorydb/client.h"

#define STRPAIR(var) toPair(#var, var)

using std::tie;
using concordUtils::toPair;
using std::chrono::steady_clock;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::time_point;
using std::chrono::system_clock;
using namespace std::placeholders;
using namespace concord::diagnostics;
using namespace concord::util;

namespace bftEngine {
namespace bcst {

void computeBlockDigest(const uint64_t blockId,
                        const char *block,
                        const uint32_t blockSize,
                        StateTransferDigest *outDigest) {
  return impl::BCStateTran::computeDigestOfBlock(blockId, block, blockSize, (impl::STDigest *)outDigest);
}

std::array<std::uint8_t, BLOCK_DIGEST_SIZE> computeBlockDigest(const uint64_t blockId,
                                                               const char *block,
                                                               const uint32_t blockSize) {
  return impl::BCStateTran::computeDigestOfBlock(blockId, block, blockSize);
}

IStateTransfer *create(const Config &config,
                       IAppState *const stateApi,
                       std::shared_ptr<concord::storage::IDBClient> dbc,
                       std::shared_ptr<concord::storage::ISTKeyManipulator> stKeyManipulator) {
  // TODO(GG): check configuration

  impl::DataStore *ds = nullptr;

  if (dynamic_cast<concord::storage::memorydb::Client *>(dbc.get()))
    ds = new impl::InMemoryDataStore(config.sizeOfReservedPage);
  else
    ds = new impl::DBDataStore(dbc, config.sizeOfReservedPage, stKeyManipulator, config.enableReservedPages);
  auto st = new impl::BCStateTran(config, stateApi, ds);
  ReservedPagesClientBase::setReservedPages(st);
  return st;
}

IStateTransfer *create(const Config &config,
                       IAppState *const stateApi,
                       std::shared_ptr<concord::storage::IDBClient> dbc,
                       std::shared_ptr<concord::storage::ISTKeyManipulator> stKeyManipulator,
                       std::shared_ptr<concordMetrics::Aggregator> aggregator) {
  auto st = static_cast<impl::BCStateTran *>(create(config, stateApi, dbc, stKeyManipulator));
  st->SetAggregator(aggregator);
  return st;
}

namespace impl {

//////////////////////////////////////////////////////////////////////////////
// Time
//////////////////////////////////////////////////////////////////////////////

static uint64_t getMonotonicTimeMilli() {
  steady_clock::time_point curTimePoint = steady_clock::now();
  auto timeSinceEpoch = curTimePoint.time_since_epoch();
  uint64_t milli = duration_cast<milliseconds>(timeSinceEpoch).count();
  return milli;
}

//////////////////////////////////////////////////////////////////////////////
// Ctor & Dtor
//////////////////////////////////////////////////////////////////////////////
static uint32_t calcMaxVBlockSize(uint32_t maxNumberOfPages, uint32_t pageSize);

static uint32_t calcMaxItemSize(uint32_t maxBlockSize, uint32_t maxNumberOfPages, uint32_t pageSize) {
  const uint32_t maxVBlockSize = calcMaxVBlockSize(maxNumberOfPages, pageSize);

  const uint32_t retVal = std::max(maxBlockSize, maxVBlockSize);

  return retVal;
}

static uint32_t calcMaxNumOfChunksInBlock(uint32_t maxItemSize,
                                          uint32_t maxBlockSize,
                                          uint32_t maxChunkSize,
                                          bool isVBlock) {
  if (!isVBlock) {
    uint32_t retVal =
        (maxBlockSize % maxChunkSize == 0) ? (maxBlockSize / maxChunkSize) : (maxBlockSize / maxChunkSize + 1);
    return retVal;
  } else {
    uint32_t retVal =
        (maxItemSize % maxChunkSize == 0) ? (maxItemSize / maxChunkSize) : (maxItemSize / maxChunkSize + 1);
    return retVal;
  }
}

// Here we assume that the set of replicas is 0,1,2,...,numberOfReplicas
// TODO(GG): change to support full dynamic reconfiguration
static set<uint16_t> generateSetOfReplicas(const int16_t numberOfReplicas) {
  std::set<uint16_t> retVal;
  for (int16_t i = 0; i < numberOfReplicas; i++) retVal.insert(i);
  return retVal;
}

BCStateTran::BCStateTran(const Config &config, IAppState *const stateApi, DataStore *ds)
    : as_{stateApi},
      psd_{ds},
      config_{config},
      replicas_{generateSetOfReplicas(config_.numReplicas)},
      maxVBlockSize_{calcMaxVBlockSize(config_.maxNumOfReservedPages, config_.sizeOfReservedPage)},
      maxItemSize_{calcMaxItemSize(config_.maxBlockSize, config_.maxNumOfReservedPages, config_.sizeOfReservedPage)},
      maxNumOfChunksInAppBlock_{
          calcMaxNumOfChunksInBlock(maxItemSize_, config_.maxBlockSize, config_.maxChunkSize, false)},
      maxNumOfChunksInVBlock_{
          calcMaxNumOfChunksInBlock(maxItemSize_, config_.maxBlockSize, config_.maxChunkSize, true)},
      maxNumOfStoredCheckpoints_{0},
      numberOfReservedPages_{0},
      cycleCounter_(0),
      randomGen_{randomDevice_()},
      sourceSelector_{allOtherReplicas(),
                      config_.fetchRetransmissionTimeoutMs,
                      config_.sourceReplicaReplacementTimeoutMs,
                      ST_SRC_LOG},
      last_metrics_dump_time_(0),
      metrics_dump_interval_in_sec_{std::chrono::seconds(config_.metricsDumpIntervalSec)},
      metrics_component_{
          concordMetrics::Component("bc_state_transfer", std::make_shared<concordMetrics::Aggregator>())},

      // We must make sure that we actually initialize all these metrics in the
      // same order as defined in the header file.
      metrics_{metrics_component_.RegisterStatus("fetching_state", stateName(FetchingState::NotFetching)),
               metrics_component_.RegisterStatus("preferred_replicas", ""),

               metrics_component_.RegisterGauge("current_source_replica", NO_REPLICA),
               metrics_component_.RegisterGauge("checkpoint_being_fetched", 0),
               metrics_component_.RegisterGauge("last_stored_checkpoint", 0),
               metrics_component_.RegisterGauge("number_of_reserved_pages", 0),
               metrics_component_.RegisterGauge("size_of_reserved_page", config_.sizeOfReservedPage),
               metrics_component_.RegisterGauge("last_msg_seq_num", lastMsgSeqNum_),
               metrics_component_.RegisterGauge("next_required_block_", nextRequiredBlock_),
               metrics_component_.RegisterGauge("num_pending_item_data_msgs_", pendingItemDataMsgs.size()),
               metrics_component_.RegisterGauge("total_size_of_pending_item_data_msgs", totalSizeOfPendingItemDataMsgs),
               metrics_component_.RegisterAtomicGauge("last_block_", 0),
               metrics_component_.RegisterGauge("last_reachable_block", 0),

               metrics_component_.RegisterCounter("sent_ask_for_checkpoint_summaries_msg"),
               metrics_component_.RegisterCounter("sent_checkpoint_summary_msg"),
               metrics_component_.RegisterCounter("sent_fetch_blocks_msg"),
               metrics_component_.RegisterCounter("sent_fetch_res_pages_msg"),
               metrics_component_.RegisterCounter("sent_reject_fetch_msg"),
               metrics_component_.RegisterCounter("sent_item_data_msg"),

               metrics_component_.RegisterCounter("received_ask_for_checkpoint_summaries_msg"),
               metrics_component_.RegisterCounter("received_checkpoint_summary_msg"),
               metrics_component_.RegisterCounter("received_fetch_blocks_msg"),
               metrics_component_.RegisterCounter("received_fetch_res_pages_msg"),
               metrics_component_.RegisterCounter("received_reject_fetching_msg"),
               metrics_component_.RegisterCounter("received_item_data_msg"),
               metrics_component_.RegisterCounter("received_illegal_msg_"),

               metrics_component_.RegisterCounter("invalid_ask_for_checkpoint_summaries_msg"),
               metrics_component_.RegisterCounter("irrelevant_ask_for_checkpoint_summaries_msg"),
               metrics_component_.RegisterCounter("invalid_checkpoint_summary_msg"),
               metrics_component_.RegisterCounter("irrelevant_checkpoint_summary_msg"),
               metrics_component_.RegisterCounter("invalid_fetch_blocks_msg"),
               metrics_component_.RegisterCounter("irrelevant_fetch_blocks_msg"),
               metrics_component_.RegisterCounter("invalid_fetch_res_pages_msg"),
               metrics_component_.RegisterCounter("irrelevant_fetch_res_pages_msg"),
               metrics_component_.RegisterCounter("invalid_reject_fetching_msg"),
               metrics_component_.RegisterCounter("irrelevant_reject_fetching_msg"),
               metrics_component_.RegisterCounter("invalid_item_data_msg"),
               metrics_component_.RegisterCounter("irrelevant_item_data_msg"),

               metrics_component_.RegisterAtomicCounter("create_checkpoint"),
               metrics_component_.RegisterCounter("mark_checkpoint_as_stable"),
               metrics_component_.RegisterCounter("load_reserved_page"),
               metrics_component_.RegisterCounter("load_reserved_page_from_pending"),
               metrics_component_.RegisterAtomicCounter("load_reserved_page_from_checkpoint"),
               metrics_component_.RegisterAtomicCounter("save_reserved_page"),
               metrics_component_.RegisterCounter("zero_reserved_page"),
               metrics_component_.RegisterCounter("start_collecting_state"),
               metrics_component_.RegisterCounter("on_timer"),
               metrics_component_.RegisterCounter("on_transferring_complete"),
               metrics_component_.RegisterCounter("handle_AskForCheckpointSummaries_msg"),
               metrics_component_.RegisterCounter("dst_handle_CheckpointsSummary_msg"),
               metrics_component_.RegisterCounter("src_handle_FetchBlocks_msg"),
               metrics_component_.RegisterCounter("src_handle_FetchResPages_msg"),
               metrics_component_.RegisterCounter("dst_handle_RejectFetching_msg"),
               metrics_component_.RegisterCounter("dst_handle_ItemData_msg"),

               metrics_component_.RegisterGauge("overall_blocks_collected", 0),
               metrics_component_.RegisterGauge("overall_blocks_throughput", 0),
               metrics_component_.RegisterGauge("overall_bytes_collected", 0),
               metrics_component_.RegisterGauge("overall_bytes_throughput", 0),
               metrics_component_.RegisterGauge("prev_win_blocks_collected", 0),
               metrics_component_.RegisterGauge("prev_win_blocks_throughput", 0),
               metrics_component_.RegisterGauge("prev_win_bytes_collected", 0),
               metrics_component_.RegisterGauge("prev_win_bytes_throughput", 0)},
      blocks_collected_(getMissingBlocksSummaryWindowSize),
      bytes_collected_(getMissingBlocksSummaryWindowSize),
      lastFetchingState_(FetchingState::NotFetching),
      sourceFlag_(false),
      src_send_batch_duration_rec_(histograms_.src_send_batch_duration),
      dst_time_between_sendFetchBlocksMsg_rec_(histograms_.dst_time_between_sendFetchBlocksMsg),
      time_in_handoff_queue_rec_(histograms_.time_in_handoff_queue) {
  ConcordAssertNE(stateApi, nullptr);
  ConcordAssertGE(replicas_.size(), 3U * config_.fVal + 1U);
  ConcordAssert(replicas_.count(config_.myReplicaId) == 1 || config.isReadOnly);
  ConcordAssertGE(config_.maxNumOfReservedPages, 2);

  // Register metrics component with the default aggregator.
  metrics_component_.Register();

  srcGetBlockContextes_.resize(config_.maxNumberOfChunksInBatch);
  for (uint16_t i{0}; i < config_.maxNumberOfChunksInBatch; ++i) {
    srcGetBlockContextes_[i].block.reset(new char[config_.maxBlockSize]);
    srcGetBlockContextes_[i].index = i;
  }
  buffer_ = new char[maxItemSize_]{};
  LOG_INFO(getLogger(), "Creating BCStateTran object: " << config_);

  if (config_.runInSeparateThread) {
    handoff_.reset(new concord::util::Handoff(config_.myReplicaId));
    messageHandler_ = std::bind(&BCStateTran::handoffMsg, this, _1, _2, _3);
    timerHandler_ = std::bind(&BCStateTran::handoffTimer, this);
  } else {
    messageHandler_ = std::bind(&BCStateTran::handleStateTransferMessageImp, this, _1, _2, _3);
    timerHandler_ = std::bind(&BCStateTran::onTimerImp, this);
  }
  // Make sure that the internal IReplicaForStateTransfer callback is always added, alongside any user-supplied
  // callbacks.
  addOnTransferringCompleteCallback(
      [this](uint64_t checkpoint_num) { replicaForStateTransfer_->onTransferringComplete(checkpoint_num); });
}
BCStateTran::~BCStateTran() {
  ConcordAssert(!running_);
  ConcordAssert(cacheOfVirtualBlockForResPages.empty());
  ConcordAssert(pendingItemDataMsgs.empty());

  delete[] buffer_;
}

// Load metrics that are saved on persistent storage
void BCStateTran::loadMetrics() {
  FetchingState fs = getFetchingState();
  metrics_.fetching_state_.Get().Set(stateName(fs));

  metrics_.last_stored_checkpoint_.Get().Set(psd_->getLastStoredCheckpoint());
  metrics_.number_of_reserved_pages_.Get().Set(psd_->getNumberOfReservedPages());
  metrics_.last_block_.Get().Set(as_->getLastBlockNum());
  metrics_.last_reachable_block_.Get().Set(as_->getLastReachableBlockNum());
}

//////////////////////////////////////////////////////////////////////////////
// IStateTransfer methods
//////////////////////////////////////////////////////////////////////////////

void BCStateTran::init(uint64_t maxNumOfRequiredStoredCheckpoints,
                       uint32_t numberOfRequiredReservedPages,
                       uint32_t sizeOfReservedPage) {
  try {
    ConcordAssert(!running_);
    ConcordAssertEQ(replicaForStateTransfer_, nullptr);
    ConcordAssertEQ(sizeOfReservedPage, config_.sizeOfReservedPage);

    maxNumOfStoredCheckpoints_ = maxNumOfRequiredStoredCheckpoints;
    numberOfReservedPages_ = numberOfRequiredReservedPages;
    metrics_.number_of_reserved_pages_.Get().Set(numberOfReservedPages_);
    metrics_.size_of_reserved_page_.Get().Set(sizeOfReservedPage);

    LOG_INFO(getLogger(),
             "Init BCStateTran object:" << KVLOG(
                 maxNumOfStoredCheckpoints_, numberOfReservedPages_, config_.sizeOfReservedPage));

    if (psd_->initialized()) {
      LOG_INFO(getLogger(), "Loading existing data from storage");

      checkConsistency(config_.pedanticChecks);

      FetchingState fs = getFetchingState();
      LOG_INFO(getLogger(), "Starting state is " << stateName(fs));

      if (fs != FetchingState::NotFetching) {
        startCollectingStats();
        if (fs == FetchingState::GettingMissingBlocks || fs == FetchingState::GettingMissingResPages)
          SetAllReplicasAsPreferred();
      }
      loadMetrics();
    } else {
      LOG_INFO(getLogger(), "Initializing a new object");

      ConcordAssertGE(maxNumOfRequiredStoredCheckpoints, 2);
      ConcordAssertLE(maxNumOfRequiredStoredCheckpoints, kMaxNumOfStoredCheckpoints);
      ConcordAssertGE(numberOfRequiredReservedPages, 2);
      ConcordAssertLE(numberOfRequiredReservedPages, config_.maxNumOfReservedPages);

      DataStoreTransaction::Guard g(psd_->beginTransaction());
      g.txn()->setReplicas(replicas_);
      g.txn()->setMyReplicaId(config_.myReplicaId);
      g.txn()->setFVal(config_.fVal);
      g.txn()->setMaxNumOfStoredCheckpoints(maxNumOfRequiredStoredCheckpoints);
      g.txn()->setNumberOfReservedPages(numberOfRequiredReservedPages);
      g.txn()->setLastStoredCheckpoint(0);
      g.txn()->setFirstStoredCheckpoint(0);
      g.txn()->setIsFetchingState(false);
      g.txn()->setFirstRequiredBlock(0);
      g.txn()->setLastRequiredBlock(0);
      g.txn()->setAsInitialized();

      ConcordAssertEQ(getFetchingState(), FetchingState::NotFetching);
    }
  } catch (const std::exception &e) {
    LOG_FATAL(getLogger(), e.what());
    std::terminate();
  }
}

void BCStateTran::startRunning(IReplicaForStateTransfer *r) {
  LOG_INFO(getLogger(), "");

  ConcordAssertNE(r, nullptr);
  running_ = true;
  replicaForStateTransfer_ = r;
  replicaForStateTransfer_->changeStateTransferTimerPeriod(config_.refreshTimerMs);
}

void BCStateTran::stopRunning() {
  LOG_INFO(getLogger(), "");

  ConcordAssert(running_);
  ConcordAssertNE(replicaForStateTransfer_, nullptr);
  if (handoff_) handoff_->stop();

  // TODO(GG): cancel timer

  // reset and free data

  maxNumOfStoredCheckpoints_ = 0;
  numberOfReservedPages_ = 0;
  running_ = false;

  lastMilliOfUniqueFetchID_ = 0;
  lastCountOfUniqueFetchID_ = 0;
  lastMsgSeqNum_ = 0;
  lastMsgSeqNumOfReplicas_.clear();

  for (auto i : cacheOfVirtualBlockForResPages) std::free(i.second);

  cacheOfVirtualBlockForResPages.clear();

  lastTimeSentAskForCheckpointSummariesMsg = 0;
  retransmissionNumberOfAskForCheckpointSummariesMsg = 0;

  for (auto i : summariesCerts) replicaForStateTransfer_->freeStateTransferMsg(reinterpret_cast<char *>(i.second));

  summariesCerts.clear();
  numOfSummariesFromOtherReplicas.clear();
  sourceSelector_.reset();

  nextRequiredBlock_ = 0;
  digestOfNextRequiredBlock.makeZero();

  for (auto i : pendingItemDataMsgs) replicaForStateTransfer_->freeStateTransferMsg(reinterpret_cast<char *>(i));

  pendingItemDataMsgs.clear();
  totalSizeOfPendingItemDataMsgs = 0;
  replicaForStateTransfer_ = nullptr;
}

bool BCStateTran::isRunning() const { return running_; }

// Create a CheckpointDesc for the given checkpointNumber.
//
// This has the side effect of filling in buffer_ with the last block of app
// data.
DataStore::CheckpointDesc BCStateTran::createCheckpointDesc(uint64_t checkpointNumber,
                                                            const STDigest &digestOfResPagesDescriptor) {
  uint64_t lastBlock = as_->getLastReachableBlockNum();
  ConcordAssertEQ(lastBlock, as_->getLastBlockNum());
  metrics_.last_block_.Get().Set(lastBlock);

  STDigest digestOfLastBlock;

  if (lastBlock > 0) {
    digestOfLastBlock = getBlockAndComputeDigest(lastBlock);
  } else {
    // if we don't have blocks, then we use zero digest
    digestOfLastBlock.makeZero();
  }

  DataStore::CheckpointDesc checkDesc;
  checkDesc.checkpointNum = checkpointNumber;
  checkDesc.lastBlock = lastBlock;
  checkDesc.digestOfLastBlock = digestOfLastBlock;
  checkDesc.digestOfResPagesDescriptor = digestOfResPagesDescriptor;

  LOG_INFO(getLogger(),
           "CheckpointDesc: " << KVLOG(checkpointNumber, lastBlock, digestOfLastBlock, digestOfResPagesDescriptor));

  return checkDesc;
}

// Associate any pending reserved pages with the current checkpoint.
// Return the digest of all the reserved pages descriptor.
//
// This has the side effect of mutating buffer_.
STDigest BCStateTran::checkpointReservedPages(uint64_t checkpointNumber, DataStoreTransaction *txn) {
  set<uint32_t> pages = txn->getNumbersOfPendingResPages();
  auto numberOfPagesInCheckpoint = pages.size();
  LOG_INFO(getLogger(),
           "Associating pending pages with checkpoint: " << KVLOG(numberOfPagesInCheckpoint, checkpointNumber));
  std::unique_ptr<char[]> buffer(new char[config_.sizeOfReservedPage]);
  for (uint32_t p : pages) {
    STDigest d;
    txn->getPendingResPage(p, buffer.get(), config_.sizeOfReservedPage);
    computeDigestOfPage(p, checkpointNumber, buffer.get(), config_.sizeOfReservedPage, d);
    txn->associatePendingResPageWithCheckpoint(p, checkpointNumber, d);
  }

  ConcordAssertEQ(txn->numOfAllPendingResPage(), 0);
  DataStore::ResPagesDescriptor *allPagesDesc = txn->getResPagesDescriptor(checkpointNumber);
  ConcordAssertEQ(allPagesDesc->numOfPages, numberOfReservedPages_);

  STDigest digestOfResPagesDescriptor;
  computeDigestOfPagesDescriptor(allPagesDesc, digestOfResPagesDescriptor);

  LOG_INFO(getLogger(), allPagesDesc->toString(digestOfResPagesDescriptor.toString()));

  txn->free(allPagesDesc);
  return digestOfResPagesDescriptor;
}

void BCStateTran::deleteOldCheckpoints(uint64_t checkpointNumber, DataStoreTransaction *txn) {
  uint64_t minRelevantCheckpoint = 0;
  if (checkpointNumber >= maxNumOfStoredCheckpoints_)
    minRelevantCheckpoint = checkpointNumber - maxNumOfStoredCheckpoints_ + 1;

  const uint64_t oldFirstStoredCheckpoint = txn->getFirstStoredCheckpoint();

  if (minRelevantCheckpoint > 0)
    while (minRelevantCheckpoint < checkpointNumber && !txn->hasCheckpointDesc(minRelevantCheckpoint))
      minRelevantCheckpoint++;

  LOG_DEBUG(getLogger(), KVLOG(minRelevantCheckpoint, oldFirstStoredCheckpoint));

  if (minRelevantCheckpoint >= 2 && minRelevantCheckpoint > oldFirstStoredCheckpoint) {
    txn->deleteDescOfSmallerCheckpoints(minRelevantCheckpoint);
    txn->deleteCoveredResPageInSmallerCheckpoints(minRelevantCheckpoint);
  }

  if (minRelevantCheckpoint > oldFirstStoredCheckpoint) txn->setFirstStoredCheckpoint(minRelevantCheckpoint);

  txn->setLastStoredCheckpoint(checkpointNumber);

  auto firstStoredCheckpoint = std::max(minRelevantCheckpoint, oldFirstStoredCheckpoint);
  auto lastStoredCheckpoint = checkpointNumber;
  LOG_INFO(getLogger(),
           KVLOG(checkpointNumber,
                 minRelevantCheckpoint,
                 oldFirstStoredCheckpoint,
                 firstStoredCheckpoint,
                 lastStoredCheckpoint));
}

void BCStateTran::createCheckpointOfCurrentState(uint64_t checkpointNumber) {
  auto lastStoredCheckpointNumber = psd_->getLastStoredCheckpoint();
  LOG_INFO(getLogger(), KVLOG(checkpointNumber, lastStoredCheckpointNumber));

  ConcordAssert(running_);
  ConcordAssert(!isFetching());
  ConcordAssertGT(checkpointNumber, 0);
  ConcordAssertGT(checkpointNumber, lastStoredCheckpointNumber);

  metrics_.create_checkpoint_++;

  {  // txn scope
    DataStoreTransaction::Guard g(psd_->beginTransaction());
    auto digestOfResPagesDescriptor = checkpointReservedPages(checkpointNumber, g.txn());
    auto checkDesc = createCheckpointDesc(checkpointNumber, digestOfResPagesDescriptor);
    g.txn()->setCheckpointDesc(checkpointNumber, checkDesc);
    deleteOldCheckpoints(checkpointNumber, g.txn());
    metrics_.last_stored_checkpoint_.Get().Set(psd_->getLastStoredCheckpoint());
  }
}

void BCStateTran::markCheckpointAsStable(uint64_t checkpointNumber) {
  ConcordAssert(running_);
  ConcordAssert(!isFetching());
  ConcordAssertGT(checkpointNumber, 0);

  metrics_.mark_checkpoint_as_stable_++;

  const uint64_t lastStoredCheckpoint = psd_->getLastStoredCheckpoint();
  metrics_.last_stored_checkpoint_.Get().Set(lastStoredCheckpoint);

  LOG_INFO(getLogger(), KVLOG(checkpointNumber, lastStoredCheckpoint));

  ConcordAssertOR((lastStoredCheckpoint < maxNumOfStoredCheckpoints_),
                  (checkpointNumber >= lastStoredCheckpoint - maxNumOfStoredCheckpoints_ + 1));
  ConcordAssertLE(checkpointNumber, psd_->getLastStoredCheckpoint());
}

void BCStateTran::getDigestOfCheckpoint(uint64_t checkpointNumber, uint16_t sizeOfDigestBuffer, char *outDigestBuffer) {
  ConcordAssert(running_);
  ConcordAssertGE(sizeOfDigestBuffer, sizeof(STDigest));
  ConcordAssertGT(checkpointNumber, 0);
  ConcordAssertGE(checkpointNumber, psd_->getFirstStoredCheckpoint());
  ConcordAssertLE(checkpointNumber, psd_->getLastStoredCheckpoint());
  ConcordAssert(psd_->hasCheckpointDesc(checkpointNumber));

  DataStore::CheckpointDesc desc = psd_->getCheckpointDesc(checkpointNumber);
  STDigest checkpointDigest;
  DigestContext c;
  c.update(reinterpret_cast<char *>(&desc), sizeof(desc));
  c.writeDigest(checkpointDigest.getForUpdate());

  LOG_INFO(getLogger(),
           KVLOG(desc.checkpointNum, desc.digestOfLastBlock, desc.digestOfResPagesDescriptor, checkpointDigest));

  uint16_t s = std::min((uint16_t)sizeof(STDigest), sizeOfDigestBuffer);
  memcpy(outDigestBuffer, checkpointDigest.get(), s);
  if (s < sizeOfDigestBuffer) {
    memset(outDigestBuffer + s, 0, sizeOfDigestBuffer - s);
  }
}

bool BCStateTran::isCollectingState() const { return isFetching(); }

uint32_t BCStateTran::numberOfReservedPages() const { return static_cast<uint32_t>(numberOfReservedPages_); }

uint32_t BCStateTran::sizeOfReservedPage() const { return config_.sizeOfReservedPage; }

bool BCStateTran::loadReservedPage(uint32_t reservedPageId, uint32_t copyLength, char *outReservedPage) const {
  ConcordAssertLT(reservedPageId, numberOfReservedPages_);
  ConcordAssertLE(copyLength, config_.sizeOfReservedPage);

  metrics_.load_reserved_page_++;

  if (psd_->hasPendingResPage(reservedPageId)) {
    LOG_DEBUG(getLogger(), "Loaded pending reserved page: " << reservedPageId);
    metrics_.load_reserved_page_from_pending_++;
    psd_->getPendingResPage(reservedPageId, outReservedPage, copyLength);
  } else {
    uint64_t lastCheckpoint = psd_->getLastStoredCheckpoint();
    // case when the system is restarted before reaching the first checkpoint
    if (lastCheckpoint == 0) return false;
    uint64_t actualCheckpoint = UINT64_MAX;
    metrics_.load_reserved_page_from_checkpoint_++;
    if (!psd_->getResPage(reservedPageId, lastCheckpoint, &actualCheckpoint, outReservedPage, copyLength)) return false;
    ConcordAssertLE(actualCheckpoint, lastCheckpoint);
    LOG_DEBUG(getLogger(),
              "Reserved page loaded from checkpoint: " << KVLOG(reservedPageId, actualCheckpoint, lastCheckpoint));
  }
  return true;
}
// TODO(TK) check if this function can have its own transaction(bftimpl)
void BCStateTran::saveReservedPage(uint32_t reservedPageId, uint32_t copyLength, const char *inReservedPage) {
  try {
    LOG_DEBUG(getLogger(), reservedPageId);

    ConcordAssert(!isFetching());
    ConcordAssertLT(reservedPageId, numberOfReservedPages_);
    ConcordAssertLE(copyLength, config_.sizeOfReservedPage);

    metrics_.save_reserved_page_++;

    psd_->setPendingResPage(reservedPageId, inReservedPage, copyLength);
  } catch (std::out_of_range &e) {
    LOG_ERROR(getLogger(), "Failed to save pending reserved page: " << e.what() << ": " << KVLOG(reservedPageId));
    throw;
  }
}
// TODO(TK) check if this function can have its own transaction(bftimpl)
void BCStateTran::zeroReservedPage(uint32_t reservedPageId) {
  LOG_DEBUG(getLogger(), reservedPageId);

  ConcordAssert(!isFetching());
  ConcordAssertLT(reservedPageId, numberOfReservedPages_);

  metrics_.zero_reserved_page_++;
  std::unique_ptr<char[]> buffer(new char[config_.sizeOfReservedPage]{});
  psd_->setPendingResPage(reservedPageId, buffer.get(), config_.sizeOfReservedPage);
}

void BCStateTran::startCollectingStats() {
  firstCollectedBlockId_ = {};
  lastCollectedBlockId_ = {};
  gettingMissingBlocksDT_.reset();
  commitToChainDT_.reset();
  gettingCheckpointSummariesDT_.reset();
  gettingMissingResPagesDT_.reset();
  cycleDT_.reset();
  betweenPutBlocksStTempDT_.reset();
  putBlocksStTempDT_.reset();

  sources_.clear();

  metrics_.overall_blocks_collected_.Get().Set(0ull);
  metrics_.overall_blocks_throughput_.Get().Set(0ull);
  metrics_.overall_bytes_collected_.Get().Set(0ull);
  metrics_.overall_bytes_throughput_.Get().Set(0ull);
  metrics_.prev_win_blocks_collected_.Get().Set(0ull);
  metrics_.prev_win_blocks_throughput_.Get().Set(0ull);
  metrics_.prev_win_bytes_collected_.Get().Set(0ull);
  metrics_.prev_win_bytes_throughput_.Get().Set(0ull);

  src_send_batch_duration_rec_.clear();
  dst_time_between_sendFetchBlocksMsg_rec_.clear();
  time_in_handoff_queue_rec_.clear();
}

void BCStateTran::startCollectingState() {
  LOG_INFO(getLogger(), "State Transfer cycle started (#" << ++cycleCounter_ << ")");
  ConcordAssert(running_);
  ConcordAssert(!isFetching());
  auto &registrar = concord::diagnostics::RegistrarSingleton::getInstance();
  registrar.perf.snapshot("state_transfer");
  registrar.perf.snapshot("state_transfer_dest");
  metrics_.start_collecting_state_++;
  startCollectingStats();

  verifyEmptyInfoAboutGettingCheckpointSummary();
  {  // txn scope
    DataStoreTransaction::Guard g(psd_->beginTransaction());
    g.txn()->deleteAllPendingPages();
    g.txn()->setIsFetchingState(true);
  }
  sendAskForCheckpointSummariesMsg();
}

// this function can be executed in context of another thread.
void BCStateTran::onTimerImp() {
  if (!running_) return;
  time_in_handoff_queue_rec_.end();
  histograms_.handoff_queue_size->record(handoff_->size());
  TimeRecorder scoped_timer(*histograms_.on_timer);

  metrics_.on_timer_++;
  // Send all metrics to the aggregator
  metrics_component_.UpdateAggregator();

  // Dump metrics to log
  FetchingState fs = getFetchingState();
  auto currTimeForDumping = duration_cast<std::chrono::seconds>(steady_clock::now().time_since_epoch());
  if (currTimeForDumping - last_metrics_dump_time_ >= metrics_dump_interval_in_sec_) {
    last_metrics_dump_time_ = currTimeForDumping;
    LOG_INFO(getLogger(), "--BCStateTransfer metrics dump--" + metrics_component_.ToJson());
  }
  auto currTime = getMonotonicTimeMilli();

  // take a snapshot and log after time passed is approx x2 of fetchRetransmissionTimeoutMs
  if (sourceFlag_ &&
      (((++sourceSnapshotCounter_) * config_.refreshTimerMs) >= (2 * config_.fetchRetransmissionTimeoutMs))) {
    auto &registrar = concord::diagnostics::RegistrarSingleton::getInstance();
    registrar.perf.snapshot("state_transfer");
    registrar.perf.snapshot("state_transfer_src");
    LOG_INFO(getLogger(), registrar.perf.toString(registrar.perf.get("state_transfer")));
    LOG_INFO(getLogger(), registrar.perf.toString(registrar.perf.get("state_transfer_src")));
    sourceFlag_ = false;
    sourceSnapshotCounter_ = 0;
  }
  if (fs == FetchingState::GettingCheckpointSummaries) {
    if ((currTime - lastTimeSentAskForCheckpointSummariesMsg) > config_.checkpointSummariesRetransmissionTimeoutMs) {
      LOG_DEBUG(getLogger(), "Retransmitting AskForCheckpointSummaries");
      if (++retransmissionNumberOfAskForCheckpointSummariesMsg > kResetCount_AskForCheckpointSummaries)
        clearInfoAboutGettingCheckpointSummary();

      sendAskForCheckpointSummariesMsg();
    }
  } else if (fs == FetchingState::GettingMissingBlocks || fs == FetchingState::GettingMissingResPages) {
    processData();
  }
  time_in_handoff_queue_rec_.start();
}

std::string BCStateTran::getStatus() {
  std::ostringstream oss;
  std::unordered_map<std::string, std::string> result, nested_data;
  result.insert(toPair("fetchingState", stateName(getFetchingState())));
  result.insert(STRPAIR(lastMsgSeqNum_));
  result.insert(toPair("cacheOfVirtualBlockForResPagesSize", cacheOfVirtualBlockForResPages.size()));

  auto current_source = sourceSelector_.currentReplica();
  auto preferred_replicas = sourceSelector_.preferredReplicasToString();

  for (auto &[id, seq_num] : lastMsgSeqNumOfReplicas_) {
    nested_data.insert(toPair(std::to_string(id), seq_num));
  }

  result.insert(toPair("lastMsgSequenceNumbers(ReplicaID:SeqNum)",
                       concordUtils::kvContainerToJson(nested_data, [](const auto &arg) { return arg; })));
  nested_data.clear();

  for (auto entry : cacheOfVirtualBlockForResPages) {
    auto vblockDescriptor = entry.first;
    nested_data.insert(
        toPair(std::to_string(vblockDescriptor.checkpointNum), vblockDescriptor.lastCheckpointKnownToRequester));
  }
  result.insert(toPair("vBlocksCacheForReservedPages",
                       concordUtils::kvContainerToJson(nested_data, [](const auto &arg) { return arg; })));
  nested_data.clear();

  if (isFetching()) {
    nested_data.insert(toPair("currentSource", current_source));
    nested_data.insert(toPair("preferredReplicas", preferred_replicas));
    nested_data.insert(toPair("nextRequiredBlock", nextRequiredBlock_));
    nested_data.insert(STRPAIR(totalSizeOfPendingItemDataMsgs));
    result.insert(toPair("fetchingStateDetails",
                         concordUtils::kvContainerToJson(nested_data, [](const auto &arg) { return arg; })));

    result.insert(toPair("collectingDetails", logsForCollectingStatus(psd_->getFirstRequiredBlock())));
  }

  result.insert(toPair("generalStateTransferMetrics", metrics_component_.ToJson()));

  oss << concordUtils::kContainerToJson(result);
  return oss.str();
}

void BCStateTran::addOnTransferringCompleteCallback(std::function<void(uint64_t)> callback,
                                                    StateTransferCallBacksPriorities priority) {
  if (on_transferring_complete_cb_registry_.find((uint64_t)priority) == on_transferring_complete_cb_registry_.end()) {
    on_transferring_complete_cb_registry_[(uint64_t)priority];  // Create a new callback registry for this priority
  }
  on_transferring_complete_cb_registry_.at(uint64_t(priority)).add(std::move(callback));
}

// this function can be executed in context of another thread.
void BCStateTran::handleStateTransferMessageImp(char *msg, uint32_t msgLen, uint16_t senderId) {
  if (!running_) return;
  time_in_handoff_queue_rec_.end();
  histograms_.handoff_queue_size->record(handoff_->size());
  bool invalidSender = (senderId >= (config_.numReplicas + config_.numRoReplicas));
  bool sentFromSelf = senderId == config_.myReplicaId;
  bool msgSizeTooSmall = msgLen < sizeof(BCStateTranBaseMsg);
  if (msgSizeTooSmall || sentFromSelf || invalidSender) {
    metrics_.received_illegal_msg_++;
    LOG_WARN(getLogger(), "Illegal message: " << KVLOG(msgLen, senderId, msgSizeTooSmall, sentFromSelf, invalidSender));
    replicaForStateTransfer_->freeStateTransferMsg(msg);
    time_in_handoff_queue_rec_.start();
    return;
  }

  BCStateTranBaseMsg *msgHeader = reinterpret_cast<BCStateTranBaseMsg *>(msg);
  LOG_DEBUG(getLogger(), "new message with type=" << msgHeader->type);

  FetchingState fs = getFetchingState();
  bool noDelete = false;
  switch (msgHeader->type) {
    case MsgType::AskForCheckpointSummaries:
      if (fs == FetchingState::NotFetching) {
        metrics_.handle_AskForCheckpointSummaries_msg_++;
        noDelete = onMessage(reinterpret_cast<AskForCheckpointSummariesMsg *>(msg), msgLen, senderId);
      }
      break;
    case MsgType::CheckpointsSummary:
      if (fs == FetchingState::GettingCheckpointSummaries) {
        metrics_.handle_CheckpointsSummary_msg_++;
        noDelete = onMessage(reinterpret_cast<CheckpointSummaryMsg *>(msg), msgLen, senderId);
      }
      break;
    case MsgType::FetchBlocks: {
      TimeRecorder scoped_timer(*histograms_.src_handle_FetchBlocks_msg);
      metrics_.handle_FetchBlocks_msg_++;
      noDelete = onMessage(reinterpret_cast<FetchBlocksMsg *>(msg), msgLen, senderId);
    } break;
    case MsgType::FetchResPages: {
      metrics_.handle_FetchResPages_msg_++;
      noDelete = onMessage(reinterpret_cast<FetchResPagesMsg *>(msg), msgLen, senderId);
    } break;
    case MsgType::RejectFetching:
      if (fs == FetchingState::GettingMissingBlocks || fs == FetchingState::GettingMissingResPages) {
        metrics_.handle_RejectFetching_msg_++;
        noDelete = onMessage(reinterpret_cast<RejectFetchingMsg *>(msg), msgLen, senderId);
      }
      break;
    case MsgType::ItemData:
      if (fs == FetchingState::GettingMissingBlocks || fs == FetchingState::GettingMissingResPages) {
        TimeRecorder scoped_timer(*histograms_.dst_handle_ItemData_msg);
        metrics_.handle_ItemData_msg_++;
        noDelete = onMessage(reinterpret_cast<ItemDataMsg *>(msg), msgLen, senderId);
      }
      break;
    default:
      break;
  }

  if (!noDelete) replicaForStateTransfer_->freeStateTransferMsg(msg);
  time_in_handoff_queue_rec_.start();
}

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

static uint32_t calcMaxVBlockSize(uint32_t maxNumberOfPages, uint32_t pageSize) {
  const uint32_t elementSize = sizeof(ElementOfVirtualBlock) + pageSize - 1;

  return sizeof(HeaderOfVirtualBlock) + (elementSize * maxNumberOfPages);
}

static uint32_t getNumberOfElements(char *virtualBlock) {
  HeaderOfVirtualBlock *h = reinterpret_cast<HeaderOfVirtualBlock *>(virtualBlock);
  return h->numberOfUpdatedPages;
}

static uint32_t getSizeOfVirtualBlock(char *virtualBlock, uint32_t pageSize) {
  HeaderOfVirtualBlock *h = reinterpret_cast<HeaderOfVirtualBlock *>(virtualBlock);

  const uint32_t elementSize = sizeof(ElementOfVirtualBlock) + pageSize - 1;
  const uint32_t size = sizeof(HeaderOfVirtualBlock) + h->numberOfUpdatedPages * elementSize;
  return size;
}

static ElementOfVirtualBlock *getVirtualElement(uint32_t index, uint32_t pageSize, char *virtualBlock) {
  HeaderOfVirtualBlock *h = reinterpret_cast<HeaderOfVirtualBlock *>(virtualBlock);
  ConcordAssertLT(index, h->numberOfUpdatedPages);

  const uint32_t elementSize = sizeof(ElementOfVirtualBlock) + pageSize - 1;
  char *p = virtualBlock + sizeof(HeaderOfVirtualBlock) + (index * elementSize);
  ElementOfVirtualBlock *retVal = reinterpret_cast<ElementOfVirtualBlock *>(p);
  return retVal;
}

static bool checkStructureOfVirtualBlock(char *virtualBlock, uint32_t virtualBlockSize, uint32_t pageSize) {
  if (virtualBlockSize < sizeof(HeaderOfVirtualBlock)) return false;

  const uint32_t arrayBlockSize = virtualBlockSize - sizeof(HeaderOfVirtualBlock);
  const uint32_t elementSize = sizeof(ElementOfVirtualBlock) + pageSize - 1;

  if (arrayBlockSize % elementSize != 0) return false;

  uint32_t numOfElements = (arrayBlockSize / elementSize);
  HeaderOfVirtualBlock *h = reinterpret_cast<HeaderOfVirtualBlock *>(virtualBlock);

  if (numOfElements != h->numberOfUpdatedPages) return false;

  uint32_t lastPageId = UINT32_MAX;
  for (uint32_t i = 0; i < numOfElements; i++) {
    char *p = virtualBlock + sizeof(HeaderOfVirtualBlock) + (i * elementSize);
    ElementOfVirtualBlock *e = reinterpret_cast<ElementOfVirtualBlock *>(p);

    if (e->checkpointNumber <= h->lastCheckpointKnownToRequester) return false;
    if (e->pageDigest.isZero()) return false;
    if (i > 0 && e->pageId <= lastPageId) return false;
    lastPageId = e->pageId;
  }
  return true;
}

///////////////////////////////////////////////////////////////////////////
// Unique message IDs
///////////////////////////////////////////////////////////////////////////

uint64_t BCStateTran::uniqueMsgSeqNum() {
  std::chrono::time_point<system_clock> n = system_clock::now();
  const uint64_t milli = duration_cast<milliseconds>(n.time_since_epoch()).count();

  if (milli > lastMilliOfUniqueFetchID_) {
    lastMilliOfUniqueFetchID_ = milli;
    lastCountOfUniqueFetchID_ = 0;
  } else {
    if (lastCountOfUniqueFetchID_ == 0x3FFFFF) {
      LOG_WARN(getLogger(), "SeqNum Counter reached max value");
      lastMilliOfUniqueFetchID_++;
      lastCountOfUniqueFetchID_ = 0;
    } else {
      lastCountOfUniqueFetchID_++;
    }
  }

  uint64_t r = (lastMilliOfUniqueFetchID_ << (64 - 42));
  ConcordAssertLE(lastCountOfUniqueFetchID_, 0x3FFFFF);
  r = r | ((uint64_t)lastCountOfUniqueFetchID_);
  return r;
}

bool BCStateTran::checkValidityAndSaveMsgSeqNum(uint16_t replicaId, uint64_t msgSeqNum) {
  uint64_t milliMsgTime = ((msgSeqNum) >> (64 - 42));

  time_point<system_clock> now = system_clock::now();
  const uint64_t milliNow = duration_cast<milliseconds>(now.time_since_epoch()).count();
  uint64_t diffMilli = ((milliMsgTime > milliNow) ? (milliMsgTime - milliNow) : (milliNow - milliMsgTime));

  if (diffMilli > config_.maxAcceptableMsgDelayMs) {
    auto excessiveMilliseconds = diffMilli - config_.maxAcceptableMsgDelayMs;
    LOG_WARN(getLogger(),
             "Msg rejected because it is too old: " << KVLOG(replicaId,
                                                             msgSeqNum,
                                                             excessiveMilliseconds,
                                                             milliNow,
                                                             diffMilli,
                                                             milliMsgTime,
                                                             config_.maxAcceptableMsgDelayMs));
    return false;
  }

  auto p = lastMsgSeqNumOfReplicas_.find(replicaId);
  if (p != lastMsgSeqNumOfReplicas_.end() && p->second >= msgSeqNum) {
    auto lastMsgSeqNum = p->second;
    LOG_WARN(
        getLogger(),
        "Msg rejected because its sequence number is not monotonic: " << KVLOG(replicaId, msgSeqNum, lastMsgSeqNum));
    return false;
  }

  lastMsgSeqNumOfReplicas_[replicaId] = msgSeqNum;
  LOG_DEBUG(getLogger(), "Msg accepted: " << KVLOG(msgSeqNum));
  return true;
}

//////////////////////////////////////////////////////////////////////////////
// State
//////////////////////////////////////////////////////////////////////////////

string BCStateTran::stateName(FetchingState fs) {
  switch (fs) {
    case FetchingState::NotFetching:
      return "NotFetching";
    case FetchingState::GettingCheckpointSummaries:
      return "GettingCheckpointSummaries";
    case FetchingState::GettingMissingBlocks:
      return "GettingMissingBlocks";
    case FetchingState::GettingMissingResPages:
      return "GettingMissingResPages";
    default:
      ConcordAssert(false);
      return "Error";
  }
}

std::ostream &operator<<(std::ostream &os, const BCStateTran::FetchingState fs) {
  os << BCStateTran::stateName(fs);
  return os;
}

bool BCStateTran::isFetching() const { return (psd_->getIsFetchingState()); }

void BCStateTran::onFetchingStateChange(FetchingState newFetchingState) {
  LOG_INFO(getLogger(),
           "FetchingState changed from " << stateName(lastFetchingState_) << " to " << stateName(newFetchingState));
  switch (lastFetchingState_) {
    case FetchingState::NotFetching:
      cycleDT_.start();
      break;
    case FetchingState::GettingCheckpointSummaries:
      gettingCheckpointSummariesDT_.pause();
      break;
    case FetchingState::GettingMissingBlocks:
      gettingMissingBlocksDT_.pause();
      break;
    case FetchingState::GettingMissingResPages:
      gettingMissingResPagesDT_.pause();
      break;
  }
  switch (newFetchingState) {
    case FetchingState::NotFetching:
      cycleDT_.pause();
      break;
    case FetchingState::GettingCheckpointSummaries:
      gettingCheckpointSummariesDT_.start();
      break;
    case FetchingState::GettingMissingBlocks:
      gettingMissingBlocksDT_.start();
      if (blocks_collected_.isStarted()) {
        blocks_collected_.resume();
        bytes_collected_.resume();
      } else {
        blocks_collected_.start();
        bytes_collected_.start();
      }
      break;
    case FetchingState::GettingMissingResPages:
      gettingMissingResPagesDT_.start();
      break;
  }
  lastFetchingState_ = newFetchingState;
}

BCStateTran::FetchingState BCStateTran::getFetchingState() {
  BCStateTran::FetchingState fs;
  if (!psd_->getIsFetchingState())
    fs = FetchingState::NotFetching;
  else {
    ConcordAssertEQ(psd_->numOfAllPendingResPage(), 0);
    if (!psd_->hasCheckpointBeingFetched())
      fs = FetchingState::GettingCheckpointSummaries;
    else if (psd_->getLastRequiredBlock() > 0)
      fs = FetchingState::GettingMissingBlocks;
    else {
      ConcordAssertEQ(psd_->getFirstRequiredBlock(), 0);
      fs = FetchingState::GettingMissingResPages;
    }
  }
  if (lastFetchingState_ != fs) {
    // state has changed, modify some of the statistics objects
    onFetchingStateChange(fs);
  }
  return fs;
}

//////////////////////////////////////////////////////////////////////////////
// Send messages
//////////////////////////////////////////////////////////////////////////////

void BCStateTran::sendToAllOtherReplicas(char *msg, uint32_t msgSize) {
  for (int16_t r : replicas_) {
    if (r == config_.myReplicaId) continue;
    replicaForStateTransfer_->sendStateTransferMessage(msg, msgSize, r);
  }
}

void BCStateTran::sendAskForCheckpointSummariesMsg() {
  ConcordAssertEQ(getFetchingState(), FetchingState::GettingCheckpointSummaries);
  metrics_.sent_ask_for_checkpoint_summaries_msg_++;

  AskForCheckpointSummariesMsg msg;
  lastTimeSentAskForCheckpointSummariesMsg = getMonotonicTimeMilli();
  lastMsgSeqNum_ = uniqueMsgSeqNum();
  metrics_.last_msg_seq_num_.Get().Set(lastMsgSeqNum_);
  SCOPED_MDC_SEQ_NUM(getSequenceNumber(config_.myReplicaId, lastMsgSeqNum_));

  msg.msgSeqNum = lastMsgSeqNum_;
  msg.minRelevantCheckpointNum = psd_->getLastStoredCheckpoint() + 1;

  LOG_DEBUG(getLogger(), KVLOG(lastMsgSeqNum_, msg.minRelevantCheckpointNum));

  sendToAllOtherReplicas(reinterpret_cast<char *>(&msg), sizeof(AskForCheckpointSummariesMsg));
}

void BCStateTran::sendFetchBlocksMsg(uint64_t firstRequiredBlock,
                                     uint64_t lastRequiredBlock,
                                     int16_t lastKnownChunkInLastRequiredBlock) {
  ConcordAssertEQ(getFetchingState(), FetchingState::GettingMissingBlocks);
  ConcordAssert(sourceSelector_.hasSource());
  metrics_.sent_fetch_blocks_msg_++;

  FetchBlocksMsg msg;
  lastMsgSeqNum_ = uniqueMsgSeqNum();
  metrics_.last_msg_seq_num_.Get().Set(lastMsgSeqNum_);

  msg.msgSeqNum = lastMsgSeqNum_;
  msg.firstRequiredBlock = firstRequiredBlock;
  msg.lastRequiredBlock = lastRequiredBlock;
  msg.lastKnownChunkInLastRequiredBlock = lastKnownChunkInLastRequiredBlock;

  LOG_DEBUG(getLogger(),
            KVLOG(sourceSelector_.currentReplica(),
                  msg.msgSeqNum,
                  msg.firstRequiredBlock,
                  msg.lastRequiredBlock,
                  msg.lastKnownChunkInLastRequiredBlock));

  sourceSelector_.setFetchingTimeStamp(getMonotonicTimeMilli());
  dst_time_between_sendFetchBlocksMsg_rec_.clear();
  dst_time_between_sendFetchBlocksMsg_rec_.start();
  replicaForStateTransfer_->sendStateTransferMessage(
      reinterpret_cast<char *>(&msg), sizeof(FetchBlocksMsg), sourceSelector_.currentReplica());
}

void BCStateTran::sendFetchResPagesMsg(int16_t lastKnownChunkInLastRequiredBlock) {
  ConcordAssertEQ(getFetchingState(), FetchingState::GettingMissingResPages);
  ConcordAssert(sourceSelector_.hasSource());
  ConcordAssert(psd_->hasCheckpointBeingFetched());

  metrics_.sent_fetch_res_pages_msg_++;

  DataStore::CheckpointDesc cp = psd_->getCheckpointBeingFetched();
  uint64_t lastStoredCheckpoint = psd_->getLastStoredCheckpoint();
  lastMsgSeqNum_ = uniqueMsgSeqNum();
  metrics_.last_msg_seq_num_.Get().Set(lastMsgSeqNum_);

  FetchResPagesMsg msg;
  msg.msgSeqNum = lastMsgSeqNum_;
  msg.lastCheckpointKnownToRequester = lastStoredCheckpoint;
  msg.requiredCheckpointNum = cp.checkpointNum;
  msg.lastKnownChunk = lastKnownChunkInLastRequiredBlock;

  LOG_DEBUG(getLogger(),
            KVLOG(sourceSelector_.currentReplica(),
                  msg.msgSeqNum,
                  msg.lastCheckpointKnownToRequester,
                  msg.requiredCheckpointNum,
                  msg.lastKnownChunk));

  sourceSelector_.setFetchingTimeStamp(getMonotonicTimeMilli());
  replicaForStateTransfer_->sendStateTransferMessage(
      reinterpret_cast<char *>(&msg), sizeof(FetchResPagesMsg), sourceSelector_.currentReplica());
}

//////////////////////////////////////////////////////////////////////////////
// Message handlers
//////////////////////////////////////////////////////////////////////////////

bool BCStateTran::onMessage(const AskForCheckpointSummariesMsg *m, uint32_t msgLen, uint16_t replicaId) {
  SCOPED_MDC_SEQ_NUM(getSequenceNumber(replicaId, m->msgSeqNum));
  LOG_DEBUG(getLogger(), KVLOG(replicaId, m->msgSeqNum));

  ConcordAssert(!psd_->getIsFetchingState());

  metrics_.received_ask_for_checkpoint_summaries_msg_++;

  // if msg is invalid
  if (msgLen < sizeof(AskForCheckpointSummariesMsg) || m->minRelevantCheckpointNum == 0 || m->msgSeqNum == 0) {
    LOG_WARN(getLogger(), "Msg is invalid: " << KVLOG(msgLen, m->minRelevantCheckpointNum, m->msgSeqNum));
    metrics_.invalid_ask_for_checkpoint_summaries_msg_++;
    return false;
  }

  // if msg is not relevant
  auto lastStoredCheckpoint = psd_->getLastStoredCheckpoint();
  if (auto seqNumInvalid = !checkValidityAndSaveMsgSeqNum(replicaId, m->msgSeqNum) ||
                           (m->minRelevantCheckpointNum > lastStoredCheckpoint)) {
    LOG_WARN(getLogger(),
             "Msg is irrelevant: " << KVLOG(seqNumInvalid, m->minRelevantCheckpointNum, lastStoredCheckpoint));
    metrics_.irrelevant_ask_for_checkpoint_summaries_msg_++;
    return false;
  }

  uint64_t toCheckpoint = lastStoredCheckpoint;
  uint64_t fromCheckpoint = std::max(m->minRelevantCheckpointNum, psd_->getFirstStoredCheckpoint());
  // TODO(GG): really need this condition?
  if (toCheckpoint > maxNumOfStoredCheckpoints_) {
    fromCheckpoint = std::max(fromCheckpoint, toCheckpoint - maxNumOfStoredCheckpoints_ + 1);
  }

  bool sent = false;
  auto toReplicaId = replicaId;
  for (uint64_t i = toCheckpoint; i >= fromCheckpoint; i--) {
    if (!psd_->hasCheckpointDesc(i)) continue;

    DataStore::CheckpointDesc c = psd_->getCheckpointDesc(i);
    CheckpointSummaryMsg checkpointSummary;

    checkpointSummary.checkpointNum = i;
    checkpointSummary.lastBlock = c.lastBlock;
    checkpointSummary.digestOfLastBlock = c.digestOfLastBlock;
    checkpointSummary.digestOfResPagesDescriptor = c.digestOfResPagesDescriptor;
    checkpointSummary.requestMsgSeqNum = m->msgSeqNum;

    LOG_INFO(getLogger(),
             "Sending CheckpointSummaryMsg: " << KVLOG(toReplicaId,
                                                       checkpointSummary.checkpointNum,
                                                       checkpointSummary.lastBlock,
                                                       checkpointSummary.digestOfLastBlock,
                                                       checkpointSummary.digestOfResPagesDescriptor,
                                                       checkpointSummary.requestMsgSeqNum));

    replicaForStateTransfer_->sendStateTransferMessage(
        reinterpret_cast<char *>(&checkpointSummary), sizeof(CheckpointSummaryMsg), replicaId);

    metrics_.sent_checkpoint_summary_msg_++;
    sent = true;
  }

  if (!sent) {
    LOG_INFO(getLogger(), "Failed to send relevant CheckpointSummaryMsg: " << KVLOG(toReplicaId));
  }
  return false;
}

bool BCStateTran::onMessage(const CheckpointSummaryMsg *m, uint32_t msgLen, uint16_t replicaId) {
  SCOPED_MDC_SEQ_NUM(getSequenceNumber(config_.myReplicaId, uniqueMsgSeqNum()));
  LOG_DEBUG(getLogger(), KVLOG(replicaId, m->checkpointNum, m->lastBlock, m->requestMsgSeqNum));
  FetchingState fs = getFetchingState();
  LOG_DEBUG(getLogger(), "Fetching state is " << stateName(fs));
  ConcordAssertEQ(fs, FetchingState::GettingCheckpointSummaries);
  metrics_.received_checkpoint_summary_msg_++;

  // if msg is invalid
  if (msgLen < sizeof(CheckpointSummaryMsg) || m->checkpointNum == 0 || m->digestOfResPagesDescriptor.isZero() ||
      m->requestMsgSeqNum == 0) {
    LOG_WARN(getLogger(),
             "Msg is invalid: " << KVLOG(
                 replicaId, msgLen, m->checkpointNum, m->digestOfResPagesDescriptor.isZero(), m->requestMsgSeqNum));
    metrics_.invalid_checkpoint_summary_msg_++;
    return false;
  }

  // if msg is not relevant
  if (m->requestMsgSeqNum != lastMsgSeqNum_ || m->checkpointNum <= psd_->getLastStoredCheckpoint()) {
    LOG_WARN(getLogger(),
             "Msg is irrelevant: " << KVLOG(
                 replicaId, m->requestMsgSeqNum, lastMsgSeqNum_, m->checkpointNum, psd_->getLastStoredCheckpoint()));
    metrics_.irrelevant_checkpoint_summary_msg_++;
    return false;
  }

  uint16_t numOfMsgsFromSender =
      (numOfSummariesFromOtherReplicas.count(replicaId) == 0) ? 0 : numOfSummariesFromOtherReplicas.at(replicaId);

  // if we have too many messages from the same replica
  if (numOfMsgsFromSender >= (psd_->getMaxNumOfStoredCheckpoints() + 1)) {
    LOG_WARN(getLogger(), "Too many messages from replica: " << KVLOG(replicaId, numOfMsgsFromSender));
    return false;
  }

  auto p = summariesCerts.find(m->checkpointNum);
  CheckpointSummaryMsgCert *cert = nullptr;

  if (p == summariesCerts.end()) {
    cert = new CheckpointSummaryMsgCert(
        replicaForStateTransfer_, replicas_.size(), config_.fVal, config_.fVal + 1, config_.myReplicaId);
    summariesCerts[m->checkpointNum] = cert;
  } else {
    cert = p->second;
  }

  bool used = cert->addMsg(const_cast<CheckpointSummaryMsg *>(m), replicaId);

  if (used) numOfSummariesFromOtherReplicas[replicaId] = numOfMsgsFromSender + 1;

  if (!cert->isComplete()) {
    LOG_DEBUG(getLogger(), "Does not have enough CheckpointSummaryMsg messages");
    return true;
  }

  LOG_INFO(getLogger(), "Has enough CheckpointSummaryMsg messages");
  CheckpointSummaryMsg *checkSummary = cert->bestCorrectMsg();

  ConcordAssertNE(checkSummary, nullptr);
  ConcordAssert(sourceSelector_.isReset());
  ConcordAssertEQ(nextRequiredBlock_, 0);
  ConcordAssert(digestOfNextRequiredBlock.isZero());
  ConcordAssert(pendingItemDataMsgs.empty());
  ConcordAssertEQ(totalSizeOfPendingItemDataMsgs, 0);

  // set the preferred replicas
  for (uint16_t r : replicas_) {  // TODO(GG): can be improved
    CheckpointSummaryMsg *t = cert->getMsgFromReplica(r);
    if (t != nullptr && CheckpointSummaryMsg::equivalent(t, checkSummary)) {
      sourceSelector_.addPreferredReplica(r);
      ConcordAssertLT(r, config_.numReplicas);
    }
  }

  metrics_.preferred_replicas_.Get().Set(sourceSelector_.preferredReplicasToString());

  ConcordAssertGE(sourceSelector_.numberOfPreferredReplicas(), config_.fVal + 1);

  // set new checkpoint
  DataStore::CheckpointDesc newCheckpoint;

  newCheckpoint.checkpointNum = checkSummary->checkpointNum;
  newCheckpoint.lastBlock = checkSummary->lastBlock;
  newCheckpoint.digestOfLastBlock = checkSummary->digestOfLastBlock;
  newCheckpoint.digestOfResPagesDescriptor = checkSummary->digestOfResPagesDescriptor;

  auto fetchingState = stateName(getFetchingState());
  {  // txn scope
    DataStoreTransaction::Guard g(psd_->beginTransaction());
    ConcordAssert(!g.txn()->hasCheckpointBeingFetched());
    g.txn()->setCheckpointBeingFetched(newCheckpoint);
    metrics_.checkpoint_being_fetched_.Get().Set(newCheckpoint.checkpointNum);

    // clean
    clearInfoAboutGettingCheckpointSummary();
    lastMsgSeqNum_ = 0;
    metrics_.last_msg_seq_num_.Get().Set(0);

    // check if we need to fetch blocks, or reserved pages
    const uint64_t lastReachableBlockNum = as_->getLastReachableBlockNum();
    metrics_.last_reachable_block_.Get().Set(lastReachableBlockNum);

    
    LOG_INFO(getLogger(),
             "Start fetching checkpoint: " << KVLOG(newCheckpoint.checkpointNum,
                                                    newCheckpoint.lastBlock,
                                                    newCheckpoint.digestOfLastBlock,
                                                    newCheckpoint.digestOfResPagesDescriptor,
                                                    lastReachableBlockNum,
                                                    fetchingState));

    if (newCheckpoint.lastBlock > lastReachableBlockNum) {
      // fetch blocks
      g.txn()->setFirstRequiredBlock(lastReachableBlockNum + 1);
      g.txn()->setLastRequiredBlock(newCheckpoint.lastBlock);
    } else {
      // fetch reserved pages (vblock)
      ConcordAssertEQ(newCheckpoint.lastBlock, lastReachableBlockNum);
      ConcordAssertEQ(g.txn()->getFirstRequiredBlock(), 0);
      ConcordAssertEQ(g.txn()->getLastRequiredBlock(), 0);
    }
  }
  metrics_.last_block_.Get().Set(newCheckpoint.lastBlock);
  fetchingState = stateName(getFetchingState());
  metrics_.fetching_state_.Get().Set(fetchingState);

  processData();
  return true;
}

uint16_t BCStateTran::asyncGetBlocksConcurrent(uint64_t nextBlockId,
                                               uint64_t firstRequiredBlock,
                                               uint16_t numBlocks,
                                               size_t startContextIndex) {
  ConcordAssertGE(config_.maxNumberOfChunksInBatch, numBlocks);
  auto j{startContextIndex};

  LOG_DEBUG(getLogger(), KVLOG(nextBlockId, firstRequiredBlock, numBlocks, startContextIndex));
  for (uint64_t i{nextBlockId}; (i >= firstRequiredBlock) && (j < startContextIndex + numBlocks); --i, ++j) {
    auto &ctx = srcGetBlockContextes_[j];
    // start the job ASAP, return result to on-stack future
    if (ctx.future.valid()) {
      // wait for previous thread to finish - we must call it explicitly here, can't relay on dtor
      // TODO(GL)- get() can be optimize by waiting 0 time and continue calling next jobs which might have finished. 1st
      // research if wait time > 0.
      ctx.future.get();
      LOG_DEBUG(getLogger(), "Waiting for previous thread to finish job on context " << KVLOG(ctx.blockId, ctx.index));
    }
    ctx.blockId = i;
    ctx.future = as_->getBlockAsync(ctx.blockId, ctx.block.get(), &ctx.blockSize);
  }

  return j;
}

bool BCStateTran::onMessage(const FetchBlocksMsg *m, uint32_t msgLen, uint16_t replicaId) {
  SCOPED_MDC_SEQ_NUM(getSequenceNumber(replicaId, m->msgSeqNum));
  LOG_DEBUG(
      getLogger(),
      KVLOG(
          replicaId, m->msgSeqNum, m->firstRequiredBlock, m->lastRequiredBlock, m->lastKnownChunkInLastRequiredBlock));
  metrics_.received_fetch_blocks_msg_++;

  // if msg is invalid
  if (msgLen < sizeof(FetchBlocksMsg) || m->msgSeqNum == 0 || m->firstRequiredBlock == 0 ||
      m->lastRequiredBlock < m->firstRequiredBlock) {
    LOG_WARN(getLogger(),
             "Msg is invalid: " << KVLOG(replicaId, m->msgSeqNum, m->firstRequiredBlock, m->lastRequiredBlock));
    metrics_.invalid_fetch_blocks_msg_++;
    return false;
  }

  // if msg is not relevant
  if (!checkValidityAndSaveMsgSeqNum(replicaId, m->msgSeqNum)) {
    LOG_WARN(getLogger(), "Msg is irrelevant: " << KVLOG(replicaId, m->msgSeqNum));
    metrics_.irrelevant_fetch_blocks_msg_++;
    return false;
  }

  FetchingState fetchingState = getFetchingState();
  auto lastReachableBlockNum = as_->getLastReachableBlockNum();

  // if msg should be rejected
  auto rejectFetchingMsg = [&]() {
    RejectFetchingMsg outMsg;

    outMsg.requestMsgSeqNum = m->msgSeqNum;
    metrics_.sent_reject_fetch_msg_++;
    LOG_WARN(getLogger(),
             "Rejecting msg. Sending RejectFetchingMsg to replica: " << KVLOG(
                 replicaId, outMsg.requestMsgSeqNum, fetchingState, m->lastRequiredBlock, lastReachableBlockNum));
    replicaForStateTransfer_->sendStateTransferMessage(
        reinterpret_cast<char *>(&outMsg), sizeof(RejectFetchingMsg), replicaId);
  };

  if (fetchingState != FetchingState::NotFetching || m->lastRequiredBlock > lastReachableBlockNum) {
    rejectFetchingMsg();
    return false;
  }

  if (!sourceFlag_) {
    // a new source - reset histograms and snapshot counter
    sourceFlag_ = true;
    sourceSnapshotCounter_ = 0;
    auto &registrar = concord::diagnostics::RegistrarSingleton::getInstance();
    registrar.perf.snapshot("state_transfer");
    registrar.perf.snapshot("state_transfer_src");
  }

  // start recording time to send a whole batch, and its size
  uint64_t batchSizeBytes = 0;
  uint64_t batchSizeBlocks = 0;
  src_send_batch_duration_rec_.clear();
  src_send_batch_duration_rec_.start();

  // compute information about next block and chunk
  uint64_t nextBlockId = m->lastRequiredBlock;
  uint16_t nextChunk = m->lastKnownChunkInLastRequiredBlock + 1;
  uint16_t numOfSentChunks = 0;

  if (!config_.enableSourceBlocksPreFetch || !srcGetBlockContextes_[0].future.valid() ||
      (srcGetBlockContextes_[0].blockId != nextBlockId)) {
    LOG_INFO(getLogger(),
             "Call asyncGetBlocksConcurrent: source blocks prefetch disabled (first batch or retransmission): "
                 << KVLOG(srcGetBlockContextes_[0].blockId, nextBlockId));
    asyncGetBlocksConcurrent(nextBlockId, m->firstRequiredBlock, config_.maxNumberOfChunksInBatch);
  }

  // Fetch blocks and send all chunks for the batch. Also, while looping start to pre-fetch next batch
  // We pre-fetch only if feature enabled, and we are not in the last batch
  // Setting preFetchBlockId to 0 disable pre-fetching on all later code.
  uint64_t preFetchBlockId = 0;
  if (config_.enableSourceBlocksPreFetch && (nextBlockId > config_.maxNumberOfChunksInBatch))
    preFetchBlockId = nextBlockId - config_.maxNumberOfChunksInBatch;
  LOG_DEBUG(getLogger(),
            "Start sending batch: " << KVLOG(m->msgSeqNum,
                                             m->firstRequiredBlock,
                                             m->lastRequiredBlock,
                                             m->lastKnownChunkInLastRequiredBlock,
                                             preFetchBlockId));
  size_t ctxIndex = 0;
  DurationTracker<std::chrono::microseconds> waitFutureDuration;  // TODO(GG) - remove when unneeded
  bool getNextBlock = true;
  char *buffer = nullptr;
  uint32_t sizeOfNextBlock = 0;
  do {
    if (getNextBlock) {
      // wait for worker to finish getting next block
      auto &ctx = srcGetBlockContextes_[ctxIndex];
      ConcordAssert(ctx.future.valid());
      waitFutureDuration.start();
      if (!ctx.future.get()) {
        LOG_ERROR(getLogger(), "Block not found in storage, abort batch:" << KVLOG(ctx.index, ctx.blockId));
        rejectFetchingMsg();
        return false;
      }
      ConcordAssertGT(ctx.blockSize, 0);
      ConcordAssertEQ(ctx.blockId, nextBlockId);
      sizeOfNextBlock = ctx.blockSize;
      buffer = ctx.block.get();
      LOG_DEBUG(
          getLogger(),
          "Start sending next block: " << KVLOG(nextBlockId, sizeOfNextBlock, waitFutureDuration.totalDuration(true)));
      waitFutureDuration.reset();

      // some statistics
      histograms_.src_get_block_size_bytes->record(ctx.blockSize);
      batchSizeBytes += sizeOfNextBlock;
      ++batchSizeBlocks;
      getNextBlock = false;
    }

    uint32_t sizeOfLastChunk = config_.maxChunkSize;
    uint32_t numOfChunksInNextBlock = sizeOfNextBlock / config_.maxChunkSize;
    if ((sizeOfNextBlock % config_.maxChunkSize) != 0) {
      sizeOfLastChunk = sizeOfNextBlock % config_.maxChunkSize;
      numOfChunksInNextBlock++;
    }

    // if msg is invalid (lastKnownChunkInLastRequiredBlock+1 does not exist)
    if ((numOfSentChunks == 0) && (nextChunk > numOfChunksInNextBlock)) {
      LOG_WARN(getLogger(),
               "Msg is invalid: illegal chunk number: " << KVLOG(replicaId, nextChunk, numOfChunksInNextBlock));
      return false;
    }

    SCOPED_MDC_SEQ_NUM(getSequenceNumber(replicaId, m->msgSeqNum, nextChunk, nextBlockId));
    uint32_t chunkSize = (nextChunk < numOfChunksInNextBlock) ? config_.maxChunkSize : sizeOfLastChunk;

    ConcordAssertGT(chunkSize, 0);

    char *pRawChunk = buffer + (nextChunk - 1) * config_.maxChunkSize;
    ItemDataMsg *outMsg = ItemDataMsg::alloc(chunkSize);  // TODO(GG): improve

    outMsg->requestMsgSeqNum = m->msgSeqNum;
    outMsg->blockNumber = nextBlockId;
    outMsg->totalNumberOfChunksInBlock = numOfChunksInNextBlock;
    outMsg->chunkNumber = nextChunk;
    outMsg->dataSize = chunkSize;
    outMsg->lastInBatch =
        ((numOfSentChunks + 1) >= config_.maxNumberOfChunksInBatch) || ((nextBlockId - 1) < m->firstRequiredBlock);
    memcpy(outMsg->data, pRawChunk, chunkSize);

    LOG_DEBUG(getLogger(),
              "Sending ItemDataMsg: " << std::boolalpha
                                      << KVLOG(replicaId,
                                               outMsg->requestMsgSeqNum,
                                               outMsg->blockNumber,
                                               outMsg->totalNumberOfChunksInBlock,
                                               outMsg->chunkNumber,
                                               outMsg->dataSize,
                                               (bool)outMsg->lastInBatch));

    metrics_.sent_item_data_msg_++;
    replicaForStateTransfer_->sendStateTransferMessage(reinterpret_cast<char *>(outMsg), outMsg->size(), replicaId);

    ItemDataMsg::free(outMsg);
    numOfSentChunks++;

    // if we've already sent enough chunks
    if (numOfSentChunks >= config_.maxNumberOfChunksInBatch) {
      LOG_DEBUG(getLogger(), "Batch end - sent enough chunks: " << KVLOG(numOfSentChunks));
      break;
    } else if (static_cast<uint16_t>(nextChunk + 1) <= numOfChunksInNextBlock) {
      // we still have chunks in block
      nextChunk++;
    } else if ((nextBlockId - 1) < m->firstRequiredBlock) {
      LOG_DEBUG(getLogger(), "Batch end - sent all relevant blocks: " << KVLOG(m->firstRequiredBlock));
      break;
    } else {
      // no more chunks in the block
      --nextBlockId;
      nextChunk = 1;
      // this context is usage us done. We can now use it to prefetch future batch block
      if (preFetchBlockId > 0) {
        asyncGetBlocksConcurrent(preFetchBlockId, m->firstRequiredBlock, 1, ctxIndex);
        --preFetchBlockId;
      }
      ++ctxIndex;
      getNextBlock = true;
    }
  } while (true);

  histograms_.src_send_batch_size_bytes->record(batchSizeBytes);
  histograms_.src_send_batch_size_blocks->record(batchSizeBlocks);
  src_send_batch_duration_rec_.end();

  if (preFetchBlockId > 0) {
    asyncGetBlocksConcurrent(preFetchBlockId, m->firstRequiredBlock, 1, ctxIndex);
  }
  return false;
}

bool BCStateTran::onMessage(const FetchResPagesMsg *m, uint32_t msgLen, uint16_t replicaId) {
  SCOPED_MDC_SEQ_NUM(getSequenceNumber(replicaId, m->msgSeqNum));
  LOG_DEBUG(
      getLogger(),
      KVLOG(replicaId, m->msgSeqNum, m->lastCheckpointKnownToRequester, m->requiredCheckpointNum, m->lastKnownChunk));
  metrics_.received_fetch_res_pages_msg_++;

  // if msg is invalid
  if (msgLen < sizeof(FetchResPagesMsg) || m->msgSeqNum == 0 || m->requiredCheckpointNum == 0) {
    LOG_WARN(getLogger(), "Msg is invalid: " << KVLOG(replicaId, msgLen, m->msgSeqNum, m->requiredCheckpointNum));
    metrics_.invalid_fetch_res_pages_msg_++;
    return false;
  }

  // if msg is not relevant
  if (!checkValidityAndSaveMsgSeqNum(replicaId, m->msgSeqNum)) {
    LOG_WARN(getLogger(), "Msg is irrelevant: " << KVLOG(replicaId, m->msgSeqNum));
    metrics_.irrelevant_fetch_res_pages_msg_++;
    return false;
  }

  FetchingState fetchingState = getFetchingState();

  // if msg should be rejected
  if (fetchingState != FetchingState::NotFetching || !psd_->hasCheckpointDesc(m->requiredCheckpointNum)) {
    RejectFetchingMsg outMsg;
    outMsg.requestMsgSeqNum = m->msgSeqNum;

    LOG_WARN(getLogger(),
             "Rejecting msg. Sending RejectFetchingMsg to replica "
                 << KVLOG(replicaId, fetchingState, outMsg.requestMsgSeqNum, m->requiredCheckpointNum));

    metrics_.sent_reject_fetch_msg_++;

    replicaForStateTransfer_->sendStateTransferMessage(
        reinterpret_cast<char *>(&outMsg), sizeof(RejectFetchingMsg), replicaId);

    return false;
  }

  // find virtual block
  DescOfVBlockForResPages descOfVBlock;
  descOfVBlock.checkpointNum = m->requiredCheckpointNum;
  descOfVBlock.lastCheckpointKnownToRequester = m->lastCheckpointKnownToRequester;
  char *vblock = getVBlockFromCache(descOfVBlock);

  // if we don't have the relevant vblock, create the vblock
  if (vblock == nullptr) {
    LOG_DEBUG(getLogger(),
              "Creating a new vblock: " << KVLOG(
                  replicaId, descOfVBlock.checkpointNum, descOfVBlock.lastCheckpointKnownToRequester));

    // TODO(GG): consider adding protection against bad replicas
    // that lead to unnecessary creations of vblocks
    vblock = createVBlock(descOfVBlock);
    ConcordAssertNE(vblock, nullptr);
    setVBlockInCache(descOfVBlock, vblock);

    ConcordAssertLE(cacheOfVirtualBlockForResPages.size(), kMaxVBlocksInCache);
  }

  uint32_t vblockSize = getSizeOfVirtualBlock(vblock, config_.sizeOfReservedPage);

  ConcordAssertGE(vblockSize, sizeof(HeaderOfVirtualBlock));
  ConcordAssert(checkStructureOfVirtualBlock(vblock, vblockSize, config_.sizeOfReservedPage));

  // compute information about next chunk
  uint32_t sizeOfLastChunk = config_.maxChunkSize;
  uint32_t numOfChunksInVBlock = vblockSize / config_.maxChunkSize;
  if (vblockSize % config_.maxChunkSize != 0) {
    sizeOfLastChunk = vblockSize % config_.maxChunkSize;
    numOfChunksInVBlock++;
  }

  // TODO (AJS): This looks like a possible overflow, since numOfChunksInVBlock
  // can be greater than nextChunk. Should we make numOfChunksInVBlock a
  // uint16_t ?
  uint16_t nextChunk = m->lastKnownChunk + 1;
  // if msg is invalid (because lastKnownChunk+1 does not exist)
  if (nextChunk > numOfChunksInVBlock) {
    LOG_WARN(getLogger(), "Msg is invalid: illegal chunk number: " << KVLOG(replicaId, nextChunk, numOfChunksInVBlock));
    return false;
  }

  // send chunks
  uint16_t numOfSentChunks = 0;
  while (true) {
    SCOPED_MDC_SEQ_NUM(getSequenceNumber(replicaId, m->msgSeqNum, nextChunk, ID_OF_VBLOCK_RES_PAGES));
    uint32_t chunkSize = (nextChunk < numOfChunksInVBlock) ? config_.maxChunkSize : sizeOfLastChunk;
    ConcordAssertGT(chunkSize, 0);

    char *pRawChunk = vblock + (nextChunk - 1) * config_.maxChunkSize;
    ItemDataMsg *outMsg = ItemDataMsg::alloc(chunkSize);

    outMsg->requestMsgSeqNum = m->msgSeqNum;
    outMsg->blockNumber = ID_OF_VBLOCK_RES_PAGES;
    outMsg->totalNumberOfChunksInBlock = numOfChunksInVBlock;
    outMsg->chunkNumber = nextChunk;
    outMsg->dataSize = chunkSize;
    memcpy(outMsg->data, pRawChunk, chunkSize);

    LOG_DEBUG(getLogger(),
              "Sending ItemDataMsg: " << KVLOG(replicaId,
                                               outMsg->requestMsgSeqNum,
                                               outMsg->blockNumber,
                                               outMsg->totalNumberOfChunksInBlock,
                                               outMsg->chunkNumber,
                                               outMsg->dataSize));
    metrics_.sent_item_data_msg_++;

    replicaForStateTransfer_->sendStateTransferMessage(reinterpret_cast<char *>(outMsg), outMsg->size(), replicaId);

    ItemDataMsg::free(outMsg);
    numOfSentChunks++;

    // if we've already sent enough chunks
    if (numOfSentChunks >= config_.maxNumberOfChunksInBatch) {
      break;
    }
    // if we still have chunks in block
    if (static_cast<uint16_t>(nextChunk + 1) <= numOfChunksInVBlock) {
      nextChunk++;
    } else {  // we sent all chunks
      break;
    }
  }
  return false;
}

bool BCStateTran::onMessage(const RejectFetchingMsg *m, uint32_t msgLen, uint16_t replicaId) {
  LOG_DEBUG(getLogger(), KVLOG(replicaId, m->requestMsgSeqNum));
  metrics_.received_reject_fetching_msg_++;

  FetchingState fs = getFetchingState();
  if (fs != FetchingState::GettingMissingBlocks && fs != FetchingState::GettingMissingResPages) {
    LOG_FATAL(getLogger(),
              "Expected Fetching State GettingMissingBlocks or GettingMissingResPages. Got: " << stateName(fs));
    ConcordAssert(false);
  }
  ConcordAssert(sourceSelector_.hasPreferredReplicas());

  // if msg is invalid
  if (msgLen < sizeof(RejectFetchingMsg)) {
    LOG_WARN(getLogger(), "Msg is invalid: " << KVLOG(replicaId, msgLen));
    metrics_.invalid_reject_fetching_msg_++;
    return false;
  }

  // if msg is not relevant
  if (sourceSelector_.currentReplica() != replicaId || lastMsgSeqNum_ != m->requestMsgSeqNum) {
    LOG_WARN(
        getLogger(),
        "Msg is irrelevant" << KVLOG(replicaId, sourceSelector_.currentReplica(), lastMsgSeqNum_, m->requestMsgSeqNum));
    metrics_.irrelevant_reject_fetching_msg_++;
    return false;
  }

  ConcordAssert(sourceSelector_.isPreferred(replicaId));

  LOG_WARN(getLogger(), "Removing replica from preferred replicas: " << KVLOG(replicaId));
  sourceSelector_.removeCurrentReplica();
  metrics_.current_source_replica_.Get().Set(NO_REPLICA);
  metrics_.preferred_replicas_.Get().Set(sourceSelector_.preferredReplicasToString());
  clearAllPendingItemsData();

  if (sourceSelector_.hasPreferredReplicas()) {
    processData();
  } else if (fs == FetchingState::GettingMissingBlocks) {
    LOG_DEBUG(getLogger(), "Adding all peer replicas to preferredReplicas_ (because preferredReplicas_.size()==0)");

    // in this case, we will try to use all other replicas
    SetAllReplicasAsPreferred();
    processData();
  } else if (fs == FetchingState::GettingMissingResPages) {
    EnterGettingCheckpointSummariesState();
  } else {
    ConcordAssert(false);
  }
  return false;
}

// Retrieve either a chunk of a block or a reserved page when fetching
bool BCStateTran::onMessage(const ItemDataMsg *m, uint32_t msgLen, uint16_t replicaId) {
  SCOPED_MDC_SEQ_NUM(getSequenceNumber(config_.myReplicaId, lastMsgSeqNum_, m->chunkNumber, m->blockNumber));
  LOG_DEBUG(getLogger(), KVLOG(replicaId, m->requestMsgSeqNum, m->blockNumber));
  metrics_.received_item_data_msg_++;

  FetchingState fs = getFetchingState();
  if (fs != FetchingState::GettingMissingBlocks && fs != FetchingState::GettingMissingResPages) {
    LOG_FATAL(getLogger(),
              "Expected Fetching State GettingMissingBlocks or GettingMissingResPages. Got: " << stateName(fs));
    ConcordAssert(false);
  }

  const auto MaxNumOfChunksInBlock =
      (fs == FetchingState::GettingMissingBlocks) ? maxNumOfChunksInAppBlock_ : maxNumOfChunksInVBlock_;

  LOG_DEBUG(getLogger(),
            std::boolalpha << KVLOG(
                m->blockNumber, m->totalNumberOfChunksInBlock, m->chunkNumber, m->dataSize, (bool)m->lastInBatch));

  // if msg is invalid
  if (msgLen < m->size() || m->requestMsgSeqNum == 0 || m->blockNumber == 0 || m->totalNumberOfChunksInBlock == 0 ||
      m->totalNumberOfChunksInBlock > MaxNumOfChunksInBlock || m->chunkNumber == 0 || m->dataSize == 0) {
    LOG_WARN(getLogger(),
             "Msg is invalid: " << KVLOG(replicaId,
                                         msgLen,
                                         m->size(),
                                         m->requestMsgSeqNum,
                                         m->blockNumber,
                                         m->totalNumberOfChunksInBlock,
                                         MaxNumOfChunksInBlock,
                                         m->chunkNumber,
                                         m->dataSize));
    metrics_.invalid_item_data_msg_++;
    return false;
  }

  //  const DataStore::CheckpointDesc fcp = psd_->getCheckpointBeingFetched();
  const uint64_t firstRequiredBlock = psd_->getFirstRequiredBlock();
  const uint64_t lastRequiredBlock = psd_->getLastRequiredBlock();

  auto fetchingState = fs;
  if (fs == FetchingState::GettingMissingBlocks) {
    // if msg is not relevant
    if ((sourceSelector_.currentReplica() != replicaId) || (m->requestMsgSeqNum != lastMsgSeqNum_) ||
        (m->blockNumber > lastRequiredBlock) || (m->blockNumber < firstRequiredBlock) ||
        (m->blockNumber + config_.maxNumberOfChunksInBatch + 1 < lastRequiredBlock) ||
        (m->dataSize + totalSizeOfPendingItemDataMsgs > config_.maxPendingDataFromSourceReplica)) {
      LOG_WARN(getLogger(),
               "Msg is irrelevant: " << KVLOG(replicaId,
                                              fetchingState,
                                              sourceSelector_.currentReplica(),
                                              m->requestMsgSeqNum,
                                              lastMsgSeqNum_,
                                              m->blockNumber,
                                              firstRequiredBlock,
                                              lastRequiredBlock,
                                              config_.maxNumberOfChunksInBatch,
                                              m->dataSize,
                                              totalSizeOfPendingItemDataMsgs,
                                              config_.maxPendingDataFromSourceReplica));
      metrics_.irrelevant_item_data_msg_++;
      return false;
    }
  } else {
    ConcordAssertEQ(firstRequiredBlock, 0);
    ConcordAssertEQ(lastRequiredBlock, 0);

    // if msg is not relevant
    if ((sourceSelector_.currentReplica() != replicaId) || (m->requestMsgSeqNum != lastMsgSeqNum_) ||
        (m->blockNumber != ID_OF_VBLOCK_RES_PAGES) ||
        (m->dataSize + totalSizeOfPendingItemDataMsgs > config_.maxPendingDataFromSourceReplica)) {
      LOG_WARN(getLogger(),
               "Msg is irrelevant: " << KVLOG(replicaId,
                                              fetchingState,
                                              sourceSelector_.currentReplica(),
                                              m->requestMsgSeqNum,
                                              lastMsgSeqNum_,
                                              (m->blockNumber == ID_OF_VBLOCK_RES_PAGES),
                                              m->dataSize,
                                              totalSizeOfPendingItemDataMsgs,
                                              config_.maxPendingDataFromSourceReplica));
      metrics_.irrelevant_item_data_msg_++;
      return false;
    }
  }

  ConcordAssert(sourceSelector_.isPreferred(replicaId));

  bool added = false;

  tie(std::ignore, added) = pendingItemDataMsgs.insert(const_cast<ItemDataMsg *>(m));
  // set fetchingTimeStamp_ while ignoring added flag - source is responsive
  sourceSelector_.setFetchingTimeStamp(getMonotonicTimeMilli());

  if (added) {
    LOG_DEBUG(getLogger(),
              "ItemDataMsg was added to pendingItemDataMsgs: " << KVLOG(replicaId, fetchingState, m->requestMsgSeqNum));
    metrics_.num_pending_item_data_msgs_.Get().Set(pendingItemDataMsgs.size());
    totalSizeOfPendingItemDataMsgs += m->dataSize;
    metrics_.total_size_of_pending_item_data_msgs_.Get().Set(totalSizeOfPendingItemDataMsgs);
    processData();
    return true;
  } else {
    LOG_INFO(
        getLogger(),
        "ItemDataMsg was NOT added to pendingItemDataMsgs: " << KVLOG(replicaId, fetchingState, m->requestMsgSeqNum));
    return false;
  }
}

//////////////////////////////////////////////////////////////////////////////
// cache that holds virtual blocks
//////////////////////////////////////////////////////////////////////////////

char *BCStateTran::getVBlockFromCache(const DescOfVBlockForResPages &desc) const {
  auto p = cacheOfVirtualBlockForResPages.find(desc);

  if (p == cacheOfVirtualBlockForResPages.end()) return nullptr;

  char *vBlock = p->second;

  ConcordAssertNE(vBlock, nullptr);

  HeaderOfVirtualBlock *header = reinterpret_cast<HeaderOfVirtualBlock *>(vBlock);

  ConcordAssertEQ(desc.lastCheckpointKnownToRequester, header->lastCheckpointKnownToRequester);

  return vBlock;
}

void BCStateTran::setVBlockInCache(const DescOfVBlockForResPages &desc, char *vBlock) {
  auto p = cacheOfVirtualBlockForResPages.find(desc);

  ConcordAssertEQ(p, cacheOfVirtualBlockForResPages.end());

  if (cacheOfVirtualBlockForResPages.size() == kMaxVBlocksInCache) {
    auto minItem = cacheOfVirtualBlockForResPages.begin();
    std::free(minItem->second);
    cacheOfVirtualBlockForResPages.erase(minItem);
  }

  cacheOfVirtualBlockForResPages[desc] = vBlock;
  ConcordAssertLE(cacheOfVirtualBlockForResPages.size(), kMaxVBlocksInCache);
}

char *BCStateTran::createVBlock(const DescOfVBlockForResPages &desc) {
  ConcordAssert(psd_->hasCheckpointDesc(desc.checkpointNum));

  // find the updated pages
  std::list<uint32_t> updatedPages;

  for (uint32_t i = 0; i < numberOfReservedPages_; i++) {
    uint64_t actualPageCheckpoint = 0;
    if (!psd_->getResPage(i, desc.checkpointNum, &actualPageCheckpoint)) continue;

    ConcordAssertLE(actualPageCheckpoint, desc.checkpointNum);

    if (actualPageCheckpoint > desc.lastCheckpointKnownToRequester) updatedPages.push_back(i);
  }

  const uint32_t numberOfUpdatedPages = updatedPages.size();

  // allocate and fill block
  const uint32_t elementSize = sizeof(ElementOfVirtualBlock) + config_.sizeOfReservedPage - 1;
  const uint32_t size = sizeof(HeaderOfVirtualBlock) + numberOfUpdatedPages * elementSize;
  char *rawVBlock = reinterpret_cast<char *>(std::malloc(size));

  HeaderOfVirtualBlock *header = reinterpret_cast<HeaderOfVirtualBlock *>(rawVBlock);
  header->lastCheckpointKnownToRequester = desc.lastCheckpointKnownToRequester;
  header->numberOfUpdatedPages = numberOfUpdatedPages;

  if (numberOfUpdatedPages == 0) {
    ConcordAssert(checkStructureOfVirtualBlock(rawVBlock, size, config_.sizeOfReservedPage));
    LOG_DEBUG(getLogger(), "New vblock contains 0 updated pages: " << KVLOG(desc.checkpointNum, size));
    return rawVBlock;
  }

  char *elements = rawVBlock + sizeof(HeaderOfVirtualBlock);

  uint32_t idx = 0;
  std::unique_ptr<char[]> buffer(new char[config_.sizeOfReservedPage]);
  for (uint32_t pageId : updatedPages) {
    ConcordAssertLT(idx, numberOfUpdatedPages);

    uint64_t actualPageCheckpoint = 0;
    STDigest pageDigest;
    psd_->getResPage(
        pageId, desc.checkpointNum, &actualPageCheckpoint, &pageDigest, buffer.get(), config_.sizeOfReservedPage);
    ConcordAssertLE(actualPageCheckpoint, desc.checkpointNum);
    ConcordAssertGT(actualPageCheckpoint, desc.lastCheckpointKnownToRequester);
    ConcordAssert(!pageDigest.isZero());

    ElementOfVirtualBlock *currElement = reinterpret_cast<ElementOfVirtualBlock *>(elements + idx * elementSize);
    currElement->pageId = pageId;
    currElement->checkpointNumber = actualPageCheckpoint;
    currElement->pageDigest = pageDigest;
    memcpy(currElement->page, buffer.get(), config_.sizeOfReservedPage);
    LOG_DEBUG(getLogger(),
              "Adding page to vBlock: " << KVLOG(
                  currElement->pageId, currElement->checkpointNumber, currElement->pageDigest));
    idx++;
  }

  ConcordAssertEQ(idx, numberOfUpdatedPages);
  ConcordAssertOR(!config_.pedanticChecks, checkStructureOfVirtualBlock(rawVBlock, size, config_.sizeOfReservedPage));

  LOG_DEBUG(getLogger(),
            "New vblock contains " << numberOfUpdatedPages << " updated pages: " << KVLOG(desc.checkpointNum, size));
  return rawVBlock;
}

///////////////////////////////////////////////////////////////////////////
// for state GettingCheckpointSummaries
///////////////////////////////////////////////////////////////////////////

void BCStateTran::clearInfoAboutGettingCheckpointSummary() {
  lastTimeSentAskForCheckpointSummariesMsg = 0;
  retransmissionNumberOfAskForCheckpointSummariesMsg = 0;

  for (auto i : summariesCerts) {
    i.second->resetAndFree();
    delete i.second;
  }

  summariesCerts.clear();
  numOfSummariesFromOtherReplicas.clear();
}

void BCStateTran::verifyEmptyInfoAboutGettingCheckpointSummary() {
  ConcordAssertEQ(lastTimeSentAskForCheckpointSummariesMsg, 0);
  ConcordAssertEQ(retransmissionNumberOfAskForCheckpointSummariesMsg, 0);
  ConcordAssert(summariesCerts.empty());
  ConcordAssert(numOfSummariesFromOtherReplicas.empty());
}

///////////////////////////////////////////////////////////////////////////
// for states GettingMissingBlocks or GettingMissingResPages
///////////////////////////////////////////////////////////////////////////

void BCStateTran::clearAllPendingItemsData() {
  LOG_DEBUG(getLogger(), "");

  for (auto i : pendingItemDataMsgs) replicaForStateTransfer_->freeStateTransferMsg(reinterpret_cast<char *>(i));

  pendingItemDataMsgs.clear();
  totalSizeOfPendingItemDataMsgs = 0;
  metrics_.num_pending_item_data_msgs_.Get().Set(0);
  metrics_.total_size_of_pending_item_data_msgs_.Get().Set(0);
}

void BCStateTran::clearPendingItemsData(uint64_t untilBlock) {
  LOG_DEBUG(getLogger(), KVLOG(untilBlock));

  if (untilBlock == 0) return;

  auto it = pendingItemDataMsgs.begin();
  while (it != pendingItemDataMsgs.end() && (*it)->blockNumber >= untilBlock) {
    ConcordAssertGE(totalSizeOfPendingItemDataMsgs, (*it)->dataSize);

    totalSizeOfPendingItemDataMsgs -= (*it)->dataSize;
    replicaForStateTransfer_->freeStateTransferMsg(reinterpret_cast<char *>(*it));
    it = pendingItemDataMsgs.erase(it);
  }
  metrics_.num_pending_item_data_msgs_.Get().Set(pendingItemDataMsgs.size());
  metrics_.total_size_of_pending_item_data_msgs_.Get().Set(totalSizeOfPendingItemDataMsgs);
}

bool BCStateTran::getNextFullBlock(uint64_t requiredBlock,
                                   bool &outBadDataDetected,
                                   int16_t &outLastChunkInRequiredBlock,
                                   char *outBlock,
                                   uint32_t &outBlockSize,
                                   bool isVBLock,
                                   bool &outLastInBatch) {
  ConcordAssertGE(requiredBlock, 1);

  const uint32_t maxSize = (isVBLock ? maxVBlockSize_ : config_.maxBlockSize);
  clearPendingItemsData(requiredBlock + 1);

  outBadDataDetected = false;
  outLastChunkInRequiredBlock = 0;
  outBlockSize = 0;

  bool badData = false;
  bool fullBlock = false;
  uint16_t totalNumberOfChunks = 0;
  uint16_t maxAvailableChunk = 0;
  uint32_t blockSize = 0;

  auto it = pendingItemDataMsgs.begin();
  while ((it != pendingItemDataMsgs.end()) && ((*it)->blockNumber == requiredBlock)) {
    ItemDataMsg *msg = *it;

    // the conditions of these asserts are checked when receiving the message
    ConcordAssertGT(msg->totalNumberOfChunksInBlock, 0);
    ConcordAssertGE(msg->chunkNumber, 1);

    if (totalNumberOfChunks == 0) totalNumberOfChunks = msg->totalNumberOfChunksInBlock;

    blockSize += msg->dataSize;
    if (totalNumberOfChunks != msg->totalNumberOfChunksInBlock || msg->chunkNumber > totalNumberOfChunks ||
        blockSize > maxSize) {
      badData = true;
      break;
    }

    if (maxAvailableChunk + 1 < msg->chunkNumber) break;  // we have a hole

    ConcordAssertEQ(maxAvailableChunk + 1, msg->chunkNumber);

    maxAvailableChunk = msg->chunkNumber;

    ConcordAssertLE(maxAvailableChunk, totalNumberOfChunks);

    if (maxAvailableChunk == totalNumberOfChunks) {
      fullBlock = true;
      break;
    }
    ++it;
  }

  if (badData) {
    ConcordAssert(!fullBlock);
    outBadDataDetected = true;
    outLastChunkInRequiredBlock = 0;
    return false;
  }

  outLastChunkInRequiredBlock = maxAvailableChunk;
  if (!fullBlock) {
    return false;
  }

  // construct the block
  uint16_t currentChunk = 0;
  uint32_t currentPos = 0;
  bool lastInBatch = false;

  it = pendingItemDataMsgs.begin();
  while (true) {
    ConcordAssertNE(it, pendingItemDataMsgs.end());
    ConcordAssertEQ((*it)->blockNumber, requiredBlock);

    ItemDataMsg *msg = *it;

    ConcordAssertGE(msg->chunkNumber, 1);
    ConcordAssertEQ(msg->totalNumberOfChunksInBlock, totalNumberOfChunks);
    ConcordAssertEQ(currentChunk + 1, msg->chunkNumber);
    ConcordAssertLE(currentPos + msg->dataSize, maxSize);

    memcpy(outBlock + currentPos, msg->data, msg->dataSize);
    currentChunk = msg->chunkNumber;
    currentPos += msg->dataSize;
    lastInBatch = msg->lastInBatch;
    totalSizeOfPendingItemDataMsgs -= (*it)->dataSize;
    replicaForStateTransfer_->freeStateTransferMsg(reinterpret_cast<char *>(*it));
    it = pendingItemDataMsgs.erase(it);
    metrics_.num_pending_item_data_msgs_.Get().Set(pendingItemDataMsgs.size());
    metrics_.total_size_of_pending_item_data_msgs_.Get().Set(totalSizeOfPendingItemDataMsgs);

    if (currentChunk == totalNumberOfChunks) {
      outBlockSize = currentPos;
      outLastInBatch = lastInBatch;
      return true;
    }
  }
}

bool BCStateTran::checkBlock(uint64_t blockNum,
                             const STDigest &expectedBlockDigest,
                             char *block,
                             uint32_t blockSize) const {
  STDigest blockDigest;
  computeDigestOfBlock(blockNum, block, blockSize, &blockDigest);

  if (blockDigest != expectedBlockDigest) {
    LOG_WARN(getLogger(), "Incorrect digest: " << KVLOG(blockNum, blockDigest, expectedBlockDigest));
    return false;
  } else {
    return true;
  }
}

bool BCStateTran::checkVirtualBlockOfResPages(const STDigest &expectedDigestOfResPagesDescriptor,
                                              char *vblock,
                                              uint32_t vblockSize) const {
  if (!checkStructureOfVirtualBlock(vblock, vblockSize, config_.sizeOfReservedPage)) {
    LOG_WARN(getLogger(), "vblock has illegal structure");
    return false;
  }

  HeaderOfVirtualBlock *h = reinterpret_cast<HeaderOfVirtualBlock *>(vblock);

  if (psd_->getLastStoredCheckpoint() != h->lastCheckpointKnownToRequester) {
    LOG_WARN(getLogger(),
             "vblock has irrelevant checkpoint: " << KVLOG(h->lastCheckpointKnownToRequester,
                                                           psd_->getLastStoredCheckpoint()));

    return false;
  }

  // build ResPagesDescriptor
  DataStore::ResPagesDescriptor *pagesDesc = psd_->getResPagesDescriptor(h->lastCheckpointKnownToRequester);

  ConcordAssertEQ(pagesDesc->numOfPages, numberOfReservedPages_);

  for (uint32_t element = 0; element < h->numberOfUpdatedPages; ++element) {
    ElementOfVirtualBlock *vElement = getVirtualElement(element, config_.sizeOfReservedPage, vblock);
    LOG_TRACE(getLogger(), KVLOG(element, vElement->pageId, vElement->checkpointNumber, vElement->pageDigest));

    STDigest computedPageDigest;
    computeDigestOfPage(
        vElement->pageId, vElement->checkpointNumber, vElement->page, config_.sizeOfReservedPage, computedPageDigest);
    if (computedPageDigest != vElement->pageDigest) {
      LOG_WARN(getLogger(),
               "vblock contains invalid digest: " << KVLOG(vElement->pageId, vElement->pageDigest, computedPageDigest));
      return false;
    }
    ConcordAssertLE(pagesDesc->d[vElement->pageId].relevantCheckpoint, h->lastCheckpointKnownToRequester);
    pagesDesc->d[vElement->pageId].pageId = vElement->pageId;
    pagesDesc->d[vElement->pageId].relevantCheckpoint = vElement->checkpointNumber;
    pagesDesc->d[vElement->pageId].pageDigest = vElement->pageDigest;
  }

  STDigest computedDigest;
  computeDigestOfPagesDescriptor(pagesDesc, computedDigest);
  LOG_INFO(getLogger(), pagesDesc->toString(computedDigest.toString()));
  psd_->free(pagesDesc);

  if (computedDigest != expectedDigestOfResPagesDescriptor) {
    LOG_WARN(getLogger(),
             "vblock defines invalid digest of pages descriptor: " << KVLOG(computedDigest,
                                                                            expectedDigestOfResPagesDescriptor));
    return false;
  }

  return true;
}

set<uint16_t> BCStateTran::allOtherReplicas() {
  set<uint16_t> others = replicas_;
  others.erase(config_.myReplicaId);
  return others;
}

void BCStateTran::SetAllReplicasAsPreferred() {
  sourceSelector_.setAllReplicasAsPreferred();
  metrics_.preferred_replicas_.Get().Set(sourceSelector_.preferredReplicasToString());
}

void BCStateTran::EnterGettingCheckpointSummariesState() {
  LOG_DEBUG(getLogger(), "");
  ConcordAssert(sourceSelector_.noPreferredReplicas());
  sourceSelector_.reset();
  metrics_.current_source_replica_.Get().Set(sourceSelector_.currentReplica());

  nextRequiredBlock_ = 0;
  digestOfNextRequiredBlock.makeZero();
  clearAllPendingItemsData();

  psd_->deleteCheckpointBeingFetched();
  ConcordAssertEQ(getFetchingState(), FetchingState::GettingCheckpointSummaries);
  verifyEmptyInfoAboutGettingCheckpointSummary();
  sendAskForCheckpointSummariesMsg();
}

void BCStateTran::reportCollectingStatus(const uint64_t firstRequiredBlock,
                                         const uint32_t actualBlockSize,
                                         bool toLog) {
  metrics_.overall_blocks_collected_++;
  metrics_.overall_bytes_collected_.Get().Set(metrics_.overall_bytes_collected_.Get().Get() + actualBlockSize);

  bytes_collected_.report(actualBlockSize, toLog);
  if (blocks_collected_.report(1, toLog)) {
    auto overall_block_results = blocks_collected_.getOverallResults();
    auto overall_bytes_results = bytes_collected_.getOverallResults();
    auto prev_win_block_results = blocks_collected_.getPrevWinResults();
    auto prev_win_bytes_results = bytes_collected_.getPrevWinResults();

    metrics_.overall_blocks_throughput_.Get().Set(overall_block_results.throughput_);
    metrics_.overall_bytes_throughput_.Get().Set(overall_bytes_results.throughput_);

    metrics_.prev_win_blocks_collected_.Get().Set(prev_win_block_results.num_processed_items_);
    metrics_.prev_win_blocks_throughput_.Get().Set(prev_win_block_results.throughput_);
    metrics_.prev_win_bytes_collected_.Get().Set(prev_win_bytes_results.num_processed_items_);
    metrics_.prev_win_bytes_throughput_.Get().Set(prev_win_bytes_results.throughput_);

    LOG_INFO(getLogger(), logsForCollectingStatus(firstRequiredBlock));
  }
}

std::string BCStateTran::logsForCollectingStatus(const uint64_t firstRequiredBlock) {
  std::ostringstream oss;
  std::unordered_map<std::string, std::string> result, nested_data, nested_nested_data;
  const DataStore::CheckpointDesc fetched_cp = psd_->getCheckpointBeingFetched();
  auto blocks_overall_r = blocks_collected_.getOverallResults();
  auto bytes_overall_r = bytes_collected_.getOverallResults();

  nested_data.insert(toPair(
      "collectRange", std::to_string(firstRequiredBlock) + ", " + std::to_string(firstCollectedBlockId_.value())));
  nested_data.insert(toPair("lastCollectedBlock", nextRequiredBlock_));
  nested_data.insert(toPair("blocksLeft", (nextRequiredBlock_ - firstRequiredBlock)));
  nested_data.insert(toPair("cycle", cycleCounter_));
  nested_data.insert(toPair("elapsedTime", std::to_string(blocks_overall_r.elapsed_time_ms_) + " ms"));
  nested_data.insert(toPair("collected",
                            std::to_string(blocks_overall_r.num_processed_items_) + " blk & " +
                                std::to_string(bytes_overall_r.num_processed_items_) + " B"));
  nested_data.insert(toPair("throughput",
                            std::to_string(blocks_overall_r.throughput_) + " blk/s & " +
                                std::to_string(bytes_overall_r.throughput_) + " B/s"));
  result.insert(
      toPair("overallStats", concordUtils::kvContainerToJson(nested_data, [](const auto &arg) { return arg; })));
  nested_data.clear();

  if (getMissingBlocksSummaryWindowSize > 0) {
    auto blocks_win_r = blocks_collected_.getPrevWinResults();
    auto bytes_win_r = bytes_collected_.getPrevWinResults();
    auto prev_win_index = blocks_collected_.getPrevWinIndex();

    nested_data.insert(toPair("index", prev_win_index));
    nested_data.insert(toPair("elapsedTime", std::to_string(blocks_win_r.elapsed_time_ms_) + " ms"));
    nested_data.insert(toPair("collected",
                              std::to_string(blocks_win_r.num_processed_items_) + " blk & " +
                                  std::to_string(bytes_win_r.num_processed_items_) + " B"));
    nested_data.insert(toPair(
        "throughput",
        std::to_string(blocks_win_r.throughput_) + " blk/s & " + std::to_string(bytes_win_r.throughput_) + " B/s"));
    result.insert(
        toPair("lastWindow", concordUtils::kvContainerToJson(nested_data, [](const auto &arg) { return arg; })));
    nested_data.clear();
  }

  nested_data.insert(toPair("lastStored", psd_->getLastStoredCheckpoint()));
  nested_nested_data.insert(toPair("checkpointNum", fetched_cp.checkpointNum));
  nested_nested_data.insert(toPair("lastBlock", fetched_cp.lastBlock));
  nested_data.insert(
      toPair("beingFetched", concordUtils::kvContainerToJson(nested_nested_data, [](const auto &arg) { return arg; })));
  result.insert(
      toPair("checkpointInfo", concordUtils::kvContainerToJson(nested_data, [](const auto &arg) { return arg; })));

  oss << concordUtils::kContainerToJson(result);
  return oss.str().c_str();
}

void BCStateTran::processData() {
  const FetchingState fs = getFetchingState();
  const auto fetchingState = fs;
  LOG_DEBUG(getLogger(), KVLOG(fetchingState));

  ConcordAssertOR(fs == FetchingState::GettingMissingBlocks, fs == FetchingState::GettingMissingResPages);
  ConcordAssert(sourceSelector_.hasPreferredReplicas());
  ConcordAssertLE(totalSizeOfPendingItemDataMsgs, config_.maxPendingDataFromSourceReplica);

  const bool isGettingBlocks = (fs == FetchingState::GettingMissingBlocks);

  ConcordAssertOR(!isGettingBlocks, psd_->getLastRequiredBlock() != 0);
  ConcordAssertOR(isGettingBlocks, psd_->getLastRequiredBlock() == 0);

  const uint64_t currTime = getMonotonicTimeMilli();
  bool badDataFromCurrentSourceReplica = false;

  while (true) {
    //////////////////////////////////////////////////////////////////////////
    // if needed, select a source replica
    //////////////////////////////////////////////////////////////////////////

    bool newSourceReplica = sourceSelector_.shouldReplaceSource(currTime, badDataFromCurrentSourceReplica);

    if (newSourceReplica) {
      sourceSelector_.removeCurrentReplica();
      if (fs == FetchingState::GettingMissingResPages && sourceSelector_.noPreferredReplicas()) {
        EnterGettingCheckpointSummariesState();
        return;
      }
      sourceSelector_.updateSource(currTime);
      auto currentSource = sourceSelector_.currentReplica();
      LOG_DEBUG(getLogger(), "Selected new source replica: " << currentSource);
      sources_.push_back(currentSource);
      metrics_.current_source_replica_.Get().Set(currentSource);
      metrics_.preferred_replicas_.Get().Set(sourceSelector_.preferredReplicasToString());
      badDataFromCurrentSourceReplica = false;
      clearAllPendingItemsData();
    }

    // We have a valid source replica at this point
    ConcordAssert(sourceSelector_.hasSource());
    ConcordAssertEQ(badDataFromCurrentSourceReplica, false);

    //////////////////////////////////////////////////////////////////////////
    // if needed, determine the next required block
    //////////////////////////////////////////////////////////////////////////
    if (nextRequiredBlock_ == 0) {
      ConcordAssert(digestOfNextRequiredBlock.isZero());

      DataStore::CheckpointDesc cp = psd_->getCheckpointBeingFetched();
      if (!isGettingBlocks) {
        nextRequiredBlock_ = ID_OF_VBLOCK_RES_PAGES;
        digestOfNextRequiredBlock = cp.digestOfResPagesDescriptor;
      } else {
        nextRequiredBlock_ = psd_->getLastRequiredBlock();

        // if this is the last block in this checkpoint
        if (cp.lastBlock == nextRequiredBlock_) {
          digestOfNextRequiredBlock = cp.digestOfLastBlock;
        } else {
          // we should already have block number nextRequiredBlock_+1
          ConcordAssert(as_->hasBlock(nextRequiredBlock_ + 1));
          as_->getPrevDigestFromBlock(nextRequiredBlock_ + 1,
                                      reinterpret_cast<StateTransferDigest *>(&digestOfNextRequiredBlock));
        }
        if (!firstCollectedBlockId_) firstCollectedBlockId_ = nextRequiredBlock_;
      }
    }

    ConcordAssertNE(nextRequiredBlock_, 0);
    ConcordAssert(!digestOfNextRequiredBlock.isZero());

    LOG_DEBUG(getLogger(), KVLOG(nextRequiredBlock_, digestOfNextRequiredBlock));

    //////////////////////////////////////////////////////////////////////////
    // Process and check the available chunks
    //////////////////////////////////////////////////////////////////////////

    int16_t lastChunkInRequiredBlock = 0;
    uint32_t actualBlockSize = 0;
    bool lastInBatch = false;

    const bool newBlock = getNextFullBlock(nextRequiredBlock_,
                                           badDataFromCurrentSourceReplica,
                                           lastChunkInRequiredBlock,
                                           buffer_,
                                           actualBlockSize,
                                           !isGettingBlocks,
                                           lastInBatch);
    bool newBlockIsValid = false;

    if (newBlock && isGettingBlocks) {
      TimeRecorder scoped_timer(*histograms_.dst_digest_calc_duration);
      ConcordAssert(!badDataFromCurrentSourceReplica);
      newBlockIsValid = checkBlock(nextRequiredBlock_, digestOfNextRequiredBlock, buffer_, actualBlockSize);
      badDataFromCurrentSourceReplica = !newBlockIsValid;
    } else if (newBlock && !isGettingBlocks) {
      ConcordAssert(!badDataFromCurrentSourceReplica);
      if (!config_.enableReservedPages)
        newBlockIsValid = true;
      else
        newBlockIsValid = checkVirtualBlockOfResPages(digestOfNextRequiredBlock, buffer_, actualBlockSize);

      badDataFromCurrentSourceReplica = !newBlockIsValid;
    } else {
      ConcordAssertAND(!newBlock, actualBlockSize == 0);
    }

    LOG_DEBUG(getLogger(), KVLOG(newBlock, newBlockIsValid, actualBlockSize));

    //////////////////////////////////////////////////////////////////////////
    // if we have a new block
    //////////////////////////////////////////////////////////////////////////
    const uint64_t firstRequiredBlock = psd_->getFirstRequiredBlock();
    if (!lastCollectedBlockId_) lastCollectedBlockId_ = firstRequiredBlock;
    if (newBlockIsValid && isGettingBlocks) {
      DataStoreTransaction::Guard g(psd_->beginTransaction());
      sourceSelector_.setSourceSelectionTime(currTime);

      ConcordAssertAND(lastChunkInRequiredBlock >= 1, actualBlockSize > 0);
      bool lastBlock = (firstRequiredBlock >= nextRequiredBlock_);

      // Report collecting status for every block collected. Log entry is created every fixed window
      // getMissingBlocksSummaryWindowSize If lastBlock is true: summarize the whole cycle without including "commit
      // to chain duration" and vblock. In that case last window might be less than the fixed
      // getMissingBlocksSummaryWindowSize
      reportCollectingStatus(firstRequiredBlock, actualBlockSize, lastBlock);
      if (lastBlock) {
        commitToChainDT_.start();
        blocks_collected_.pause();
        bytes_collected_.pause();
      } else {
        putBlocksStTempDT_.start();
      }
      betweenPutBlocksStTempDT_.pause();
      LOG_DEBUG(getLogger(), "Add block: " << std::boolalpha << KVLOG(lastBlock, nextRequiredBlock_, actualBlockSize));
      {
        TimeRecorder scoped_timer(*histograms_.dst_put_block_duration);
        ConcordAssert(as_->putBlock(nextRequiredBlock_, buffer_, actualBlockSize));
      }
      if (!lastBlock) {
        putBlocksStTempDT_.pause();
        betweenPutBlocksStTempDT_.start();
        as_->getPrevDigestFromBlock(nextRequiredBlock_,
                                    reinterpret_cast<StateTransferDigest *>(&digestOfNextRequiredBlock));
        nextRequiredBlock_--;
        g.txn()->setLastRequiredBlock(nextRequiredBlock_);
        if (lastInBatch) {
          //  last block in batch - send another FetchBlocksMsg since we havn't reach yet to firstRequiredBlock
          ConcordAssertEQ(psd_->getLastRequiredBlock(), nextRequiredBlock_);
          dst_time_between_sendFetchBlocksMsg_rec_.end();
          LOG_DEBUG(getLogger(), "Sending FetchBlocksMsg: lastInBatch is true");
          sendFetchBlocksMsg(firstRequiredBlock, nextRequiredBlock_, 0);
          break;
        }
      } else {
        // this is the last block we need
        // report collecting status (without vblock) into log
        commitToChainDT_.pause();
        g.txn()->setFirstRequiredBlock(0);
        g.txn()->setLastRequiredBlock(0);
        clearAllPendingItemsData();
        nextRequiredBlock_ = 0;
        digestOfNextRequiredBlock.makeZero();

        ConcordAssertEQ(getFetchingState(), FetchingState::GettingMissingResPages);

        // Log histograms for destination when GettingMissingBlocks is done
        // Do it for a cycle that lasted more than 10 seconds
        auto duration = cycleDT_.pause();
        if (duration > 10000) {
          auto &registrar = concord::diagnostics::RegistrarSingleton::getInstance();
          registrar.perf.snapshot("state_transfer");
          registrar.perf.snapshot("state_transfer_dest");
          LOG_INFO(getLogger(), registrar.perf.toString(registrar.perf.get("state_transfer")));
          LOG_INFO(getLogger(), registrar.perf.toString(registrar.perf.get("state_transfer_dest")));
        } else
          LOG_INFO(getLogger(),
                   "skip logging snapshots, cycle is very short (not enough statistics)" << KVLOG(duration));
        cycleDT_.start();
        LOG_DEBUG(getLogger(), "Moved to GettingMissingResPages");
        sendFetchResPagesMsg(0);
        break;
      }
    }
    //////////////////////////////////////////////////////////////////////////
    // if we have a new vblock
    //////////////////////////////////////////////////////////////////////////
    else if (newBlockIsValid && !isGettingBlocks) {
      DataStoreTransaction::Guard g(psd_->beginTransaction());
      sourceSelector_.setSourceSelectionTime(currTime);

      if (config_.enableReservedPages) {
        // set the updated pages
        uint32_t numOfUpdates = getNumberOfElements(buffer_);
        LOG_DEBUG(getLogger(), "numOfUpdates in vblock: " << numOfUpdates);
        for (uint32_t i = 0; i < numOfUpdates; i++) {
          ElementOfVirtualBlock *e = getVirtualElement(i, config_.sizeOfReservedPage, buffer_);
          g.txn()->setResPage(e->pageId, e->checkpointNumber, e->pageDigest, e->page);
          LOG_DEBUG(getLogger(), "Update page " << e->pageId);
        }
      }
      ConcordAssert(g.txn()->hasCheckpointBeingFetched());

      DataStore::CheckpointDesc cp = g.txn()->getCheckpointBeingFetched();

      // set stored data
      ConcordAssertEQ(g.txn()->getFirstRequiredBlock(), 0);
      ConcordAssertEQ(g.txn()->getLastRequiredBlock(), 0);
      ConcordAssertGT(cp.checkpointNum, g.txn()->getLastStoredCheckpoint());

      g.txn()->setCheckpointDesc(cp.checkpointNum, cp);
      g.txn()->deleteCheckpointBeingFetched();

      deleteOldCheckpoints(cp.checkpointNum, g.txn());

      sourceSelector_.reset();
      metrics_.preferred_replicas_.Get().Set("");
      metrics_.current_source_replica_.Get().Set(NO_REPLICA);

      nextRequiredBlock_ = 0;
      digestOfNextRequiredBlock.makeZero();
      clearAllPendingItemsData();

      // Metrics set at the end of the block to prevent transaction abort from
      // leaving inconsistencies.
      metrics_.preferred_replicas_.Get().Set("");
      metrics_.current_source_replica_.Get().Set(sourceSelector_.currentReplica());
      metrics_.last_stored_checkpoint_.Get().Set(cp.checkpointNum);
      metrics_.checkpoint_being_fetched_.Get().Set(0);

      checkConsistency(config_.pedanticChecks);

      // Completion
      LOG_INFO(getLogger(),
               "Invoking onTransferringComplete callbacks for checkpoint number: " << KVLOG(cp.checkpointNum));
      metrics_.on_transferring_complete_++;
      std::set<uint64_t> cb_keys;
      for (const auto &kv : on_transferring_complete_cb_registry_) {
        kv.second.invokeAll(cp.checkpointNum);
      }
      g.txn()->setIsFetchingState(false);
      ConcordAssertEQ(getFetchingState(), FetchingState::NotFetching);
      cycleEndSummary();
      break;
    }
    //////////////////////////////////////////////////////////////////////////
    // if we don't have new full block/vblock (but we did not detect a problem)
    //////////////////////////////////////////////////////////////////////////
    else if (!badDataFromCurrentSourceReplica) {
      bool retransmissionTimeoutExpired = sourceSelector_.retransmissionTimeoutExpired(currTime);
      if (newSourceReplica || retransmissionTimeoutExpired) {
        if (isGettingBlocks) {
          ConcordAssertEQ(psd_->getLastRequiredBlock(), nextRequiredBlock_);
          LOG_INFO(getLogger(), "Sending FetchBlocksMsg: " << KVLOG(newSourceReplica, retransmissionTimeoutExpired));
          sendFetchBlocksMsg(psd_->getFirstRequiredBlock(), nextRequiredBlock_, lastChunkInRequiredBlock);
        } else {
          sendFetchResPagesMsg(lastChunkInRequiredBlock);
        }
      }
      break;
    }
  }
}

void BCStateTran::cycleEndSummary() {
  Throughput::Results blocksCollectedResults;
  Throughput::Results bytesCollectedResults;
  std::ostringstream sources_str;
  std::string firstCollectedBlockIdstr;

  if (gettingMissingBlocksDT_.totalDuration() == 0)
    // we print summary only if we were collecting blocks
    return;

  blocksCollectedResults = blocks_collected_.getOverallResults();
  bytesCollectedResults = bytes_collected_.getOverallResults();
  blocks_collected_.end();
  bytes_collected_.end();
  std::copy(sources_.begin(), sources_.end() - 1, std::ostream_iterator<uint16_t>(sources_str, ","));
  sources_str << sources_.back();
  auto cycleDuration = cycleDT_.totalDuration(true);
  auto gettingCheckpointSummariesDuration = gettingCheckpointSummariesDT_.totalDuration(true);
  auto gettingMissingBlocksDuration = gettingMissingBlocksDT_.totalDuration(true);
  auto commitToChainDuration = commitToChainDT_.totalDuration(true);
  auto gettingMissingResPagesDuration = gettingMissingResPagesDT_.totalDuration(true);
  auto betweenPutBlocksStDuration = betweenPutBlocksStTempDT_.totalDuration(true);
  auto putBlocksStTempDuration = putBlocksStTempDT_.totalDuration(true);
  LOG_INFO(getLogger(),
           "State Transfer cycle ended (#"
               << cycleCounter_ << "), Total Duration: " << cycleDuration << "ms, "
               << "Time to get checkpoint summaries: " << gettingCheckpointSummariesDuration << "ms, "
               << "Time to fetch missing blocks: " << gettingMissingBlocksDuration << "ms, "
               << "Time to commit to chain: " << commitToChainDuration << "ms, "
               << "Time to get reserved pages (vblock): " << gettingMissingResPagesDuration << "ms, "
               << "Total time between putblock (GettingMissingBlocks) " << betweenPutBlocksStDuration << "ms, "
               << "Total time for putblock (GettingMissingBlocks) " << putBlocksStTempDuration << "ms, "
               << "Collected blocks range [" << std::to_string(lastCollectedBlockId_.value()) << ", "
               << std::to_string(firstCollectedBlockId_.value()) << "], Collected "
               << std::to_string(blocksCollectedResults.num_processed_items_) + " blocks and " +
                      std::to_string(bytesCollectedResults.num_processed_items_) + " bytes,"
               << " Throughput {GettingMissingBlocks}: " << blocksCollectedResults.throughput_ << " blocks/sec and "
               << bytesCollectedResults.throughput_ << " bytes/sec, Throughput {cycle}: "
               << static_cast<uint64_t>((1000 * blocksCollectedResults.num_processed_items_) / cycleDuration)
               << " blocks/sec and "
               << static_cast<uint64_t>((1000 * bytesCollectedResults.num_processed_items_) / cycleDuration)
               << " bytes/sec, #" << sources_.size() << " sources (first to last): [" << sources_str.str() << "]");
}

//////////////////////////////////////////////////////////////////////////////
// Consistency
//////////////////////////////////////////////////////////////////////////////

void BCStateTran::checkConsistency(bool checkAllBlocks) {
  ConcordAssert(psd_->initialized());
  const uint64_t lastReachableBlockNum = as_->getLastReachableBlockNum();
  const uint64_t lastBlockNum = as_->getLastBlockNum();
  const uint64_t genesisBlockNum = as_->getGenesisBlockNum();
  LOG_INFO(getLogger(), KVLOG(lastBlockNum, lastReachableBlockNum));

  const uint64_t firstStoredCheckpoint = psd_->getFirstStoredCheckpoint();
  const uint64_t lastStoredCheckpoint = psd_->getLastStoredCheckpoint();
  LOG_INFO(getLogger(), KVLOG(firstStoredCheckpoint, lastStoredCheckpoint));

  checkConfig();
  checkFirstAndLastCheckpoint(firstStoredCheckpoint, lastStoredCheckpoint);
  if (checkAllBlocks) {
    checkReachableBlocks(genesisBlockNum, lastReachableBlockNum);
  }
  checkUnreachableBlocks(lastReachableBlockNum, lastBlockNum);
  checkBlocksBeingFetchedNow(checkAllBlocks, lastReachableBlockNum, lastBlockNum);
  checkStoredCheckpoints(firstStoredCheckpoint, lastStoredCheckpoint);

  if (!psd_->getIsFetchingState()) {
    ConcordAssert(!psd_->hasCheckpointBeingFetched());
    ConcordAssertEQ(psd_->getFirstRequiredBlock(), 0);
    ConcordAssertEQ(psd_->getLastRequiredBlock(), 0);
  } else if (!psd_->hasCheckpointBeingFetched()) {
    ConcordAssertEQ(psd_->getFirstRequiredBlock(), 0);
    ConcordAssertEQ(psd_->getLastRequiredBlock(), 0);
    ConcordAssertEQ(psd_->numOfAllPendingResPage(), 0);
  } else if (psd_->getLastRequiredBlock() > 0) {
    ConcordAssertGT(psd_->getFirstRequiredBlock(), 0);
    ConcordAssertEQ(psd_->numOfAllPendingResPage(), 0);
  } else {
    ConcordAssertEQ(psd_->numOfAllPendingResPage(), 0);
  }
}

void BCStateTran::checkConfig() {
  ConcordAssertEQ(replicas_, psd_->getReplicas());
  ConcordAssertEQ(config_.myReplicaId, psd_->getMyReplicaId());
  ConcordAssertEQ(config_.fVal, psd_->getFVal());
  ConcordAssertEQ(maxNumOfStoredCheckpoints_, psd_->getMaxNumOfStoredCheckpoints());
  ConcordAssertEQ(numberOfReservedPages_, psd_->getNumberOfReservedPages());
}

void BCStateTran::checkFirstAndLastCheckpoint(uint64_t firstStoredCheckpoint, uint64_t lastStoredCheckpoint) {
  ConcordAssertGE(lastStoredCheckpoint, firstStoredCheckpoint);
  ConcordAssertLE(lastStoredCheckpoint - firstStoredCheckpoint + 1, maxNumOfStoredCheckpoints_);
  ConcordAssertOR((lastStoredCheckpoint == 0), psd_->hasCheckpointDesc(lastStoredCheckpoint));
  if ((firstStoredCheckpoint != 0) && (firstStoredCheckpoint != lastStoredCheckpoint) &&
      !psd_->hasCheckpointDesc(firstStoredCheckpoint)) {
    LOG_FATAL(getLogger(),
              KVLOG(firstStoredCheckpoint, lastStoredCheckpoint, psd_->hasCheckpointDesc(firstStoredCheckpoint)));
    ConcordAssert(false);
  }
}

void BCStateTran::checkReachableBlocks(uint64_t genesisBlockNum, uint64_t lastReachableBlockNum) {
  if (lastReachableBlockNum > 0) {
    for (uint64_t currBlock = lastReachableBlockNum - 1; currBlock >= genesisBlockNum; currBlock--) {
      auto currDigest = getBlockAndComputeDigest(currBlock);
      ConcordAssert(!currDigest.isZero());
      STDigest prevFromNextBlockDigest;
      prevFromNextBlockDigest.makeZero();
      as_->getPrevDigestFromBlock(currBlock + 1, reinterpret_cast<StateTransferDigest *>(&prevFromNextBlockDigest));
      ConcordAssertEQ(currDigest, prevFromNextBlockDigest);
    }
  }
}

void BCStateTran::checkUnreachableBlocks(uint64_t lastReachableBlockNum, uint64_t lastBlockNum) {
  ConcordAssertGE(lastBlockNum, lastReachableBlockNum);
  if (lastBlockNum > lastReachableBlockNum) {
    ConcordAssertEQ(getFetchingState(), FetchingState::GettingMissingBlocks);
    uint64_t x = lastBlockNum - 1;
    while (as_->hasBlock(x)) x--;

    // we should have a hole
    ConcordAssertGT(x, lastReachableBlockNum);

    // we should have a single hole
    for (uint64_t i = lastReachableBlockNum + 1; i <= x; i++) ConcordAssert(!as_->hasBlock(i));
  }
}

void BCStateTran::checkBlocksBeingFetchedNow(bool checkAllBlocks,
                                             uint64_t lastReachableBlockNum,
                                             uint64_t lastBlockNum) {
  if (lastBlockNum > lastReachableBlockNum) {
    ConcordAssertAND(psd_->getIsFetchingState(), psd_->hasCheckpointBeingFetched());
    ConcordAssertEQ(psd_->getFirstRequiredBlock() - 1, as_->getLastReachableBlockNum());
    ConcordAssertGE(psd_->getLastRequiredBlock(), psd_->getFirstRequiredBlock());

    if (checkAllBlocks) {
      uint64_t lastRequiredBlock = psd_->getLastRequiredBlock();

      for (uint64_t currBlock = lastBlockNum - 1; currBlock >= lastRequiredBlock + 1; currBlock--) {
        auto currDigest = getBlockAndComputeDigest(currBlock);
        ConcordAssert(!currDigest.isZero());

        STDigest prevFromNextBlockDigest;
        prevFromNextBlockDigest.makeZero();
        as_->getPrevDigestFromBlock(currBlock + 1, reinterpret_cast<StateTransferDigest *>(&prevFromNextBlockDigest));
        ConcordAssertEQ(currDigest, prevFromNextBlockDigest);
      }
    }
  }
}

void BCStateTran::checkStoredCheckpoints(uint64_t firstStoredCheckpoint, uint64_t lastStoredCheckpoint) {
  // check stored checkpoints
  if (lastStoredCheckpoint > 0) {
    uint64_t prevLastBlockNum = 0;
    for (uint64_t chkp = firstStoredCheckpoint; chkp <= lastStoredCheckpoint; chkp++) {
      if (!psd_->hasCheckpointDesc(chkp)) continue;

      DataStore::CheckpointDesc desc = psd_->getCheckpointDesc(chkp);
      ConcordAssertEQ(desc.checkpointNum, chkp);
      ConcordAssertLE(desc.lastBlock, as_->getLastReachableBlockNum());
      ConcordAssertGE(desc.lastBlock, prevLastBlockNum);
      prevLastBlockNum = desc.lastBlock;

      if (desc.lastBlock != 0 && desc.lastBlock >= as_->getGenesisBlockNum()) {
        auto computedBlockDigest = getBlockAndComputeDigest(desc.lastBlock);
        // Extra debugging needed here for BC-2821
        if (computedBlockDigest != desc.digestOfLastBlock) {
          uint32_t blockSize = 0;
          as_->getBlock(desc.lastBlock, buffer_, &blockSize);
          concordUtils::HexPrintBuffer blockData{buffer_, blockSize};
          LOG_FATAL(getLogger(), "Invalid stored checkpoint: " << KVLOG(desc.checkpointNum, desc.lastBlock, blockData));
          ConcordAssertEQ(computedBlockDigest, desc.digestOfLastBlock);
        }
      }
      if (config_.enableReservedPages) {
        // check all pages descriptor
        DataStore::ResPagesDescriptor *allPagesDesc = psd_->getResPagesDescriptor(chkp);
        ConcordAssertEQ(allPagesDesc->numOfPages, numberOfReservedPages_);
        {
          STDigest computedDigestOfResPagesDescriptor;
          computeDigestOfPagesDescriptor(allPagesDesc, computedDigestOfResPagesDescriptor);
          LOG_INFO(getLogger(), allPagesDesc->toString(computedDigestOfResPagesDescriptor.toString()));
          ConcordAssertEQ(computedDigestOfResPagesDescriptor, desc.digestOfResPagesDescriptor);
        }
        // check all pages descriptors
        std::unique_ptr<char[]> buffer(new char[config_.sizeOfReservedPage]);
        for (uint32_t pageId = 0; pageId < numberOfReservedPages_; pageId++) {
          uint64_t actualCheckpoint = 0;
          if (!psd_->getResPage(pageId, chkp, &actualCheckpoint, buffer.get(), config_.sizeOfReservedPage)) continue;

          ConcordAssertEQ(allPagesDesc->d[pageId].pageId, pageId);
          ConcordAssertLE(allPagesDesc->d[pageId].relevantCheckpoint, chkp);
          ConcordAssertGT(allPagesDesc->d[pageId].relevantCheckpoint, 0);
          ConcordAssertEQ(allPagesDesc->d[pageId].relevantCheckpoint, actualCheckpoint);

          STDigest computedDigestOfPage;
          computeDigestOfPage(pageId, actualCheckpoint, buffer.get(), config_.sizeOfReservedPage, computedDigestOfPage);
          ConcordAssertEQ(computedDigestOfPage, allPagesDesc->d[pageId].pageDigest);
        }
        psd_->free(allPagesDesc);
      }
    }
  }
}

///////////////////////////////////////////////////////////////////////////
// Compute digests
///////////////////////////////////////////////////////////////////////////

void BCStateTran::computeDigestOfPage(
    const uint32_t pageId, const uint64_t checkpointNumber, const char *page, uint32_t pageSize, STDigest &outDigest) {
  DigestContext c;
  c.update(reinterpret_cast<const char *>(&pageId), sizeof(pageId));
  c.update(reinterpret_cast<const char *>(&checkpointNumber), sizeof(checkpointNumber));
  if (checkpointNumber > 0) c.update(page, pageSize);
  c.writeDigest(reinterpret_cast<char *>(&outDigest));
}

void BCStateTran::computeDigestOfPagesDescriptor(const DataStore::ResPagesDescriptor *pagesDesc, STDigest &outDigest) {
  DigestContext c;
  c.update(reinterpret_cast<const char *>(pagesDesc), pagesDesc->size());
  c.writeDigest(reinterpret_cast<char *>(&outDigest));
}

static void computeDigestOfBlockImpl(const uint64_t blockNum,
                                     const char *block,
                                     const uint32_t blockSize,
                                     char *outDigest) {
  ConcordAssertGT(blockNum, 0);
  ConcordAssertGT(blockSize, 0);
  DigestContext c;
  c.update(reinterpret_cast<const char *>(&blockNum), sizeof(blockNum));
  c.update(block, blockSize);
  c.writeDigest(outDigest);
}

void BCStateTran::computeDigestOfBlock(const uint64_t blockNum,
                                       const char *block,
                                       const uint32_t blockSize,
                                       STDigest *outDigest) {
  /*
  // for debug (the digest will be the block number)
  memset(outDigest, 0, sizeof(STDigest));
  uint64_t* p = (uint64_t*)outDigest;
  *p = blockNum;
  */
  computeDigestOfBlockImpl(blockNum, block, blockSize, reinterpret_cast<char *>(outDigest));
}

std::array<std::uint8_t, BLOCK_DIGEST_SIZE> BCStateTran::computeDigestOfBlock(const uint64_t blockNum,
                                                                              const char *block,
                                                                              const uint32_t blockSize) {
  std::array<std::uint8_t, BLOCK_DIGEST_SIZE> outDigest;
  computeDigestOfBlockImpl(blockNum, block, blockSize, reinterpret_cast<char *>(outDigest.data()));
  return outDigest;
}

STDigest BCStateTran::getBlockAndComputeDigest(uint64_t currBlock) {
  // This function is called among others during checkpointing of current state,
  // which can occur while this replica is a source replica.
  // In order to make it thread safe, instead of using buffer_, a local buffer is allocated .
  static std::unique_ptr<char[]> buffer(new char[maxItemSize_]);
  STDigest currDigest;
  uint32_t blockSize = 0;
  as_->getBlock(currBlock, buffer.get(), &blockSize);
  computeDigestOfBlock(currBlock, buffer.get(), blockSize, &currDigest);
  return currDigest;
}

void BCStateTran::SetAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) {
  metrics_component_.SetAggregator(aggregator);
}

inline std::string BCStateTran::getSequenceNumber(uint16_t replicaId,
                                                  uint64_t seqNum,
                                                  uint16_t blockNum,
                                                  uint64_t chunkNum) {
  return std::to_string(replicaId) + "-" + std::to_string(seqNum) + "-" + std::to_string(blockNum) + "-" +
         std::to_string(chunkNum);
}

}  // namespace impl
}  // namespace bcst
}  // namespace bftEngine
