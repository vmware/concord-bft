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
#include <list>
#include <set>
#include <string>
#include <sstream>
#include <functional>

#include "assertUtils.hpp"
#include "hex_tools.h"
#include "BCStateTran.hpp"
#include "STDigest.hpp"
#include "InMemoryDataStore.hpp"

#include "DBDataStore.hpp"
#include "storage/db_interface.h"
#include "storage/key_manipulator_interface.h"
#include "memorydb/client.h"
#include "Handoff.hpp"

// TODO(GG): for debugging - remove
// #define DEBUG_SEND_CHECKPOINTS_IN_REVERSE_ORDER (1)

using std::tie;
using std::chrono::steady_clock;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::time_point;
using std::chrono::system_clock;
using namespace std::placeholders;

namespace bftEngine {
namespace SimpleBlockchainStateTransfer {

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
    ds = new impl::DBDataStore(dbc, config.sizeOfReservedPage, stKeyManipulator);
  return new impl::BCStateTran(config, stateApi, ds);
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
// Logger
//////////////////////////////////////////////////////////////////////////////

logging::Logger STLogger = logging::getLogger("state-transfer");

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

static uint16_t calcMaxNumOfChunksInBlock(uint32_t maxItemSize,
                                          uint32_t maxBlockSize,
                                          uint32_t maxChunkSize,
                                          bool isVBlock) {
  if (!isVBlock) {
    uint16_t retVal =
        (maxBlockSize % maxChunkSize == 0) ? (maxBlockSize / maxChunkSize) : (maxBlockSize / maxChunkSize + 1);
    return retVal;
  } else {
    uint16_t retVal =
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
      randomGen_{randomDevice_()},
      sourceSelector_{SourceSelector(
          allOtherReplicas(), config_.fetchRetransmissionTimeoutMilli, config_.sourceReplicaReplacementTimeoutMilli)},
      last_metrics_dump_time_(0),
      metrics_dump_interval_in_sec_{config_.metricsDumpIntervalSeconds},
      metrics_component_{
          concordMetrics::Component("bc_state_transfer", std::make_shared<concordMetrics::Aggregator>())},

      // We must make sure that we actually initialize all these metrics in the
      // same order as defined in the header file.
      metrics_{
          metrics_component_.RegisterStatus("fetching_state", stateName(FetchingState::NotFetching)),
          metrics_component_.RegisterStatus("pedantic_checks_enabled", config_.pedanticChecks ? "true" : "false"),
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
          metrics_component_.RegisterGauge("last_block_", 0),
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

          metrics_component_.RegisterCounter("create_checkpoint"),
          metrics_component_.RegisterCounter("mark_checkpoint_as_stable"),
          metrics_component_.RegisterCounter("load_reserved_page"),
          metrics_component_.RegisterCounter("load_reserved_page_from_pending"),
          metrics_component_.RegisterCounter("load_reserved_page_from_checkpoint"),
          metrics_component_.RegisterCounter("save_reserved_page"),
          metrics_component_.RegisterCounter("zero_reserved_page"),
          metrics_component_.RegisterCounter("start_collecting_state"),
          metrics_component_.RegisterCounter("on_timer"),
          metrics_component_.RegisterCounter("on_transferring_complete"),
      } {
  AssertNE(stateApi, nullptr);
  AssertGE(replicas_.size(), 3U * config_.fVal + 1U);
  AssertEQ(replicas_.count(config_.myReplicaId), 1);
  AssertGE(config_.maxNumOfReservedPages, 2);

  // Register metrics component with the default aggregator.
  metrics_component_.Register();

  // TODO(GG): more asserts
  buffer_ = reinterpret_cast<char *>(std::malloc(maxItemSize_));
  LOG_INFO(STLogger,
           "Creating BCStateTran object:"
               << " myId_=" << config_.myReplicaId << " fVal_=" << config_.fVal << " maxVBlockSize_=" << maxVBlockSize_
               << " maxChunkSize_=" << config_.maxChunkSize
               << " maxNumberOfChunksInBatch_=" << config_.maxNumberOfChunksInBatch
               << " maxPendingDataFromSourceReplica_=" << config_.maxPendingDataFromSourceReplica
               << " maxNumOfReservedPages_=" << config_.maxNumOfReservedPages << "config_.sizeOfReservedPage_="
               << config_.sizeOfReservedPage << " refreshTimerMilli_=" << config_.refreshTimerMilli
               << " checkpointSummariesRetransmissionTimeoutMilli_="
               << config_.checkpointSummariesRetransmissionTimeoutMilli
               << " maxAcceptableMsgDelayMilli_=" << config_.maxAcceptableMsgDelayMilli
               << " sourceReplicaReplacementTimeoutMilli_=" << config_.sourceReplicaReplacementTimeoutMilli
               << " fetchRetransmissionTimeoutMilli_=" << config_.fetchRetransmissionTimeoutMilli << " maxBlockSize_="
               << config_.maxBlockSize << " maxNumOfChunksInAppBlock_=" << maxNumOfChunksInAppBlock_
               << " maxNumOfChunksInVBlock_=" << maxNumOfChunksInVBlock_);

  if (config_.isReadOnly) {
    messageHandler_ = std::bind(&BCStateTran::handoff, this, _1, _2, _3);
  } else {
    messageHandler_ = std::bind(&BCStateTran::handleStateTransferMessageImp, this, _1, _2, _3);
  }
}

BCStateTran::~BCStateTran() {
  Assert(!running_);
  Assert(cacheOfVirtualBlockForResPages.empty());
  Assert(pendingItemDataMsgs.empty());

  std::free(buffer_);
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
    Assert(!running_);
    AssertEQ(replicaForStateTransfer_, nullptr);
    AssertEQ(sizeOfReservedPage, config_.sizeOfReservedPage);

    maxNumOfStoredCheckpoints_ = maxNumOfRequiredStoredCheckpoints;
    numberOfReservedPages_ = numberOfRequiredReservedPages;
    metrics_.number_of_reserved_pages_.Get().Set(numberOfReservedPages_);
    metrics_.size_of_reserved_page_.Get().Set(sizeOfReservedPage);

    memset(buffer_, 0, maxItemSize_);

    LOG_INFO(STLogger,
             "Init BCStateTran object:" << KVLOG(
                 maxNumOfStoredCheckpoints_, numberOfReservedPages_, config_.sizeOfReservedPage));

    if (psd_->initialized()) {
      LOG_INFO(STLogger, "Loading existing data from storage");

      checkConsistency(config_.pedanticChecks);

      FetchingState fs = getFetchingState();
      LOG_INFO(STLogger, "Starting state is " << stateName(fs));

      if (fs == FetchingState::GettingMissingBlocks || fs == FetchingState::GettingMissingResPages) {
        SetAllReplicasAsPreferred();
      }
      loadMetrics();
    } else {
      LOG_INFO(STLogger, "Initializing a new object");

      AssertGE(maxNumOfRequiredStoredCheckpoints, 2);
      AssertLE(maxNumOfRequiredStoredCheckpoints, kMaxNumOfStoredCheckpoints);
      AssertGE(numberOfRequiredReservedPages, 2);
      AssertLE(numberOfRequiredReservedPages, config_.maxNumOfReservedPages);

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

      AssertEQ(getFetchingState(), FetchingState::NotFetching);
    }
  } catch (const std::exception &e) {
    LOG_FATAL(STLogger, e.what());
    exit(1);
  }
}

void BCStateTran::startRunning(IReplicaForStateTransfer *r) {
  LOG_INFO(STLogger, "");

  AssertNE(r, nullptr);
  running_ = true;
  replicaForStateTransfer_ = r;
  replicaForStateTransfer_->changeStateTransferTimerPeriod(config_.refreshTimerMilli);
}

void BCStateTran::stopRunning() {
  LOG_INFO(STLogger, "");

  Assert(running_);
  AssertNE(replicaForStateTransfer_, nullptr);

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
  AssertEQ(lastBlock, as_->getLastBlockNum());
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

  LOG_INFO(STLogger,
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
  LOG_INFO(STLogger,
           "Associating pending pages with checkpoint: " << KVLOG(numberOfPagesInCheckpoint, checkpointNumber));

  for (uint32_t p : pages) {
    STDigest d;
    txn->getPendingResPage(p, buffer_, config_.sizeOfReservedPage);
    computeDigestOfPage(p, checkpointNumber, buffer_, config_.sizeOfReservedPage, d);
    txn->associatePendingResPageWithCheckpoint(p, checkpointNumber, d);
  }

  memset(buffer_, 0, config_.sizeOfReservedPage);
  AssertEQ(txn->numOfAllPendingResPage(), 0);
  DataStore::ResPagesDescriptor *allPagesDesc = txn->getResPagesDescriptor(checkpointNumber);
  AssertEQ(allPagesDesc->numOfPages, numberOfReservedPages_);

  STDigest digestOfResPagesDescriptor;
  computeDigestOfPagesDescriptor(allPagesDesc, digestOfResPagesDescriptor);

  txn->free(allPagesDesc);
  return digestOfResPagesDescriptor;
}

void BCStateTran::deleteOldCheckpoints(uint64_t checkpointNumber, DataStoreTransaction *txn) {
  uint64_t minRelevantCheckpoint = 0;
  if (checkpointNumber >= maxNumOfStoredCheckpoints_) {
    minRelevantCheckpoint = checkpointNumber - maxNumOfStoredCheckpoints_ + 1;
  }

  const uint64_t oldFirstStoredCheckpoint = txn->getFirstStoredCheckpoint();

  if (minRelevantCheckpoint >= 2 && minRelevantCheckpoint > oldFirstStoredCheckpoint) {
    txn->deleteDescOfSmallerCheckpoints(minRelevantCheckpoint);
    txn->deleteCoveredResPageInSmallerCheckpoints(minRelevantCheckpoint);
  }

  if (minRelevantCheckpoint > oldFirstStoredCheckpoint) {
    txn->setFirstStoredCheckpoint(minRelevantCheckpoint);
  }

  txn->setLastStoredCheckpoint(checkpointNumber);

  auto firstStoredCheckpoint = std::max(minRelevantCheckpoint, oldFirstStoredCheckpoint);
  auto lastStoredCheckpoint = checkpointNumber;
  LOG_INFO(STLogger,
           KVLOG(checkpointNumber,
                 minRelevantCheckpoint,
                 oldFirstStoredCheckpoint,
                 firstStoredCheckpoint,
                 lastStoredCheckpoint));
}

void BCStateTran::createCheckpointOfCurrentState(uint64_t checkpointNumber) {
  auto lastStoredCheckpointNumber = psd_->getLastStoredCheckpoint();
  LOG_INFO(STLogger, KVLOG(checkpointNumber, lastStoredCheckpointNumber));

  Assert(running_);
  Assert(!isFetching());
  AssertGT(checkpointNumber, 0);
  AssertGT(checkpointNumber, lastStoredCheckpointNumber);

  metrics_.create_checkpoint_.Get().Inc();

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
  Assert(running_);
  Assert(!isFetching());
  AssertGT(checkpointNumber, 0);

  metrics_.mark_checkpoint_as_stable_.Get().Inc();

  const uint64_t lastStoredCheckpoint = psd_->getLastStoredCheckpoint();
  metrics_.last_stored_checkpoint_.Get().Set(lastStoredCheckpoint);

  LOG_INFO(STLogger, KVLOG(checkpointNumber, lastStoredCheckpoint));

  AssertOR((lastStoredCheckpoint < maxNumOfStoredCheckpoints_),
           (checkpointNumber >= lastStoredCheckpoint - maxNumOfStoredCheckpoints_ + 1));
  AssertLE(checkpointNumber, psd_->getLastStoredCheckpoint());
}

void BCStateTran::getDigestOfCheckpoint(uint64_t checkpointNumber, uint16_t sizeOfDigestBuffer, char *outDigestBuffer) {
  Assert(running_);
  Assert(!isFetching());
  AssertGE(sizeOfDigestBuffer, sizeof(STDigest));
  AssertGT(checkpointNumber, 0);
  AssertGE(checkpointNumber, psd_->getFirstStoredCheckpoint());
  AssertLE(checkpointNumber, psd_->getLastStoredCheckpoint());
  Assert(psd_->hasCheckpointDesc(checkpointNumber));

  DataStore::CheckpointDesc desc = psd_->getCheckpointDesc(checkpointNumber);
  STDigest checkpointDigest;
  DigestContext c;
  c.update(reinterpret_cast<char *>(&desc), sizeof(desc));
  c.writeDigest(checkpointDigest.getForUpdate());

  LOG_INFO(STLogger,
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
  AssertLT(reservedPageId, numberOfReservedPages_);
  AssertLE(copyLength, config_.sizeOfReservedPage);

  metrics_.load_reserved_page_.Get().Inc();

  if (psd_->hasPendingResPage(reservedPageId)) {
    LOG_DEBUG(STLogger, "Loaded pending reserved page: " << reservedPageId);
    metrics_.load_reserved_page_from_pending_.Get().Inc();
    psd_->getPendingResPage(reservedPageId, outReservedPage, copyLength);
  } else {
    uint64_t lastCheckpoint = psd_->getLastStoredCheckpoint();
    // case when the system is restarted before reaching the first checkpoint
    if (lastCheckpoint == 0) return false;
    uint64_t actualCheckpoint = UINT64_MAX;
    metrics_.load_reserved_page_from_checkpoint_.Get().Inc();
    if (!psd_->getResPage(reservedPageId, lastCheckpoint, &actualCheckpoint, outReservedPage, copyLength)) return false;
    AssertLE(actualCheckpoint, lastCheckpoint);
    LOG_DEBUG(STLogger,
              "Reserved page loaded from checkpoint: " << KVLOG(reservedPageId, actualCheckpoint, lastCheckpoint));
  }
  return true;
}
// TODO(TK) check if this function can have its own transaction(bftimpl)
void BCStateTran::saveReservedPage(uint32_t reservedPageId, uint32_t copyLength, const char *inReservedPage) {
  try {
    LOG_DEBUG(STLogger, reservedPageId);

    Assert(!isFetching());
    AssertLT(reservedPageId, numberOfReservedPages_);
    AssertLE(copyLength, config_.sizeOfReservedPage);

    metrics_.save_reserved_page_.Get().Inc();

    psd_->setPendingResPage(reservedPageId, inReservedPage, copyLength);
  } catch (std::out_of_range &e) {
    LOG_ERROR(STLogger, "Failed to save pending reserved page: " << e.what() << ": " << KVLOG(reservedPageId));
    throw;
  }
}
// TODO(TK) check if this function can have its own transaction(bftimpl)
void BCStateTran::zeroReservedPage(uint32_t reservedPageId) {
  LOG_DEBUG(STLogger, reservedPageId);

  Assert(!isFetching());
  AssertLT(reservedPageId, numberOfReservedPages_);

  metrics_.zero_reserved_page_.Get().Inc();
  memset(buffer_, 0, config_.sizeOfReservedPage);
  psd_->setPendingResPage(reservedPageId, buffer_, config_.sizeOfReservedPage);
}

void BCStateTran::startCollectingState() {
  LOG_INFO(STLogger, "");

  Assert(running_);
  Assert(!isFetching());
  metrics_.start_collecting_state_.Get().Inc();

  verifyEmptyInfoAboutGettingCheckpointSummary();
  {  // txn scope
    DataStoreTransaction::Guard g(psd_->beginTransaction());
    g.txn()->deleteAllPendingPages();
    g.txn()->setIsFetchingState(true);
  }
  sendAskForCheckpointSummariesMsg();
}

void BCStateTran::onTimer() {
  if (!running_) return;

  metrics_.on_timer_.Get().Inc();
  // Send all metrics to the aggregator
  metrics_component_.UpdateAggregator();

  // Dump metrics to log
  auto currTimeForDumping =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch());
  if (currTimeForDumping - last_metrics_dump_time_ >= metrics_dump_interval_in_sec_) {
    last_metrics_dump_time_ = currTimeForDumping;
    LOG_INFO(STLogger, "--BCStateTransfer metrics dump--" + metrics_component_.ToJson());
  }
  auto currTime = getMonotonicTimeMilli();
  FetchingState fs = getFetchingState();
  if (fs == FetchingState::GettingCheckpointSummaries) {
    if ((currTime - lastTimeSentAskForCheckpointSummariesMsg) > config_.checkpointSummariesRetransmissionTimeoutMilli) {
      if (++retransmissionNumberOfAskForCheckpointSummariesMsg > kResetCount_AskForCheckpointSummaries)
        clearInfoAboutGettingCheckpointSummary();

      sendAskForCheckpointSummariesMsg();
    }
  } else if (fs == FetchingState::GettingMissingBlocks || fs == FetchingState::GettingMissingResPages) {
    processData();
  }
}

void BCStateTran::handleStateTransferMessage(char *msg, uint32_t msgLen, uint16_t senderId) {
  Assert(running_);
  bool invalidSender = replicas_.count(senderId) == 0;
  bool sentFromSelf = senderId == config_.myReplicaId;
  bool msgSizeTooSmall = msgLen < sizeof(BCStateTranBaseMsg);
  if (msgSizeTooSmall || sentFromSelf || invalidSender) {
    metrics_.received_illegal_msg_.Get().Inc();
    LOG_WARN(STLogger, "Illegal message: " << KVLOG(msgLen, senderId, msgSizeTooSmall, sentFromSelf, invalidSender));
    replicaForStateTransfer_->freeStateTransferMsg(msg);
    return;
  }
  messageHandler_(msg, msgLen, senderId);
}

std::string BCStateTran::getStatus() {
  std::ostringstream oss;
  auto state = getFetchingState();
  auto current_source = sourceSelector_.currentReplica();
  auto preferred_replicas = sourceSelector_.preferredReplicasToString();

  oss << "Fetching state: " << stateName(state) << std::endl;
  oss << KVLOG(lastMsgSeqNum_, cacheOfVirtualBlockForResPages.size()) << std::endl << std::endl;
  oss << "Last Msg Sequence Numbers (Replica ID: SeqNum):" << std::endl;
  for (auto &[id, seq_num] : lastMsgSeqNumOfReplicas_) {
    oss << "  " << id << ": " << seq_num << std::endl;
  }
  oss << std::endl << std::endl;

  oss << "Cache Of virtual blocks for reserved pages:" << std::endl;
  for (auto entry : cacheOfVirtualBlockForResPages) {
    auto vblockDescriptor = entry.first;
    oss << "  " << KVLOG(vblockDescriptor.checkpointNum, vblockDescriptor.lastCheckpointKnownToRequester) << std::endl;
  }
  oss << std::endl << std::endl;

  if (isFetching()) {
    oss << KVLOG(current_source, preferred_replicas, nextRequiredBlock_, totalSizeOfPendingItemDataMsgs) << std::endl;
  }
  return oss.str();
}

void BCStateTran::handoff(char *msg, uint32_t msgLen, uint16_t senderId) {
  static concord::util::Handoff handoff_(config_.myReplicaId);
  handoff_.push(std::bind(&BCStateTran::handleStateTransferMessageImp, this, msg, msgLen, senderId));
}

// this function can be executed in context of another thread.
void BCStateTran::handleStateTransferMessageImp(char *msg, uint32_t msgLen, uint16_t senderId) {
  BCStateTranBaseMsg *msgHeader = reinterpret_cast<BCStateTranBaseMsg *>(msg);
  LOG_DEBUG(STLogger, "new message with type=" << msgHeader->type);

  FetchingState fs = getFetchingState();
  bool noDelete = false;
  switch (msgHeader->type) {
    case MsgType::AskForCheckpointSummaries:
      if (fs == FetchingState::NotFetching)
        noDelete = onMessage(reinterpret_cast<AskForCheckpointSummariesMsg *>(msg), msgLen, senderId);
      break;
    case MsgType::CheckpointsSummary:
      if (fs == FetchingState::GettingCheckpointSummaries)
        noDelete = onMessage(reinterpret_cast<CheckpointSummaryMsg *>(msg), msgLen, senderId);
      break;
    case MsgType::FetchBlocks:
      noDelete = onMessage(reinterpret_cast<FetchBlocksMsg *>(msg), msgLen, senderId);
      break;
    case MsgType::FetchResPages:
      noDelete = onMessage(reinterpret_cast<FetchResPagesMsg *>(msg), msgLen, senderId);
      break;
    case MsgType::RejectFetching:
      if (fs == FetchingState::GettingMissingBlocks || fs == FetchingState::GettingMissingResPages)
        noDelete = onMessage(reinterpret_cast<RejectFetchingMsg *>(msg), msgLen, senderId);
      break;
    case MsgType::ItemData:
      if (fs == FetchingState::GettingMissingBlocks || fs == FetchingState::GettingMissingResPages)
        noDelete = onMessage(reinterpret_cast<ItemDataMsg *>(msg), msgLen, senderId);
      break;
    default:
      break;
  }

  if (!noDelete) replicaForStateTransfer_->freeStateTransferMsg(msg);
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
  AssertLT(index, h->numberOfUpdatedPages);

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
  std::chrono::time_point<std::chrono::system_clock> n = std::chrono::system_clock::now();
  const uint64_t milli = std::chrono::duration_cast<std::chrono::milliseconds>(n.time_since_epoch()).count();

  if (milli > lastMilliOfUniqueFetchID_) {
    lastMilliOfUniqueFetchID_ = milli;
    lastCountOfUniqueFetchID_ = 0;
  } else {
    if (lastCountOfUniqueFetchID_ == 0x3FFFFF) {
      LOG_WARN(STLogger, "SeqNum Counter reached max value");
      lastMilliOfUniqueFetchID_++;
      lastCountOfUniqueFetchID_ = 0;
    } else {
      lastCountOfUniqueFetchID_++;
    }
  }

  uint64_t r = (lastMilliOfUniqueFetchID_ << (64 - 42));
  AssertLE(lastCountOfUniqueFetchID_, 0x3FFFFF);
  r = r | ((uint64_t)lastCountOfUniqueFetchID_);
  return r;
}

bool BCStateTran::checkValidityAndSaveMsgSeqNum(uint16_t replicaId, uint64_t msgSeqNum) {
  uint64_t milliMsgTime = ((msgSeqNum) >> (64 - 42));

  time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
  const uint64_t milliNow = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
  uint64_t diffMilli = ((milliMsgTime > milliNow) ? (milliMsgTime - milliNow) : (milliNow - milliMsgTime));

  if (diffMilli > config_.maxAcceptableMsgDelayMilli) {
    auto excessiveMilliseconds = diffMilli - config_.maxAcceptableMsgDelayMilli;
    LOG_WARN(STLogger, "Msg rejected because it is too old: " << KVLOG(replicaId, msgSeqNum, excessiveMilliseconds));
    return false;
  }

  auto p = lastMsgSeqNumOfReplicas_.find(replicaId);
  if (p != lastMsgSeqNumOfReplicas_.end() && p->second >= msgSeqNum) {
    auto lastMsgSeqNum = p->second;
    LOG_WARN(
        STLogger,
        "Msg rejected because its sequence number is not monotonic: " << KVLOG(replicaId, msgSeqNum, lastMsgSeqNum));
    return false;
  }

  lastMsgSeqNumOfReplicas_[replicaId] = msgSeqNum;
  LOG_DEBUG(STLogger, "Msg accepted: " << KVLOG(msgSeqNum));
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
      Assert(false);
      return "Error";
  }
}

std::ostream &operator<<(std::ostream &os, const BCStateTran::FetchingState fs) {
  os << BCStateTran::stateName(fs);
  return os;
}

bool BCStateTran::isFetching() const { return (psd_->getIsFetchingState()); }

BCStateTran::FetchingState BCStateTran::getFetchingState() const {
  if (!psd_->getIsFetchingState()) return FetchingState::NotFetching;

  AssertEQ(psd_->numOfAllPendingResPage(), 0);

  if (!psd_->hasCheckpointBeingFetched()) return FetchingState::GettingCheckpointSummaries;

  if (psd_->getLastRequiredBlock() > 0) return FetchingState::GettingMissingBlocks;

  AssertEQ(psd_->getFirstRequiredBlock(), 0);

  return FetchingState::GettingMissingResPages;
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
  AssertEQ(getFetchingState(), FetchingState::GettingCheckpointSummaries);
  metrics_.sent_ask_for_checkpoint_summaries_msg_.Get().Inc();

  AskForCheckpointSummariesMsg msg;
  lastTimeSentAskForCheckpointSummariesMsg = getMonotonicTimeMilli();
  lastMsgSeqNum_ = uniqueMsgSeqNum();
  metrics_.last_msg_seq_num_.Get().Set(lastMsgSeqNum_);

  msg.msgSeqNum = lastMsgSeqNum_;
  msg.minRelevantCheckpointNum = psd_->getLastStoredCheckpoint() + 1;

  LOG_DEBUG(STLogger, KVLOG(lastMsgSeqNum_, msg.minRelevantCheckpointNum));

  sendToAllOtherReplicas(reinterpret_cast<char *>(&msg), sizeof(AskForCheckpointSummariesMsg));
}

void BCStateTran::sendFetchBlocksMsg(uint64_t firstRequiredBlock,
                                     uint64_t lastRequiredBlock,
                                     int16_t lastKnownChunkInLastRequiredBlock) {
  Assert(sourceSelector_.hasSource());
  metrics_.sent_fetch_blocks_msg_.Get().Inc();

  FetchBlocksMsg msg;
  lastMsgSeqNum_ = uniqueMsgSeqNum();
  metrics_.last_msg_seq_num_.Get().Set(lastMsgSeqNum_);

  msg.msgSeqNum = lastMsgSeqNum_;
  msg.firstRequiredBlock = firstRequiredBlock;
  msg.lastRequiredBlock = lastRequiredBlock;
  msg.lastKnownChunkInLastRequiredBlock = lastKnownChunkInLastRequiredBlock;

  LOG_DEBUG(STLogger,
            KVLOG(sourceSelector_.currentReplica(),
                  msg.msgSeqNum,
                  msg.firstRequiredBlock,
                  msg.lastRequiredBlock,
                  msg.lastKnownChunkInLastRequiredBlock));

  sourceSelector_.setSendTime(getMonotonicTimeMilli());
  replicaForStateTransfer_->sendStateTransferMessage(
      reinterpret_cast<char *>(&msg), sizeof(FetchBlocksMsg), sourceSelector_.currentReplica());
}

void BCStateTran::sendFetchResPagesMsg(int16_t lastKnownChunkInLastRequiredBlock) {
  Assert(sourceSelector_.hasSource());
  Assert(psd_->hasCheckpointBeingFetched());

  metrics_.sent_fetch_res_pages_msg_.Get().Inc();

  DataStore::CheckpointDesc cp = psd_->getCheckpointBeingFetched();
  uint64_t lastStoredCheckpoint = psd_->getLastStoredCheckpoint();
  lastMsgSeqNum_ = uniqueMsgSeqNum();
  metrics_.last_msg_seq_num_.Get().Set(lastMsgSeqNum_);

  FetchResPagesMsg msg;
  msg.msgSeqNum = lastMsgSeqNum_;
  msg.lastCheckpointKnownToRequester = lastStoredCheckpoint;
  msg.requiredCheckpointNum = cp.checkpointNum;
  msg.lastKnownChunk = lastKnownChunkInLastRequiredBlock;

  LOG_DEBUG(STLogger,
            KVLOG(sourceSelector_.currentReplica(),
                  msg.msgSeqNum,
                  msg.lastCheckpointKnownToRequester,
                  msg.requiredCheckpointNum,
                  msg.lastKnownChunk));

  sourceSelector_.setSendTime(getMonotonicTimeMilli());
  replicaForStateTransfer_->sendStateTransferMessage(
      reinterpret_cast<char *>(&msg), sizeof(FetchResPagesMsg), sourceSelector_.currentReplica());
}

//////////////////////////////////////////////////////////////////////////////
// Message handlers
//////////////////////////////////////////////////////////////////////////////

bool BCStateTran::onMessage(const AskForCheckpointSummariesMsg *m, uint32_t msgLen, uint16_t replicaId) {
  LOG_DEBUG(STLogger, "");

  Assert(!psd_->getIsFetchingState());

  metrics_.received_ask_for_checkpoint_summaries_msg_.Get().Inc();

  // if msg is invalid
  if (msgLen < sizeof(AskForCheckpointSummariesMsg) || m->minRelevantCheckpointNum == 0 || m->msgSeqNum == 0) {
    LOG_WARN(STLogger, "Msg is invalid: " << KVLOG(msgLen, m->minRelevantCheckpointNum, m->msgSeqNum));
    metrics_.invalid_ask_for_checkpoint_summaries_msg_.Get().Inc();
    return false;
  }

  // if msg is not relevant
  auto lastStoredCheckpoint = psd_->getLastStoredCheckpoint();
  if (auto seqNumInvalid = !checkValidityAndSaveMsgSeqNum(replicaId, m->msgSeqNum) ||
                           (m->minRelevantCheckpointNum > lastStoredCheckpoint)) {
    LOG_WARN(STLogger,
             "Msg is irrelevant: " << KVLOG(seqNumInvalid, m->minRelevantCheckpointNum, lastStoredCheckpoint));
    metrics_.irrelevant_ask_for_checkpoint_summaries_msg_.Get().Inc();
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

#ifdef DEBUG_SEND_CHECKPOINTS_IN_REVERSE_ORDER
  for (uint64_t i = fromCheckpoint; i <= toCheckpoint; i++)
#else
  for (uint64_t i = toCheckpoint; i >= fromCheckpoint; i--)
#endif
  {
    if (!psd_->hasCheckpointDesc(i)) continue;

    DataStore::CheckpointDesc c = psd_->getCheckpointDesc(i);
    CheckpointSummaryMsg checkpointSummary;

    checkpointSummary.checkpointNum = i;
    checkpointSummary.lastBlock = c.lastBlock;
    checkpointSummary.digestOfLastBlock = c.digestOfLastBlock;
    checkpointSummary.digestOfResPagesDescriptor = c.digestOfResPagesDescriptor;
    checkpointSummary.requestMsgSeqNum = m->msgSeqNum;

    LOG_INFO(STLogger,
             "Sending CheckpointSummaryMsg: " << KVLOG(toReplicaId,
                                                       checkpointSummary.checkpointNum,
                                                       checkpointSummary.lastBlock,
                                                       checkpointSummary.digestOfLastBlock,
                                                       checkpointSummary.digestOfResPagesDescriptor,
                                                       checkpointSummary.requestMsgSeqNum));

    replicaForStateTransfer_->sendStateTransferMessage(
        reinterpret_cast<char *>(&checkpointSummary), sizeof(CheckpointSummaryMsg), replicaId);

    metrics_.sent_checkpoint_summary_msg_.Get().Inc();
    sent = true;
  }

  if (!sent) {
    LOG_INFO(STLogger, "Failed to send relevant CheckpointSummaryMsg: " << KVLOG(toReplicaId));
  }
  return false;
}

bool BCStateTran::onMessage(const CheckpointSummaryMsg *m, uint32_t msgLen, uint16_t replicaId) {
  LOG_DEBUG(STLogger, "");

  FetchingState fs = getFetchingState();
  AssertEQ(fs, FetchingState::GettingCheckpointSummaries);
  metrics_.received_checkpoint_summary_msg_.Get().Inc();

  // if msg is invalid
  if (msgLen < sizeof(CheckpointSummaryMsg) || m->checkpointNum == 0 || m->digestOfResPagesDescriptor.isZero() ||
      m->requestMsgSeqNum == 0) {
    LOG_WARN(STLogger,
             "Msg is invalid: " << KVLOG(
                 replicaId, msgLen, m->checkpointNum, m->digestOfResPagesDescriptor.isZero(), m->requestMsgSeqNum));
    metrics_.invalid_checkpoint_summary_msg_.Get().Inc();
    return false;
  }

  // if msg is not relevant
  if (m->requestMsgSeqNum != lastMsgSeqNum_ || m->checkpointNum <= psd_->getLastStoredCheckpoint()) {
    LOG_WARN(STLogger,
             "Msg is irrelevant: " << KVLOG(
                 replicaId, m->requestMsgSeqNum, lastMsgSeqNum_, m->checkpointNum, psd_->getLastStoredCheckpoint()));
    metrics_.irrelevant_checkpoint_summary_msg_.Get().Inc();
    return false;
  }

  uint16_t numOfMsgsFromSender =
      (numOfSummariesFromOtherReplicas.count(replicaId) == 0) ? 0 : numOfSummariesFromOtherReplicas.at(replicaId);

  // if we have too many messages from the same replica
  if (numOfMsgsFromSender >= (psd_->getMaxNumOfStoredCheckpoints() + 1)) {
    LOG_WARN(STLogger, "Too many messages from replica: " << KVLOG(replicaId, numOfMsgsFromSender));
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
    LOG_DEBUG(STLogger, "Does not have enough CheckpointSummaryMsg messages");
    return true;
  }

  LOG_DEBUG(STLogger, "Has enough CheckpointSummaryMsg messages");
  CheckpointSummaryMsg *checkSummary = cert->bestCorrectMsg();

  AssertNE(checkSummary, nullptr);
  Assert(sourceSelector_.isReset());
  AssertEQ(nextRequiredBlock_, 0);
  Assert(digestOfNextRequiredBlock.isZero());
  Assert(pendingItemDataMsgs.empty());
  AssertEQ(totalSizeOfPendingItemDataMsgs, 0);

  // set the preferred replicas
  for (uint16_t r : replicas_) {  // TODO(GG): can be improved
    CheckpointSummaryMsg *t = cert->getMsgFromReplica(r);
    if (t != nullptr && CheckpointSummaryMsg::equivalent(t, checkSummary)) sourceSelector_.addPreferredReplica(r);
  }

  metrics_.preferred_replicas_.Get().Set(sourceSelector_.preferredReplicasToString());

  AssertGE(sourceSelector_.numberOfPreferredReplicas(), config_.fVal + 1);

  // set new checkpoint
  DataStore::CheckpointDesc newCheckpoint;

  newCheckpoint.checkpointNum = checkSummary->checkpointNum;
  newCheckpoint.lastBlock = checkSummary->lastBlock;
  newCheckpoint.digestOfLastBlock = checkSummary->digestOfLastBlock;
  newCheckpoint.digestOfResPagesDescriptor = checkSummary->digestOfResPagesDescriptor;

  auto fetchingState = stateName(getFetchingState());
  {  // txn scope
    DataStoreTransaction::Guard g(psd_->beginTransaction());
    Assert(!g.txn()->hasCheckpointBeingFetched());
    g.txn()->setCheckpointBeingFetched(newCheckpoint);
    metrics_.checkpoint_being_fetched_.Get().Set(newCheckpoint.checkpointNum);

    // clean
    clearInfoAboutGettingCheckpointSummary();
    lastMsgSeqNum_ = 0;
    metrics_.last_msg_seq_num_.Get().Set(0);

    // check if we need to fetch blocks, or reserved pages
    const uint64_t lastReachableBlockNum = as_->getLastReachableBlockNum();
    metrics_.last_reachable_block_.Get().Set(lastReachableBlockNum);

    LOG_INFO(STLogger,
             "Start fetching checkpoint: " << KVLOG(newCheckpoint.checkpointNum,
                                                    newCheckpoint.lastBlock,
                                                    newCheckpoint.digestOfLastBlock,
                                                    newCheckpoint.digestOfResPagesDescriptor,
                                                    lastReachableBlockNum,
                                                    fetchingState));

    if (newCheckpoint.lastBlock > lastReachableBlockNum) {
      g.txn()->setFirstRequiredBlock(lastReachableBlockNum + 1);
      g.txn()->setLastRequiredBlock(newCheckpoint.lastBlock);
    } else {
      AssertEQ(newCheckpoint.lastBlock, lastReachableBlockNum);
      AssertEQ(g.txn()->getFirstRequiredBlock(), 0);
      AssertEQ(g.txn()->getLastRequiredBlock(), 0);
    }
  }
  metrics_.last_block_.Get().Set(newCheckpoint.lastBlock);
  metrics_.fetching_state_.Get().Set(fetchingState);

  processData();
  return true;
}

bool BCStateTran::onMessage(const FetchBlocksMsg *m, uint32_t msgLen, uint16_t replicaId) {
  LOG_DEBUG(STLogger, "");
  metrics_.received_fetch_blocks_msg_.Get().Inc();

  // if msg is invalid
  if (msgLen < sizeof(FetchBlocksMsg) || m->msgSeqNum == 0 || m->firstRequiredBlock == 0 ||
      m->lastRequiredBlock < m->firstRequiredBlock) {
    LOG_WARN(STLogger,
             "Msg is invalid: " << KVLOG(replicaId, m->msgSeqNum, m->firstRequiredBlock, m->lastRequiredBlock));
    metrics_.invalid_fetch_blocks_msg_.Get().Inc();
    return false;
  }

  // if msg is not relevant
  if (!checkValidityAndSaveMsgSeqNum(replicaId, m->msgSeqNum)) {
    LOG_WARN(STLogger, "Msg is irrelevant: " << KVLOG(replicaId, m->msgSeqNum));
    metrics_.irrelevant_fetch_blocks_msg_.Get().Inc();
    return false;
  }

  FetchingState fetchingState = getFetchingState();
  auto lastReachableBlockNum = as_->getLastReachableBlockNum();

  // if msg should be rejected
  if (fetchingState != FetchingState::NotFetching || m->lastRequiredBlock > lastReachableBlockNum) {
    RejectFetchingMsg outMsg;
    outMsg.requestMsgSeqNum = m->msgSeqNum;

    LOG_WARN(STLogger,
             "Rejecting msg. Sending RejectFetchingMsg to replica: " << KVLOG(
                 replicaId, outMsg.requestMsgSeqNum, fetchingState, m->lastRequiredBlock, lastReachableBlockNum));
    metrics_.sent_reject_fetch_msg_.Get().Inc();

    replicaForStateTransfer_->sendStateTransferMessage(
        reinterpret_cast<char *>(&outMsg), sizeof(RejectFetchingMsg), replicaId);
    return false;
  }

  // compute information about next block and chunk
  uint64_t nextBlock = m->lastRequiredBlock;
  uint32_t sizeOfNextBlock = 0;
  bool tmp = as_->getBlock(nextBlock, buffer_, &sizeOfNextBlock);
  Assert(tmp);
  AssertGT(sizeOfNextBlock, 0);

  uint32_t sizeOfLastChunk = config_.maxChunkSize;
  uint32_t numOfChunksInNextBlock = sizeOfNextBlock / config_.maxChunkSize;
  if (sizeOfNextBlock % config_.maxChunkSize != 0) {
    sizeOfLastChunk = sizeOfNextBlock % config_.maxChunkSize;
    numOfChunksInNextBlock++;
  }

  uint16_t nextChunk = m->lastKnownChunkInLastRequiredBlock + 1;

  // if msg is invalid (lastKnownChunkInLastRequiredBlock+1 does not exist)
  if (nextChunk > numOfChunksInNextBlock) {
    LOG_WARN(STLogger, "Msg is invalid: illegal chunk number: " << KVLOG(replicaId, nextChunk, numOfChunksInNextBlock));
    memset(buffer_, 0, sizeOfNextBlock);
    return false;
  }

  // send chunks
  uint16_t numOfSentChunks = 0;
  while (true) {
    uint32_t chunkSize = (nextChunk < numOfChunksInNextBlock) ? config_.maxChunkSize : sizeOfLastChunk;

    AssertGT(chunkSize, 0);

    char *pRawChunk = buffer_ + (nextChunk - 1) * config_.maxChunkSize;
    ItemDataMsg *outMsg = ItemDataMsg::alloc(chunkSize);  // TODO(GG): improve

    outMsg->requestMsgSeqNum = m->msgSeqNum;
    outMsg->blockNumber = nextBlock;
    outMsg->totalNumberOfChunksInBlock = numOfChunksInNextBlock;
    outMsg->chunkNumber = nextChunk;
    outMsg->dataSize = chunkSize;
    memcpy(outMsg->data, pRawChunk, chunkSize);

    LOG_DEBUG(STLogger,
              "Sending ItemDataMsg: " << KVLOG(replicaId,
                                               outMsg->requestMsgSeqNum,
                                               outMsg->blockNumber,
                                               outMsg->totalNumberOfChunksInBlock,
                                               outMsg->chunkNumber,
                                               outMsg->dataSize));

    metrics_.sent_item_data_msg_.Get().Inc();
    replicaForStateTransfer_->sendStateTransferMessage(reinterpret_cast<char *>(outMsg), outMsg->size(), replicaId);

    ItemDataMsg::free(outMsg);
    numOfSentChunks++;

    // if we've already sent enough chunks
    if (numOfSentChunks >= config_.maxNumberOfChunksInBatch) {
      LOG_DEBUG(STLogger, "Sent enough chunks: " << KVLOG(numOfSentChunks));
      break;
    }
    // if we still have chunks in block
    else if (static_cast<uint16_t>(nextChunk + 1) <= numOfChunksInNextBlock) {
      nextChunk++;
    }
    // we sent all relevant blocks
    else if (nextBlock - 1 < m->firstRequiredBlock) {
      LOG_DEBUG(STLogger, "Sent all relevant blocks: " << KVLOG(m->firstRequiredBlock));
      break;
    } else {
      nextBlock--;
      LOG_DEBUG(STLogger, "Start sending next block: " << KVLOG(nextBlock));
      memset(buffer_, 0, sizeOfNextBlock);
      sizeOfNextBlock = 0;
      bool tmp2 = as_->getBlock(nextBlock, buffer_, &sizeOfNextBlock);
      Assert(tmp2);
      AssertGT(sizeOfNextBlock, 0);

      sizeOfLastChunk = config_.maxChunkSize;
      numOfChunksInNextBlock = sizeOfNextBlock / config_.maxChunkSize;
      if (sizeOfNextBlock % config_.maxChunkSize != 0) {
        sizeOfLastChunk = sizeOfNextBlock % config_.maxChunkSize;
        numOfChunksInNextBlock++;
      }
      nextChunk = 1;
    }
  }
  memset(buffer_, 0, sizeOfNextBlock);
  return false;
}

bool BCStateTran::onMessage(const FetchResPagesMsg *m, uint32_t msgLen, uint16_t replicaId) {
  LOG_DEBUG(STLogger, "");
  metrics_.received_fetch_res_pages_msg_.Get().Inc();

  // if msg is invalid
  if (msgLen < sizeof(FetchResPagesMsg) || m->msgSeqNum == 0 || m->requiredCheckpointNum == 0) {
    LOG_WARN(STLogger, "Msg is invalid: " << KVLOG(replicaId, msgLen, m->msgSeqNum, m->requiredCheckpointNum));
    metrics_.invalid_fetch_res_pages_msg_.Get().Inc();
    return false;
  }

  // if msg is not relevant
  if (!checkValidityAndSaveMsgSeqNum(replicaId, m->msgSeqNum)) {
    LOG_WARN(STLogger, "Msg is irrelevant: " << KVLOG(replicaId, m->msgSeqNum));
    metrics_.irrelevant_fetch_res_pages_msg_.Get().Inc();
    return false;
  }

  FetchingState fetchingState = getFetchingState();

  // if msg should be rejected
  if (fetchingState != FetchingState::NotFetching || !psd_->hasCheckpointDesc(m->requiredCheckpointNum)) {
    RejectFetchingMsg outMsg;
    outMsg.requestMsgSeqNum = m->msgSeqNum;

    LOG_WARN(STLogger,
             "Rejecting msg. Sending RejectFetchingMsg to replica "
                 << KVLOG(replicaId, fetchingState, outMsg.requestMsgSeqNum, m->requiredCheckpointNum));

    metrics_.sent_reject_fetch_msg_.Get().Inc();

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
    LOG_DEBUG(STLogger,
              "Creating a new vblock: " << KVLOG(
                  replicaId, descOfVBlock.checkpointNum, descOfVBlock.lastCheckpointKnownToRequester));

    // TODO(GG): consider adding protection against bad replicas
    // that lead to unnecessary creations of vblocks
    vblock = createVBlock(descOfVBlock);
    AssertNE(vblock, nullptr);
    setVBlockInCache(descOfVBlock, vblock);

    AssertLE(cacheOfVirtualBlockForResPages.size(), kMaxVBlocksInCache);
  }

  uint32_t vblockSize = getSizeOfVirtualBlock(vblock, config_.sizeOfReservedPage);

  AssertGE(vblockSize, sizeof(HeaderOfVirtualBlock));
  Assert(checkStructureOfVirtualBlock(vblock, vblockSize, config_.sizeOfReservedPage));

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
    LOG_WARN(STLogger, "Msg is invalid: illegal chunk number: " << KVLOG(replicaId, nextChunk, numOfChunksInVBlock));
    return false;
  }

  // send chunks
  uint16_t numOfSentChunks = 0;
  while (true) {
    uint32_t chunkSize = (nextChunk < numOfChunksInVBlock) ? config_.maxChunkSize : sizeOfLastChunk;
    AssertGT(chunkSize, 0);

    char *pRawChunk = vblock + (nextChunk - 1) * config_.maxChunkSize;
    ItemDataMsg *outMsg = ItemDataMsg::alloc(chunkSize);

    outMsg->requestMsgSeqNum = m->msgSeqNum;
    outMsg->blockNumber = ID_OF_VBLOCK_RES_PAGES;
    outMsg->totalNumberOfChunksInBlock = numOfChunksInVBlock;
    outMsg->chunkNumber = nextChunk;
    outMsg->dataSize = chunkSize;
    memcpy(outMsg->data, pRawChunk, chunkSize);

    LOG_DEBUG(STLogger,
              "Sending ItemDataMsg: " << KVLOG(replicaId,
                                               outMsg->requestMsgSeqNum,
                                               outMsg->blockNumber,
                                               outMsg->totalNumberOfChunksInBlock,
                                               outMsg->chunkNumber,
                                               outMsg->dataSize));
    metrics_.sent_item_data_msg_.Get().Inc();

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
  LOG_DEBUG(STLogger, "");
  metrics_.received_reject_fetching_msg_.Get().Inc();

  FetchingState fs = getFetchingState();
  if (fs != FetchingState::GettingMissingBlocks && fs != FetchingState::GettingMissingResPages) {
    LOG_FATAL(STLogger,
              "Expected Fetching State GettingMissingBlocks or GettingMissingResPages. Got: " << stateName(fs));
    Assert(false);
  }
  Assert(sourceSelector_.hasPreferredReplicas());

  // if msg is invalid
  if (msgLen < sizeof(RejectFetchingMsg)) {
    LOG_WARN(STLogger, "Msg is invalid: " << KVLOG(replicaId, msgLen));
    metrics_.invalid_reject_fetching_msg_.Get().Inc();
    return false;
  }

  // if msg is not relevant
  if (sourceSelector_.currentReplica() != replicaId || lastMsgSeqNum_ != m->requestMsgSeqNum) {
    LOG_WARN(
        STLogger,
        "Msg is irrelevant" << KVLOG(replicaId, sourceSelector_.currentReplica(), lastMsgSeqNum_, m->requestMsgSeqNum));
    metrics_.irrelevant_reject_fetching_msg_.Get().Inc();
    return false;
  }

  Assert(sourceSelector_.isPreferred(replicaId));

  LOG_WARN(STLogger, "Removing replica from preferred replicas: " << KVLOG(replicaId));
  sourceSelector_.removeCurrentReplica();
  metrics_.current_source_replica_.Get().Set(NO_REPLICA);
  metrics_.preferred_replicas_.Get().Set(sourceSelector_.preferredReplicasToString());
  clearAllPendingItemsData();

  if (sourceSelector_.hasPreferredReplicas()) {
    processData();
  } else if (fs == FetchingState::GettingMissingBlocks) {
    LOG_DEBUG(STLogger, "Adding all peer replicas to preferredReplicas_ (because preferredReplicas_.size()==0)");

    // in this case, we will try to use all other replicas
    SetAllReplicasAsPreferred();
    processData();
  } else if (fs == FetchingState::GettingMissingResPages) {
    EnterGettingCheckpointSummariesState();
  } else {
    Assert(false);
  }
  return false;
}

// Retrieve either a chunk of a block or a reserved page when fetching
bool BCStateTran::onMessage(const ItemDataMsg *m, uint32_t msgLen, uint16_t replicaId) {
  LOG_DEBUG(STLogger, "");
  metrics_.received_item_data_msg_.Get().Inc();

  FetchingState fs = getFetchingState();
  if (fs != FetchingState::GettingMissingBlocks && fs != FetchingState::GettingMissingResPages) {
    LOG_FATAL(STLogger,
              "Expected Fetching State GettingMissingBlocks or GettingMissingResPages. Got: " << stateName(fs));
    Assert(false);
  }

  const uint16_t MaxNumOfChunksInBlock =
      (fs == FetchingState::GettingMissingBlocks) ? maxNumOfChunksInAppBlock_ : maxNumOfChunksInVBlock_;

  LOG_DEBUG(STLogger, KVLOG(m->blockNumber, m->totalNumberOfChunksInBlock, m->chunkNumber, m->dataSize));

  // if msg is invalid
  if (msgLen < m->size() || m->requestMsgSeqNum == 0 || m->blockNumber == 0 || m->totalNumberOfChunksInBlock == 0 ||
      m->totalNumberOfChunksInBlock > MaxNumOfChunksInBlock || m->chunkNumber == 0 || m->dataSize == 0) {
    LOG_WARN(STLogger,
             "Msg is invalid: " << KVLOG(replicaId,
                                         msgLen,
                                         m->size(),
                                         m->requestMsgSeqNum,
                                         m->blockNumber,
                                         m->totalNumberOfChunksInBlock,
                                         MaxNumOfChunksInBlock,
                                         m->chunkNumber,
                                         m->dataSize));
    metrics_.invalid_item_data_msg_.Get().Inc();
    return false;
  }

  //  const DataStore::CheckpointDesc fcp = psd_->getCheckpointBeingFetched();
  const uint64_t firstRequiredBlock = psd_->getFirstRequiredBlock();
  const uint64_t lastRequiredBlock = psd_->getLastRequiredBlock();

  auto fetchingState = fs;
  if (fs == FetchingState::GettingMissingBlocks) {
    // if msg is not relevant
    if (sourceSelector_.currentReplica() != replicaId || m->requestMsgSeqNum != lastMsgSeqNum_ ||
        m->blockNumber > lastRequiredBlock || m->blockNumber < firstRequiredBlock ||
        (m->blockNumber + config_.maxNumberOfChunksInBatch + 1 < lastRequiredBlock) ||
        m->dataSize + totalSizeOfPendingItemDataMsgs > config_.maxPendingDataFromSourceReplica) {
      LOG_WARN(STLogger,
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
      metrics_.irrelevant_item_data_msg_.Get().Inc();
      return false;
    }
  } else {
    AssertEQ(firstRequiredBlock, 0);
    AssertEQ(lastRequiredBlock, 0);

    // if msg is not relevant
    if (sourceSelector_.currentReplica() != replicaId || m->requestMsgSeqNum != lastMsgSeqNum_ ||
        m->blockNumber != ID_OF_VBLOCK_RES_PAGES ||
        m->dataSize + totalSizeOfPendingItemDataMsgs > config_.maxPendingDataFromSourceReplica) {
      LOG_WARN(STLogger,
               "Msg is irrelevant: " << KVLOG(replicaId,
                                              fetchingState,
                                              sourceSelector_.currentReplica(),
                                              m->requestMsgSeqNum,
                                              lastMsgSeqNum_,
                                              (m->blockNumber == ID_OF_VBLOCK_RES_PAGES),
                                              m->dataSize,
                                              totalSizeOfPendingItemDataMsgs,
                                              config_.maxPendingDataFromSourceReplica));
      metrics_.irrelevant_item_data_msg_.Get().Inc();
      return false;
    }
  }

  Assert(sourceSelector_.isPreferred(replicaId));

  bool added = false;

  tie(std::ignore, added) = pendingItemDataMsgs.insert(const_cast<ItemDataMsg *>(m));

  if (added) {
    LOG_DEBUG(STLogger,
              "ItemDataMsg was added to pendingItemDataMsgs: " << KVLOG(replicaId, fetchingState, m->requestMsgSeqNum));
    metrics_.num_pending_item_data_msgs_.Get().Set(pendingItemDataMsgs.size());
    totalSizeOfPendingItemDataMsgs += m->dataSize;
    metrics_.total_size_of_pending_item_data_msgs_.Get().Set(totalSizeOfPendingItemDataMsgs);
    processData();
    return true;
  } else {
    LOG_INFO(
        STLogger,
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

  AssertNE(vBlock, nullptr);

  HeaderOfVirtualBlock *header = reinterpret_cast<HeaderOfVirtualBlock *>(vBlock);

  AssertEQ(desc.lastCheckpointKnownToRequester, header->lastCheckpointKnownToRequester);

  return vBlock;
}

void BCStateTran::setVBlockInCache(const DescOfVBlockForResPages &desc, char *vBlock) {
  auto p = cacheOfVirtualBlockForResPages.find(desc);

  AssertEQ(p, cacheOfVirtualBlockForResPages.end());

  if (cacheOfVirtualBlockForResPages.size() == kMaxVBlocksInCache) {
    auto minItem = cacheOfVirtualBlockForResPages.begin();
    std::free(minItem->second);
    cacheOfVirtualBlockForResPages.erase(minItem);
  }

  cacheOfVirtualBlockForResPages[desc] = vBlock;
  AssertLE(cacheOfVirtualBlockForResPages.size(), kMaxVBlocksInCache);
}

char *BCStateTran::createVBlock(const DescOfVBlockForResPages &desc) {
  Assert(psd_->hasCheckpointDesc(desc.checkpointNum));

  // find the updated pages
  std::list<uint32_t> updatedPages;

  for (uint32_t i = 0; i < numberOfReservedPages_; i++) {
    uint64_t actualPageCheckpoint = 0;
    if (!psd_->getResPage(i, desc.checkpointNum, &actualPageCheckpoint)) continue;

    AssertLE(actualPageCheckpoint, desc.checkpointNum);

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
    Assert(checkStructureOfVirtualBlock(rawVBlock, size, config_.sizeOfReservedPage));
    LOG_DEBUG(STLogger, "New vblock contains 0 updated pages: " << KVLOG(desc.checkpointNum, size));
    return rawVBlock;
  }

  char *elements = rawVBlock + sizeof(HeaderOfVirtualBlock);

  uint32_t idx = 0;
  for (uint32_t pageId : updatedPages) {
    AssertLT(idx, numberOfUpdatedPages);

    uint64_t actualPageCheckpoint = 0;
    STDigest pageDigest;
    psd_->getResPage(
        pageId, desc.checkpointNum, &actualPageCheckpoint, &pageDigest, buffer_, config_.sizeOfReservedPage);
    AssertLE(actualPageCheckpoint, desc.checkpointNum);
    AssertGT(actualPageCheckpoint, desc.lastCheckpointKnownToRequester);
    Assert(!pageDigest.isZero());

    ElementOfVirtualBlock *currElement = reinterpret_cast<ElementOfVirtualBlock *>(elements + idx * elementSize);
    currElement->pageId = pageId;
    currElement->checkpointNumber = actualPageCheckpoint;
    currElement->pageDigest = pageDigest;
    memcpy(currElement->page, buffer_, config_.sizeOfReservedPage);
    memset(buffer_, 0, config_.sizeOfReservedPage);

    LOG_DEBUG(STLogger,
              "Adding page to vBlock: " << KVLOG(
                  currElement->pageId, currElement->checkpointNumber, currElement->pageDigest));
    idx++;
  }

  AssertEQ(idx, numberOfUpdatedPages);
  AssertOR(!config_.pedanticChecks, checkStructureOfVirtualBlock(rawVBlock, size, config_.sizeOfReservedPage));

  LOG_DEBUG(STLogger,
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
  AssertEQ(lastTimeSentAskForCheckpointSummariesMsg, 0);
  AssertEQ(retransmissionNumberOfAskForCheckpointSummariesMsg, 0);
  Assert(summariesCerts.empty());
  Assert(numOfSummariesFromOtherReplicas.empty());
}

///////////////////////////////////////////////////////////////////////////
// for states GettingMissingBlocks or GettingMissingResPages
///////////////////////////////////////////////////////////////////////////

void BCStateTran::clearAllPendingItemsData() {
  LOG_DEBUG(STLogger, "");

  for (auto i : pendingItemDataMsgs) replicaForStateTransfer_->freeStateTransferMsg(reinterpret_cast<char *>(i));

  pendingItemDataMsgs.clear();
  totalSizeOfPendingItemDataMsgs = 0;
  metrics_.num_pending_item_data_msgs_.Get().Set(0);
  metrics_.total_size_of_pending_item_data_msgs_.Get().Set(0);
}

void BCStateTran::clearPendingItemsData(uint64_t untilBlock) {
  LOG_DEBUG(STLogger, KVLOG(untilBlock));

  if (untilBlock == 0) return;

  auto it = pendingItemDataMsgs.begin();
  while (it != pendingItemDataMsgs.end() && (*it)->blockNumber >= untilBlock) {
    AssertGE(totalSizeOfPendingItemDataMsgs, (*it)->dataSize);

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
                                   bool isVBLock) {
  AssertGE(requiredBlock, 1);

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
    AssertGT(msg->totalNumberOfChunksInBlock, 0);
    AssertGE(msg->chunkNumber, 1);

    if (totalNumberOfChunks == 0) totalNumberOfChunks = msg->totalNumberOfChunksInBlock;

    blockSize += msg->dataSize;
    if (totalNumberOfChunks != msg->totalNumberOfChunksInBlock || msg->chunkNumber > totalNumberOfChunks ||
        blockSize > maxSize) {
      badData = true;
      break;
    }

    if (maxAvailableChunk + 1 < msg->chunkNumber) break;  // we have a hole

    AssertEQ(maxAvailableChunk + 1, msg->chunkNumber);

    maxAvailableChunk = msg->chunkNumber;

    AssertLE(maxAvailableChunk, totalNumberOfChunks);

    if (maxAvailableChunk == totalNumberOfChunks) {
      fullBlock = true;
      break;
    }
    ++it;
  }

  if (badData) {
    Assert(!fullBlock);
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

  it = pendingItemDataMsgs.begin();
  while (true) {
    AssertNE(it, pendingItemDataMsgs.end());
    AssertEQ((*it)->blockNumber, requiredBlock);

    ItemDataMsg *msg = *it;

    AssertGE(msg->chunkNumber, 1);
    AssertEQ(msg->totalNumberOfChunksInBlock, totalNumberOfChunks);
    AssertEQ(currentChunk + 1, msg->chunkNumber);
    AssertLE(currentPos + msg->dataSize, maxSize);

    memcpy(outBlock + currentPos, msg->data, msg->dataSize);
    currentChunk = msg->chunkNumber;
    currentPos += msg->dataSize;
    totalSizeOfPendingItemDataMsgs -= (*it)->dataSize;
    it = pendingItemDataMsgs.erase(it);
    metrics_.num_pending_item_data_msgs_.Get().Set(pendingItemDataMsgs.size());
    metrics_.total_size_of_pending_item_data_msgs_.Get().Set(totalSizeOfPendingItemDataMsgs);

    if (currentChunk == totalNumberOfChunks) {
      outBlockSize = currentPos;
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
    LOG_WARN(STLogger, "Incorrect digest: " << KVLOG(blockNum, blockDigest, expectedBlockDigest));
    return false;
  } else {
    return true;
  }
}

bool BCStateTran::checkVirtualBlockOfResPages(const STDigest &expectedDigestOfResPagesDescriptor,
                                              char *vblock,
                                              uint32_t vblockSize) const {
  LOG_DEBUG(STLogger, "");
  if (!checkStructureOfVirtualBlock(vblock, vblockSize, config_.sizeOfReservedPage)) {
    LOG_WARN(STLogger, "vblock has illegal structure");
    return false;
  }

  HeaderOfVirtualBlock *h = reinterpret_cast<HeaderOfVirtualBlock *>(vblock);

  if (psd_->getLastStoredCheckpoint() != h->lastCheckpointKnownToRequester) {
    LOG_WARN(STLogger,
             "vblock has irrelevant checkpoint: " << KVLOG(h->lastCheckpointKnownToRequester,
                                                           psd_->getLastStoredCheckpoint()));

    return false;
  }

  // build ResPagesDescriptor
  DataStore::ResPagesDescriptor *pagesDesc = psd_->getResPagesDescriptor(h->lastCheckpointKnownToRequester);

  AssertEQ(pagesDesc->numOfPages, numberOfReservedPages_);

  for (uint32_t element = 0; element < h->numberOfUpdatedPages; ++element) {
    ElementOfVirtualBlock *vElement = getVirtualElement(element, config_.sizeOfReservedPage, vblock);
    LOG_DEBUG(STLogger, KVLOG(element, vElement->pageId, vElement->checkpointNumber, vElement->pageDigest));

    STDigest computedPageDigest;
    computeDigestOfPage(
        vElement->pageId, vElement->checkpointNumber, vElement->page, config_.sizeOfReservedPage, computedPageDigest);
    if (computedPageDigest != vElement->pageDigest) {
      LOG_WARN(STLogger,
               "vblock contains invalid digest: " << KVLOG(vElement->pageId, vElement->pageDigest, computedPageDigest));
      return false;
    }
    AssertLE(pagesDesc->d[vElement->pageId].relevantCheckpoint, h->lastCheckpointKnownToRequester);
    pagesDesc->d[vElement->pageId].pageId = vElement->pageId;
    pagesDesc->d[vElement->pageId].relevantCheckpoint = vElement->checkpointNumber;
    pagesDesc->d[vElement->pageId].pageDigest = vElement->pageDigest;
  }

  STDigest computedDigest;
  computeDigestOfPagesDescriptor(pagesDesc, computedDigest);
  psd_->free(pagesDesc);

  if (computedDigest != expectedDigestOfResPagesDescriptor) {
    LOG_WARN(STLogger,
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
  LOG_DEBUG(STLogger, "");
  Assert(sourceSelector_.noPreferredReplicas());
  sourceSelector_.reset();
  metrics_.current_source_replica_.Get().Set(sourceSelector_.currentReplica());

  nextRequiredBlock_ = 0;
  digestOfNextRequiredBlock.makeZero();
  clearAllPendingItemsData();

  psd_->deleteCheckpointBeingFetched();
  AssertEQ(getFetchingState(), FetchingState::GettingCheckpointSummaries);
  verifyEmptyInfoAboutGettingCheckpointSummary();
  sendAskForCheckpointSummariesMsg();
}

void BCStateTran::processData() {
  const FetchingState fs = getFetchingState();
  const auto fetchingState = fs;
  LOG_DEBUG(STLogger, KVLOG(fetchingState));

  AssertOR(fs == FetchingState::GettingMissingBlocks, fs == FetchingState::GettingMissingResPages);
  Assert(sourceSelector_.hasPreferredReplicas());
  AssertLE(totalSizeOfPendingItemDataMsgs, config_.maxPendingDataFromSourceReplica);

  const bool isGettingBlocks = (fs == FetchingState::GettingMissingBlocks);

  AssertOR(!isGettingBlocks, psd_->getLastRequiredBlock() != 0);
  AssertOR(isGettingBlocks, psd_->getLastRequiredBlock() == 0);

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
      LOG_DEBUG(STLogger, "Selected new source replica: " << (sourceSelector_.currentReplica()));
      metrics_.current_source_replica_.Get().Set(sourceSelector_.currentReplica());
      metrics_.preferred_replicas_.Get().Set(sourceSelector_.preferredReplicasToString());
      badDataFromCurrentSourceReplica = false;
      clearAllPendingItemsData();
    }

    // We have a valid source replica at this point
    Assert(sourceSelector_.hasSource());
    AssertEQ(badDataFromCurrentSourceReplica, false);

    //////////////////////////////////////////////////////////////////////////
    // if needed, determine the next required block
    //////////////////////////////////////////////////////////////////////////
    if (nextRequiredBlock_ == 0) {
      Assert(digestOfNextRequiredBlock.isZero());

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
          Assert(as_->hasBlock(nextRequiredBlock_ + 1));
          as_->getPrevDigestFromBlock(nextRequiredBlock_ + 1,
                                      reinterpret_cast<StateTransferDigest *>(&digestOfNextRequiredBlock));
        }
      }
    }

    AssertNE(nextRequiredBlock_, 0);
    Assert(!digestOfNextRequiredBlock.isZero());

    LOG_DEBUG(STLogger, KVLOG(nextRequiredBlock_, digestOfNextRequiredBlock));

    //////////////////////////////////////////////////////////////////////////
    // Process and check the available chunks
    //////////////////////////////////////////////////////////////////////////

    int16_t lastChunkInRequiredBlock = 0;
    uint32_t actualBlockSize = 0;

    const bool newBlock = getNextFullBlock(nextRequiredBlock_,
                                           badDataFromCurrentSourceReplica,
                                           lastChunkInRequiredBlock,
                                           buffer_,
                                           actualBlockSize,
                                           !isGettingBlocks);
    bool newBlockIsValid = false;

    if (newBlock && isGettingBlocks) {
      Assert(!badDataFromCurrentSourceReplica);
      newBlockIsValid = checkBlock(nextRequiredBlock_, digestOfNextRequiredBlock, buffer_, actualBlockSize);
      badDataFromCurrentSourceReplica = !newBlockIsValid;
    } else if (newBlock && !isGettingBlocks) {
      Assert(!badDataFromCurrentSourceReplica);
      newBlockIsValid = checkVirtualBlockOfResPages(digestOfNextRequiredBlock, buffer_, actualBlockSize);
      badDataFromCurrentSourceReplica = !newBlockIsValid;
    } else {
      AssertAND(!newBlock, actualBlockSize == 0);
    }

    LOG_DEBUG(STLogger, KVLOG(newBlock, newBlockIsValid, actualBlockSize));

    //////////////////////////////////////////////////////////////////////////
    // if we have a new block
    //////////////////////////////////////////////////////////////////////////
    if (newBlockIsValid && isGettingBlocks) {
      DataStoreTransaction::Guard g(psd_->beginTransaction());
      sourceSelector_.setSourceSelectionTime(currTime);

      AssertAND(lastChunkInRequiredBlock >= 1, actualBlockSize > 0);

      LOG_DEBUG(STLogger, "Add block: " << KVLOG(nextRequiredBlock_, actualBlockSize));

      Assert(as_->putBlock(nextRequiredBlock_, buffer_, actualBlockSize));

      memset(buffer_, 0, actualBlockSize);
      const uint64_t firstRequiredBlock = g.txn()->getFirstRequiredBlock();

      if (firstRequiredBlock < nextRequiredBlock_) {
        as_->getPrevDigestFromBlock(nextRequiredBlock_,
                                    reinterpret_cast<StateTransferDigest *>(&digestOfNextRequiredBlock));
        nextRequiredBlock_--;
        g.txn()->setLastRequiredBlock(nextRequiredBlock_);
      } else {
        // this is the last block we need
        g.txn()->setFirstRequiredBlock(0);
        g.txn()->setLastRequiredBlock(0);
        clearAllPendingItemsData();
        nextRequiredBlock_ = 0;
        digestOfNextRequiredBlock.makeZero();

        AssertEQ(getFetchingState(), FetchingState::GettingMissingResPages);

        LOG_DEBUG(STLogger, "Moved to GettingMissingResPages");
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

      // set the updated pages
      uint32_t numOfUpdates = getNumberOfElements(buffer_);
      LOG_DEBUG(STLogger, "numOfUpdates in vblock: " << numOfUpdates);
      for (uint32_t i = 0; i < numOfUpdates; i++) {
        ElementOfVirtualBlock *e = getVirtualElement(i, config_.sizeOfReservedPage, buffer_);
        g.txn()->setResPage(e->pageId, e->checkpointNumber, e->pageDigest, e->page);
        LOG_DEBUG(STLogger, "Update page " << e->pageId);
      }
      memset(buffer_, 0, actualBlockSize);

      Assert(g.txn()->hasCheckpointBeingFetched());

      DataStore::CheckpointDesc cp = g.txn()->getCheckpointBeingFetched();

      // set stored data
      AssertEQ(g.txn()->getFirstRequiredBlock(), 0);
      AssertEQ(g.txn()->getLastRequiredBlock(), 0);
      AssertGT(cp.checkpointNum, g.txn()->getLastStoredCheckpoint());

      g.txn()->setCheckpointDesc(cp.checkpointNum, cp);
      g.txn()->setLastStoredCheckpoint(cp.checkpointNum);
      g.txn()->deleteCheckpointBeingFetched();
      g.txn()->setIsFetchingState(false);

      // delete old checkpoints
      uint64_t minRelevantCheckpoint = 0;
      if (cp.checkpointNum >= maxNumOfStoredCheckpoints_)
        minRelevantCheckpoint = cp.checkpointNum - maxNumOfStoredCheckpoints_ + 1;

      if (minRelevantCheckpoint > 0) {
        while (minRelevantCheckpoint < cp.checkpointNum && !g.txn()->hasCheckpointDesc(minRelevantCheckpoint))
          minRelevantCheckpoint++;
      }

      const uint64_t oldFirstStoredCheckpoint = g.txn()->getFirstStoredCheckpoint();
      LOG_DEBUG(STLogger, KVLOG(minRelevantCheckpoint, oldFirstStoredCheckpoint));

      if (minRelevantCheckpoint >= 2 && minRelevantCheckpoint > oldFirstStoredCheckpoint) {
        g.txn()->deleteDescOfSmallerCheckpoints(minRelevantCheckpoint);
        g.txn()->deleteCoveredResPageInSmallerCheckpoints(minRelevantCheckpoint);
      }

      if (minRelevantCheckpoint > oldFirstStoredCheckpoint) g.txn()->setFirstStoredCheckpoint(minRelevantCheckpoint);

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
      LOG_INFO(STLogger, "Calling onTransferringComplete: " << KVLOG(cp.checkpointNum));
      metrics_.on_transferring_complete_.Get().Inc();
      replicaForStateTransfer_->onTransferringComplete(cp.checkpointNum);

      break;
    }
    //////////////////////////////////////////////////////////////////////////
    // if we don't have new full block/vblock (but we did not detect a problem)
    //////////////////////////////////////////////////////////////////////////
    else if (!badDataFromCurrentSourceReplica && isGettingBlocks) {
      if (newBlock) memset(buffer_, 0, actualBlockSize);
      if (newSourceReplica || sourceSelector_.retransmissionTimeoutExpired(currTime)) {
        AssertEQ(psd_->getLastRequiredBlock(), nextRequiredBlock_);
        sendFetchBlocksMsg(psd_->getFirstRequiredBlock(), nextRequiredBlock_, lastChunkInRequiredBlock);
      }
      break;
    } else if (!badDataFromCurrentSourceReplica && !isGettingBlocks) {
      if (newBlock) memset(buffer_, 0, actualBlockSize);

      if (newSourceReplica || sourceSelector_.retransmissionTimeoutExpired(currTime)) {
        sendFetchResPagesMsg(lastChunkInRequiredBlock);
      }
      break;
    } else {
      if (newBlock) memset(buffer_, 0, actualBlockSize);
    }
  }
}

//////////////////////////////////////////////////////////////////////////////
// Consistency
//////////////////////////////////////////////////////////////////////////////

void BCStateTran::checkConsistency(bool checkAllBlocks) {
  Assert(psd_->initialized());

  const uint64_t lastReachableBlockNum = as_->getLastReachableBlockNum();
  const uint64_t lastBlockNum = as_->getLastBlockNum();
  LOG_INFO(STLogger, KVLOG(lastBlockNum, lastReachableBlockNum));

  const uint64_t firstStoredCheckpoint = psd_->getFirstStoredCheckpoint();
  const uint64_t lastStoredCheckpoint = psd_->getLastStoredCheckpoint();
  LOG_INFO(STLogger, KVLOG(firstStoredCheckpoint, lastStoredCheckpoint));

  checkConfig();
  checkFirstAndLastCheckpoint(firstStoredCheckpoint, lastStoredCheckpoint);
  if (checkAllBlocks) {
    checkReachableBlocks(lastReachableBlockNum);
  }
  checkUnreachableBlocks(lastReachableBlockNum, lastBlockNum);
  checkBlocksBeingFetchedNow(checkAllBlocks, lastReachableBlockNum, lastBlockNum);
  checkStoredCheckpoints(firstStoredCheckpoint, lastStoredCheckpoint);

  if (!psd_->getIsFetchingState()) {
    Assert(!psd_->hasCheckpointBeingFetched());
    AssertEQ(psd_->getFirstRequiredBlock(), 0);
    AssertEQ(psd_->getLastRequiredBlock(), 0);
  } else if (!psd_->hasCheckpointBeingFetched()) {
    AssertEQ(psd_->getFirstRequiredBlock(), 0);
    AssertEQ(psd_->getLastRequiredBlock(), 0);
    AssertEQ(psd_->numOfAllPendingResPage(), 0);
  } else if (psd_->getLastRequiredBlock() > 0) {
    AssertGT(psd_->getFirstRequiredBlock(), 0);
    AssertEQ(psd_->numOfAllPendingResPage(), 0);
  } else {
    AssertEQ(psd_->numOfAllPendingResPage(), 0);
  }
}

void BCStateTran::checkConfig() {
  AssertEQ(replicas_, psd_->getReplicas());
  AssertEQ(config_.myReplicaId, psd_->getMyReplicaId());
  AssertEQ(config_.fVal, psd_->getFVal());
  AssertEQ(maxNumOfStoredCheckpoints_, psd_->getMaxNumOfStoredCheckpoints());
  AssertEQ(numberOfReservedPages_, psd_->getNumberOfReservedPages());
}

void BCStateTran::checkFirstAndLastCheckpoint(uint64_t firstStoredCheckpoint, uint64_t lastStoredCheckpoint) {
  AssertGE(lastStoredCheckpoint, firstStoredCheckpoint);
  AssertLE(lastStoredCheckpoint - firstStoredCheckpoint + 1, maxNumOfStoredCheckpoints_);
  AssertOR((lastStoredCheckpoint == 0), psd_->hasCheckpointDesc(lastStoredCheckpoint));
  if ((firstStoredCheckpoint != 0) && (firstStoredCheckpoint != lastStoredCheckpoint) &&
      !psd_->hasCheckpointDesc(firstStoredCheckpoint)) {
    LOG_FATAL(STLogger,
              KVLOG(firstStoredCheckpoint, lastStoredCheckpoint, psd_->hasCheckpointDesc(firstStoredCheckpoint)));
    Assert(false);
  }
}

void BCStateTran::checkReachableBlocks(uint64_t lastReachableBlockNum) {
  if (lastReachableBlockNum > 0) {
    for (uint64_t currBlock = lastReachableBlockNum - 1; currBlock >= 1; currBlock--) {
      auto currDigest = getBlockAndComputeDigest(currBlock);
      Assert(!currDigest.isZero());
      STDigest prevFromNextBlockDigest;
      prevFromNextBlockDigest.makeZero();
      as_->getPrevDigestFromBlock(currBlock + 1, reinterpret_cast<StateTransferDigest *>(&prevFromNextBlockDigest));
      AssertEQ(currDigest, prevFromNextBlockDigest);
    }
  }
}

void BCStateTran::checkUnreachableBlocks(uint64_t lastReachableBlockNum, uint64_t lastBlockNum) {
  AssertGE(lastBlockNum, lastReachableBlockNum);
  if (lastBlockNum > lastReachableBlockNum) {
    AssertEQ(getFetchingState(), FetchingState::GettingMissingBlocks);
    uint64_t x = lastBlockNum - 1;
    while (as_->hasBlock(x)) x--;

    // we should have a hole
    AssertGT(x, lastReachableBlockNum);

    // we should have a single hole
    for (uint64_t i = lastReachableBlockNum + 1; i <= x; i++) Assert(!as_->hasBlock(i));
  }
}

void BCStateTran::checkBlocksBeingFetchedNow(bool checkAllBlocks,
                                             uint64_t lastReachableBlockNum,
                                             uint64_t lastBlockNum) {
  if (lastBlockNum > lastReachableBlockNum) {
    AssertAND(psd_->getIsFetchingState(), psd_->hasCheckpointBeingFetched());
    AssertEQ(psd_->getFirstRequiredBlock() - 1, as_->getLastReachableBlockNum());
    AssertGE(psd_->getLastRequiredBlock(), psd_->getFirstRequiredBlock());

    if (checkAllBlocks) {
      uint64_t lastRequiredBlock = psd_->getLastRequiredBlock();

      for (uint64_t currBlock = lastBlockNum - 1; currBlock >= lastRequiredBlock + 1; currBlock--) {
        auto currDigest = getBlockAndComputeDigest(currBlock);
        Assert(!currDigest.isZero());

        STDigest prevFromNextBlockDigest;
        prevFromNextBlockDigest.makeZero();
        as_->getPrevDigestFromBlock(currBlock + 1, reinterpret_cast<StateTransferDigest *>(&prevFromNextBlockDigest));
        AssertEQ(currDigest, prevFromNextBlockDigest);
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
      AssertEQ(desc.checkpointNum, chkp);
      AssertLE(desc.lastBlock, as_->getLastReachableBlockNum());
      AssertGE(desc.lastBlock, prevLastBlockNum);
      prevLastBlockNum = desc.lastBlock;

      if (desc.lastBlock > 0) {
        auto computedBlockDigest = getBlockAndComputeDigest(desc.lastBlock);
        // Extra debugging needed here for BC-2821
        if (computedBlockDigest != desc.digestOfLastBlock) {
          uint32_t blockSize = 0;
          as_->getBlock(desc.lastBlock, buffer_, &blockSize);
          concordUtils::HexPrintBuffer blockData{buffer_, blockSize};
          LOG_FATAL(STLogger, "Invalid stored checkpoint: " << KVLOG(desc.checkpointNum, desc.lastBlock, blockData));
          AssertEQ(computedBlockDigest, desc.digestOfLastBlock);
        }
      }
      // check all pages descriptor
      DataStore::ResPagesDescriptor *allPagesDesc = psd_->getResPagesDescriptor(chkp);
      AssertEQ(allPagesDesc->numOfPages, numberOfReservedPages_);
      {
        STDigest computedDigestOfResPagesDescriptor;
        computeDigestOfPagesDescriptor(allPagesDesc, computedDigestOfResPagesDescriptor);
        AssertEQ(computedDigestOfResPagesDescriptor, desc.digestOfResPagesDescriptor);
      }
      // check all pages descriptors
      for (uint32_t pageId = 0; pageId < numberOfReservedPages_; pageId++) {
        uint64_t actualCheckpoint = 0;
        if (!psd_->getResPage(pageId, chkp, &actualCheckpoint, buffer_, config_.sizeOfReservedPage)) continue;

        AssertEQ(allPagesDesc->d[pageId].pageId, pageId);
        AssertLE(allPagesDesc->d[pageId].relevantCheckpoint, chkp);
        AssertGT(allPagesDesc->d[pageId].relevantCheckpoint, 0);
        AssertEQ(allPagesDesc->d[pageId].relevantCheckpoint, actualCheckpoint);

        STDigest computedDigestOfPage;
        computeDigestOfPage(pageId, actualCheckpoint, buffer_, config_.sizeOfReservedPage, computedDigestOfPage);
        AssertEQ(computedDigestOfPage, allPagesDesc->d[pageId].pageDigest);
      }
      memset(buffer_, 0, config_.sizeOfReservedPage);
      psd_->free(allPagesDesc);
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
  AssertGT(blockNum, 0);
  AssertGT(blockSize, 0);
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
  STDigest currDigest;
  uint32_t blockSize = 0;
  as_->getBlock(currBlock, buffer_, &blockSize);
  computeDigestOfBlock(currBlock, buffer_, blockSize, &currDigest);
  memset(buffer_, 0, blockSize);
  return currDigest;
}

void BCStateTran::SetAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) {
  metrics_component_.SetAggregator(aggregator);
}

}  // namespace impl
}  // namespace SimpleBlockchainStateTransfer
}  // namespace bftEngine
