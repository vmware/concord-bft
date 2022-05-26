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
#include <exception>
#include <list>
#include <sstream>
#include <functional>
#include <utility>
#include <iterator>
#include <iomanip>

#include "assertUtils.hpp"
#include "hex_tools.h"
#include "BCStateTran.hpp"
#include "Digest.hpp"
#include "InMemoryDataStore.hpp"
#include "json_output.hpp"
#include "ReservedPagesClient.hpp"
#include "DBDataStore.hpp"
#include "storage/db_interface.h"
#include "storage/key_manipulator_interface.h"
#include "memorydb/client.h"
#include "client/reconfiguration/client_reconfiguration_engine.hpp"
#include "client/reconfiguration/poll_based_state_client.hpp"
#include "RVBManager.hpp"

using std::tie;
using namespace std::placeholders;
using namespace concord::diagnostics;
using namespace concord::util;
using concord::util::digest::DigestUtil;

namespace bftEngine {
namespace bcst {

void computeBlockDigest(const uint64_t blockId,
                        const char *block,
                        const uint32_t blockSize,
                        StateTransferDigest *outDigest) {
  return impl::BCStateTran::computeDigestOfBlock(blockId, block, blockSize, (Digest *)outDigest);
}

BlockDigest computeBlockDigest(const uint64_t blockId, const char *block, const uint32_t blockSize) {
  return impl::BCStateTran::computeDigestOfBlock(blockId, block, blockSize);
}

IStateTransfer *create(const Config &config,
                       IAppState *const stateApi,
                       std::shared_ptr<concord::storage::IDBClient> dbc,
                       std::shared_ptr<concord::storage::ISTKeyManipulator> stKeyManipulator) {
  // TODO(GG): check configuration

  impl::DataStore *ds = nullptr;

  if (dynamic_cast<concord::storage::memorydb::Client *>(dbc.get()) || config.isReadOnly)
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
// Ctor & Dtor
//////////////////////////////////////////////////////////////////////////////

uint32_t BCStateTran::calcMaxItemSize(uint32_t maxBlockSize, uint32_t maxNumberOfPages, uint32_t pageSize) {
  const uint32_t maxVBlockSize = calcMaxVBlockSize(maxNumberOfPages, pageSize);

  const uint32_t retVal = std::max(maxBlockSize, maxVBlockSize);

  return retVal;
}

uint32_t BCStateTran::calcMaxNumOfChunksInBlock(uint32_t maxItemSize,
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
set<uint16_t> BCStateTran::generateSetOfReplicas(const int16_t numberOfReplicas) {
  std::set<uint16_t> retVal;
  for (int16_t i = 0; i < numberOfReplicas; i++) retVal.insert(i);
  return retVal;
}

BCStateTran::Metrics BCStateTran::createRegisterMetrics() {
  // We must make sure that we actually initialize all these metrics in the
  // same order as defined in the header file.
  return BCStateTran::Metrics{
      metrics_component_.RegisterStatus("fetching_state", stateName(FetchingState::NotFetching)),
      metrics_component_.RegisterGauge("checkpoint_being_fetched", 0),
      metrics_component_.RegisterGauge("last_stored_checkpoint", 0),
      metrics_component_.RegisterGauge("number_of_reserved_pages", 0),
      metrics_component_.RegisterGauge("size_of_reserved_page", config_.sizeOfReservedPage),
      metrics_component_.RegisterGauge("last_msg_seq_num", lastMsgSeqNum_),
      metrics_component_.RegisterGauge("next_required_block_", fetchState_.nextBlockId),
      metrics_component_.RegisterGauge("next_block_id_to_commit", commitState_.nextBlockId),
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
      metrics_component_.RegisterAtomicCounter("load_reserved_page"),
      metrics_component_.RegisterAtomicCounter("load_reserved_page_from_pending"),
      metrics_component_.RegisterAtomicCounter("load_reserved_page_from_checkpoint"),
      metrics_component_.RegisterAtomicCounter("save_reserved_page"),
      metrics_component_.RegisterCounter("zero_reserved_page"),
      metrics_component_.RegisterCounter("start_collecting_state"),
      metrics_component_.RegisterCounter("on_timer"),
      metrics_component_.RegisterCounter("one_shot_timer"),
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
      metrics_component_.RegisterGauge("prev_win_bytes_throughput", 0),

      metrics_component_.RegisterCounter("overall_rvb_digests_validated"),
      metrics_component_.RegisterCounter("overall_rvb_digest_groups_validated"),
      metrics_component_.RegisterCounter("overall_rvb_digests_failed_validation"),
      metrics_component_.RegisterCounter("overall_rvb_digest_groups_failed_validation"),
      metrics_component_.RegisterStatus("current_rvb_data_state", "")};
}

void BCStateTran::bindEventsHandlers() {
  if (config_.runInSeparateThread) {
    incomingEventsQ_ = std::make_unique<concord::util::Handoff>(config_.myReplicaId, "incomingEventsQ");
    incomingStateTransferMsgHandler_ = std::bind(&BCStateTran::handleIncomingStateTransferMessage, this, _1, _2, _3);
    timeoutHandler_ = std::bind(&BCStateTran::handleTimeout, this);
    startCollectingStateHandler_ = std::bind(&BCStateTran::handoffStartCollectingState, this);
  } else {
    incomingEventsQ_.reset(nullptr);  // make it explicit
    incomingStateTransferMsgHandler_ = std::bind(&BCStateTran::handleStateTransferMessageImp, this, _1, _2, _3, _4);
    timeoutHandler_ = std::bind(&BCStateTran::onTimerImp, this);
    startCollectingStateHandler_ = std::bind(&BCStateTran::onStartCollectingStateImp, this);
  }
}

void BCStateTran::rvbm_deleter::operator()(RVBManager *ptr) const { delete ptr; }  // used for pimpl
size_t BCStateTran::BlockIOContext::sizeOfBlockData = 0;
BCStateTran::BCStateTran(const Config &config, IAppState *const stateApi, DataStore *ds)
    : logger_(ST_SRC_LOG),
      postProcessingQ_{config.isReadOnly
                           ? nullptr
                           : std::make_unique<concord::util::Handoff>(config.myReplicaId, "postProcessingQ")},
      postProcessingUpperBoundBlockId_(0),
      maxPostprocessedBlockId_{0},
      as_{stateApi},
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
      cycleCounter_{0},
      internalCycleCounter_{0},
      buffer_(new char[maxItemSize_]),
      randomGen_{randomDevice_()},
      sourceSelector_{allOtherReplicas(),
                      config_.fetchRetransmissionTimeoutMs,
                      config_.sourceReplicaReplacementTimeoutMs,
                      config_.maxFetchRetransmissions,
                      config_.minPrePrepareMsgsForPrimaryAwareness,
                      ST_SRC_LOG},
      fetchState_{0},
      commitState_{0},
      postponedSendFetchBlocksMsg_(false),
      ioPool_(
          config_.maxNumberOfChunksInBatch,
          nullptr,                                     // alloc callback
          [&](std::shared_ptr<BlockIOContext> &ctx) {  // free callback
            if (ctx->future.valid()) {
              try {
                LOG_DEBUG(logger_, "Waiting for previous thread to finish job on context " << KVLOG(ctx->blockId));
                ctx->future.get();
              } catch (...) {
                // ignore and continue, this job is irrelevant
                LOG_WARN(logger_, "Exception:" << KVLOG(ctx->blockId, ctx->actualBlockSize));
                throw;
              }
            }
          },
          [&]() {  // ctor callback
            BCStateTran::BlockIOContext::sizeOfBlockData = config_.maxBlockSize;
          }),
      oneShotTimerFlag_(true),
      rvbm_{new RVBManager(config_, as_, psd_)},
      metrics_component_{
          concordMetrics::Component("bc_state_transfer", std::make_shared<concordMetrics::Aggregator>())},
      metrics_{createRegisterMetrics()},
      blocksFetched_(config_.gettingMissingBlocksSummaryWindowSize, "blocksFetched_"),
      bytesFetched_(config_.gettingMissingBlocksSummaryWindowSize, "bytesFetched_"),
      blocksPostProcessed_(blocksPostProcessedReportWindow, "blocksPostProcessed_"),
      cycleDT_{"cycleDT_"},
      postProcessingDT_{"postProcessingDT_"},
      gettingCheckpointSummariesDT_{"gettingCheckpointSummariesDT_"},
      gettingMissingBlocksDT_{"gettingMissingBlocksDT_"},
      gettingMissingResPagesDT_{"gettingMissingResPagesDT_"},
      lastFetchingState_(FetchingState::NotFetching),
      sourceSession_(logger_, config.sourceSessionExpiryDurationMs),
      src_send_batch_duration_rec_(histograms_.src_send_batch_duration),
      dst_time_between_sendFetchBlocksMsg_rec_(histograms_.dst_time_between_sendFetchBlocksMsg),
      time_in_incoming_events_queue_rec_(histograms_.time_in_incoming_events_queue) {
  // Validate input parameters and some of the configuration
  ConcordAssertNE(stateApi, nullptr);
  ConcordAssertGE(replicas_.size(), 3U * config_.fVal + 1U);
  ConcordAssert(replicas_.count(config_.myReplicaId) == 1 || config.isReadOnly);
  ConcordAssertGE(config_.maxNumOfReservedPages, 2);
  ConcordAssertLT(finalizePutblockTimeoutMilli_, config_.refreshTimerMs);
  ConcordAssertGT(config_.sourceSessionExpiryDurationMs, config_.fetchRetransmissionTimeoutMs);

  // Register metrics component with the default aggregator.
  metrics_component_.Register();

  LOG_INFO(logger_, "Creating BCStateTran object: " << config_);

  // Bind events handlers according to runInSeparateThread configuration
  bindEventsHandlers();

  // Make sure that the internal IReplicaForStateTransfer callback is always added, alongside any user-supplied
  // callbacks.
  addOnTransferringCompleteCallback(
      [this](uint64_t checkpoint_num) { replicaForStateTransfer_->onTransferringComplete(checkpoint_num); });
}

BCStateTran::~BCStateTran() {
  ConcordAssert(!running_);
  ConcordAssert(cacheOfVirtualBlockForResPages.empty());
  ConcordAssert(pendingItemDataMsgs.empty());
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

    LOG_INFO(logger_,
             "Init BCStateTran object:" << KVLOG(
                 maxNumOfStoredCheckpoints_, numberOfReservedPages_, config_.sizeOfReservedPage));

    FetchingState fs{FetchingState::NotFetching};
    if (psd_->initialized()) {
      LOG_INFO(logger_, "Loading existing data from storage");

      checkConsistency(config_.pedanticChecks, true);

      rvbm_->init(psd_->getIsFetchingState());
      metrics_.current_rvb_data_state_.Get().Set(rvbm_->getStateOfRvbData());

      fs = getFetchingState();
      LOG_INFO(logger_, "Starting state is " << stateName(fs));

      if (isActiveDest(fs)) {
        LOG_INFO(logger_, "State Transfer cycle continues");
        startCollectingStats();
        if ((fs == FetchingState::GettingMissingBlocks) || (fs == FetchingState::GettingMissingResPages)) {
          SetAllReplicasAsPreferred();
        }

        if (fs == FetchingState::GettingMissingBlocks) {
          auto lastReachableBlockNum = as_->getLastReachableBlockNum();
          auto lastRequiredBlock = psd_->getLastRequiredBlock();
          ConcordAssertLE(lastReachableBlockNum, lastRequiredBlock);
          if (lastReachableBlockNum == lastRequiredBlock) {
            DataStoreTransaction::Guard g(psd_->beginTransaction());
            onGettingMissingBlocksEnd(g.txn());
          } else {  // lastReachableBlockNum < lastRequiredBlock -> continue GettingMissingBlocks
            triggerPostProcessing();
            minBlockIdToCollectInCycle_ = lastReachableBlockNum + 1;
            maxBlockIdToCollectInCycle_ = lastRequiredBlock;
            ConcordAssertGE(maxBlockIdToCollectInCycle_, minBlockIdToCollectInCycle_);
            totalBlocksLeftToCollectInCycle_ = maxBlockIdToCollectInCycle_ - minBlockIdToCollectInCycle_ + 1;
          }
        }
      }
      loadMetrics();
    } else {
      LOG_INFO(logger_, "Initializing a new object");
      {
        DataStoreTransaction::Guard g(psd_->beginTransaction());
        stReset(g.txn(), true, true, true);
      }
      ConcordAssertGE(maxNumOfRequiredStoredCheckpoints, 2);
      ConcordAssertLE(maxNumOfRequiredStoredCheckpoints, kMaxNumOfStoredCheckpoints);
      ConcordAssertGE(numberOfRequiredReservedPages, 2);
      ConcordAssertLE(numberOfRequiredReservedPages, config_.maxNumOfReservedPages);

      ConcordAssertEQ(getFetchingState(), FetchingState::NotFetching);
      rvbm_->init(false);
      metrics_.current_rvb_data_state_.Get().Set(rvbm_->getStateOfRvbData());
    }
    {
      DataStoreTransaction::Guard g(psd_->beginTransaction());
      g.txn()->setReplicas(replicas_);
      g.txn()->setMyReplicaId(config_.myReplicaId);
      g.txn()->setFVal(config_.fVal);
      g.txn()->setMaxNumOfStoredCheckpoints(maxNumOfRequiredStoredCheckpoints);
      g.txn()->setNumberOfReservedPages(numberOfRequiredReservedPages);
    }
  } catch (const std::exception &e) {
    LOG_FATAL(logger_, e.what());
    std::terminate();
  }
}

void BCStateTran::startRunning(IReplicaForStateTransfer *r) {
  LOG_INFO(logger_, "Starting");
  FetchingState fs = getFetchingState();

  // TODO - The next lines up to comment 'XXX' do not belong here (CRE) - move outside
  if (!config_.isReadOnly && cre_) {
    cre_->halt();
  }
  if (cre_) {
    cre_->start();
  }
  ConcordAssertNE(r, nullptr);
  if ((!config_.isReadOnly) && (fs != FetchingState::NotFetching)) {
    LOG_INFO(logger_, "State Transfer cycle continues, starts async reconfiguration engine");
    if (cre_) {
      cre_->resume();
    }
  }
  /// XXX - end of section to be moved out

  running_ = true;
  replicaForStateTransfer_ = r;
  replicaForStateTransfer_->changeStateTransferTimerPeriod(config_.refreshTimerMs);
}

// timer is cancelled in the calling context, see ReplicaForStateTransfer::stop
void BCStateTran::stopRunning() {
  LOG_INFO(logger_, "Stopping");
  ConcordAssert(running_);
  ConcordAssertNE(replicaForStateTransfer_, nullptr);
  if (incomingEventsQ_) {
    incomingEventsQ_->stop();
  }
  if (postProcessingQ_) {
    postProcessingQ_->stop();
  }
  {
    DataStoreTransaction::Guard g(psd_->beginTransaction());
    stReset(g.txn(), true, false, false);
  }
  running_ = false;
  replicaForStateTransfer_ = nullptr;
}

bool BCStateTran::isRunning() const { return running_; }

// Create a CheckpointDesc for the given checkpointNumber.
//
// This has the side effect of filling in buffer_ with the last block of app
// data.
DataStore::CheckpointDesc BCStateTran::createCheckpointDesc(uint64_t checkpointNumber,
                                                            const Digest &digestOfResPagesDescriptor) {
  LOG_TRACE(logger_, KVLOG(checkpointNumber, digestOfResPagesDescriptor));
  uint64_t maxBlockId = as_->getLastReachableBlockNum();
  ConcordAssertEQ(maxBlockId, as_->getLastBlockNum());
  metrics_.last_block_.Get().Set(maxBlockId);

  Digest digestOfMaxBlockId;
  if (maxBlockId > 0) {
    digestOfMaxBlockId = getBlockAndComputeDigest(maxBlockId);
  } else {
    // if we don't have blocks, then we use zero digest
    digestOfMaxBlockId.makeZero();
  }

  DataStore::CheckpointDesc checkDesc;
  checkDesc.checkpointNum = checkpointNumber;
  checkDesc.maxBlockId = maxBlockId;
  checkDesc.digestOfMaxBlockId = digestOfMaxBlockId;
  checkDesc.digestOfResPagesDescriptor = digestOfResPagesDescriptor;
  rvbm_->updateRvbDataDuringCheckpoint(checkDesc);
  metrics_.current_rvb_data_state_.Get().Set(rvbm_->getStateOfRvbData());

  LOG_INFO(logger_,
           "CheckpointDesc: " << KVLOG(checkpointNumber,
                                       maxBlockId,
                                       digestOfMaxBlockId,
                                       digestOfResPagesDescriptor,
                                       checkDesc.rvbData.size(),
                                       rvbm_->getStateOfRvbData()));

  return checkDesc;
}

// Associate any pending reserved pages with the current checkpoint.
// Return the digest of all the reserved pages descriptor.
//
// This has the side effect of mutating buffer_.
Digest BCStateTran::checkpointReservedPages(uint64_t checkpointNumber, DataStoreTransaction *txn) {
  set<uint32_t> pages = txn->getNumbersOfPendingResPages();
  auto numberOfPagesInCheckpoint = pages.size();
  LOG_INFO(logger_,
           "Associating pending pages with checkpoint: " << KVLOG(numberOfPagesInCheckpoint, checkpointNumber));
  std::unique_ptr<char[]> buffer(new char[config_.sizeOfReservedPage]);
  for (uint32_t p : pages) {
    Digest d;
    txn->getPendingResPage(p, buffer.get(), config_.sizeOfReservedPage);
    computeDigestOfPage(p, checkpointNumber, buffer.get(), config_.sizeOfReservedPage, d);
    txn->associatePendingResPageWithCheckpoint(p, checkpointNumber, d);
  }

  ConcordAssertEQ(txn->numOfAllPendingResPage(), 0);
  DataStore::ResPagesDescriptor *allPagesDesc = txn->getResPagesDescriptor(checkpointNumber);
  ConcordAssertEQ(allPagesDesc->numOfPages, numberOfReservedPages_);

  Digest digestOfResPagesDescriptor;
  computeDigestOfPagesDescriptor(allPagesDesc, digestOfResPagesDescriptor);

  LOG_INFO(logger_, allPagesDesc->toString(digestOfResPagesDescriptor.toString()));

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

  LOG_DEBUG(logger_, KVLOG(minRelevantCheckpoint, oldFirstStoredCheckpoint));

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
  LOG_INFO(logger_,
           KVLOG(checkpointNumber,
                 minRelevantCheckpoint,
                 oldFirstStoredCheckpoint,
                 firstStoredCheckpoint,
                 lastStoredCheckpoint));
}

void BCStateTran::createCheckpointOfCurrentState(uint64_t checkpointNumber) {
  auto lastStoredCheckpointNumber = psd_->getLastStoredCheckpoint();
  LOG_INFO(logger_, KVLOG(checkpointNumber, lastStoredCheckpointNumber));

  ConcordAssert(running_);
  ConcordAssert(!isFetching());
  ConcordAssertGT(checkpointNumber, 0);
  if (checkpointNumber == lastStoredCheckpointNumber) {
    // We persist the lastStoredCheckpointNumber in a separate transaction from the actual
    // update of the lastExecutedSeqNum. Since we have no mechanism to batch the 2 transactions
    // together we need to handle this rare recovery situation.
    LOG_WARN(logger_, "checkpointNumber == lastStoredCheckpointNumber" << KVLOG(checkpointNumber));
    return;
  }
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
  LOG_INFO(logger_, "Done creating (and persisting) checkpoint of current state!" << KVLOG(checkpointNumber));
}

void BCStateTran::markCheckpointAsStable(uint64_t checkpointNumber) {
  ConcordAssert(running_);
  ConcordAssert(!isFetching());
  ConcordAssertGT(checkpointNumber, 0);

  metrics_.mark_checkpoint_as_stable_++;

  const uint64_t lastStoredCheckpoint = psd_->getLastStoredCheckpoint();
  metrics_.last_stored_checkpoint_.Get().Set(lastStoredCheckpoint);

  LOG_INFO(logger_, KVLOG(checkpointNumber, lastStoredCheckpoint));

  ConcordAssertOR((lastStoredCheckpoint < maxNumOfStoredCheckpoints_),
                  (checkpointNumber >= lastStoredCheckpoint - maxNumOfStoredCheckpoints_ + 1));
  ConcordAssertLE(checkpointNumber, psd_->getLastStoredCheckpoint());
}

void BCStateTran::getDigestOfCheckpoint(uint64_t checkpointNumber,
                                        uint16_t sizeOfDigestBuffer,
                                        uint64_t &outBlockId,
                                        char *outStateDigest,
                                        char *outOtherDigest) {
  ConcordAssert(running_);
  ConcordAssertGE(sizeOfDigestBuffer, sizeof(Digest));
  ConcordAssertGT(checkpointNumber, 0);
  ConcordAssertGE(checkpointNumber, psd_->getFirstStoredCheckpoint());
  ConcordAssertLE(checkpointNumber, psd_->getLastStoredCheckpoint());
  ConcordAssert(psd_->hasCheckpointDesc(checkpointNumber));

  DataStore::CheckpointDesc desc = psd_->getCheckpointDesc(checkpointNumber);
  LOG_INFO(logger_,
           KVLOG(desc.checkpointNum, desc.maxBlockId, desc.digestOfMaxBlockId, desc.digestOfResPagesDescriptor));

  uint16_t s = std::min((uint16_t)sizeof(Digest), sizeOfDigestBuffer);
  memcpy(outStateDigest, desc.digestOfMaxBlockId.get(), s);

  if (s < sizeOfDigestBuffer) {
    memset(outStateDigest + s, 0, sizeOfDigestBuffer - s);
  }
  memcpy(outOtherDigest, desc.digestOfResPagesDescriptor.get(), s);

  if (s < sizeOfDigestBuffer) {
    memset(outOtherDigest + s, 0, sizeOfDigestBuffer - s);
  }
  outBlockId = desc.maxBlockId;
}

bool BCStateTran::isCollectingState() const { return isFetching(); }

uint32_t BCStateTran::numberOfReservedPages() const { return static_cast<uint32_t>(numberOfReservedPages_); }

uint32_t BCStateTran::sizeOfReservedPage() const { return config_.sizeOfReservedPage; }

bool BCStateTran::loadReservedPage(uint32_t reservedPageId, uint32_t copyLength, char *outReservedPage) const {
  ConcordAssertLT(reservedPageId, numberOfReservedPages_);
  ConcordAssertLE(copyLength, config_.sizeOfReservedPage);

  metrics_.load_reserved_page_++;

  if (psd_->hasPendingResPage(reservedPageId)) {
    LOG_DEBUG(logger_, "Loaded pending reserved page: " << reservedPageId);
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
    LOG_DEBUG(logger_,
              "Reserved page loaded from checkpoint: " << KVLOG(reservedPageId, actualCheckpoint, lastCheckpoint));
  }
  return true;
}

// TODO(TK) check if this function can have its own transaction(bftimpl)
void BCStateTran::saveReservedPage(uint32_t reservedPageId, uint32_t copyLength, const char *inReservedPage) {
  try {
    LOG_DEBUG(logger_, KVLOG(reservedPageId));

    ConcordAssert(!isFetching());
    ConcordAssertLT(reservedPageId, numberOfReservedPages_);
    ConcordAssertLE(copyLength, config_.sizeOfReservedPage);

    metrics_.save_reserved_page_++;

    psd_->setPendingResPage(reservedPageId, inReservedPage, copyLength);
  } catch (std::out_of_range &e) {
    LOG_FATAL(logger_, "Failed to save pending reserved page: " << e.what() << ": " << KVLOG(reservedPageId));
    throw;
  }
}

// TODO(TK) check if this function can have its own transaction(bftimpl)
void BCStateTran::zeroReservedPage(uint32_t reservedPageId) {
  LOG_DEBUG(logger_, reservedPageId);

  ConcordAssert(!isFetching());
  ConcordAssertLT(reservedPageId, numberOfReservedPages_);

  metrics_.zero_reserved_page_++;
  std::unique_ptr<char[]> buffer(new char[config_.sizeOfReservedPage]{});
  psd_->setPendingResPage(reservedPageId, buffer.get(), config_.sizeOfReservedPage);
}

std::string BCStateTran::convertUInt64ToReadableStr(uint64_t num, std::string &&trailer) const {
  std::ostringstream oss;
  bool addTrailingSpace = false;

  // fixed point notation and 2 digits precision
  oss << std::fixed;
  oss << std::setprecision(2);

  double dnum = static_cast<double>(num);
  if (num < (1 << 10)) {  // raw
    oss << dnum;
    addTrailingSpace = true;
  } else if (num < (1ULL << 20)) {  // Kilo
    oss << (dnum / (1 << 10));
    oss << " K";
  } else if (num < (1ULL << 30)) {  // Mega
    oss << (dnum / (1 << 20));
    oss << " M";
  } else if (num < (1ULL << 40)) {  // Giga
    oss << (dnum / (1ULL << 30));
    oss << " G";
  } else if (num < (1ULL << 50)) {  // Tera
    oss << (dnum / (1ULL << 40));
    oss << " T";
  } else if (num < (1ULL << 60)) {  // Peta
    oss << (dnum / (1ULL << 50));
    oss << " P";
  } else {
    oss << dnum;  // very large numbers are not expected, return raw
    addTrailingSpace = true;
  }
  auto str = oss.str();
  // remove trailing zeroes and possible dot
  str.erase(str.find_last_not_of('0') + 1, std::string::npos);
  if (str.back() == '.') {
    str.resize(str.size() - 1);
  }
  if (str.empty()) {
    str = "NA";
  }
  if (addTrailingSpace) {
    str += " ";
  }
  return (str + trailer);
}

// TODO - move to "TimeUtils.hpp", after converting to a template function
std::string BCStateTran::convertMillisecToReadableStr(uint64_t ms) const {
  if (ms == 0) {
    return "NA";
  }
  bool shouldPad = false;
  std::string str, legend;
  auto updateReadbleStrAndLegend =
      [&](uint64_t input, std::string &&addToLegend, size_t expectedSize, std::string &&sep = ":") {
        std::string strInput = std::to_string(input);
        ConcordAssertGE(expectedSize, strInput.size());
        if (shouldPad) {
          auto padSize = expectedSize - strInput.size();
          for (size_t i{}; i < padSize; ++i) {
            strInput.insert(0, "0");
          }
        }
        str += strInput + sep;
        legend += addToLegend + sep;
        shouldPad = true;
      };

  uint64_t n = ms;
  constexpr uint64_t ms_per_sec = 1000;
  constexpr uint64_t sec_per_min = 60;
  constexpr uint64_t min_per_hr = 60;
  constexpr uint64_t hr_per_day = 24;
  auto mls = n % ms_per_sec;
  n /= ms_per_sec;
  auto sec = n % sec_per_min;
  n /= sec_per_min;
  auto min = n % min_per_hr;
  n /= min_per_hr;
  auto hr = n % hr_per_day;
  n /= hr_per_day;
  auto days = n;

  if (days) {
    updateReadbleStrAndLegend(days, "DD", 2);
  }
  if (hr || !legend.empty()) {
    updateReadbleStrAndLegend(hr, "HH", 2);
  }
  if (min || !legend.empty()) {
    updateReadbleStrAndLegend(min, "MM", 2);
  }
  if (sec || !legend.empty()) {
    updateReadbleStrAndLegend(sec, "SS", 2, ".");
  }
  if (mls || !legend.empty()) {
    updateReadbleStrAndLegend(mls, "ms", 3, "");
  }
  if (str.empty()) {
    str = "NA";
    legend = "";
  }
  return str + " " + legend;
}

void BCStateTran::startCollectingStats() {
  // reset natives
  maxBlockIdToCollectInCycle_ = 0;
  minBlockIdToCollectInCycle_ = 0;
  totalBlocksLeftToCollectInCycle_ = 0;

  // reset duration trackers
  gettingMissingBlocksDT_.stop(true);
  postProcessingDT_.stop(true);
  gettingCheckpointSummariesDT_.stop(true);
  gettingMissingResPagesDT_.stop(true);
  cycleDT_.stop(true);

  // reset metrics
  metrics_.overall_blocks_collected_.Get().Set(0ull);
  metrics_.overall_blocks_throughput_.Get().Set(0ull);
  metrics_.overall_bytes_collected_.Get().Set(0ull);
  metrics_.overall_bytes_throughput_.Get().Set(0ull);
  metrics_.prev_win_blocks_collected_.Get().Set(0ull);
  metrics_.prev_win_blocks_throughput_.Get().Set(0ull);
  metrics_.prev_win_bytes_collected_.Get().Set(0ull);
  metrics_.prev_win_bytes_throughput_.Get().Set(0ull);
  metrics_.next_required_block_.Get().Set(0);

  // reset recorders
  src_send_batch_duration_rec_.clear();
  dst_time_between_sendFetchBlocksMsg_rec_.clear();
  time_in_incoming_events_queue_rec_.clear();

  // snapshot and increment cycle counter
  auto &registrar = RegistrarSingleton::getInstance();
  registrar.perf.snapshot("state_transfer");
  registrar.perf.snapshot("state_transfer_dest");
  metrics_.start_collecting_state_++;
}

void BCStateTran::startCollectingStateInternal() {
  LOG_DEBUG(logger_, KVLOG(internalCycleCounter_));
  ConcordAssert(sourceSelector_.noPreferredReplicas());
  ++internalCycleCounter_;

  {  // txn scope
    DataStoreTransaction::Guard g(psd_->beginTransaction());
    g.txn()->deleteCheckpointBeingFetched();
    g.txn()->setFirstRequiredBlock(0);
    g.txn()->setLastRequiredBlock(0);
    g.txn()->setIsFetchingState(false);
  }

  // print cycle summary
  cycleEndSummary();
  startCollectingState();
}

void BCStateTran::onStartCollectingStateImp() {
  ConcordAssert(running_);
  if (isFetching()) {
    LOG_WARN(logger_, "Already in State Transfer, ignore call...");
    return;
  }
  LOG_INFO(
      logger_,
      std::boolalpha << "State Transfer cycle started (#" << ++cycleCounter_ << ")," << KVLOG(internalCycleCounter_));

  {  // txn scope
    DataStoreTransaction::Guard g(psd_->beginTransaction());
    stReset(g.txn(), true, false, false);
    g.txn()->deleteAllPendingPages();
    g.txn()->setIsFetchingState(true);
  }

  // TODO - The next 4 lines do not belong here (CRE) - move outside
  LOG_INFO(logger_, "Starts async reconfiguration engine");
  if (!config_.isReadOnly && cre_) {
    cre_->resume();
  }

  startCollectingStats();
  ConcordAssertEQ(getFetchingState(), FetchingState::GettingCheckpointSummaries);
  sendAskForCheckpointSummariesMsg();
}

// this function can be executed in context of another thread.
void BCStateTran::onTimerImp() {
  oneShotTimerFlag_ = true;
  if (!running_) {
    return;
  }
  auto currentTimeMilli = getMonotonicTimeMilli();
  thread_local auto lastAggregatorUpdateTimeMilli{currentTimeMilli};
  thread_local auto lastSrcSnapshotTimeMilli{currentTimeMilli};
  thread_local auto lastMetricsDumpTimeMilli{currentTimeMilli};
  TimeRecorder scoped_timer(*histograms_.on_timer);
  metrics_.on_timer_++;
  if (incomingEventsQ_) {
    time_in_incoming_events_queue_rec_.end();
    histograms_.incoming_events_queue_size->record(incomingEventsQ_->size());
  }

  // General comment: Since this call may be triggered also by one shot timer, some backgroundoperations should be done
  // in a configurable frequency

  // Send all metrics to the aggregator
  if ((currentTimeMilli - lastAggregatorUpdateTimeMilli) > config_.refreshTimerMs) {
    LOG_TRACE(logger_, "Updating all aggregators...");
    metrics_component_.UpdateAggregator();
    sourceSelector_.updateMetricToAggregator();
    rvbm_->updateMetricToAggregator();
    lastAggregatorUpdateTimeMilli = currentTimeMilli;
  }

  // Source perf snapshots into logs
  if (((currentTimeMilli - lastSrcSnapshotTimeMilli) / 1000) > config_.sourcePerformanceSnapshotFrequencySec) {
    auto &registrar = RegistrarSingleton::getInstance();
    registrar.perf.snapshot("state_transfer");
    registrar.perf.snapshot("state_transfer_src");
    LOG_INFO(logger_, registrar.perf.toString(registrar.perf.get("state_transfer")));
    LOG_INFO(logger_, registrar.perf.toString(registrar.perf.get("state_transfer_src")));
    lastSrcSnapshotTimeMilli = currentTimeMilli;
  }

  // Dump metrics to log
  if (((currentTimeMilli - lastMetricsDumpTimeMilli) / 1000) >= config_.metricsDumpIntervalSec) {
    lastMetricsDumpTimeMilli = currentTimeMilli;
    LOG_DEBUG(logger_, "--BCStateTransfer metrics dump--" + metrics_component_.ToJson());
    LOG_DEBUG(logger_, "--SourceSelector metrics dump--" + sourceSelector_.getMetricComponent().ToJson());
    LOG_DEBUG(logger_, "--RVBManager metrics dump--" + rvbm_->getMetricComponent().ToJson());
  }

  // Close expired session
  if ((sourceSession_.isOpen()) && sourceSession_.expired()) {
    sourceSession_.close();
    clearIoContexts();
  }

  // Retransmit AskForCheckpointSummariesMsg if needed
  FetchingState fs = getFetchingState();
  if (fs == FetchingState::GettingCheckpointSummaries) {
    auto currTime = getMonotonicTimeMilli();
    if ((currTime - lastTimeSentAskForCheckpointSummariesMsg) > config_.checkpointSummariesRetransmissionTimeoutMs) {
      LOG_DEBUG(logger_,
                KVLOG(retransmissionNumberOfAskForCheckpointSummariesMsg, kResetCount_AskForCheckpointSummaries));
      if (++retransmissionNumberOfAskForCheckpointSummariesMsg > kResetCount_AskForCheckpointSummaries)
        clearInfoAboutGettingCheckpointSummary();

      sendAskForCheckpointSummariesMsg();
    }
    // process data if fetching
  } else if (fs == FetchingState::GettingMissingBlocks || fs == FetchingState::GettingMissingResPages) {
    processData();
  }
  time_in_incoming_events_queue_rec_.start();
}

std::string BCStateTran::getStatus() {
  concordUtils::BuildJson bj;

  bj.startJson();

  bj.addKv("fetchingState", stateName(getFetchingState()));
  bj.addKv("lastMsgSeqNum_", lastMsgSeqNum_);
  bj.addKv("cacheOfVirtualBlockForResPagesSize", cacheOfVirtualBlockForResPages.size());

  auto current_source = sourceSelector_.currentReplica();
  auto preferred_replicas = sourceSelector_.preferredReplicasToString();

  bj.startNested("lastMsgSequenceNumbers(ReplicaID:SeqNum)");
  for (auto &[id, seq_num] : lastMsgSeqNumOfReplicas_) {
    bj.addKv(std::to_string(id), seq_num);
  }
  bj.endNested();

  bj.startNested("vBlocksCacheForReservedPages");
  for (auto entry : cacheOfVirtualBlockForResPages) {
    auto vblockDescriptor = entry.first;
    bj.addKv(std::to_string(vblockDescriptor.checkpointNum), vblockDescriptor.lastCheckpointKnownToRequester);
  }
  bj.endNested();

  if (isFetching()) {
    bj.startNested("fetchingStateDetails");
    bj.addKv("currentSource", current_source);
    bj.addKv("preferredReplicas", preferred_replicas);
    bj.addKv("nextRequiredBlock", fetchState_.nextBlockId);
    bj.addKv("totalSizeOfPendingItemDataMsgs", totalSizeOfPendingItemDataMsgs);
    bj.endNested();

    bj.addNestedJson("collectingDetails", logsForCollectingStatus());
  }

  bj.addNestedJson("StateTransferMetrics", metrics_component_.ToJson());
  bj.addNestedJson("SourceSelectorMetrics", sourceSelector_.getMetricComponent().ToJson());
  bj.addNestedJson("RVBManagerMetrics", rvbm_->getMetricComponent().ToJson());

  bj.endJson();
  return bj.getJson();
}

void BCStateTran::addOnTransferringCompleteCallback(std::function<void(uint64_t)> callback,
                                                    StateTransferCallBacksPriorities priority) {
  if (on_transferring_complete_cb_registry_.find((uint64_t)priority) == on_transferring_complete_cb_registry_.end()) {
    on_transferring_complete_cb_registry_[(uint64_t)priority];  // Create a new callback registry for this priority
  }
  on_transferring_complete_cb_registry_.at(uint64_t(priority)).add(std::move(callback));
}
// TODO - This next line should be integrated as a callback into on_transferring_complete_cb_registry_.
// on_fetching_state_change_cb_registry_ should be removed
void BCStateTran::addOnFetchingStateChangeCallback(std::function<void(uint64_t)> cb) {
  if (cb) on_fetching_state_change_cb_registry_.add(std::move(cb));
}

// this function can be executed in context of another thread.
void BCStateTran::handleStateTransferMessageImp(char *msg,
                                                uint32_t msgLen,
                                                uint16_t senderId,
                                                LocalTimePoint msgArrivalTime) {
  if (!running_) {
    return;
  }
  if (incomingEventsQ_) {
    time_in_incoming_events_queue_rec_.end();
    histograms_.incoming_events_queue_size->record(incomingEventsQ_->size());
  }
  bool invalidSender = (senderId >= (config_.numReplicas + config_.numRoReplicas));
  bool sentFromSelf = senderId == config_.myReplicaId;
  bool msgSizeTooSmall = msgLen < sizeof(BCStateTranBaseMsg);
  if (msgSizeTooSmall || sentFromSelf || invalidSender) {
    metrics_.received_illegal_msg_++;
    LOG_WARN(logger_, "Illegal message: " << KVLOG(msgLen, senderId, msgSizeTooSmall, sentFromSelf, invalidSender));
    replicaForStateTransfer_->freeStateTransferMsg(msg);
    time_in_incoming_events_queue_rec_.start();
    return;
  }

  BCStateTranBaseMsg *msgHeader = reinterpret_cast<BCStateTranBaseMsg *>(msg);
  LOG_DEBUG(logger_, "new message with type=" << msgHeader->type);

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
        noDelete = onMessage(reinterpret_cast<ItemDataMsg *>(msg), msgLen, senderId, msgArrivalTime);
      }
      break;
    default:
      break;
  }

  if (!noDelete) replicaForStateTransfer_->freeStateTransferMsg(msg);
  time_in_incoming_events_queue_rec_.start();
}

//////////////////////////////////////////////////////////////////////////////
// Virtual Blocks that are used to pass the reserved pages
// (private to the file)
//////////////////////////////////////////////////////////////////////////////

uint32_t BCStateTran::calcMaxVBlockSize(uint32_t maxNumberOfPages, uint32_t pageSize) {
  const uint32_t elementSize = sizeof(ElementOfVirtualBlock) + pageSize - 1;

  return sizeof(HeaderOfVirtualBlock) + (elementSize * maxNumberOfPages);
}

uint32_t BCStateTran::getNumberOfElements(char *virtualBlock) {
  HeaderOfVirtualBlock *h = reinterpret_cast<HeaderOfVirtualBlock *>(virtualBlock);
  return h->numberOfUpdatedPages;
}

uint32_t BCStateTran::getSizeOfVirtualBlock(char *virtualBlock, uint32_t pageSize) {
  HeaderOfVirtualBlock *h = reinterpret_cast<HeaderOfVirtualBlock *>(virtualBlock);

  const uint32_t elementSize = sizeof(ElementOfVirtualBlock) + pageSize - 1;
  const uint32_t size = sizeof(HeaderOfVirtualBlock) + h->numberOfUpdatedPages * elementSize;
  return size;
}

BCStateTran::ElementOfVirtualBlock *BCStateTran::getVirtualElement(uint32_t index,
                                                                   uint32_t pageSize,
                                                                   char *virtualBlock) {
  HeaderOfVirtualBlock *h = reinterpret_cast<HeaderOfVirtualBlock *>(virtualBlock);
  ConcordAssertLT(index, h->numberOfUpdatedPages);

  const uint32_t elementSize = sizeof(ElementOfVirtualBlock) + pageSize - 1;
  char *p = virtualBlock + sizeof(HeaderOfVirtualBlock) + (index * elementSize);
  return reinterpret_cast<ElementOfVirtualBlock *>(p);
}

bool BCStateTran::checkStructureOfVirtualBlock(char *virtualBlock,
                                               uint32_t virtualBlockSize,
                                               uint32_t pageSize,
                                               logging::Logger &logger) {
  if (virtualBlockSize < sizeof(HeaderOfVirtualBlock)) {
    LOG_ERROR(logger, KVLOG(virtualBlockSize, sizeof(HeaderOfVirtualBlock)));
    return false;
  }

  const uint32_t arrayBlockSize = virtualBlockSize - sizeof(HeaderOfVirtualBlock);
  const uint32_t elementSize = sizeof(ElementOfVirtualBlock) + pageSize - 1;

  if (arrayBlockSize % elementSize != 0) {
    LOG_ERROR(logger, KVLOG(arrayBlockSize, elementSize));
    return false;
  }

  uint32_t numOfElements = (arrayBlockSize / elementSize);
  HeaderOfVirtualBlock *h = reinterpret_cast<HeaderOfVirtualBlock *>(virtualBlock);

  if (numOfElements != h->numberOfUpdatedPages) {
    LOG_ERROR(logger, KVLOG(numOfElements, h->numberOfUpdatedPages));
    return false;
  }

  uint32_t lastPageId = UINT32_MAX;
  for (uint32_t i = 0; i < numOfElements; i++) {
    char *p = virtualBlock + sizeof(HeaderOfVirtualBlock) + (i * elementSize);
    ElementOfVirtualBlock *e = reinterpret_cast<ElementOfVirtualBlock *>(p);

    if (e->checkpointNumber <= h->lastCheckpointKnownToRequester) {
      LOG_ERROR(logger, KVLOG(e->checkpointNumber, h->lastCheckpointKnownToRequester, i, e->pageId));
      return false;
    }
    if (e->pageDigest.isZero()) {
      LOG_ERROR(logger, "pageDigest is zero!" << KVLOG(i, e->pageId));
      return false;
    }
    if (i > 0 && e->pageId <= lastPageId) {
      LOG_ERROR(logger, KVLOG(i, e->pageId, lastPageId));
      return false;
    }
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
      LOG_WARN(logger_, "SeqNum Counter reached max value");
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
    LOG_WARN(logger_,
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
        logger_,
        "Msg rejected because its sequence number is not monotonic: " << KVLOG(replicaId, msgSeqNum, lastMsgSeqNum));
    return false;
  }

  lastMsgSeqNumOfReplicas_[replicaId] = msgSeqNum;
  LOG_DEBUG(logger_, "Msg accepted: " << KVLOG(msgSeqNum));
  return true;
}

//////////////////////////////////////////////////////////////////////////////
// BCStateTran::BlocksBatchDesc
//////////////////////////////////////////////////////////////////////////////

inline std::ostream &operator<<(std::ostream &os, const BCStateTran::BlocksBatchDesc &b) {
  os << b.toString();
  return os;
}

inline std::string BCStateTran::BlocksBatchDesc::toString() const {
  std::string str;
  str += KVLOG(minBlockId, maxBlockId, nextBlockId, upperBoundBlockId);
  return str;
}

inline void BCStateTran::BlocksBatchDesc::reset() {
  minBlockId = 0;
  maxBlockId = 0;
  nextBlockId = 0;
  upperBoundBlockId = 0;
}

// BlocksBatchDesc A is 'behind' (aka <) BlocksBatchDesc B if:
// If A.minBlockId == B.minBlockId: then if A.nextBlockId > B.nextBlockId -> A < B
// If A.minBlockId != B.minBlockId: then if A.minBlockId < B.minBlockId -> A < B
// This is due to the way ST collects blocks, from newer to older inside ranges, and from older to newer ranges.
inline bool BCStateTran::BlocksBatchDesc::operator<(const BCStateTran::BlocksBatchDesc &rhs) const {
  if (minBlockId != rhs.minBlockId) {
    return (minBlockId < rhs.minBlockId);
  }
  // minBlockId == rhs.minBlockId
  return nextBlockId > rhs.nextBlockId;
}

inline bool BCStateTran::BlocksBatchDesc::operator==(const BCStateTran::BlocksBatchDesc &rhs) const {
  return (minBlockId == rhs.minBlockId) && (maxBlockId == rhs.maxBlockId) && (nextBlockId == rhs.nextBlockId) &&
         (upperBoundBlockId == rhs.upperBoundBlockId);
}

inline bool BCStateTran::BlocksBatchDesc::isValid() const {
  bool valid = ((minBlockId != 0) && (minBlockId <= maxBlockId) && (minBlockId <= nextBlockId) &&
                (nextBlockId <= maxBlockId) && (maxBlockId <= upperBoundBlockId));
  if (!valid) {
    LOG_ERROR(ST_DST_LOG, *this);
  }
  return valid;
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

bool BCStateTran::isActiveDest(FetchingState fs) {
  return (fs == FetchingState::GettingMissingBlocks) || (fs == FetchingState::GettingCheckpointSummaries) ||
         (fs == FetchingState::GettingMissingResPages);
}

inline std::ostream &operator<<(std::ostream &os, const BCStateTran::FetchingState fs) {
  os << BCStateTran::stateName(fs);
  return os;
}

bool BCStateTran::isFetching() const { return (psd_->getIsFetchingState()); }

void BCStateTran::onFetchingStateChange(FetchingState newFetchingState) {
  LOG_INFO(logger_,
           "FetchingState changed from " << stateName(lastFetchingState_) << " to " << stateName(newFetchingState));
  switch (lastFetchingState_) {
    case FetchingState::NotFetching:
      break;
    case FetchingState::GettingCheckpointSummaries:
      gettingCheckpointSummariesDT_.stop();
      break;
    case FetchingState::GettingMissingBlocks:
      break;
    case FetchingState::GettingMissingResPages:
      gettingMissingResPagesDT_.stop();
      break;
  }
  switch (newFetchingState) {
    case FetchingState::NotFetching:
      cycleDT_.stop(true);
      break;
    case FetchingState::GettingCheckpointSummaries:
      cycleDT_.start();
      gettingCheckpointSummariesDT_.start();
      break;
    case FetchingState::GettingMissingBlocks:
      targetCheckpointDesc_ = psd_->getCheckpointBeingFetched();
      // Determine the next required block
      ConcordAssert(digestOfNextRequiredBlock_.isZero());
      fetchState_ = computeNextBatchToFetch(psd_->getFirstRequiredBlock());
      commitState_ = fetchState_;
      LOG_INFO(logger_, KVLOG(fetchState_, commitState_));

      gettingMissingBlocksDT_.start();
      blocksFetched_.start();
      bytesFetched_.start();
      break;
    case FetchingState::GettingMissingResPages:
      gettingMissingResPagesDT_.start();

      targetCheckpointDesc_ = psd_->getCheckpointBeingFetched();
      fetchState_.nextBlockId = ID_OF_VBLOCK_RES_PAGES;
      digestOfNextRequiredBlock_ = targetCheckpointDesc_.digestOfResPagesDescriptor;
      break;
  }

  logger_ = (newFetchingState == FetchingState::NotFetching) ? ST_SRC_LOG : ST_DST_LOG;
  metrics_.fetching_state_.Get().Set(stateName(newFetchingState));
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

  LOG_INFO(logger_, KVLOG(lastMsgSeqNum_, msg.minRelevantCheckpointNum));

  sendToAllOtherReplicas(reinterpret_cast<char *>(&msg), sizeof(AskForCheckpointSummariesMsg));
}

void BCStateTran::trySendFetchBlocksMsg(int16_t lastKnownChunkInLastRequiredBlock, string &&reason) {
  ConcordAssertEQ(getFetchingState(), FetchingState::GettingMissingBlocks);
  ConcordAssert(sourceSelector_.hasSource());

  // If the ioPool_ is empty, we don't have any more capacity to call for new jobs. no reason to ask for more data
  // from source.We need to wait.
  if (ioPool_.empty()) {
    LOG_WARN(logger_, "Postpone sending FetchBlocksMsg while ioPool_ is empty!");
    postponedSendFetchBlocksMsg_ = true;
    return;
  }

  FetchBlocksMsg msg;
  lastMsgSeqNum_ = uniqueMsgSeqNum();
  metrics_.last_msg_seq_num_.Get().Set(lastMsgSeqNum_);

  msg.msgSeqNum = lastMsgSeqNum_;
  msg.minBlockId = fetchState_.minBlockId;
  msg.maxBlockId = fetchState_.maxBlockId;
  msg.lastKnownChunkInLastRequiredBlock = lastKnownChunkInLastRequiredBlock;
  msg.rvbGroupId = rvbm_->getFetchBlocksRvbGroupId(msg.minBlockId, msg.maxBlockId);

  LOG_INFO(logger_,
           "Sending FetchBlocksMsg:" << reason
                                     << KVLOG(sourceSelector_.currentReplica(),
                                              msg.msgSeqNum,
                                              msg.minBlockId,
                                              msg.maxBlockId,
                                              msg.lastKnownChunkInLastRequiredBlock,
                                              msg.rvbGroupId));

  replicaForStateTransfer_->sendStateTransferMessage(
      reinterpret_cast<char *>(&msg), sizeof(FetchBlocksMsg), sourceSelector_.currentReplica());
  sourceSelector_.setFetchingTimeStamp(getMonotonicTimeMilli(), true);
  metrics_.sent_fetch_blocks_msg_++;
  dst_time_between_sendFetchBlocksMsg_rec_.end();  // if it was never started, this operation does nothing
  dst_time_between_sendFetchBlocksMsg_rec_.start();
  postponedSendFetchBlocksMsg_ = false;
}

void BCStateTran::sendFetchResPagesMsg(int16_t lastKnownChunkInLastRequiredBlock) {
  ConcordAssertEQ(getFetchingState(), FetchingState::GettingMissingResPages);
  ConcordAssert(sourceSelector_.hasSource());
  ConcordAssert(psd_->hasCheckpointBeingFetched());

  uint64_t lastStoredCheckpoint = psd_->getLastStoredCheckpoint();
  lastMsgSeqNum_ = uniqueMsgSeqNum();
  metrics_.last_msg_seq_num_.Get().Set(lastMsgSeqNum_);

  FetchResPagesMsg msg;
  msg.msgSeqNum = lastMsgSeqNum_;
  msg.lastCheckpointKnownToRequester = lastStoredCheckpoint;
  msg.requiredCheckpointNum = targetCheckpointDesc_.checkpointNum;
  msg.lastKnownChunk = lastKnownChunkInLastRequiredBlock;

  LOG_INFO(logger_,
           KVLOG(sourceSelector_.currentReplica(),
                 msg.msgSeqNum,
                 msg.lastCheckpointKnownToRequester,
                 msg.requiredCheckpointNum,
                 msg.lastKnownChunk));

  sourceSelector_.setFetchingTimeStamp(getMonotonicTimeMilli(), true);
  replicaForStateTransfer_->sendStateTransferMessage(
      reinterpret_cast<char *>(&msg), sizeof(FetchResPagesMsg), sourceSelector_.currentReplica());
  metrics_.sent_fetch_res_pages_msg_++;
}

//////////////////////////////////////////////////////////////////////////////
// Message handlers
//////////////////////////////////////////////////////////////////////////////

bool BCStateTran::onMessage(const AskForCheckpointSummariesMsg *m, uint32_t msgLen, uint16_t replicaId) {
  SCOPED_MDC_SEQ_NUM(getSequenceNumber(replicaId, m->msgSeqNum));
  LOG_INFO(logger_, KVLOG(replicaId, m->msgSeqNum));

  metrics_.received_ask_for_checkpoint_summaries_msg_++;

  // if msg is invalid
  if (msgLen < sizeof(AskForCheckpointSummariesMsg) || m->minRelevantCheckpointNum == 0 || m->msgSeqNum == 0) {
    LOG_WARN(logger_, "Msg is invalid: " << KVLOG(msgLen, m->minRelevantCheckpointNum, m->msgSeqNum));
    metrics_.invalid_ask_for_checkpoint_summaries_msg_++;
    return false;
  }

  // if msg is not relevant
  bool isFetching = psd_->getIsFetchingState();
  auto lastStoredCheckpoint = psd_->getLastStoredCheckpoint();
  if (auto seqNumInvalid = !checkValidityAndSaveMsgSeqNum(replicaId, m->msgSeqNum) || isFetching ||
                           (m->minRelevantCheckpointNum > lastStoredCheckpoint)) {
    LOG_WARN(
        logger_,
        "Msg is irrelevant: " << KVLOG(isFetching, seqNumInvalid, m->minRelevantCheckpointNum, lastStoredCheckpoint));
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

    DataStore::CheckpointDesc cpDesc = psd_->getCheckpointDesc(i);
    auto deleter = [](CheckpointSummaryMsg *msg) {
      char *bytes = reinterpret_cast<char *>(msg);
      delete[] bytes;
    };
    auto msg = std::unique_ptr<CheckpointSummaryMsg, decltype(deleter)>(
        CheckpointSummaryMsg::create(cpDesc.rvbData.size()), deleter);

    msg->checkpointNum = i;
    msg->maxBlockId = cpDesc.maxBlockId;
    msg->digestOfMaxBlockId = cpDesc.digestOfMaxBlockId;
    msg->digestOfResPagesDescriptor = cpDesc.digestOfResPagesDescriptor;
    msg->requestMsgSeqNum = m->msgSeqNum;
    std::copy(cpDesc.rvbData.begin(), cpDesc.rvbData.end(), msg->data);

    LOG_INFO(logger_,
             "Sending CheckpointSummaryMsg: " << KVLOG(toReplicaId,
                                                       msg->checkpointNum,
                                                       msg->maxBlockId,
                                                       msg->digestOfMaxBlockId,
                                                       msg->digestOfResPagesDescriptor,
                                                       msg->requestMsgSeqNum,
                                                       msg->sizeofRvbData()));

    replicaForStateTransfer_->sendStateTransferMessage(reinterpret_cast<char *>(msg.get()), msg->size(), replicaId);

    metrics_.sent_checkpoint_summary_msg_++;
    sent = true;
  }

  if (!sent) {
    LOG_INFO(logger_, "Failed to send relevant CheckpointSummaryMsg: " << KVLOG(toReplicaId));
  }
  return false;
}

bool BCStateTran::onMessage(const CheckpointSummaryMsg *m, uint32_t msgLen, uint16_t replicaId) {
  SCOPED_MDC_SEQ_NUM(getSequenceNumber(config_.myReplicaId, uniqueMsgSeqNum()));
  LOG_INFO(logger_, KVLOG(replicaId, m->checkpointNum, m->maxBlockId, m->requestMsgSeqNum, m->sizeofRvbData()));

  metrics_.received_checkpoint_summary_msg_++;
  FetchingState fs = getFetchingState();
  if (fs != FetchingState::GettingCheckpointSummaries) {
    auto fetchingState = stateName(getFetchingState());
    LOG_WARN(logger_, "Msg is irrelevant: " << KVLOG(fetchingState));
    metrics_.irrelevant_checkpoint_summary_msg_++;
    return false;
  }

  // if msg is invalid
  if (msgLen != m->size() || m->checkpointNum == 0 || m->digestOfResPagesDescriptor.isZero() ||
      m->requestMsgSeqNum == 0) {
    LOG_WARN(logger_,
             "Msg is invalid: " << KVLOG(
                 replicaId, msgLen, m->checkpointNum, m->digestOfResPagesDescriptor.isZero(), m->requestMsgSeqNum));
    metrics_.invalid_checkpoint_summary_msg_++;
    return false;
  }

  // if msg is not relevant
  if (m->requestMsgSeqNum != lastMsgSeqNum_ || m->checkpointNum <= psd_->getLastStoredCheckpoint()) {
    LOG_WARN(logger_,
             "Msg is irrelevant: " << KVLOG(
                 replicaId, m->requestMsgSeqNum, lastMsgSeqNum_, m->checkpointNum, psd_->getLastStoredCheckpoint()));
    metrics_.irrelevant_checkpoint_summary_msg_++;
    return false;
  }

  uint16_t numOfMsgsFromSender =
      (numOfSummariesFromOtherReplicas.count(replicaId) == 0) ? 0 : numOfSummariesFromOtherReplicas.at(replicaId);

  // if we have too many messages from the same replica
  if (numOfMsgsFromSender >= (psd_->getMaxNumOfStoredCheckpoints() + 1)) {
    LOG_WARN(logger_, "Too many messages from replica: " << KVLOG(replicaId, numOfMsgsFromSender));
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
    LOG_INFO(logger_, "Does not have enough CheckpointSummaryMsg messages");
    return true;
  }

  LOG_INFO(logger_, "Has enough CheckpointSummaryMsg messages");
  CheckpointSummaryMsg *cpSummaryMsg = cert->bestCorrectMsg();

  ConcordAssertNE(cpSummaryMsg, nullptr);
  ConcordAssert(sourceSelector_.isReset());
  ConcordAssertEQ(fetchState_.nextBlockId, 0);
  ConcordAssert(digestOfNextRequiredBlock_.isZero());
  ConcordAssert(pendingItemDataMsgs.empty());
  ConcordAssert(ioContexts_.empty());
  ConcordAssert(ioPool_.full());
  ConcordAssertEQ(totalSizeOfPendingItemDataMsgs, 0);

  // Set (overwrite) the RVB data. We set it even if the RVB data is empty, to make sure that an empty tree
  // is acceptable
  auto sizeofRvbData = cpSummaryMsg->sizeofRvbData();
  if (!rvbm_->setRvbData((sizeofRvbData > 0) ? cpSummaryMsg->data : nullptr,
                         sizeofRvbData,
                         as_->getLastReachableBlockNum() + 1,
                         cpSummaryMsg->maxBlockId)) {
    LOG_ERROR(logger_, "Failed to set new RVT data! restart cycle...");
    // enter new cycle
    startCollectingStateInternal();
    return true;
  }
  metrics_.current_rvb_data_state_.Get().Set(rvbm_->getStateOfRvbData());

  // set the preferred replicas
  for (uint16_t r : replicas_) {  // TODO(GG): can be improved
    CheckpointSummaryMsg *t = cert->getMsgFromReplica(r);
    if (t != nullptr && CheckpointSummaryMsg::equivalent(t, cpSummaryMsg)) {
      sourceSelector_.addPreferredReplica(r);
      ConcordAssertLT(r, config_.numReplicas);
    }
  }
  ConcordAssertGE(sourceSelector_.numberOfPreferredReplicas(), config_.fVal + 1);

  // set new checkpoint
  DataStore::CheckpointDesc newCheckpoint;
  newCheckpoint.checkpointNum = cpSummaryMsg->checkpointNum;
  newCheckpoint.maxBlockId = cpSummaryMsg->maxBlockId;
  newCheckpoint.digestOfMaxBlockId = cpSummaryMsg->digestOfMaxBlockId;
  newCheckpoint.digestOfResPagesDescriptor = cpSummaryMsg->digestOfResPagesDescriptor;
  newCheckpoint.rvbData.insert(
      newCheckpoint.rvbData.begin(), cpSummaryMsg->data, cpSummaryMsg->data + cpSummaryMsg->sizeofRvbData());

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

    LOG_INFO(logger_,
             "Start fetching checkpoint: " << KVLOG(newCheckpoint.checkpointNum,
                                                    newCheckpoint.maxBlockId,
                                                    newCheckpoint.digestOfMaxBlockId,
                                                    lastReachableBlockNum,
                                                    fetchingState));

    if (newCheckpoint.maxBlockId > lastReachableBlockNum) {
      // fetch blocks
      g.txn()->setFirstRequiredBlock(lastReachableBlockNum + 1);
      g.txn()->setLastRequiredBlock(newCheckpoint.maxBlockId);
      minBlockIdToCollectInCycle_ = lastReachableBlockNum + 1;
      maxBlockIdToCollectInCycle_ = newCheckpoint.maxBlockId;
      ConcordAssertGE(maxBlockIdToCollectInCycle_, minBlockIdToCollectInCycle_);
      totalBlocksLeftToCollectInCycle_ = maxBlockIdToCollectInCycle_ - minBlockIdToCollectInCycle_ + 1;
    } else {
      // fetch reserved pages (vblock)
      ConcordAssertEQ(newCheckpoint.maxBlockId, lastReachableBlockNum);
      ConcordAssertEQ(g.txn()->getFirstRequiredBlock(), 0);
      ConcordAssertEQ(g.txn()->getLastRequiredBlock(), 0);
    }
  }
  metrics_.last_block_.Get().Set(newCheckpoint.maxBlockId);

  processData();
  return true;
}

uint16_t BCStateTran::getBlocksConcurrentAsync(uint64_t nextBlockId, uint64_t firstRequiredBlock, uint16_t numBlocks) {
  ConcordAssertGE(config_.maxNumberOfChunksInBatch, numBlocks);
  size_t j{};

  LOG_DEBUG(logger_, KVLOG(nextBlockId, firstRequiredBlock, numBlocks, ioPool_.numFreeElements()));
  for (uint64_t i{nextBlockId}; (i >= firstRequiredBlock) && (j < numBlocks) && !ioPool_.empty(); --i, ++j) {
    if (!ioContexts_.empty()) {
      ConcordAssertEQ(i + 1, ioContexts_.back()->blockId);
    }
    auto ctx = ioPool_.alloc();
    ctx->blockId = i;
    ctx->future = as_->getBlockAsync(ctx->blockId, ctx->blockData.get(), config_.maxBlockSize, &ctx->actualBlockSize);
    ioContexts_.push_back(std::move(ctx));
  }

  return j;
}

bool BCStateTran::onMessage(const FetchBlocksMsg *m, uint32_t msgLen, uint16_t replicaId) {
  SCOPED_MDC_SEQ_NUM(getSequenceNumber(replicaId, m->msgSeqNum));
  LOG_INFO(logger_, KVLOG(replicaId, m->msgSeqNum, m->minBlockId, m->maxBlockId, m->lastKnownChunkInLastRequiredBlock));
  metrics_.received_fetch_blocks_msg_++;

  // if msg is invalid
  if (msgLen < sizeof(FetchBlocksMsg) || m->msgSeqNum == 0 || m->minBlockId == 0 || m->maxBlockId < m->minBlockId) {
    LOG_WARN(logger_, "Msg is invalid: " << KVLOG(replicaId, m->msgSeqNum, m->minBlockId, m->maxBlockId));
    metrics_.invalid_fetch_blocks_msg_++;
    return false;
  }

  // if msg is not relevant
  if (!checkValidityAndSaveMsgSeqNum(replicaId, m->msgSeqNum)) {
    LOG_WARN(logger_, "Msg is irrelevant: " << KVLOG(replicaId, m->msgSeqNum));
    metrics_.irrelevant_fetch_blocks_msg_++;
    return false;
  }

  FetchingState fetchingState = getFetchingState();
  auto lastReachableBlockNum = as_->getLastReachableBlockNum();

  // if msg should be rejected
  auto sendRejectFetchingMsg = [&](const char *reason) {
    RejectFetchingMsg outMsg;

    outMsg.requestMsgSeqNum = m->msgSeqNum;
    metrics_.sent_reject_fetch_msg_++;
    LOG_WARN(logger_,
             "Rejecting msg. Sending RejectFetchingMsg to replica: " << KVLOG(reason,
                                                                              replicaId,
                                                                              outMsg.requestMsgSeqNum,
                                                                              stateName(fetchingState),
                                                                              m->maxBlockId,
                                                                              lastReachableBlockNum));
    replicaForStateTransfer_->sendStateTransferMessage(
        reinterpret_cast<char *>(&outMsg), sizeof(RejectFetchingMsg), replicaId);
  };

  if (isActiveDest(fetchingState)) {
    sendRejectFetchingMsg("In state transfer");
    return false;
  }
  if (m->maxBlockId > lastReachableBlockNum) {
    sendRejectFetchingMsg("Required blocks range is not present");
    return false;
  }
  auto [sessionOpened, otherReplicaSessionClosed] = sourceSession_.tryOpen(replicaId);
  if (sessionOpened == false) {
    auto reason = "Active session with ownerDestReplicaId=" + std::to_string(sourceSession_.ownerDestReplicaId());
    sendRejectFetchingMsg(reason.c_str());
    return false;
  }
  if (otherReplicaSessionClosed) {
    clearIoContexts();
  }

  // start recording time to send a whole batch, and its size
  uint64_t batchSizeBytes = 0;
  uint64_t batchSizeChunks = 0;
  src_send_batch_duration_rec_.clear();
  src_send_batch_duration_rec_.start();

  // compute information about next block and chunk
  uint64_t nextBlockId = m->maxBlockId;
  uint16_t nextChunk = m->lastKnownChunkInLastRequiredBlock + 1;
  uint16_t numOfSentChunks = 0;

  if (uint64_t numBlocksInCurrentBatch = std::min(static_cast<uint64_t>(config_.maxNumberOfChunksInBatch),
                                                  static_cast<uint64_t>(nextBlockId - m->minBlockId + 1));
      !config_.enableSourceBlocksPreFetch || ioContexts_.empty() || (ioContexts_.front()->blockId != nextBlockId) ||
      (numBlocksInCurrentBatch > ioContexts_.size()) || !ioContexts_.front()->future.valid()) {
    if (ioContexts_.empty()) {
      LOG_INFO(logger_,
               "Call getBlocksConcurrentAsync(1): source blocks prefetch disabled (first batch or retransmission):"
                   << KVLOG(config_.enableSourceBlocksPreFetch));
      getBlocksConcurrentAsync(nextBlockId, m->minBlockId, numBlocksInCurrentBatch);
    } else if (ioContexts_.front()->blockId != nextBlockId) {
      LOG_INFO(logger_,
               "Call getBlocksConcurrentAsync(2): source blocks prefetch disabled (first batch or retransmission):"
                   << KVLOG(config_.enableSourceBlocksPreFetch, ioContexts_.front()->blockId, nextBlockId));
      clearIoContexts();
      getBlocksConcurrentAsync(nextBlockId, m->minBlockId, numBlocksInCurrentBatch);
    } else if ((numBlocksInCurrentBatch > ioContexts_.size())) {
      LOG_INFO(logger_,
               "Call getBlocksConcurrentAsync(3): source blocks prefetch disabled (first batch or retransmission):"
                   << KVLOG(config_.enableSourceBlocksPreFetch,
                            ioContexts_.front()->blockId,
                            nextBlockId,
                            numBlocksInCurrentBatch,
                            ioContexts_.size(),
                            m->minBlockId));
      getBlocksConcurrentAsync(
          nextBlockId - ioContexts_.size(), m->minBlockId, numBlocksInCurrentBatch - ioContexts_.size());
    }
  }

  // Fetch blocks and send all chunks for the batch. Also, while looping start to pre-fetch next batch
  // We pre-fetch only if feature enabled, and we are not in the last batch
  // Setting preFetchBlockId to 0 disable pre-fetching on all later code.
  uint64_t preFetchBlockId = (config_.enableSourceBlocksPreFetch && (nextBlockId > config_.maxNumberOfChunksInBatch))
                                 ? (nextBlockId - config_.maxNumberOfChunksInBatch)
                                 : 0;
  LOG_INFO(logger_,
           "Start sending batch: " << KVLOG(m->msgSeqNum,
                                            m->minBlockId,
                                            m->maxBlockId,
                                            m->lastKnownChunkInLastRequiredBlock,
                                            m->rvbGroupId,
                                            preFetchBlockId));
  ++sourceSession_.batchCounter_;
  bool getNextBlock = (nextChunk == 1);
  char *buffer = nullptr;
  uint32_t sizeOfNextBlock = 0;
  // Source is asking all digests for RVBGroup rvbGroupId. Piggyback this data on the 1st message sent.
  size_t rvbGroupDigestsExpectedSize =
      (m->rvbGroupId != 0) ? rvbm_->getSerializedDigestsOfRvbGroup(m->rvbGroupId, nullptr, 0, true) : 0;
  if ((rvbGroupDigestsExpectedSize == 0) && (m->rvbGroupId != 0)) {
    // Destination RVB Group request cannot be fullfiled, reject
    auto reason = "RVB Group request cannot be fullfiled, rejecting request:" + KVLOG(m->rvbGroupId);
    sendRejectFetchingMsg(reason.c_str());
    return false;
  }
  do {
    auto &ctx = ioContexts_.front();
    if (getNextBlock) {
      // wait for worker to finish getting next block
      ConcordAssert(ctx->future.valid());
      try {
        TimeRecorder scoped_timer(*histograms_.src_next_block_wait_duration);
        if (!ctx->future.get()) {
          auto reason = "Block not found in storage, abort batch:" + KVLOG(ctx->blockId);
          sendRejectFetchingMsg(reason.c_str());
          return false;
        }
      } catch (const std::exception &ex) {
        LOG_FATAL(logger_, "exception:" << ex.what());
        ConcordAssert(false);
      }
      ConcordAssertGT(ctx->actualBlockSize, 0);
      ConcordAssertEQ(ctx->blockId, nextBlockId);
      LOG_DEBUG(logger_,
                "Start sending next block: " << KVLOG(sourceSession_.batchCounter_, nextBlockId, ctx->actualBlockSize));
      histograms_.src_get_block_size_bytes->record(ctx->actualBlockSize);
      getNextBlock = false;
    }
    buffer = ctx->blockData.get();
    sizeOfNextBlock = ctx->actualBlockSize;

    uint32_t sizeOfLastChunk = config_.maxChunkSize;
    uint32_t numOfChunksInNextBlock = sizeOfNextBlock / config_.maxChunkSize;
    if ((sizeOfNextBlock % config_.maxChunkSize) != 0) {
      sizeOfLastChunk = sizeOfNextBlock % config_.maxChunkSize;
      numOfChunksInNextBlock++;
    }

    // if msg is invalid (lastKnownChunkInLastRequiredBlock+1 does not exist)
    if ((numOfSentChunks == 0) && (nextChunk > numOfChunksInNextBlock)) {
      auto reason = "Msg is invalid (illegal chunk number):" + KVLOG(replicaId, nextChunk, numOfChunksInNextBlock);
      sendRejectFetchingMsg(reason.c_str());
      return false;
    }

    SCOPED_MDC_SEQ_NUM(getSequenceNumber(replicaId, m->msgSeqNum, nextChunk, nextBlockId));
    uint32_t chunkSize = (nextChunk < numOfChunksInNextBlock) ? config_.maxChunkSize : sizeOfLastChunk;
    batchSizeBytes += chunkSize;
    ++batchSizeChunks;

    ConcordAssertGT(chunkSize, 0);

    char *pRawChunk = buffer + (nextChunk - 1) * config_.maxChunkSize;
    ItemDataMsg *outMsg = ItemDataMsg::alloc(chunkSize + rvbGroupDigestsExpectedSize);  // TODO(GG): improve

    outMsg->requestMsgSeqNum = m->msgSeqNum;
    outMsg->blockNumber = nextBlockId;
    outMsg->totalNumberOfChunksInBlock = numOfChunksInNextBlock;
    outMsg->chunkNumber = nextChunk;
    outMsg->dataSize = chunkSize + rvbGroupDigestsExpectedSize;

    outMsg->lastInBatch =
        ((numOfSentChunks + 1) >= config_.maxNumberOfChunksInBatch) || ((nextBlockId - 1) < m->minBlockId);
    // TODO - this is a rare request, coming once in many blocks (configurable).
    // For now, we fetch from storage and serialize at the last moment.
    // Performance can be improved by performing this operation earlier.
    if (rvbGroupDigestsExpectedSize > 0) {
      // Serialize RVB digests
      DurationTracker<std::chrono::milliseconds> serialize_digests_dt("serialize_digests_dt", true);
      size_t rvbGroupDigestsActualSize =
          rvbm_->getSerializedDigestsOfRvbGroup(m->rvbGroupId, outMsg->data, rvbGroupDigestsExpectedSize, false);
      if ((rvbGroupDigestsActualSize == 0) || (rvbGroupDigestsExpectedSize != rvbGroupDigestsActualSize)) {
        auto reason = "Rejecting message - not holding all requested digests (or some other error)" +
                      KVLOG(rvbGroupDigestsExpectedSize, rvbGroupDigestsActualSize);
        ItemDataMsg::free(outMsg);
        sendRejectFetchingMsg(reason.c_str());
        return false;
      }
      auto total_duration = serialize_digests_dt.totalDuration(true);
      LOG_INFO(logger_, "Done getting serialized digests," << KVLOG(total_duration) << " ms");
      ConcordAssertLE(rvbGroupDigestsActualSize, rvbGroupDigestsExpectedSize);
      outMsg->rvbDigestsSize = rvbGroupDigestsActualSize;
      memcpy(outMsg->data + rvbGroupDigestsActualSize, pRawChunk, chunkSize);
      rvbGroupDigestsExpectedSize = 0;  // send only once
    } else {
      memcpy(outMsg->data, pRawChunk, chunkSize);
      outMsg->rvbDigestsSize = 0;
    }

    LOG_DEBUG(logger_,
              "Sending ItemDataMsg: " << std::boolalpha
                                      << KVLOG(replicaId,
                                               outMsg->requestMsgSeqNum,
                                               outMsg->blockNumber,
                                               outMsg->totalNumberOfChunksInBlock,
                                               outMsg->chunkNumber,
                                               outMsg->dataSize,
                                               outMsg->rvbDigestsSize,
                                               (bool)outMsg->lastInBatch));

    metrics_.sent_item_data_msg_++;
    replicaForStateTransfer_->sendStateTransferMessage(reinterpret_cast<char *>(outMsg), outMsg->size(), replicaId);
    ItemDataMsg::free(outMsg);
    numOfSentChunks++;

    auto finalizeContext = [&]() {
      ioPool_.free(ctx);
      ioContexts_.pop_front();

      // We are done using this context. We can now use it to prefetch future batch block.
      if (preFetchBlockId > 0) {
        getBlocksConcurrentAsync(preFetchBlockId, m->minBlockId, 1);
        --preFetchBlockId;
      }
    };

    // if we've already sent enough chunks
    if (numOfSentChunks >= config_.maxNumberOfChunksInBatch) {
      LOG_INFO(logger_, "Batch end - sent enough chunks: " << KVLOG(numOfSentChunks, m->minBlockId, m->maxBlockId));
      if (nextChunk == numOfChunksInNextBlock) {
        finalizeContext();
      }
      break;
    } else if (nextChunk < numOfChunksInNextBlock) {
      // we still have chunks in block
      nextChunk++;
    } else {
      finalizeContext();

      if ((nextBlockId - 1) < m->minBlockId) {
        LOG_INFO(logger_, "Batch end - sent all relevant blocks: " << KVLOG(m->minBlockId, m->maxBlockId));
        break;
      } else {
        // no more chunks in the block, continue to next block
        --nextBlockId;
        nextChunk = 1;
        getNextBlock = true;
      }
    }
  } while (true);

  histograms_.src_send_batch_size_bytes->record(batchSizeBytes);
  histograms_.src_send_batch_num_of_chunks->record(batchSizeChunks);
  src_send_batch_duration_rec_.end();

  return false;
}

bool BCStateTran::onMessage(const FetchResPagesMsg *m, uint32_t msgLen, uint16_t replicaId) {
  SCOPED_MDC_SEQ_NUM(getSequenceNumber(replicaId, m->msgSeqNum));
  LOG_INFO(
      logger_,
      KVLOG(replicaId, m->msgSeqNum, m->lastCheckpointKnownToRequester, m->requiredCheckpointNum, m->lastKnownChunk));
  metrics_.received_fetch_res_pages_msg_++;

  // if msg is invalid
  if (msgLen < sizeof(FetchResPagesMsg) || m->msgSeqNum == 0 || m->requiredCheckpointNum == 0) {
    LOG_WARN(logger_, "Msg is invalid: " << KVLOG(replicaId, msgLen, m->msgSeqNum, m->requiredCheckpointNum));
    metrics_.invalid_fetch_res_pages_msg_++;
    return false;
  }

  // if msg is not relevant
  if (!checkValidityAndSaveMsgSeqNum(replicaId, m->msgSeqNum)) {
    LOG_WARN(logger_, "Msg is irrelevant: " << KVLOG(replicaId, m->msgSeqNum));
    metrics_.irrelevant_fetch_res_pages_msg_++;
    return false;
  }

  FetchingState fetchingState = getFetchingState();

  // if msg should be rejected
  if ((isActiveDest(fetchingState)) || (!psd_->hasCheckpointDesc(m->requiredCheckpointNum))) {
    RejectFetchingMsg outMsg;
    outMsg.requestMsgSeqNum = m->msgSeqNum;

    LOG_WARN(logger_,
             "Rejecting msg. Sending RejectFetchingMsg to replica " << KVLOG(replicaId,
                                                                             fetchingState,
                                                                             outMsg.requestMsgSeqNum,
                                                                             m->msgSeqNum,
                                                                             m->lastCheckpointKnownToRequester,
                                                                             m->requiredCheckpointNum,
                                                                             m->lastKnownChunk));

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
    LOG_DEBUG(logger_,
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
  ConcordAssert(checkStructureOfVirtualBlock(vblock, vblockSize, config_.sizeOfReservedPage, logger_));

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
    LOG_WARN(logger_, "Msg is invalid: illegal chunk number: " << KVLOG(replicaId, nextChunk, numOfChunksInVBlock));
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
    outMsg->lastInBatch =
        ((numOfSentChunks + 1) >= config_.maxNumberOfChunksInBatch || (nextChunk == numOfChunksInVBlock));
    memcpy(outMsg->data, pRawChunk, chunkSize);

    LOG_DEBUG(logger_,
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
      LOG_DEBUG(logger_, "Batch end - sent enough chunks: " << KVLOG(numOfSentChunks));
      break;
    }
    // if we still have chunks in block
    if (nextChunk < numOfChunksInVBlock) {
      nextChunk++;
    } else {  // we sent all chunks
      LOG_DEBUG(logger_, "Batch end - sent all relevant chunks");
      break;
    }
  }
  return false;
}

bool BCStateTran::onMessage(const RejectFetchingMsg *m, uint32_t msgLen, uint16_t replicaId) {
  LOG_DEBUG(logger_, KVLOG(replicaId, m->requestMsgSeqNum));
  metrics_.received_reject_fetching_msg_++;

  FetchingState fs = getFetchingState();
  if (fs != FetchingState::GettingMissingBlocks && fs != FetchingState::GettingMissingResPages) {
    LOG_ERROR(logger_,
              "Expected Fetching State GettingMissingBlocks or GettingMissingResPages. Got: " << stateName(fs));
    return false;
  }

  // if msg is invalid
  if (msgLen < sizeof(RejectFetchingMsg)) {
    LOG_WARN(logger_, "Msg is invalid: " << KVLOG(replicaId, msgLen));
    metrics_.invalid_reject_fetching_msg_++;
    return false;
  }

  // if msg is not relevant
  if (sourceSelector_.currentReplica() != replicaId || lastMsgSeqNum_ != m->requestMsgSeqNum) {
    LOG_WARN(
        logger_,
        "Msg is irrelevant" << KVLOG(replicaId, sourceSelector_.currentReplica(), lastMsgSeqNum_, m->requestMsgSeqNum));
    metrics_.irrelevant_reject_fetching_msg_++;
    return false;
  }

  if (sourceSelector_.isPreferredSourceId(replicaId)) {
    LOG_WARN(logger_, "Removing replica from preferred replicas: " << KVLOG(replicaId));
    sourceSelector_.removeCurrentReplica();
    clearAllPendingItemsData();
  }

  if (sourceSelector_.hasPreferredReplicas()) {
    processData();
  } else if (fs == FetchingState::GettingMissingBlocks) {
    LOG_DEBUG(logger_, "Adding all peer replicas to preferredReplicas_ (because preferredReplicas_.size()==0)");

    // in this case, we will try to use all other replicas (remove current replica)
    SetAllReplicasAsPreferred();
    sourceSelector_.removeCurrentReplica();
    processData();
  } else if (fs == FetchingState::GettingMissingResPages) {
    // enter new cycle
    startCollectingStateInternal();
  } else {
    ConcordAssert(false);
  }
  return false;
}

// Retrieve either a chunk of a block or a reserved page when fetching
bool BCStateTran::onMessage(const ItemDataMsg *m, uint32_t msgLen, uint16_t replicaId, LocalTimePoint msgArrivalTime) {
  SCOPED_MDC_SEQ_NUM(getSequenceNumber(config_.myReplicaId, lastMsgSeqNum_, m->chunkNumber, m->blockNumber));
  metrics_.received_item_data_msg_++;

  FetchingState fs = getFetchingState();
  if ((fs != FetchingState::GettingMissingBlocks) && (fs != FetchingState::GettingMissingResPages)) {
    LOG_ERROR(logger_,
              "Expected Fetching State GettingMissingBlocks or GettingMissingResPages. Got: " << stateName(fs));
    return false;
  }

  if (!sourceSelector_.isValidSourceId(replicaId)) {
    LOG_ERROR(logger_, "Msg received from invalid source " << replicaId);
    return false;
  }

  const auto MaxNumOfChunksInBlock =
      (fs == FetchingState::GettingMissingBlocks) ? maxNumOfChunksInAppBlock_ : maxNumOfChunksInVBlock_;

  LOG_DEBUG(logger_,
            std::boolalpha << KVLOG(replicaId,
                                    m->requestMsgSeqNum,
                                    m->blockNumber,
                                    m->totalNumberOfChunksInBlock,
                                    m->chunkNumber,
                                    m->dataSize,
                                    (bool)m->lastInBatch,
                                    m->rvbDigestsSize));

  // if msg is invalid
  if ((msgLen != m->size()) || (m->requestMsgSeqNum == 0) || (m->blockNumber == 0) ||
      (m->totalNumberOfChunksInBlock == 0) || (m->totalNumberOfChunksInBlock > MaxNumOfChunksInBlock) ||
      (m->chunkNumber == 0) || (m->dataSize == 0) || (m->rvbDigestsSize >= m->dataSize)) {
    LOG_WARN(logger_,
             "Msg is invalid: " << KVLOG(replicaId,
                                         msgLen,
                                         m->size(),
                                         m->requestMsgSeqNum,
                                         m->blockNumber,
                                         m->totalNumberOfChunksInBlock,
                                         MaxNumOfChunksInBlock,
                                         m->chunkNumber,
                                         m->rvbDigestsSize,
                                         m->dataSize));
    metrics_.invalid_item_data_msg_++;
    return false;
  }

  auto fetchingState = fs;
  if (fs == FetchingState::GettingMissingBlocks) {
    // Reasons for dropping a message as "irrelevant" for this state:
    // 1) Not the source we chose
    // 2) Block ID is out of expected range [fetchState_.minBlockId, fetchState_.nextBlockId]
    // 3) Not enough memory to put block
    // We do not drop on different requestMsgSeqNum - the block arrives from the expected source and might have been
    // delayed due to retransmissions, but it should still be valid block with an expected ID. No reason to drop.
    if ((sourceSelector_.currentReplica() != replicaId) || (fetchState_.minBlockId > m->blockNumber) ||
        (fetchState_.nextBlockId < m->blockNumber) ||
        (m->dataSize + totalSizeOfPendingItemDataMsgs > config_.maxPendingDataFromSourceReplica)) {
      LOG_WARN(logger_,
               "Msg is irrelevant: " << KVLOG(replicaId,
                                              fetchingState,
                                              sourceSelector_.currentReplica(),
                                              m->requestMsgSeqNum,
                                              lastMsgSeqNum_,
                                              m->blockNumber,
                                              fetchState_,
                                              config_.maxNumberOfChunksInBatch,
                                              m->rvbDigestsSize,
                                              m->dataSize,
                                              totalSizeOfPendingItemDataMsgs,
                                              config_.maxPendingDataFromSourceReplica));
      metrics_.irrelevant_item_data_msg_++;
      return false;
    }
  } else {
    ConcordAssertEQ(psd_->getFirstRequiredBlock(), 0);
    ConcordAssertEQ(psd_->getLastRequiredBlock(), 0);

    if ((sourceSelector_.currentReplica() != replicaId) || (m->requestMsgSeqNum != lastMsgSeqNum_) ||
        (m->blockNumber != ID_OF_VBLOCK_RES_PAGES) ||
        (m->dataSize + totalSizeOfPendingItemDataMsgs > config_.maxPendingDataFromSourceReplica) ||
        (m->rvbDigestsSize > 0)) {
      LOG_WARN(logger_,
               "Msg is irrelevant: " << KVLOG(replicaId,
                                              fetchingState,
                                              sourceSelector_.currentReplica(),
                                              m->requestMsgSeqNum,
                                              lastMsgSeqNum_,
                                              (m->blockNumber == ID_OF_VBLOCK_RES_PAGES),
                                              m->rvbDigestsSize,
                                              m->dataSize,
                                              totalSizeOfPendingItemDataMsgs,
                                              config_.maxPendingDataFromSourceReplica));
      metrics_.irrelevant_item_data_msg_++;
      return false;
    }
  }

  bool added = false;
  tie(std::ignore, added) = pendingItemDataMsgs.insert(const_cast<ItemDataMsg *>(m));

  // Log time spent in handoff queue
  auto fetchingTimeStamp = getMonotonicTimeMilli();
  if (msgArrivalTime != UNDEFINED_LOCAL_TIME_POINT) {
    auto timeInIncomingEventsQueueMilli = duration_cast<milliseconds>(steady_clock::now() - msgArrivalTime).count();
    LOG_TRACE(
        logger_,
        KVLOG(fetchingTimeStamp, timeInIncomingEventsQueueMilli, (fetchingTimeStamp - timeInIncomingEventsQueueMilli)));
    histograms_.dst_time_ItemData_msg_in_incoming_events_queue->record(timeInIncomingEventsQueueMilli);
  }
  // Set fetchingTimeStamp_ while ignoring added flag - source is responsive
  sourceSelector_.setFetchingTimeStamp(fetchingTimeStamp, false);

  if (added) {
    LOG_DEBUG(logger_,
              "ItemDataMsg was added to pendingItemDataMsgs: " << KVLOG(replicaId, fetchingState, m->requestMsgSeqNum));
    metrics_.num_pending_item_data_msgs_.Get().Set(pendingItemDataMsgs.size());
    totalSizeOfPendingItemDataMsgs += m->dataSize;
    metrics_.total_size_of_pending_item_data_msgs_.Get().Set(totalSizeOfPendingItemDataMsgs);
    processData(m->lastInBatch, m->rvbDigestsSize);
    return true;
  } else {
    LOG_INFO(
        logger_,
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
    ConcordAssert(checkStructureOfVirtualBlock(rawVBlock, size, config_.sizeOfReservedPage, logger_));
    LOG_DEBUG(logger_, "New vblock contains 0 updated pages: " << KVLOG(desc.checkpointNum, size));
    return rawVBlock;
  }

  char *elements = rawVBlock + sizeof(HeaderOfVirtualBlock);

  uint32_t idx = 0;
  std::unique_ptr<char[]> buffer(new char[config_.sizeOfReservedPage]);
  for (uint32_t pageId : updatedPages) {
    ConcordAssertLT(idx, numberOfUpdatedPages);

    uint64_t actualPageCheckpoint = 0;
    Digest pageDigest;
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
    LOG_DEBUG(logger_,
              "Adding page to vBlock: " << KVLOG(
                  currElement->pageId, currElement->checkpointNumber, currElement->pageDigest));
    idx++;
  }

  ConcordAssertEQ(idx, numberOfUpdatedPages);
  ConcordAssertOR(!config_.pedanticChecks,
                  checkStructureOfVirtualBlock(rawVBlock, size, config_.sizeOfReservedPage, logger_));

  LOG_DEBUG(logger_,
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
  LOG_DEBUG(logger_, "");

  for (auto i : pendingItemDataMsgs) {
    replicaForStateTransfer_->freeStateTransferMsg(reinterpret_cast<char *>(i));
  }

  pendingItemDataMsgs.clear();
  totalSizeOfPendingItemDataMsgs = 0;
  metrics_.num_pending_item_data_msgs_.Get().Set(0);
  metrics_.total_size_of_pending_item_data_msgs_.Get().Set(0);
}

void BCStateTran::clearPendingItemsData(uint64_t fromBlock, uint64_t untilBlock) {
  LOG_DEBUG(logger_, KVLOG(fromBlock, untilBlock));
  ConcordAssertLE(fromBlock, untilBlock);
  if (fromBlock == 0) return;

  auto it = pendingItemDataMsgs.begin();
  while (it != pendingItemDataMsgs.end()) {
    ConcordAssertGE(totalSizeOfPendingItemDataMsgs, (*it)->dataSize);

    if (((*it)->blockNumber >= fromBlock) && ((*it)->blockNumber <= untilBlock)) {
      totalSizeOfPendingItemDataMsgs -= (*it)->dataSize;
      replicaForStateTransfer_->freeStateTransferMsg(reinterpret_cast<char *>(*it));
      it = pendingItemDataMsgs.erase(it);
    }
    ++it;
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
  ConcordAssertGE(requiredBlock, 1);

  const uint32_t maxSize = (isVBLock ? maxVBlockSize_ : config_.maxBlockSize);

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
    blockSize += (msg->dataSize - msg->rvbDigestsSize);
    if (totalNumberOfChunks != msg->totalNumberOfChunksInBlock || msg->chunkNumber > totalNumberOfChunks ||
        blockSize > maxSize) {
      badData = true;
      break;
    }

    if (maxAvailableChunk + 1 < msg->chunkNumber) {
      break;  // we have a hole
    }
    ConcordAssertEQ(maxAvailableChunk + 1, msg->chunkNumber);
    maxAvailableChunk = msg->chunkNumber;
    ConcordAssertLE(maxAvailableChunk, totalNumberOfChunks);
    if (maxAvailableChunk == totalNumberOfChunks) {
      fullBlock = true;
      break;
    }
    ++it;
  }  // while

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

  it = pendingItemDataMsgs.begin();
  while (true) {
    ConcordAssertNE(it, pendingItemDataMsgs.end());
    ConcordAssertEQ((*it)->blockNumber, requiredBlock);

    ItemDataMsg *msg = *it;

    ConcordAssertGE(msg->chunkNumber, 1);
    ConcordAssertEQ(msg->totalNumberOfChunksInBlock, totalNumberOfChunks);
    ConcordAssertEQ(currentChunk + 1, msg->chunkNumber);
    ConcordAssertLE(currentPos + msg->dataSize - msg->rvbDigestsSize, maxSize);

    memcpy(outBlock + currentPos, msg->data, msg->dataSize);
    currentChunk = msg->chunkNumber;
    currentPos += msg->dataSize;
    totalSizeOfPendingItemDataMsgs -= (*it)->dataSize;
    replicaForStateTransfer_->freeStateTransferMsg(reinterpret_cast<char *>(*it));
    it = pendingItemDataMsgs.erase(it);
    metrics_.num_pending_item_data_msgs_.Get().Set(pendingItemDataMsgs.size());
    metrics_.total_size_of_pending_item_data_msgs_.Get().Set(totalSizeOfPendingItemDataMsgs);

    if (currentChunk == totalNumberOfChunks) {
      outBlockSize = currentPos;
      return true;
    }
  }  // while (true)
}

bool BCStateTran::checkBlock(uint64_t blockId, char *block, uint32_t blockSize) const {
  Digest computedBlockDigest;
  {
    histograms_.compute_block_digest_size->record(blockSize);
    TimeRecorder scoped_timer(*histograms_.compute_block_digest_duration);
    this->computeDigestOfBlock(blockId, block, blockSize, &computedBlockDigest);
  }
  if (isRvbBlockId(blockId)) {
    auto rvbDigest = rvbm_->getDigestFromStoredRvb(blockId);
    std::string rvbDigestStr = !rvbDigest ? "" : rvbDigest.value().get().toString();
    if (!rvbDigest || (rvbDigest.value().get() != computedBlockDigest)) {
      metrics_.overall_rvb_digests_validation_failed_++;
      LOG_ERROR(logger_, "Digest validation failed (RVB):" << KVLOG(blockId, rvbDigestStr, computedBlockDigest));
      return false;
    }
    LOG_INFO(logger_, "Digest validation success (RVB):" << KVLOG(blockId, rvbDigestStr, computedBlockDigest));
    metrics_.overall_rvb_digests_validated_++;
    ++totalRvbsValidatedInCycle_;
    return true;
  }
  if (isMaxFetchedBlockIdInCycle(blockId)) {
    if (targetCheckpointDesc_.digestOfMaxBlockId != computedBlockDigest) {
      LOG_ERROR(logger_,
                "Digest validation failed (max ID in cycle):" << KVLOG(
                    blockId, targetCheckpointDesc_.digestOfMaxBlockId, computedBlockDigest));
      return false;
    }
    return true;
  }
  ConcordAssert(!digestOfNextRequiredBlock_.isZero());
  if (computedBlockDigest != digestOfNextRequiredBlock_) {
    LOG_WARN(logger_,
             "Digest validation failed (regular):" << KVLOG(blockId, computedBlockDigest, digestOfNextRequiredBlock_));
    return false;
  }
  return true;
}

bool BCStateTran::checkVirtualBlockOfResPages(const Digest &expectedDigestOfResPagesDescriptor,
                                              char *vblock,
                                              uint32_t vblockSize) const {
  if (!checkStructureOfVirtualBlock(vblock, vblockSize, config_.sizeOfReservedPage, logger_)) {
    LOG_WARN(logger_, "vblock has illegal structure");
    return false;
  }

  HeaderOfVirtualBlock *h = reinterpret_cast<HeaderOfVirtualBlock *>(vblock);

  if (psd_->getLastStoredCheckpoint() != h->lastCheckpointKnownToRequester) {
    LOG_WARN(logger_,
             "vblock has irrelevant checkpoint: " << KVLOG(h->lastCheckpointKnownToRequester,
                                                           psd_->getLastStoredCheckpoint()));

    return false;
  }

  // build ResPagesDescriptor
  DataStore::ResPagesDescriptor *pagesDesc = psd_->getResPagesDescriptor(h->lastCheckpointKnownToRequester);

  ConcordAssertEQ(pagesDesc->numOfPages, numberOfReservedPages_);

  for (uint32_t element = 0; element < h->numberOfUpdatedPages; ++element) {
    ElementOfVirtualBlock *vElement = getVirtualElement(element, config_.sizeOfReservedPage, vblock);
    LOG_TRACE(logger_, KVLOG(element, vElement->pageId, vElement->checkpointNumber, vElement->pageDigest));

    Digest computedPageDigest;
    computeDigestOfPage(
        vElement->pageId, vElement->checkpointNumber, vElement->page, config_.sizeOfReservedPage, computedPageDigest);
    if (computedPageDigest != vElement->pageDigest) {
      LOG_WARN(logger_,
               "vblock contains invalid digest: " << KVLOG(vElement->pageId, vElement->pageDigest, computedPageDigest));
      return false;
    }
    ConcordAssertLE(pagesDesc->d[vElement->pageId].relevantCheckpoint, h->lastCheckpointKnownToRequester);
    pagesDesc->d[vElement->pageId].pageId = vElement->pageId;
    pagesDesc->d[vElement->pageId].relevantCheckpoint = vElement->checkpointNumber;
    pagesDesc->d[vElement->pageId].pageDigest = vElement->pageDigest;
  }

  Digest computedDigest;
  computeDigestOfPagesDescriptor(pagesDesc, computedDigest);
  LOG_INFO(logger_, pagesDesc->toString(computedDigest.toString()));
  psd_->free(pagesDesc);

  if (computedDigest != expectedDigestOfResPagesDescriptor) {
    LOG_WARN(logger_,
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

void BCStateTran::SetAllReplicasAsPreferred() { sourceSelector_.setAllReplicasAsPreferred(); }

void BCStateTran::reportCollectingStatus(const uint32_t actualBlockSize, bool toLog) {
  metrics_.overall_blocks_collected_++;
  metrics_.overall_bytes_collected_.Get().Set(metrics_.overall_bytes_collected_.Get().Get() + actualBlockSize);
  --totalBlocksLeftToCollectInCycle_;

  bytesFetched_.report(actualBlockSize, toLog);
  if (blocksFetched_.report(1, toLog)) {
    auto overall_block_results = blocksFetched_.getOverallResults();
    auto overall_bytes_results = bytesFetched_.getOverallResults();
    auto prev_win_block_results = blocksFetched_.getPrevWinResults();
    auto prev_win_bytes_results = bytesFetched_.getPrevWinResults();

    metrics_.overall_blocks_throughput_.Get().Set(overall_block_results.throughput_);
    metrics_.overall_bytes_throughput_.Get().Set(overall_bytes_results.throughput_);

    metrics_.prev_win_blocks_collected_.Get().Set(prev_win_block_results.num_processed_items_);
    metrics_.prev_win_blocks_throughput_.Get().Set(prev_win_block_results.throughput_);
    metrics_.prev_win_bytes_collected_.Get().Set(prev_win_bytes_results.num_processed_items_);
    metrics_.prev_win_bytes_throughput_.Get().Set(prev_win_bytes_results.throughput_);

    LOG_INFO(logger_, logsForCollectingStatus());
  }
}

std::string BCStateTran::logsForCollectingStatus() {
  auto blocks_overall_r = blocksFetched_.getOverallResults();
  auto bytes_overall_r = blocksFetched_.getOverallResults();
  concordUtils::BuildJson bj;

  bj.startJson();
  bj.startNested("overallStats");

  bj.addKv("cycle", cycleCounter_);
  bj.addKv("Cycle elapsedTime", convertMillisecToReadableStr(cycleDT_.totalDuration(false)));
  bj.addKv("GettingMissingBlocks elapsedTime", convertMillisecToReadableStr(blocks_overall_r.elapsed_time_ms_));
  bj.addKv("collectRange",
           std::to_string(minBlockIdToCollectInCycle_) + ", " + std::to_string(maxBlockIdToCollectInCycle_));
  bj.addKv("lastCollectedBlock", fetchState_.nextBlockId);
  bj.addKv("blocksLeft", totalBlocksLeftToCollectInCycle_);
  bj.addKv("fetchState", fetchState_.toString());
  bj.addKv("post-processing upper bound block id", std::to_string(postProcessingUpperBoundBlockId_));
  bj.addKv("post-processed max block id", std::to_string(maxPostprocessedBlockId_));
  bj.addKv("RVB digests validated", std::to_string(metrics_.overall_rvb_digests_validated_.Get().Get()));
  bj.addKv("collected",
           convertUInt64ToReadableStr(blocks_overall_r.num_processed_items_, "Blocks") +
               convertUInt64ToReadableStr(bytes_overall_r.num_processed_items_, ", B"));
  bj.addKv("throughput",
           convertUInt64ToReadableStr(blocks_overall_r.throughput_, "Blocks/s") +
               convertUInt64ToReadableStr(bytes_overall_r.throughput_, ", B/s"));

  if (config_.gettingMissingBlocksSummaryWindowSize > 0) {
    auto blocks_win_r = blocksFetched_.getPrevWinResults();
    auto bytes_win_r = bytesFetched_.getPrevWinResults();
    auto prev_win_index = blocksFetched_.getPrevWinIndex();

    bj.startNested("lastWindow");
    bj.addKv("index", prev_win_index);
    bj.addKv("elapsedTime", convertMillisecToReadableStr(blocks_win_r.elapsed_time_ms_));
    bj.addKv("collected",
             convertUInt64ToReadableStr(blocks_win_r.num_processed_items_, "Blocks") + " / " +
                 convertUInt64ToReadableStr(bytes_win_r.num_processed_items_, "B "));
    bj.addKv("throughput",
             convertUInt64ToReadableStr(blocks_win_r.throughput_, "Block/s ") + " / " +
                 convertUInt64ToReadableStr(bytes_win_r.throughput_, "B/s "));
    bj.endNested();
  }

  bj.startNested("checkpointInfo");
  bj.addKv("lastStored checkpoint", psd_->getLastStoredCheckpoint());

  bj.startNested("beingFetched");
  bj.addKv("target checkpointNum", targetCheckpointDesc_.checkpointNum);
  bj.addKv("maxBlockId", targetCheckpointDesc_.maxBlockId);
  bj.endNested();

  bj.endNested();
  bj.endJson();

  return bj.getJson();
}

bool BCStateTran::finalizePutblockAsync(PutBlockWaitPolicy waitPolicy, DataStoreTransaction *txn) {
  // WAIT_SINGLE_JOB: We have a block ready in buffer_, but no free context. let's wait for one job to finish.
  // NO_WAIT: Opportunistic: finalize all jobs that are done, don't wait for the ongoing ones.
  // WAIT_ALL_JOBS: wait for all jobs to finish (blocking call).
  // Comment on committing asynchronously:
  // In the very rare case of a core dump or termination, we will just fetch the committed blocks again.
  // Putting an existing block is completely valid operation as long as the block we put before core dump and the
  // block we put now are identical.
  bool doneProcesssing = true;

  if (ioContexts_.empty()) {
    return doneProcesssing;
  }
  ConcordAssertGT(commitState_.nextBlockId, 0);

  uint64_t firstRequiredBlockId = std::numeric_limits<uint64_t>::max();
  while (!ioContexts_.empty()) {
    auto &ctx = ioContexts_.front();
    ConcordAssert(ctx->future.valid());
    if ((waitPolicy == PutBlockWaitPolicy::NO_WAIT) &&
        (ctx->future.wait_for(std::chrono::nanoseconds(0)) != std::future_status::ready)) {
      doneProcesssing = false;
      // to reduce the number of one shot timer invocations by more than 90%, we do an approximation and use
      // oneShotTimerFlag_
      if (oneShotTimerFlag_) {
        // processing not done. We must call finalizePutblockAsync in a short time to finish commit
        metrics_.one_shot_timer_++;
        replicaForStateTransfer_->addOneShotTimer(finalizePutblockTimeoutMilli_);
        oneShotTimerFlag_ = false;
      }
      break;
    }
    // currently, fetch must wait to commit before moving into the next batch, so the next assert must always be true
    ConcordAssertEQ(ctx->blockId, commitState_.nextBlockId);
    try {
      ConcordAssertEQ(ctx->future.get(), true);
    } catch (const std::exception &e) {
      LOG_FATAL(logger_, e.what());
      ConcordAssert(false);
    }

    LOG_DEBUG(logger_, "Block Committed (written to ST blockchain):" << KVLOG(ctx->blockId));

    // Report block as collected only after it is also committed
    reportCollectingStatus(ctx->actualBlockSize, isLastFetchedBlockIdInCycle(ctx->blockId));

    ioPool_.free(ctx);
    ioContexts_.pop_front();
    if (commitState_.nextBlockId == commitState_.minBlockId) {
      if (postProcessingQ_) {
        // Completed batch commit to blockchain ST - now, lets post-process these blocks
        if (commitState_.minBlockId == minBlockIdToCollectInCycle_) {
          // we measure the total time to commit to chain, assuming that post-processing thread always has what to do
          // if this incorrect, it will include "idle" durations
          postProcessingDT_.start();
          blocksPostProcessed_.start();
        }
        postProcessingQ_->push(std::bind(&BCStateTran::postProcessNextBatch, this, commitState_.maxBlockId));
        postProcessingUpperBoundBlockId_ = commitState_.maxBlockId;
      }
      firstRequiredBlockId = commitState_.maxBlockId + 1;
      auto oldCommitState_ = commitState_;
      ConcordAssertLE(firstRequiredBlockId, psd_->getLastRequiredBlock());
      LOG_INFO(logger_,
               "Done committing blocks [" << oldCommitState_.minBlockId << "," << oldCommitState_.maxBlockId
                                          << "], new commitState_:" << commitState_
                                          << KVLOG(firstRequiredBlockId, postProcessingUpperBoundBlockId_));
      ConcordAssert(ioContexts_.empty());
      break;
    } else {
      --commitState_.nextBlockId;
      LOG_TRACE(logger_, KVLOG(commitState_));
    }
    if (waitPolicy == PutBlockWaitPolicy::WAIT_SINGLE_JOB) {
      waitPolicy = PutBlockWaitPolicy::NO_WAIT;
    }
  }  // while

  if (firstRequiredBlockId != std::numeric_limits<uint64_t>::max()) {
    txn->setFirstRequiredBlock(firstRequiredBlockId);
  }
  return doneProcesssing;
}

// Compute the next batch reqired, taking into accont: minRequiredBlockId
// and configuration parameters fetchRangeSize and maxNumberOfChunksInBatch
BCStateTran::BlocksBatchDesc BCStateTran::computeNextBatchToFetch(uint64_t minRequiredBlockId) {
  uint64_t maxRequiredBlockId = minRequiredBlockId + config_.maxNumberOfChunksInBatch - 1;
  if (!isRvbBlockId(maxRequiredBlockId)) {
    uint64_t deltaToNearestRVB = maxRequiredBlockId % config_.fetchRangeSize;
    if ((maxRequiredBlockId >= deltaToNearestRVB) && (maxRequiredBlockId - deltaToNearestRVB >= minRequiredBlockId))
      maxRequiredBlockId = maxRequiredBlockId - deltaToNearestRVB;
  }
  auto lastRequiredBlock = psd_->getLastRequiredBlock();
  maxRequiredBlockId = std::min(maxRequiredBlockId, lastRequiredBlock);

  // Check with RVB manager that we are not between borders of RVB groups. This is rare, but we want to avoid the case
  // where we will need to ask for multiple digest groups. This make code more complicated.
  BlockId rvbmUpperBound = rvbm_->getRvbGroupMaxBlockIdOfNonStoredRvbGroup(minRequiredBlockId, maxRequiredBlockId);
  maxRequiredBlockId = std::min(maxRequiredBlockId, rvbmUpperBound);
  BlocksBatchDesc fetchBatch;
  fetchBatch.maxBlockId = maxRequiredBlockId;
  fetchBatch.nextBlockId = maxRequiredBlockId;
  fetchBatch.upperBoundBlockId = maxRequiredBlockId;
  fetchBatch.minBlockId = minRequiredBlockId;
  digestOfNextRequiredBlock_.makeZero();
  ConcordAssert(fetchBatch.isValid());
  ConcordAssertLT(fetchState_.nextBlockId, config_.maxNumberOfChunksInBatch + fetchBatch.minBlockId);
  LOG_INFO(logger_, KVLOG(minRequiredBlockId, maxRequiredBlockId, rvbmUpperBound, fetchBatch, lastRequiredBlock));
  return fetchBatch;
}

bool BCStateTran::isMinBlockIdInFetchRange(uint64_t blockId) const {
  ConcordAssertGT(blockId, 0);
  return ((blockId % config_.fetchRangeSize) == 1);
}

bool BCStateTran::isMaxBlockIdInFetchRange(uint64_t blockId) const {
  ConcordAssertGT(blockId, 0);
  return ((blockId % config_.fetchRangeSize) == 0);
}

bool BCStateTran::isRvbBlockId(uint64_t blockId) const {
  ConcordAssertGT(blockId, 0);
  return ((blockId % config_.fetchRangeSize) == 0);
}

uint64_t BCStateTran::prevRvbBlockId(uint64_t block_id) const {
  return config_.fetchRangeSize * (block_id / config_.fetchRangeSize);
}

uint64_t BCStateTran::nextRvbBlockId(uint64_t block_id) const {
  uint64_t next_rvb_id = config_.fetchRangeSize * (block_id / config_.fetchRangeSize);
  if (next_rvb_id < block_id) {
    next_rvb_id += config_.fetchRangeSize;
  }
  return next_rvb_id;
}

bool BCStateTran::isLastFetchedBlockIdInCycle(uint64_t blockId) const {
  return (blockId == fetchState_.minBlockId) && (psd_->getLastRequiredBlock() == fetchState_.maxBlockId);
}

bool BCStateTran::isMaxFetchedBlockIdInCycle(uint64_t blockId) const {
  return (blockId == fetchState_.maxBlockId) && (psd_->getLastRequiredBlock() == fetchState_.maxBlockId);
}

void BCStateTran::postProcessNextBatch(uint64_t upperBoundBlockId) {
  static uint64_t iteration{};

  if (upperBoundBlockId == maxPostprocessedBlockId_) {
    // This can happen due to trigger mechanism, ignore
    LOG_INFO(logger_, "Ignore request:" << KVLOG(upperBoundBlockId, maxPostprocessedBlockId_));
    return;
  }

  ConcordAssertGT(upperBoundBlockId, maxPostprocessedBlockId_);
  LOG_DEBUG(logger_, "Before postProcessUntilBlockId" << KVLOG(upperBoundBlockId));
  auto totalBlocksProcessed = as_->postProcessUntilBlockId(upperBoundBlockId);
  ++iteration;
  bool reportToLog = (iteration % blocksPostProcessedReportWindow) == 0;
  if (totalBlocksProcessed) {
    blocksPostProcessed_.report(totalBlocksProcessed, reportToLog);
  }
  if (reportToLog) {
    std::ostringstream oss;
    oss << "Done post-processing (iteration #" << iteration << ") " << totalBlocksProcessed << " blocks, range=["
        << (maxPostprocessedBlockId_ + 1) << "," << upperBoundBlockId << "]"
        << KVLOG(upperBoundBlockId, maxPostprocessedBlockId_, postProcessingQ_->size());
    auto overallResults = blocksPostProcessed_.getOverallResults();
    auto prevWinResults = blocksPostProcessed_.getPrevWinResults();
    auto blocksLeftToPostProcess = maxBlockIdToCollectInCycle_ - maxPostprocessedBlockId_;
    uint64_t timeToCompleteMs =
        (overallResults.throughput_ > 0) ? (blocksLeftToPostProcess * 1000) / overallResults.throughput_ : 0;
    oss << " ,Throughput (overall): " << convertUInt64ToReadableStr(overallResults.throughput_, "Block/sec")
        << " ,Throughput (last " << blocksPostProcessedReportWindow
        << " iterations): " << convertUInt64ToReadableStr(prevWinResults.throughput_, "Block/sec")
        << " ,Estimated time to reach max cycle block ID " << maxBlockIdToCollectInCycle_ << ": "
        << convertMillisecToReadableStr(timeToCompleteMs);
    LOG_INFO(logger_, oss.str());
  } else {
    LOG_DEBUG(logger_, "Done postProcessUntilBlockId" << KVLOG(upperBoundBlockId));
  }
  maxPostprocessedBlockId_ = upperBoundBlockId;
}

void BCStateTran::stReset(DataStoreTransaction *txn, bool resetRvbm, bool resetStoredCp, bool resetDataStore) {
  LOG_INFO(logger_, "");
  if (sourceSession_.isOpen()) {
    sourceSession_.close();
  }
  if (commitState_.nextBlockId > 0) {
    finalizePutblockAsync(PutBlockWaitPolicy::WAIT_ALL_JOBS, txn);
  }
  digestOfNextRequiredBlock_.makeZero();
  clearAllPendingItemsData();
  clearInfoAboutGettingCheckpointSummary();
  fetchState_.reset();
  commitState_.reset();
  sourceSelector_.reset();
  targetCheckpointDesc_.makeZero();
  postponedSendFetchBlocksMsg_ = false;
  postProcessingUpperBoundBlockId_ = 0;
  maxPostprocessedBlockId_ = 0;
  lastMilliOfUniqueFetchID_ = 0;
  lastCountOfUniqueFetchID_ = 0;
  totalRvbsValidatedInCycle_ = 0;
  lastMsgSeqNum_ = 0;
  lastMsgSeqNumOfReplicas_.clear();
  for (auto i : cacheOfVirtualBlockForResPages) {
    std::free(i.second);
  }
  cacheOfVirtualBlockForResPages.clear();
  lastTimeSentAskForCheckpointSummariesMsg = 0;
  retransmissionNumberOfAskForCheckpointSummariesMsg = 0;
  for (auto i : summariesCerts) {
    replicaForStateTransfer_->freeStateTransferMsg(reinterpret_cast<char *>(i.second));
  }
  summariesCerts.clear();
  numOfSummariesFromOtherReplicas.clear();
  clearIoContexts();
  ConcordAssert(ioPool_.full());
  verifyEmptyInfoAboutGettingCheckpointSummary();

  if (resetStoredCp) {
    ConcordAssert(txn != nullptr);
    txn->setLastStoredCheckpoint(0);
    txn->setFirstStoredCheckpoint(0);
  }
  if (resetDataStore) {
    ConcordAssert(txn != nullptr);
    txn->setIsFetchingState(false);
    txn->setFirstRequiredBlock(0);
    txn->setLastRequiredBlock(0);
    txn->setAsInitialized();
    ConcordAssertEQ(getFetchingState(), FetchingState::NotFetching);
  }
  if (resetRvbm) {
    rvbm_->reset();
  }
}

void BCStateTran::processData(bool lastInBatch, uint32_t rvbDigestsSize) {
  const FetchingState fs = getFetchingState();
  const auto fetchingState = fs;
  LOG_DEBUG(logger_, KVLOG(fetchingState));
  const bool isGettingBlocks = (fs == FetchingState::GettingMissingBlocks);
  const uint64_t currTime = getMonotonicTimeMilli();
  bool badDataFromCurrentSourceReplica = false;

  ConcordAssertOR(fs == FetchingState::GettingMissingBlocks, fs == FetchingState::GettingMissingResPages);
  ConcordAssert(sourceSelector_.hasPreferredReplicas());
  ConcordAssertLE(totalSizeOfPendingItemDataMsgs, config_.maxPendingDataFromSourceReplica);
  ConcordAssertOR(isGettingBlocks && (psd_->getLastRequiredBlock() != 0),
                  !isGettingBlocks && (psd_->getLastRequiredBlock() == 0));
  while (true) {
    //////////////////////////////////////////////////////////////////////////
    // Select a source replica (if need to)
    /////////////////////////////////////////////////////////////////////////
    const auto srcReplacementMode =
        sourceSelector_.shouldReplaceSource(currTime, badDataFromCurrentSourceReplica, lastInBatch);
    bool newSourceReplica = (srcReplacementMode != SourceReplacementMode::DO_NOT);
    if (newSourceReplica) {
      if ((fs == FetchingState::GettingMissingResPages) && sourceSelector_.noPreferredReplicas()) {
        // enter a new cycle
        startCollectingStateInternal();
        return;
      }
      sourceSelector_.updateSource(currTime);
      badDataFromCurrentSourceReplica = false;
      if (srcReplacementMode == SourceReplacementMode::IMMEDIATE) {
        clearAllPendingItemsData();
      }
    }

    // We have a valid source replica at this point
    ConcordAssert(sourceSelector_.hasSource());
    ConcordAssertEQ(badDataFromCurrentSourceReplica, false);
    bool onGettingMissingBlocks =
        (fetchingState == FetchingState::GettingMissingBlocks) && (commitState_.isValid()) && (fetchState_.isValid()) &&
        (commitState_ <= fetchState_) &&
        ((fetchState_.nextBlockId == fetchState_.maxBlockId) || !digestOfNextRequiredBlock_.isZero());
    bool onGettingMissingResPages = (fetchingState == FetchingState::GettingMissingResPages) &&
                                    (fetchState_.nextBlockId == ID_OF_VBLOCK_RES_PAGES) &&
                                    !digestOfNextRequiredBlock_.isZero();
    if (!onGettingMissingBlocks && !onGettingMissingResPages) {
      LOG_FATAL(logger_,
                "Invalid fetch/commit state:" << KVLOG(
                    stateName(fetchingState), fetchState_, commitState_, digestOfNextRequiredBlock_));
      ConcordAssert(false);
    }
    LOG_DEBUG(
        logger_,
        "fetchState_:" << fetchState_ << " commitState_:" << commitState_
                       << KVLOG(
                              maxPostprocessedBlockId_, postProcessingUpperBoundBlockId_, digestOfNextRequiredBlock_));

    //////////////////////////////////////////////////////////////////////////
    // Process and check the available chunks
    //////////////////////////////////////////////////////////////////////////
    int16_t lastChunkInRequiredBlock = 0;
    uint32_t actualBuffersize = 0;

    // TODO (GL) - for now (for simplicity) to support chunking, we call with buffer_ as an input. Later on we copy
    // buffer_ into BlockIOContext::blockData when the block is full.
    // We can save this copy by calling with BlockIOContext::blockData.
    // But this is more complex. In general, copying memory shouldn't impact performance much (micro-seconds)
    // so we are OK with it now.
    const bool newBlock = getNextFullBlock(fetchState_.nextBlockId,
                                           badDataFromCurrentSourceReplica,
                                           lastChunkInRequiredBlock,
                                           buffer_.get(),
                                           actualBuffersize,
                                           !isGettingBlocks);
    bool newBlockIsValid = false;
    char *blockData = buffer_.get() + rvbDigestsSize;
    size_t blockDataSize = actualBuffersize - rvbDigestsSize;
    char *rvbDigests = (rvbDigestsSize > 0) ? buffer_.get() : nullptr;
    if (newBlock && isGettingBlocks) {
      TimeRecorder scoped_timer(*histograms_.dst_digest_calc_duration);
      ConcordAssert(!badDataFromCurrentSourceReplica);

      if (rvbDigestsSize > 0) {
        LOG_INFO(logger_, "Setting RVB digests into RVB manager:" << KVLOG(rvbDigestsSize));
        if (!rvbm_->setSerializedDigestsOfRvbGroup(rvbDigests,
                                                   rvbDigestsSize,
                                                   fetchState_.minBlockId,
                                                   fetchState_.maxBlockId,
                                                   targetCheckpointDesc_.maxBlockId)) {
          metrics_.overall_rvb_digest_groups_validation_failed_++;
          LOG_ERROR(logger_, "Setting RVB digests into RVB manager failed!");
          badDataFromCurrentSourceReplica = true;
        } else {
          metrics_.overall_rvb_digest_groups_validated_++;
        }
      }

      if (!badDataFromCurrentSourceReplica) {
        newBlockIsValid = checkBlock(fetchState_.nextBlockId, blockData, blockDataSize);
        badDataFromCurrentSourceReplica = !newBlockIsValid;
      }
    } else if (newBlock && !isGettingBlocks) {
      ConcordAssert(!badDataFromCurrentSourceReplica);
      if (!config_.enableReservedPages)
        newBlockIsValid = true;
      else
        newBlockIsValid = checkVirtualBlockOfResPages(digestOfNextRequiredBlock_, buffer_.get(), actualBuffersize);

      badDataFromCurrentSourceReplica = !newBlockIsValid;
    } else {
      ConcordAssertAND(!newBlock, actualBuffersize == 0);
    }

    LOG_DEBUG(logger_,
              std::boolalpha << KVLOG(newBlock,
                                      newBlockIsValid,
                                      actualBuffersize,
                                      blockDataSize,
                                      rvbDigestsSize,
                                      badDataFromCurrentSourceReplica,
                                      lastInBatch));

    if (newBlockIsValid) {
      if (isGettingBlocks) {
        DataStoreTransaction::Guard g(psd_->beginTransaction());
        //////////////////////////////////////////////////////////////////////////
        // if we have a new block
        //////////////////////////////////////////////////////////////////////////
        ConcordAssertAND(lastChunkInRequiredBlock >= 1, actualBuffersize > 0);

        sourceSelector_.onReceivedValidBlockFromSource();
        bool lastFetchedBlockIdInCycle = isLastFetchedBlockIdInCycle(fetchState_.nextBlockId);
        bool minBlockIdInCurrentBatch = fetchState_.isMinBlockId(fetchState_.nextBlockId);

        // WAIT_SINGLE_JOB: We have a block ready in buffer_, but no free context. let's wait for one job to finish.
        // NO_WAIT: Opportunistic - finalize all jobs that are done, don't wait for the onging ones
        finalizePutblockAsync(ioPool_.empty() ? PutBlockWaitPolicy::WAIT_SINGLE_JOB : PutBlockWaitPolicy::NO_WAIT,
                              g.txn());

        // Put the block. We distinguishe between last block in cycle, minimal block ID in fetch range (last one), and
        // a 'regular' block
        std::stringstream ss;
        ss << "Before putBlock id " << fetchState_.nextBlockId << ":" << std::boolalpha
           << KVLOG(lastFetchedBlockIdInCycle, minBlockIdInCurrentBatch, actualBuffersize);
        if (!lastFetchedBlockIdInCycle) {
          //////////////////////////////////////////////////////////////////////////
          // Not the last block to collect in cycle
          //////////////////////////////////////////////////////////////////////////
          LOG_DEBUG(logger_, ss.str());
          auto ctx = ioPool_.alloc();
          ctx->blockId = fetchState_.nextBlockId;
          ctx->actualBlockSize = blockDataSize;
          // TODO - this can probably be optimized - see TODO above getNextFullBlock
          memcpy(ctx->blockData.get(), blockData, blockDataSize);
          clearPendingItemsData(fetchState_.nextBlockId, fetchState_.nextBlockId);
          ctx->future = as_->putBlockAsync(fetchState_.nextBlockId, ctx->blockData.get(), ctx->actualBlockSize, false);
          ioContexts_.push_back(std::move(ctx));
          histograms_.dst_num_pending_blocks_to_commit->record(ioContexts_.size());
          as_->getPrevDigestFromBlock(
              blockData, blockDataSize, reinterpret_cast<StateTransferDigest *>(&digestOfNextRequiredBlock_));
          if (minBlockIdInCurrentBatch) {
            //////////////////////////////////////////////////////////////////////////
            // Last block Collected in batch!
            //////////////////////////////////////////////////////////////////////////
            uint64_t nextBatcheMinBlockId = fetchState_.maxBlockId + 1;
            ConcordAssertLE(nextBatcheMinBlockId, g.txn()->getLastRequiredBlock());
            auto oldFetchState_ = fetchState_;

            // Currently, for simplicity - wait for temproary commit to end.
            // TODO - it should be possible to push fetchState_ into a new data structure and replace it with
            // commitState upperBound  work on in the next batch
            finalizePutblockAsync(PutBlockWaitPolicy::WAIT_ALL_JOBS, g.txn());
            fetchState_ = computeNextBatchToFetch(nextBatcheMinBlockId);
            commitState_ = fetchState_;
            LOG_TRACE(logger_, KVLOG(fetchState_, nextBatcheMinBlockId));
            ConcordAssert(commitState_.isValid());
            LOG_INFO(logger_,
                     "Done putting (async) blocks [" << oldFetchState_.minBlockId << "," << oldFetchState_.maxBlockId
                                                     << "]," << KVLOG(fetchState_));
          } else {
            --fetchState_.nextBlockId;
          }
          if (lastInBatch || postponedSendFetchBlocksMsg_ || newSourceReplica) {
            trySendFetchBlocksMsg(0, KVLOG(lastInBatch, postponedSendFetchBlocksMsg_, newSourceReplica));
            break;
          }
        } else {  // lastFetchedBlockIdInCycle == true
          //////////////////////////////////////////////////////////////////////////
          // Last block to collect in cycle
          //////////////////////////////////////////////////////////////////////////
          // 1) Finalizes all blocks with WAIT_ALL_JOBS and block
          // 2) Waits for post-processing thread to finish
          // 3) Put the last block and performs the last post-processing in ST main thread context

          // Wait for all jobs to finish
          finalizePutblockAsync(PutBlockWaitPolicy::WAIT_ALL_JOBS, g.txn());

          // At this stage we haven't yet committed the last block in cycle so we expect the next assert:
          ConcordAssertEQ(fetchState_, commitState_);
          ConcordAssert(ioContexts_.empty());
          blocksFetched_.stop();
          bytesFetched_.stop();
          gettingMissingBlocksDT_.stop();

          // TODO - Due to planned near-future changes, this code is written with simplicity considirations:
          // Wait on an infinite loop for the post-processing thread to finish. This was the similar behavior util now
          // - ST main thread used to post-process here until all blocks are taken our from ST temporary blockchain
          // and moved to consensus blockchain.
          // Comment: Collecting reserved pages is very fast - no reason to do it later.
          if (postProcessingQ_) {
            triggerPostProcessing();
            LOG_INFO(logger_, "Waiting for post processor thread to finish all jobs in queue...");
            while (!postProcessingQ_->empty() || (postProcessingUpperBoundBlockId_ != maxPostprocessedBlockId_)) {
              std::this_thread::sleep_for(std::chrono::milliseconds(1000));
              ConcordAssertLE(maxPostprocessedBlockId_, postProcessingUpperBoundBlockId_);
            }
          }

          // Write the last block and post-process last range in a cycle in ST main thread context.
          // After this put all blocks are in place, there is no need to update the RVB data since it reflects the
          // exact content of the storage after this last put.
          ConcordAssert(as_->putBlock(fetchState_.nextBlockId, blockData, blockDataSize, true));
          LOG_INFO(logger_,
                   "Done putting, committing and post-processing blocks [" << fetchState_.minBlockId << ","
                                                                           << fetchState_.maxBlockId << "]");
          postProcessingDT_.stop();
          blocksPostProcessed_.stop();
          onGettingMissingBlocksEnd(g.txn());

          // Log histograms for destination when GettingMissingBlocks is done
          // Do it for a cycle that lasted more than 10 seconds
          auto duration = cycleDT_.totalDuration(false);
          if (duration > 10'000) {
            auto &registrar = RegistrarSingleton::getInstance();
            registrar.perf.snapshot("state_transfer");
            registrar.perf.snapshot("state_transfer_dest");
            LOG_INFO(logger_, registrar.perf.toString(registrar.perf.get("state_transfer")));
            LOG_INFO(logger_, registrar.perf.toString(registrar.perf.get("state_transfer_dest")));
          } else {
            LOG_INFO(logger_, "skip logging snapshots, cycle is very short (not enough statistics)" << KVLOG(duration));
          }

          sendFetchResPagesMsg(0);
          break;
        }
      } else {  // isGettingBlocks == false
        //////////////////////////////////////////////////////////////////////////
        // if we have a new vblock
        //////////////////////////////////////////////////////////////////////////
        DataStoreTransaction::Guard g(psd_->beginTransaction());
        sourceSelector_.onReceivedValidBlockFromSource();

        if (config_.enableReservedPages) {
          // set the updated pages
          uint32_t numOfUpdates = getNumberOfElements(buffer_.get());
          LOG_DEBUG(logger_, "numOfUpdates in vblock: " << numOfUpdates);
          for (uint32_t i = 0; i < numOfUpdates; i++) {
            ElementOfVirtualBlock *e = getVirtualElement(i, config_.sizeOfReservedPage, buffer_.get());
            g.txn()->setResPage(e->pageId, e->checkpointNumber, e->pageDigest, e->page);
            LOG_DEBUG(logger_, "Update page " << e->pageId);
          }
        }
        ConcordAssert(g.txn()->hasCheckpointBeingFetched());

        DataStore::CheckpointDesc cp = g.txn()->getCheckpointBeingFetched();

        // set stored data
        ConcordAssertEQ(g.txn()->getFirstRequiredBlock(), 0);
        ConcordAssertEQ(g.txn()->getLastRequiredBlock(), 0);
        ConcordAssertGT(cp.checkpointNum, g.txn()->getLastStoredCheckpoint());
        ConcordAssert(ioContexts_.empty());

        g.txn()->setCheckpointDesc(cp.checkpointNum, cp);
        g.txn()->deleteCheckpointBeingFetched();
        deleteOldCheckpoints(cp.checkpointNum, g.txn());
        LOG_INFO(logger_, "Done fetching reserved pages!");

        // Metrics set at the end of the block to prevent transaction abort from
        // leaving inconsistencies.
        metrics_.last_stored_checkpoint_.Get().Set(cp.checkpointNum);
        metrics_.checkpoint_being_fetched_.Get().Set(0);

        checkConsistency(config_.pedanticChecks);


        // Report Completion
        LOG_INFO(logger_,
                 "Invoking onTransferringComplete callbacks for checkpoint number: " << KVLOG(cp.checkpointNum));
        metrics_.on_transferring_complete_++;
        for (const auto &kv : on_transferring_complete_cb_registry_) {
          kv.second.invokeAll(cp.checkpointNum);
        }
        cycleEndSummary();
        stReset(g.txn());
        g.txn()->setIsFetchingState(false);
        ConcordAssertEQ(getFetchingState(), FetchingState::NotFetching);

        // TODO - This next line should be integrated as a callback into on_transferring_complete_cb_registry_.
        // on_fetching_state_change_cb_registry_ should be removed
        on_fetching_state_change_cb_registry_.invokeAll(cp.checkpointNum);
        break;
      }  // isGettingBlocks == false
    }    // if (newBlockIsValid) {
    else if (!badDataFromCurrentSourceReplica) {
      //////////////////////////////////////////////////////////////////////////
      // if we don't have new full block/vblock (but we did not detect a problem)
      //////////////////////////////////////////////////////////////////////////
      bool retransmissionTimeoutExpired = sourceSelector_.retransmissionTimeoutExpired(currTime);
      if (newSourceReplica || retransmissionTimeoutExpired || postponedSendFetchBlocksMsg_ || lastInBatch) {
        if (isGettingBlocks) {
          DataStoreTransaction::Guard g(psd_->beginTransaction());
          finalizePutblockAsync(PutBlockWaitPolicy::NO_WAIT, g.txn());
          trySendFetchBlocksMsg(
              lastChunkInRequiredBlock,
              KVLOG(newSourceReplica, retransmissionTimeoutExpired, postponedSendFetchBlocksMsg_, lastInBatch));
        } else {
          LOG_INFO(logger_,
                   "Sending FetchResPagesMsg: " << KVLOG(newSourceReplica, retransmissionTimeoutExpired, lastInBatch));
          sendFetchResPagesMsg(lastChunkInRequiredBlock);
        }
      }
      break;
    }
  }  //  while
}  // processData

void BCStateTran::onGettingMissingBlocksEnd(DataStoreTransaction *txn) {
  LOG_INFO(logger_, "Done collecting blocks!");
  fetchState_.reset();
  commitState_.reset();
  clearAllPendingItemsData();
  digestOfNextRequiredBlock_ = targetCheckpointDesc_.digestOfResPagesDescriptor;
  txn->setFirstRequiredBlock(0);
  txn->setLastRequiredBlock(0);
  ConcordAssertEQ(getFetchingState(), FetchingState::GettingMissingResPages);
}

// TODO - print correct data in case we are entering a new cycle internally
// up to the point we have reached
void BCStateTran::cycleEndSummary() {
  Throughput::Results blocksCollectedResults, bytesCollectedResults, blocksPostProcessedResults;
  std::ostringstream sources_str;
  const auto &sources_ = sourceSelector_.getActualSources();

  if ((gettingMissingBlocksDT_.totalDuration() == 0) || (cycleDT_.totalDuration() == 0)) {
    // we print full summary only if we were collecting blocks
    LOG_INFO(logger_, "State Transfer cycle ended (#" << cycleCounter_ << ")," << KVLOG(internalCycleCounter_));
    return;
  }

  blocksCollectedResults = blocksFetched_.getOverallResults();
  bytesCollectedResults = bytesFetched_.getOverallResults();
  blocksPostProcessedResults = blocksPostProcessed_.getOverallResults();
  blocksFetched_.stop(true);
  bytesFetched_.stop(true);
  blocksPostProcessed_.stop(true);

  std::copy(sources_.begin(), sources_.end() - 1, std::ostream_iterator<uint16_t>(sources_str, ","));
  sources_str << sources_.back();
  auto cycleDuration = cycleDT_.totalDuration(true);
  auto gettingCheckpointSummariesDuration = gettingCheckpointSummariesDT_.totalDuration(true);
  auto gettingMissingBlocksDuration = gettingMissingBlocksDT_.totalDuration(true);
  auto postProcessingDuration = postProcessingDT_.totalDuration(true);
  auto gettingMissingResPagesDuration = gettingMissingResPagesDT_.totalDuration(true);
  LOG_INFO(
      logger_,
      "State Transfer cycle ended (#"
          << cycleCounter_ << ")," << KVLOG(internalCycleCounter_)
          << " ,Total Duration: " << convertMillisecToReadableStr(cycleDuration)
          << " ,Time to get checkpoint summaries: " << convertMillisecToReadableStr(gettingCheckpointSummariesDuration)
          << " ,Time to fetch missing blocks: " << convertMillisecToReadableStr(gettingMissingBlocksDuration)
          << " ,Time to post-process blocks: " << convertMillisecToReadableStr(postProcessingDuration)
          << " ,Time to get reserved pages (vblock): " << convertMillisecToReadableStr(gettingMissingResPagesDuration)
          << " ,Collected " << convertUInt64ToReadableStr(blocksCollectedResults.num_processed_items_, "Blocks")
          << " / " << convertUInt64ToReadableStr(bytesCollectedResults.num_processed_items_, "B")
          << " ,Collected Blocks Range=[" << std::to_string(minBlockIdToCollectInCycle_) << ", "
          << std::to_string(maxBlockIdToCollectInCycle_) << "]"
          << " ,#RVB digests validated: "
          << std::to_string(totalRvbsValidatedInCycle_)
          // GettingMissingBlocks Throughput
          << " ,GettingMissingBlocks Throughput: "
          << convertUInt64ToReadableStr(blocksCollectedResults.throughput_, "Blocks/sec") << " / "
          << convertUInt64ToReadableStr(bytesCollectedResults.throughput_, "B/sec")
          // Post-Processing Throughput
          << " ,Post-Processing Throughput: "
          << convertUInt64ToReadableStr(blocksPostProcessedResults.throughput_, "Blocks/sec")
          // cycle Throughput
          << " ,Cycle Throughput: "
          << convertUInt64ToReadableStr(
                 static_cast<uint64_t>((1000 * blocksCollectedResults.num_processed_items_) / cycleDuration),
                 "Blocks/sec")
          << " / "
          << convertUInt64ToReadableStr(
                 static_cast<uint64_t>((1000 * bytesCollectedResults.num_processed_items_) / cycleDuration), "B/sec")
          << ", #" << sources_.size() << " sources (first to last): [" << sources_str.str() << "]");
}

//////////////////////////////////////////////////////////////////////////////
// Consistency
//////////////////////////////////////////////////////////////////////////////

void BCStateTran::checkConsistency(bool checkAllBlocks, bool duringInit) {
  ConcordAssert(psd_->initialized());
  const uint64_t lastReachableBlockNum = as_->getLastReachableBlockNum();
  const uint64_t lastBlockNum = as_->getLastBlockNum();
  const uint64_t genesisBlockNum = as_->getGenesisBlockNum();
  const uint64_t firstStoredCheckpoint = psd_->getFirstStoredCheckpoint();
  const uint64_t lastStoredCheckpoint = psd_->getLastStoredCheckpoint();
  LOG_INFO(logger_,
           KVLOG(firstStoredCheckpoint, lastStoredCheckpoint, checkAllBlocks, lastBlockNum, lastReachableBlockNum));

  checkConfig();
  checkFirstAndLastCheckpoint(firstStoredCheckpoint, lastStoredCheckpoint);
  if (checkAllBlocks) {
    checkReachableBlocks(genesisBlockNum, lastReachableBlockNum);
  }

  checkUnreachableBlocks(lastReachableBlockNum, lastBlockNum, duringInit);
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

  ConcordAssert(rvbm_->validate());
}

void BCStateTran::checkConfig() {
  ConcordAssertEQ(replicas_, psd_->getReplicas());
  ConcordAssertEQ(config_.myReplicaId, psd_->getMyReplicaId());
  ConcordAssertEQ(config_.fVal, psd_->getFVal());
  ConcordAssertEQ(maxNumOfStoredCheckpoints_, psd_->getMaxNumOfStoredCheckpoints());
  ConcordAssertEQ(numberOfReservedPages_, psd_->getNumberOfReservedPages());
  // TODO - support any configuration, although config_.fetchRangeSize * config_.RVT_K <=
  // config_.maxNumberOfChunksInBatch is probably only for testing
  ConcordAssertGT(config_.fetchRangeSize * config_.RVT_K, config_.maxNumberOfChunksInBatch);
  // TODO Supporting fetchRangeSize > maxNumberOfChunksInBatch make things more complicated. We will have to
  // delete blocks from temporary blockchain in case that RVB validation failed since some batches will be without any
  // single validation until reaching the next RVB. For now, it is reasonable to have this restriction. To be improved
  // later.
  ConcordAssertLE(config_.fetchRangeSize, config_.maxNumberOfChunksInBatch);
}

void BCStateTran::checkFirstAndLastCheckpoint(uint64_t firstStoredCheckpoint, uint64_t lastStoredCheckpoint) {
  ConcordAssertGE(lastStoredCheckpoint, firstStoredCheckpoint);
  ConcordAssertLE(lastStoredCheckpoint - firstStoredCheckpoint + 1, maxNumOfStoredCheckpoints_);
  ConcordAssertOR((lastStoredCheckpoint == 0), psd_->hasCheckpointDesc(lastStoredCheckpoint));
  if ((firstStoredCheckpoint != 0) && (firstStoredCheckpoint != lastStoredCheckpoint) &&
      !psd_->hasCheckpointDesc(firstStoredCheckpoint)) {
    LOG_FATAL(logger_,
              KVLOG(firstStoredCheckpoint, lastStoredCheckpoint, psd_->hasCheckpointDesc(firstStoredCheckpoint)));
    ConcordAssert(false);
  }
}

void BCStateTran::checkReachableBlocks(uint64_t genesisBlockNum, uint64_t lastReachableBlockNum) {
  if (lastReachableBlockNum > 0) {
    LOG_INFO(logger_, KVLOG(genesisBlockNum, lastReachableBlockNum));
    for (uint64_t currBlock = lastReachableBlockNum - 1; currBlock >= genesisBlockNum; currBlock--) {
      auto currDigest = getBlockAndComputeDigest(currBlock);
      ConcordAssert(!currDigest.isZero());
      Digest prevFromNextBlockDigest;
      prevFromNextBlockDigest.makeZero();
      ConcordAssert(as_->getPrevDigestFromBlock(currBlock + 1,
                                                reinterpret_cast<StateTransferDigest *>(&prevFromNextBlockDigest)));
      ConcordAssertEQ(currDigest, prevFromNextBlockDigest);
    }
  }
}

void BCStateTran::checkUnreachableBlocks(uint64_t lastReachableBlockNum, uint64_t lastBlockNum, bool duringInit) {
  ConcordAssertGE(lastBlockNum, lastReachableBlockNum);
  if (lastBlockNum > lastReachableBlockNum) {
    LOG_INFO(logger_, std::boolalpha << KVLOG(lastReachableBlockNum, lastBlockNum, duringInit));
    ConcordAssertEQ(getFetchingState(), FetchingState::GettingMissingBlocks);
    uint64_t x = lastBlockNum - 1;
    while (as_->hasBlock(x)) {
      x--;
    }

    // we should have a hole
    ConcordAssertGT(x, lastReachableBlockNum);

    // During init:
    // We might see more than a single hole due to the fact that putBlock is done concurrently, and block N might
    // have been written while block M was not (due to a possibly abrupt shotdown/termination), for any pair of
    // blocks in the range [lastReachableBlockNum + 1, x] where ID(N) < ID(M). Actions: 1) In the extream scenario,
    // we expect a single non-existing block (this is x) and then up to ioPool_.maxElements()-1 already-written
    // blocks. 2) From block X = x - ioPool_.maxElements() - 1 ,if X > lastReachableBlockNum, all blocks should not
    // exist.
    //
    // Not during init: we must have a single hole
    uint64_t maxAlreadyWrittenBlocks = ioPool_.maxElements() - 1;
    for (uint64_t i = x - 1, n = 0; i >= lastReachableBlockNum + 1; --i, ++n) {
      auto hasBlock = as_->hasBlock(i);
      if (duringInit) {
        if (!hasBlock) continue;
        ConcordAssert(n < maxAlreadyWrittenBlocks);
      } else {
        ConcordAssert(!hasBlock);
      }
      LOG_WARN(logger_, "BlockId " << i << " exist!" << KVLOG(n, lastReachableBlockNum, maxAlreadyWrittenBlocks));
    }
  }
}

void BCStateTran::checkBlocksBeingFetchedNow(bool checkAllBlocks,
                                             uint64_t lastReachableBlockNum,
                                             uint64_t lastBlockNum) {
  if (lastBlockNum > lastReachableBlockNum) {
    LOG_INFO(logger_, KVLOG(checkAllBlocks, lastReachableBlockNum, lastBlockNum));
    ConcordAssertAND(psd_->getIsFetchingState(), psd_->hasCheckpointBeingFetched());
    ConcordAssertEQ(psd_->getFirstRequiredBlock() - 1, as_->getLastReachableBlockNum());
    ConcordAssertGE(psd_->getLastRequiredBlock(), psd_->getFirstRequiredBlock());

    if (checkAllBlocks) {
      uint64_t lastRequiredBlock = psd_->getLastRequiredBlock();

      for (uint64_t currBlock = lastBlockNum - 1; currBlock >= lastRequiredBlock + 1; currBlock--) {
        auto currDigest = getBlockAndComputeDigest(currBlock);
        ConcordAssert(!currDigest.isZero());

        Digest prevFromNextBlockDigest;
        prevFromNextBlockDigest.makeZero();
        ConcordAssert(as_->getPrevDigestFromBlock(currBlock + 1,
                                                  reinterpret_cast<StateTransferDigest *>(&prevFromNextBlockDigest)));
        ConcordAssertEQ(currDigest, prevFromNextBlockDigest);
      }
    }
  }
}

void BCStateTran::checkStoredCheckpoints(uint64_t firstStoredCheckpoint, uint64_t lastStoredCheckpoint) {
  // check stored checkpoints
  if (lastStoredCheckpoint > 0) {
    LOG_INFO(logger_, KVLOG(firstStoredCheckpoint, lastStoredCheckpoint));
    uint64_t prevLastBlockNum = 0;
    for (uint64_t chkp = firstStoredCheckpoint; chkp <= lastStoredCheckpoint; chkp++) {
      if (!psd_->hasCheckpointDesc(chkp)) continue;

      DataStore::CheckpointDesc desc = psd_->getCheckpointDesc(chkp);
      ConcordAssertEQ(desc.checkpointNum, chkp);
      ConcordAssertLE(desc.maxBlockId, as_->getLastReachableBlockNum());
      ConcordAssertGE(desc.maxBlockId, prevLastBlockNum);
      prevLastBlockNum = desc.maxBlockId;

      if (desc.maxBlockId != 0 && desc.maxBlockId >= as_->getGenesisBlockNum()) {
        auto computedBlockDigest = getBlockAndComputeDigest(desc.maxBlockId);
        // Extra debugging needed here for BC-2821
        if (computedBlockDigest != desc.digestOfMaxBlockId) {
          uint32_t blockSize = 0;
          as_->getBlock(desc.maxBlockId, buffer_.get(), config_.maxBlockSize, &blockSize);
          concordUtils::HexPrintBuffer blockData{buffer_.get(), blockSize};
          LOG_FATAL(logger_,
                    "Invalid stored checkpoint: " << KVLOG(desc.checkpointNum,
                                                           blockSize,
                                                           desc.maxBlockId,
                                                           computedBlockDigest,
                                                           desc.digestOfMaxBlockId,
                                                           blockData));
          ConcordAssertEQ(computedBlockDigest, desc.digestOfMaxBlockId);
        }
      }
      if (config_.enableReservedPages) {
        // check all pages descriptor
        DataStore::ResPagesDescriptor *allPagesDesc = psd_->getResPagesDescriptor(chkp);
        ConcordAssertEQ(allPagesDesc->numOfPages, numberOfReservedPages_);
        {
          Digest computedDigestOfResPagesDescriptor;
          computeDigestOfPagesDescriptor(allPagesDesc, computedDigestOfResPagesDescriptor);
          LOG_INFO(logger_, allPagesDesc->toString(computedDigestOfResPagesDescriptor.toString()));
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

          Digest computedDigestOfPage;
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
    const uint32_t pageId, const uint64_t checkpointNumber, const char *page, uint32_t pageSize, Digest &outDigest) {
  DigestUtil::Context c;
  c.update(reinterpret_cast<const char *>(&pageId), sizeof(pageId));
  c.update(reinterpret_cast<const char *>(&checkpointNumber), sizeof(checkpointNumber));
  if (checkpointNumber > 0) c.update(page, pageSize);
  c.writeDigest(reinterpret_cast<char *>(&outDigest));
}

void BCStateTran::computeDigestOfPagesDescriptor(const DataStore::ResPagesDescriptor *pagesDesc, Digest &outDigest) {
  DigestUtil::Context c;
  c.update(reinterpret_cast<const char *>(pagesDesc), pagesDesc->size());
  c.writeDigest(reinterpret_cast<char *>(&outDigest));
}

void BCStateTran::computeDigestOfBlockImpl(const uint64_t blockNum,
                                           const char *block,
                                           const uint32_t blockSize,
                                           char *outDigest) {
  ConcordAssertGT(blockNum, 0);
  ConcordAssertGT(blockSize, 0);
  DigestUtil::Context c;
  c.update(reinterpret_cast<const char *>(&blockNum), sizeof(blockNum));
  c.update(block, blockSize);
  c.writeDigest(outDigest);
}

void BCStateTran::computeDigestOfBlock(const uint64_t blockNum,
                                       const char *block,
                                       const uint32_t blockSize,
                                       Digest *outDigest) {
  computeDigestOfBlockImpl(blockNum, block, blockSize, reinterpret_cast<char *>(outDigest));
}

BlockDigest BCStateTran::computeDigestOfBlock(const uint64_t blockNum, const char *block, const uint32_t blockSize) {
  BlockDigest outDigest;
  computeDigestOfBlockImpl(blockNum, block, blockSize, reinterpret_cast<char *>(outDigest.data()));
  return outDigest;
}

Digest BCStateTran::getBlockAndComputeDigest(uint64_t currBlock) {
  // This function is called among others during checkpointing of current state,
  // which can occur while this replica is a source replica.
  // In order to make it thread safe, instead of using buffer_, a local buffer is allocated .
  static std::unique_ptr<char[]> buffer(new char[maxItemSize_]);
  Digest currDigest;
  uint32_t blockSize = 0;
  as_->getBlock(currBlock, buffer.get(), config_.maxBlockSize, &blockSize);
  {
    histograms_.compute_block_digest_size->record(blockSize);
    TimeRecorder scoped_timer(*histograms_.compute_block_digest_duration);
    computeDigestOfBlock(currBlock, buffer.get(), blockSize, &currDigest);
  }
  return currDigest;
}

void BCStateTran::SetAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) {
  sourceSelector_.setAggregator(aggregator);
  metrics_component_.SetAggregator(aggregator);
}

inline std::string BCStateTran::getSequenceNumber(uint16_t replicaId,
                                                  uint64_t seqNum,
                                                  uint16_t blockNum,
                                                  uint64_t chunkNum) {
  return std::to_string(replicaId) + "-" + std::to_string(seqNum) + "-" + std::to_string(blockNum) + "-" +
         std::to_string(chunkNum);
}

void BCStateTran::peekConsensusMessage(const shared_ptr<ConsensusMsg> &msg) {
  if (incomingEventsQ_) {
    histograms_.incoming_events_queue_size->record(incomingEventsQ_->size());
    time_in_incoming_events_queue_rec_.end();
  }
  auto msg_type = msg->type_;
  LOG_TRACE(logger_, KVLOG(msg_type, msg->sender_id_));

  switch (msg_type) {
    case MsgCode::PrePrepare:
      if (config_.enableSourceSelectorPrimaryAwareness) {
        sourceSelector_.updateCurrentPrimary(msg->sender_id_);
      }
      break;
    default: {
      LOG_FATAL(logger_, "Unexpected message type" << KVLOG(msg_type));
      ConcordAssert(false);
    }
  }
  time_in_incoming_events_queue_rec_.start();
}

void BCStateTran::reportLastAgreedPrunableBlockId(uint64_t lastAgreedPrunableBlockId) {
  if (isFetching()) {
    // This is another thread context
    // Worst scenario - moving from not fetching -> fetching (time of check/ time of use)
    // The other case cannot happen.
    // In the worst case redundant blocks will be saved and persisted and removed later.
    LOG_WARN(logger_, "Report about pruned blocks while fetching!");
    return;
  }
  rvbm_->reportLastAgreedPrunableBlockId(lastAgreedPrunableBlockId);
}

void BCStateTran::triggerPostProcessing() {
  if (maxPostprocessedBlockId_ != 0) {
    return;
  }
  uint64_t lastReachableBlockId = as_->getLastReachableBlockNum();
  uint64_t firstRequiredBlockId = psd_->getFirstRequiredBlock();
  LOG_INFO(logger_, KVLOG(firstRequiredBlockId, lastReachableBlockId));
  if ((firstRequiredBlockId > 1) && ((firstRequiredBlockId - 1) > (lastReachableBlockId + 1))) {
    postProcessingDT_.start();
    blocksPostProcessed_.start();
    postProcessingQ_->push(std::bind(&BCStateTran::postProcessNextBatch, this, firstRequiredBlockId - 1));
  }
}

void BCStateTran::SourceSession::close() {
  LOG_INFO(logger_,
           "SourceSession: Session closed:"
               << std::boolalpha << KVLOG(replicaId_, startTime_, batchCounter_, activeDuration(), expired()));
  replicaId_ = UINT16_MAX;
  startTime_ = 0;
  batchCounter_ = 0;
}

void BCStateTran::SourceSession::open(uint16_t replicaId) {
  replicaId_ = replicaId;
  startTime_ = getMonotonicTimeMilli();
  LOG_INFO(logger_, "SourceSession:" << KVLOG(replicaId, startTime_));

  auto &registrar = RegistrarSingleton::getInstance();
  registrar.perf.snapshot("state_transfer");
  registrar.perf.snapshot("state_transfer_src");
}

std::pair<bool, bool> BCStateTran::SourceSession::tryOpen(uint16_t replicaId) {
  auto now = getMonotonicTimeMilli();
  if (!isOpen()) {
    LOG_TRACE(logger_, "SourceSession: Open a new session:" << KVLOG(replicaId, now));
    open(replicaId);
    return std::pair(true, false);
  } else {
    // session was not open
    if (replicaId_ == replicaId) {
      // Serving session peer: log last  activity time
      // Not checking time to see if msg arrived after expiryDurationMs
      // Retransmission count maintained at destination would look out for new source
      LOG_TRACE(logger_, "SourceSession: Active session update:" << KVLOG(replicaId, startTime_));
      startTime_ = now;
      return std::pair(true, false);
    } else {
      // Active session but request comes from a replica which is not current session peer.
      if (expired()) {
        // Time to entertain a new destination
        LOG_TRACE(logger_, "SourceSession: Active session expired:" << KVLOG(replicaId, now, activeDuration()));
        close();
        open(replicaId);
        return std::pair(true, true);
      } else {
        // Session with another peer is active, do not open a new session!
        LOG_TRACE(
            logger_,
            "SourceSession: Active session with another peer:" << KVLOG(replicaId, replicaId_, now, activeDuration()));
        return std::pair(false, false);
      }
    }
  }
  return std::pair(true, false);
}

}  // namespace impl
}  // namespace bcst
}  // namespace bftEngine
