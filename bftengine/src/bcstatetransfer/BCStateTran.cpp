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

#include <cassert>
#include <algorithm>
#include <chrono>
#include <iostream>
#include <list>
#include <set>
#include <string>

#include "BCStateTran.hpp"
#include "STDigest.hpp"
#include "InMemoryDataStore.hpp"

// TODO(GG): for debugging - remove
// #define DEBUG_SEND_CHECKPOINTS_IN_REVERSE_ORDER (1)

using std::cout;
using std::tie;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::steady_clock;
using std::chrono::system_clock;
using std::chrono::time_point;

#define Assert(expr)                                                   \
  {                                                                    \
    if ((expr) != true) {                                              \
      STLogger.error("'%s' is NOT true (in function '%s' in %s:%d)\n", \
                     #expr,                                            \
                     __FUNCTION__,                                     \
                     __FILE__,                                         \
                     __LINE__);                                        \
      assert(false);                                                   \
    }                                                                  \
  }

namespace bftEngine {
namespace SimpleBlockchainStateTransfer {

void computeBlockDigest(const uint64_t blockId,
                        const char* block,
                        const uint32_t blockSize,
                        StateTransferDigest* outDigest) {
  return impl::BCStateTran::computeDigestOfBlock(
      blockId, block, blockSize, (impl::STDigest*)outDigest);
}

IStateTransfer* create(const Config& config,
                       IAppState* const stateApi,
                       const bool persistentDataStore) {
  // TODO(GG): check configuration

  impl::BCStateTran* p =
      new impl::BCStateTran(persistentDataStore, config, stateApi);

  return p;
}

namespace impl {

//////////////////////////////////////////////////////////////////////////////
// Logger
//////////////////////////////////////////////////////////////////////////////

concordlogger::Logger STLogger =
    concordlogger::Logger::getLogger("state-transfer");

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

static DataStore* createDataStore(bool persistentDataStore,
                                  uint32_t sizeOfReservedPage) {
  Assert(!persistentDataStore);  // TODO(GG): support PersistentDataStore
  return new InMemoryDataStore(sizeOfReservedPage);
}

static uint32_t calcMaxVBlockSize(uint32_t maxNumberOfPages, uint32_t pageSize);

static uint32_t calcMaxItemSize(uint32_t maxBlockSize,
                                uint32_t maxNumberOfPages,
                                uint32_t pageSize) {
  const uint32_t maxVBlockSize = calcMaxVBlockSize(maxNumberOfPages, pageSize);

  const uint32_t retVal = std::max(maxBlockSize, maxVBlockSize);

  return retVal;
}

static uint16_t calcMaxNumOfChunksInBlock(uint32_t maxItemSize,
                                          uint32_t maxBlockSize,
                                          uint32_t maxChunkSize,
                                          bool isVBlock) {
  if (!isVBlock) {
    uint16_t retVal = (maxBlockSize % maxChunkSize == 0)
                          ? (maxBlockSize / maxChunkSize)
                          : (maxBlockSize / maxChunkSize + 1);

    return retVal;
  } else {
    uint16_t retVal = (maxItemSize % maxChunkSize == 0)
                          ? (maxItemSize / maxChunkSize)
                          : (maxItemSize / maxChunkSize + 1);

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

BCStateTran::BCStateTran(const bool persistentDataStore,
                         const Config& config,
                         IAppState* const stateApi)
    : pedanticChecks_{config.pedanticChecks},
      as_{stateApi},
      psd_{createDataStore(persistentDataStore, kSizeOfReservedPage)},
      replicas_{
          generateSetOfReplicas((3 * config.fVal) + (2 * config.cVal) + 1)},
      myId_{config.myReplicaId},
      fVal_{config.fVal},
      maxBlockSize_{config.maxBlockSize},
      maxChunkSize_{config.maxChunkSize},
      maxNumberOfChunksInBatch_{config.maxNumberOfChunksInBatch},
      maxPendingDataFromSourceReplica_{config.maxPendingDataFromSourceReplica},
      maxNumOfReservedPages_{config.maxNumOfReservedPages},
      refreshTimerMilli_{config.refreshTimerMilli},
      checkpointSummariesRetransmissionTimeoutMilli_{
          config.checkpointSummariesRetransmissionTimeoutMilli},
      maxAcceptableMsgDelayMilli_{config.maxAcceptableMsgDelayMilli},
      sourceReplicaReplacementTimeoutMilli_{
          config.sourceReplicaReplacementTimeoutMilli},
      fetchRetransmissionTimeoutMilli_{config.fetchRetransmissionTimeoutMilli},
      maxVBlockSize_{
          calcMaxVBlockSize(config.maxNumOfReservedPages, kSizeOfReservedPage)},
      maxItemSize_{calcMaxItemSize(config.maxBlockSize,
                                   config.maxNumOfReservedPages,
                                   kSizeOfReservedPage)},
      maxNumOfChunksInAppBlock_{calcMaxNumOfChunksInBlock(
          maxItemSize_, config.maxBlockSize, config.maxChunkSize, false)},
      maxNumOfChunksInVBlock_{calcMaxNumOfChunksInBlock(
          maxItemSize_, config.maxBlockSize, config.maxChunkSize, true)},
      maxNumOfStoredCheckpoints_{0},
      numberOfReservedPages_{0},
      randomGen_{randomDevice_()} {
  Assert(stateApi != nullptr);
  Assert(psd_ != nullptr);
  Assert(replicas_.size() >= 3U * fVal_ + 1U);
  Assert(replicas_.count(myId_) == 1);
  Assert(maxNumOfReservedPages_ >= 2);

  // TODO(GG): more asserts

  buffer_ = reinterpret_cast<char*>(std::malloc(maxItemSize_));

  LOG_INFO(STLogger,
           "Creating BCStateTran object:"
               << " myId_=" << myId_ << " fVal_=" << fVal_
               << " kSizeOfReservedPage=" << kSizeOfReservedPage);
}

BCStateTran::~BCStateTran() {
  Assert(!running_);
  Assert(cacheOfVirtualBlockForResPages.empty());
  Assert(pendingItemDataMsgs.empty());

  delete psd_;

  std::free(buffer_);
}

//////////////////////////////////////////////////////////////////////////////
// IStateTransfer methods
//////////////////////////////////////////////////////////////////////////////

void BCStateTran::init(uint64_t maxNumOfRequiredStoredCheckpoints,
                       uint32_t numberOfRequiredReservedPages,
                       uint32_t sizeOfReservedPage) {
  Assert(!running_);
  Assert(replicaForStateTransfer_ == nullptr);

  Assert(sizeOfReservedPage == kSizeOfReservedPage);

  maxNumOfStoredCheckpoints_ = maxNumOfRequiredStoredCheckpoints;
  numberOfReservedPages_ = numberOfRequiredReservedPages;

  memset(buffer_, 0, maxItemSize_);

  LOG_INFO(STLogger,
           "Init BCStateTran object:"
               << " maxNumOfStoredCheckpoints_=" << maxNumOfStoredCheckpoints_
               << " numberOfReservedPages_=" << numberOfReservedPages_
               << " kSizeOfReservedPage=" << kSizeOfReservedPage);

  if (psd_->initialized()) {
    LOG_INFO(STLogger,
             "BCStateTran::init - loading existing data from storage");

    bool consistent = checkConsistency(pedanticChecks_);
    Assert(consistent);

    FetchingState fs = getFetchingState();

    LOG_INFO(STLogger, "starting state is " << stateName(fs));

    if (fs == FetchingState::GettingMissingBlocks ||
        fs == FetchingState::GettingMissingResPages) {
      // TODO(GG): clean this code section
      // (see also similar code seions in this file)
      set<uint16_t> tmp = replicas_;
      tmp.erase(myId_);
      preferredReplicas_ = tmp;  // in this case, we try to use all replicas
    }
  } else {
    LOG_INFO(STLogger, "BCStateTran::init - initializing a new object");

    Assert(maxNumOfRequiredStoredCheckpoints >= 2 &&
           maxNumOfRequiredStoredCheckpoints <= kMaxNumOfStoredCheckpoints);

    Assert(numberOfRequiredReservedPages >= 2 &&
           numberOfRequiredReservedPages <= maxNumOfReservedPages_);

    psd_->setReplicas(replicas_);
    psd_->setMyReplicaId(myId_);
    psd_->setFVal(fVal_);
    psd_->setMaxNumOfStoredCheckpoints(maxNumOfRequiredStoredCheckpoints);
    psd_->setNumberOfReservedPages(numberOfRequiredReservedPages);
    psd_->setLastStoredCheckpoint(0);
    psd_->setFirstStoredCheckpoint(0);

    for (uint32_t i = 0; i < numberOfReservedPages_; i++)  // reset all pages
      psd_->setPendingResPage(i, buffer_, kSizeOfReservedPage);

    psd_->setIsFetchingState(false);
    psd_->setFirstRequiredBlock(0);
    psd_->setLastRequiredBlock(0);

    psd_->setAsInitialized();

    Assert(getFetchingState() == FetchingState::NotFetching);
  }
}

void BCStateTran::startRunning(IReplicaForStateTransfer* r) {
  LOG_INFO(STLogger, "BCStateTran::startRunning");

  Assert(r != nullptr);
  Assert(!running_);
  Assert(replicaForStateTransfer_ == nullptr);

  // TODO(GG): add asserts for all relevant data members

  running_ = true;
  replicaForStateTransfer_ = r;

  replicaForStateTransfer_->changeStateTransferTimerPeriod(refreshTimerMilli_);
}

void BCStateTran::stopRunning() {
  LOG_INFO(STLogger, "BCStateTran::stopRunning");

  Assert(running_);
  Assert(replicaForStateTransfer_ != nullptr);

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

  for (auto i : summariesCerts)
    replicaForStateTransfer_->freeStateTransferMsg(
        reinterpret_cast<char*>(i.second));

  summariesCerts.clear();

  numOfSummariesFromOtherReplicas.clear();

  preferredReplicas_.clear();

  currentSourceReplica = NO_REPLICA;

  timeMilliCurrentSourceReplica = 0;

  nextRequiredBlock = 0;

  digestOfNextRequiredBlock.makeZero();

  for (auto i : pendingItemDataMsgs)
    replicaForStateTransfer_->freeStateTransferMsg(reinterpret_cast<char*>(i));

  pendingItemDataMsgs.clear();

  totalSizeOfPendingItemDataMsgs = 0;

  replicaForStateTransfer_ = nullptr;
}

bool BCStateTran::isRunning() const { return running_; }

void BCStateTran::createCheckpointOfCurrentState(uint64_t checkpointNumber) {
  LOG_INFO(STLogger,
           "BCStateTran::createCheckpointOfCurrentState - checkpointNumber= "
               << checkpointNumber);

  Assert(running_);
  Assert(!isFetching());
  Assert(checkpointNumber > 0);
  Assert(checkpointNumber > psd_->getLastStoredCheckpoint());

  // reserved pages

  set<uint32_t> pages = psd_->getNumbersOfPendingResPages();

  LOG_INFO(STLogger,
           "associating " << pages.size() << " pending pages with checkpoint "
                          << checkpointNumber);

  for (uint32_t p : pages) {
    psd_->getPendingResPage(p, buffer_, kSizeOfReservedPage);

    STDigest d;
    computeDigestOfPage(p, checkpointNumber, buffer_, d);

    psd_->associatePendingResPageWithCheckpoint(p, checkpointNumber, d);
  }

  memset(buffer_, 0, kSizeOfReservedPage);

  Assert(psd_->numOfAllPendingResPage() == 0);

  DataStore::ResPagesDescriptor* allPagesDesc =
      psd_->getResPagesDescriptor(checkpointNumber);
  Assert(allPagesDesc->numOfPages == numberOfReservedPages_);

  STDigest digestOfResPagesDescriptor;

  computeDigestOfPagesDescriptor(allPagesDesc, digestOfResPagesDescriptor);

  psd_->free(allPagesDesc);

  // blocks
  uint64_t lastBlock = as_->getLastReachableBlockNum();
  Assert(lastBlock == as_->getLastBlockNum());

  /*
    uint64_t aa = as_->getLastBlockNum();
    LOG_INFO(STLogger,  "as_->getLastReachableBlockNum()==" << lastBlock);
    LOG_INFO(STLogger,  "as_->getLastBlockNum()==" << aa);
    Assert(lastBlock == aa);
  */
  LOG_INFO(STLogger, "last block = " << lastBlock);

  STDigest digestOfLastBlock;

  if (lastBlock > 0) {
    uint32_t blockSize = 0;
    as_->getBlock(lastBlock, buffer_, &blockSize);
    computeDigestOfBlock(lastBlock, buffer_, blockSize, &digestOfLastBlock);
    memset(buffer_, 0, blockSize);
  } else {
    // if we don't have blocks, then we use zero digest
    digestOfLastBlock.makeZero();
  }

  // store checkpoint

  DataStore::CheckpointDesc checkDesc;
  checkDesc.checkpointNum = checkpointNumber;
  checkDesc.lastBlock = lastBlock;
  checkDesc.digestOfLastBlock = digestOfLastBlock;
  checkDesc.digestOfResPagesDescriptor = digestOfResPagesDescriptor;

  psd_->setCheckpointDesc(checkpointNumber, checkDesc);

  // delete old checkpoints

  uint64_t minRelevantCheckpoint = 0;
  if (checkpointNumber > maxNumOfStoredCheckpoints_)
    minRelevantCheckpoint = checkpointNumber - maxNumOfStoredCheckpoints_;

  LOG_INFO(STLogger, "minRelevantCheckpoint is " << minRelevantCheckpoint);

  const uint64_t oldFirstStoredCheckpoint = psd_->getFirstStoredCheckpoint();

  if (minRelevantCheckpoint >= 2 &&
      minRelevantCheckpoint > oldFirstStoredCheckpoint) {
    psd_->deleteDescOfSmallerCheckpoints(minRelevantCheckpoint);
    psd_->deleteCoveredResPageInSmallerCheckpoints(minRelevantCheckpoint);
  }

  // set

  if (minRelevantCheckpoint > oldFirstStoredCheckpoint)
    psd_->setFirstStoredCheckpoint(minRelevantCheckpoint);

  psd_->setLastStoredCheckpoint(checkpointNumber);

  LOG_INFO(STLogger,
           "first stored checkpoint="
               << std::max(minRelevantCheckpoint, oldFirstStoredCheckpoint)
               << "; last stored checkpoint=" << checkpointNumber);
}

void BCStateTran::markCheckpointAsStable(uint64_t checkpointNumber) {
  LOG_INFO(STLogger,
           "BCStateTran::markCheckpointAsStable - checkpointNumber="
               << checkpointNumber);

  Assert(running_);
  Assert(!isFetching());

  Assert(checkpointNumber > 0);

  const uint64_t lastStoredCheckpoint = psd_->getLastStoredCheckpoint();

  // Assert(checkpointNumber >= psd_->getFirstStoredCheckpoint());
  Assert((lastStoredCheckpoint < maxNumOfStoredCheckpoints_) ||
         (checkpointNumber >=
          lastStoredCheckpoint - maxNumOfStoredCheckpoints_ + 1));

  Assert(checkpointNumber <= psd_->getLastStoredCheckpoint());

  // Assert(psd_->hasCheckpointDesc(checkpointNumber));

  // nothing to do here
}

void BCStateTran::getDigestOfCheckpoint(uint64_t checkpointNumber,
                                        uint16_t sizeOfDigestBuffer,
                                        char* outDigestBuffer) {
  LOG_INFO(STLogger,
           "BCStateTran::getDigestOfCheckpoint - checkpointNumber="
               << checkpointNumber);

  Assert(running_);
  Assert(!isFetching());

  Assert(sizeOfDigestBuffer >= sizeof(STDigest));
  Assert(checkpointNumber > 0);
  Assert(checkpointNumber >= psd_->getFirstStoredCheckpoint());
  Assert(checkpointNumber <= psd_->getLastStoredCheckpoint());
  Assert(psd_->hasCheckpointDesc(checkpointNumber));

  DataStore::CheckpointDesc desc = psd_->getCheckpointDesc(checkpointNumber);

  STDigest retVal;

  DigestContext c;
  c.update(reinterpret_cast<char*>(&desc), sizeof(desc));
  c.writeDigest(reinterpret_cast<char*>(&retVal));

  LOG_INFO(STLogger,
           "BCStateTran::getDigestOfCheckpoint - digest=" << retVal.toString());

  uint16_t s = std::min((uint16_t)sizeof(STDigest), sizeOfDigestBuffer);

  memcpy(outDigestBuffer, &retVal, s);
  if (s < sizeOfDigestBuffer)
    memset(outDigestBuffer + s, 0, sizeOfDigestBuffer - s);
}

bool BCStateTran::isCollectingState() const { return isFetching(); }

uint32_t BCStateTran::numberOfReservedPages() const {
  return static_cast<uint32_t>(numberOfReservedPages_);
}

uint32_t BCStateTran::sizeOfReservedPage() const { return kSizeOfReservedPage; }

bool BCStateTran::loadReservedPage(uint32_t reservedPageId,
                                   uint32_t copyLength,
                                   char* outReservedPage) const {
  LOG_INFO(STLogger,
           "BCStateTran::loadReservedPage - reservedPageId=" << reservedPageId);

  Assert(running_);
  Assert(!isFetching());
  Assert(reservedPageId < numberOfReservedPages_);
  Assert(copyLength <= kSizeOfReservedPage);

  if (psd_->hasPendingResPage(reservedPageId)) {
    LOG_INFO(STLogger, "loaded from pending page");

    psd_->getPendingResPage(reservedPageId, outReservedPage, copyLength);
  } else {
    uint64_t lastCheckpoint = psd_->getLastStoredCheckpoint();
    uint64_t t = UINT64_MAX;
    psd_->getResPage(
        reservedPageId, lastCheckpoint, &t, outReservedPage, copyLength);
    Assert(t <= lastCheckpoint);

    LOG_INFO(STLogger, "loaded from checkpoint" << t);
  }

  return true;
}

void BCStateTran::saveReservedPage(uint32_t reservedPageId,
                                   uint32_t copyLength,
                                   const char* inReservedPage) {
  LOG_INFO(STLogger,
           "BCStateTran::saveReservedPage - reservedPageId=" << reservedPageId);

  Assert(!isFetching());
  Assert(reservedPageId < numberOfReservedPages_);
  Assert(copyLength <= kSizeOfReservedPage);

  psd_->setPendingResPage(reservedPageId, inReservedPage, copyLength);
}

void BCStateTran::zeroReservedPage(uint32_t reservedPageId) {
  LOG_INFO(STLogger,
           "BCStateTran::zeroReservedPage - reservedPageId=" << reservedPageId);

  Assert(!isFetching());
  Assert(reservedPageId < numberOfReservedPages_);

  memset(buffer_, 0, kSizeOfReservedPage);

  psd_->setPendingResPage(reservedPageId, buffer_, kSizeOfReservedPage);
}

void BCStateTran::startCollectingState() {
  LOG_INFO(STLogger, "BCStateTran::startCollectingState");

  Assert(running_);
  Assert(!isFetching());

  verifyEmptyInfoAboutGettingCheckpointSummary();

  psd_->deleteAllPendingPages();

  psd_->setIsFetchingState(true);

  sendAskForCheckpointSummariesMsg();
}

void BCStateTran::onTimer() {
  if (!running_) return;

  FetchingState fs = getFetchingState();

  if (fs == FetchingState::GettingCheckpointSummaries) {
    uint64_t currTime = getMonotonicTimeMilli();

    if ((currTime - lastTimeSentAskForCheckpointSummariesMsg) >
        checkpointSummariesRetransmissionTimeoutMilli_) {
      if (++retransmissionNumberOfAskForCheckpointSummariesMsg >
          kResetCount_AskForCheckpointSummaries)
        clearInfoAboutGettingCheckpointSummary();

      sendAskForCheckpointSummariesMsg();
    }
  } else if (fs == FetchingState::GettingMissingBlocks ||
             fs == FetchingState::GettingMissingResPages) {
    processData();
  }
}

void BCStateTran::handleStateTransferMessage(char* msg,
                                             uint32_t msgLen,
                                             uint16_t senderId) {
  Assert(running_);
  if (msgLen < sizeof(BCStateTranBaseMsg) || senderId == myId_ ||
      replicas_.count(senderId) == 0) {
    // TODO(GG): report about illegal message

    LOG_WARN(STLogger,
             "BCStateTran::handleStateTransferMessage - illegal message");

    replicaForStateTransfer_->freeStateTransferMsg(msg);
    return;
  }

  BCStateTranBaseMsg* msgHeader = reinterpret_cast<BCStateTranBaseMsg*>(msg);

  LOG_INFO(STLogger,
           "BCStateTran::handleStateTransferMessage - new message with type="
               << msgHeader->type);

  FetchingState fs = getFetchingState();

  bool noDelete = false;

  switch (msgHeader->type) {
    case MsgType::AskForCheckpointSummaries:
      if (fs == FetchingState::NotFetching)
        noDelete =
            onMessage(reinterpret_cast<AskForCheckpointSummariesMsg*>(msg),
                      msgLen,
                      senderId);
      break;
    case MsgType::CheckpointsSummary:
      if (fs == FetchingState::GettingCheckpointSummaries)
        noDelete = onMessage(
            reinterpret_cast<CheckpointSummaryMsg*>(msg), msgLen, senderId);
      break;
    case MsgType::FetchBlocks:
      noDelete =
          onMessage(reinterpret_cast<FetchBlocksMsg*>(msg), msgLen, senderId);
      break;
    case MsgType::FetchResPages:
      noDelete =
          onMessage(reinterpret_cast<FetchResPagesMsg*>(msg), msgLen, senderId);
      break;
    case MsgType::RejectFetching:
      if (fs == FetchingState::GettingMissingBlocks ||
          fs == FetchingState::GettingMissingResPages)
        noDelete = onMessage(
            reinterpret_cast<RejectFetchingMsg*>(msg), msgLen, senderId);
      break;
    case MsgType::ItemData:
      if (fs == FetchingState::GettingMissingBlocks ||
          fs == FetchingState::GettingMissingResPages)
        noDelete =
            onMessage(reinterpret_cast<ItemDataMsg*>(msg), msgLen, senderId);
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
  char page[1];  // the actual size is kSizeOfReservedPage bytes
};
#pragma pack(pop)

static uint32_t calcMaxVBlockSize(uint32_t maxNumberOfPages,
                                  uint32_t pageSize) {
  const uint32_t elementSize = sizeof(ElementOfVirtualBlock) + pageSize - 1;

  return sizeof(HeaderOfVirtualBlock) + (elementSize * maxNumberOfPages);
}

static uint32_t getNumberOfElements(char* virtualBlock) {
  HeaderOfVirtualBlock* h =
      reinterpret_cast<HeaderOfVirtualBlock*>(virtualBlock);
  return h->numberOfUpdatedPages;
}

static uint32_t getSizeOfVirtualBlock(char* virtualBlock, uint32_t pageSize) {
  HeaderOfVirtualBlock* h =
      reinterpret_cast<HeaderOfVirtualBlock*>(virtualBlock);

  const uint32_t elementSize = sizeof(ElementOfVirtualBlock) + pageSize - 1;

  const uint32_t size =
      sizeof(HeaderOfVirtualBlock) + h->numberOfUpdatedPages * elementSize;

  return size;
}

static ElementOfVirtualBlock* getVirtualElement(uint32_t index,
                                                uint32_t pageSize,
                                                char* virtualBlock) {
  HeaderOfVirtualBlock* h =
      reinterpret_cast<HeaderOfVirtualBlock*>(virtualBlock);
  Assert(index < h->numberOfUpdatedPages);

  const uint32_t elementSize = sizeof(ElementOfVirtualBlock) + pageSize - 1;

  char* p = virtualBlock + sizeof(HeaderOfVirtualBlock) + (index * elementSize);
  ElementOfVirtualBlock* retVal = reinterpret_cast<ElementOfVirtualBlock*>(p);
  return retVal;
}

static bool checkStructureOfVirtualBlock(char* virtualBlock,
                                         uint32_t virtualBlockSize,
                                         uint32_t pageSize) {
  if (virtualBlockSize < sizeof(HeaderOfVirtualBlock)) return false;

  const uint32_t arrayBlockSize =
      virtualBlockSize - sizeof(HeaderOfVirtualBlock);

  const uint32_t elementSize = sizeof(ElementOfVirtualBlock) + pageSize - 1;

  if (arrayBlockSize % elementSize != 0) return false;

  uint32_t numOfElements = (arrayBlockSize / elementSize);

  HeaderOfVirtualBlock* h =
      reinterpret_cast<HeaderOfVirtualBlock*>(virtualBlock);

  if (numOfElements != h->numberOfUpdatedPages) return false;

  uint32_t lastPageId = UINT32_MAX;

  for (uint32_t i = 0; i < numOfElements; i++) {
    char* p = virtualBlock + sizeof(HeaderOfVirtualBlock) + (i * elementSize);
    ElementOfVirtualBlock* e = reinterpret_cast<ElementOfVirtualBlock*>(p);

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
  std::chrono::time_point<std::chrono::system_clock> n =
      std::chrono::system_clock::now();

  const uint64_t milli = std::chrono::duration_cast<std::chrono::milliseconds>(
                             n.time_since_epoch())
                             .count();

  if (milli > lastMilliOfUniqueFetchID_) {
    lastMilliOfUniqueFetchID_ = milli;
    lastCountOfUniqueFetchID_ = 0;
  } else {
    if (lastCountOfUniqueFetchID_ == 0x3FFFFF) lastMilliOfUniqueFetchID_++;
    lastCountOfUniqueFetchID_++;
  }

  uint64_t r = (lastMilliOfUniqueFetchID_ << (64 - 42));
  Assert(lastCountOfUniqueFetchID_ <= 0x3FFFFF);
  r = r | ((uint64_t)lastCountOfUniqueFetchID_);

  return r;
}

// static time_point<system_clock>  getTimeOfUniqueMsgSeqNum(uint64_t seqNum) {
//   uint64_t milli = ((seqNum) >> (64 - 42));
//   milliseconds d{ milli };
//   time_point<system_clock> n(d);
//   return n;
// }

bool BCStateTran::checkValidityAndSaveMsgSeqNum(uint16_t replicaId,
                                                uint64_t msgSeqNum) {
  uint64_t milliMsgTime = ((msgSeqNum) >> (64 - 42));

  time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
  const uint64_t milliNow =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          now.time_since_epoch())
          .count();

  uint64_t diffMilli = ((milliMsgTime > milliNow) ? (milliMsgTime - milliNow)
                                                  : (milliNow - milliMsgTime));

  if (diffMilli > maxAcceptableMsgDelayMilli_) {
    LOG_WARN(STLogger,
             "BCStateTran::checkValidityAndSaveMsgSeqNum - msgSeqNum "
                 << msgSeqNum
                 << " was rejected because diffMilli=" << diffMilli);

    return false;
  }

  auto p = lastMsgSeqNumOfReplicas_.find(replicaId);

  if (p != lastMsgSeqNumOfReplicas_.end() && p->second >= msgSeqNum) {
    LOG_WARN(STLogger,
             "BCStateTran::checkValidityAndSaveMsgSeqNum -"
                 << " msgSeqNum " << msgSeqNum << " was rejected"
                 << " because this is not the last msgSeqNum from replica"
                 << replicaId);

    return false;
  }

  lastMsgSeqNumOfReplicas_[replicaId] = msgSeqNum;

  LOG_INFO(STLogger,
           "BCStateTran::checkValidityAndSaveMsgSeqNum -"
               << " msgSeqNum " << msgSeqNum << " was accepted");

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

bool BCStateTran::isFetching() const { return (psd_->getIsFetchingState()); }

BCStateTran::FetchingState BCStateTran::getFetchingState() const {
  if (!psd_->getIsFetchingState()) return FetchingState::NotFetching;

  Assert(psd_->numOfAllPendingResPage() == 0);

  if (!psd_->hasCheckpointBeingFetched())
    return FetchingState::GettingCheckpointSummaries;

  if (psd_->getLastRequiredBlock() > 0)
    return FetchingState::GettingMissingBlocks;

  Assert(psd_->getFirstRequiredBlock() == 0);

  return FetchingState::GettingMissingResPages;
}

//////////////////////////////////////////////////////////////////////////////
// Send messages
//////////////////////////////////////////////////////////////////////////////

void BCStateTran::sendToAllOtherReplicas(char* msg, uint32_t msgSize) {
  for (int16_t r : replicas_) {
    if (r == myId_) continue;
    replicaForStateTransfer_->sendStateTransferMessage(msg, msgSize, r);
  }
}

void BCStateTran::sendAskForCheckpointSummariesMsg() {
  Assert(getFetchingState() == FetchingState::GettingCheckpointSummaries);

  AskForCheckpointSummariesMsg msg;

  lastTimeSentAskForCheckpointSummariesMsg = getMonotonicTimeMilli();
  lastMsgSeqNum_ = uniqueMsgSeqNum();

  msg.msgSeqNum = lastMsgSeqNum_;

  msg.minRelevantCheckpointNum = psd_->getLastStoredCheckpoint() + 1;

  LOG_INFO(STLogger,
           "BCStateTran::sendAskForCheckpointSummariesMsg (lastMsgSeqNum="
               << lastMsgSeqNum_ << ", minRelevantCheckpointNum="
               << msg.minRelevantCheckpointNum << ")");

  sendToAllOtherReplicas(reinterpret_cast<char*>(&msg),
                         sizeof(AskForCheckpointSummariesMsg));
}

void BCStateTran::sendFetchBlocksMsg(
    uint64_t firstRequiredBlock,
    uint64_t lastRequiredBlock,
    int16_t lastKnownChunkInLastRequiredBlock) {
  Assert(currentSourceReplica != NO_REPLICA);

  FetchBlocksMsg msg;

  lastMsgSeqNum_ = uniqueMsgSeqNum();

  msg.msgSeqNum = lastMsgSeqNum_;
  msg.firstRequiredBlock = firstRequiredBlock;
  msg.lastRequiredBlock = lastRequiredBlock;
  msg.lastKnownChunkInLastRequiredBlock = lastKnownChunkInLastRequiredBlock;

  LOG_INFO(STLogger,
           "BCStateTran::sendFetchBlocksMsg ("
               << " destination" << currentSourceReplica << " msgSeqNum"
               << msg.msgSeqNum << " firstRequiredBlock"
               << msg.firstRequiredBlock << " lastRequiredBlock"
               << msg.lastRequiredBlock << " lastKnownChunkInLastRequiredBlock"
               << msg.lastKnownChunkInLastRequiredBlock << " )");

  replicaForStateTransfer_->sendStateTransferMessage(
      reinterpret_cast<char*>(&msg),
      sizeof(FetchBlocksMsg),
      currentSourceReplica);
}

void BCStateTran::sendFetchResPagesMsg(
    int16_t lastKnownChunkInLastRequiredBlock) {
  Assert(currentSourceReplica != NO_REPLICA);

  Assert(psd_->hasCheckpointBeingFetched());

  DataStore::CheckpointDesc cp = psd_->getCheckpointBeingFetched();

  uint64_t lastStoredCheckpoint = psd_->getLastStoredCheckpoint();

  FetchResPagesMsg msg;

  lastMsgSeqNum_ = uniqueMsgSeqNum();

  msg.msgSeqNum = lastMsgSeqNum_;

  msg.lastCheckpointKnownToRequester = lastStoredCheckpoint;

  msg.requiredCheckpointNum = cp.checkpointNum;
  msg.lastKnownChunk = lastKnownChunkInLastRequiredBlock;

  LOG_INFO(STLogger,
           "BCStateTran::sendFetchResPagesMsg ("
               << " destination" << currentSourceReplica << " msgSeqNum"
               << msg.msgSeqNum << " lastCheckpointKnownToRequester"
               << msg.lastCheckpointKnownToRequester << " requiredCheckpointNum"
               << msg.requiredCheckpointNum << " lastKnownChunk"
               << msg.lastKnownChunk << " )");

  replicaForStateTransfer_->sendStateTransferMessage(
      reinterpret_cast<char*>(&msg),
      sizeof(FetchResPagesMsg),
      currentSourceReplica);
}

//////////////////////////////////////////////////////////////////////////////
// Message handlers
//////////////////////////////////////////////////////////////////////////////

bool BCStateTran::onMessage(const AskForCheckpointSummariesMsg* m,
                            uint32_t msgLen,
                            uint16_t replicaId) {
  LOG_INFO(STLogger, "BCStateTran::onMessage - AskForCheckpointSummariesMsg");

  Assert(!psd_->getIsFetchingState());

  // if msg is invalid
  if (msgLen < sizeof(AskForCheckpointSummariesMsg) ||
      m->minRelevantCheckpointNum == 0 || m->msgSeqNum == 0) {
    LOG_WARN(STLogger, "msg is invalid");
    return false;
  }

  // if msg is not relevant
  if (!checkValidityAndSaveMsgSeqNum(replicaId, m->msgSeqNum) ||
      (m->minRelevantCheckpointNum > psd_->getLastStoredCheckpoint())) {
    LOG_WARN(STLogger, "msg is irrelevant");
    return false;
  }

  uint64_t toCheckpoint = psd_->getLastStoredCheckpoint();

  uint64_t fromCheckpoint =
      std::max(m->minRelevantCheckpointNum, psd_->getFirstStoredCheckpoint());
  // TODO(GG): really need this condition?
  if (toCheckpoint > maxNumOfStoredCheckpoints_)
    fromCheckpoint =
        std::max(fromCheckpoint, toCheckpoint - maxNumOfStoredCheckpoints_ + 1);

  bool sent = false;

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
             "Sending CheckpointSummaryMsg ("
                 << " destination" << replicaId << " checkpointNum"
                 << checkpointSummary.checkpointNum << " lastBlock"
                 << checkpointSummary.lastBlock << " digestOfLastBlock"
                 << checkpointSummary.digestOfLastBlock.toString()
                 << " digestOfResPagesDescriptor"
                 << checkpointSummary.digestOfResPagesDescriptor.toString()
                 << " requestMsgSeqNum" << checkpointSummary.requestMsgSeqNum
                 << " )");

    replicaForStateTransfer_->sendStateTransferMessage(
        reinterpret_cast<char*>(&checkpointSummary),
        sizeof(CheckpointSummaryMsg),
        replicaId);

    sent = true;
  }

  if (!sent) {
    LOG_INFO(STLogger,
             "Replicas was not able to send relevant CheckpointSummaryMsg");
  }

  return false;
}

bool BCStateTran::onMessage(const CheckpointSummaryMsg* m,
                            uint32_t msgLen,
                            uint16_t replicaId) {
  LOG_INFO(STLogger, "BCStateTran::onMessage - CheckpointSummaryMsg");

  FetchingState fs = getFetchingState();
  Assert(fs == FetchingState::GettingCheckpointSummaries);

  // if msg is invalid
  if (msgLen < sizeof(CheckpointSummaryMsg) || m->checkpointNum == 0 ||
      //    m->lastBlock == 0 ||
      //    m->digestOfLastBlock.isZero() ||
      m->digestOfResPagesDescriptor.isZero() || m->requestMsgSeqNum == 0) {
    LOG_WARN(STLogger, "msg is invalid");
    return false;
  }

  // if msg is not relevant
  if (m->requestMsgSeqNum != lastMsgSeqNum_ ||
      m->checkpointNum <= psd_->getLastStoredCheckpoint()) {
    LOG_WARN(STLogger, "msg is irrelevant");
    return false;
  }

  uint16_t numOfMsgsFromSender =
      (numOfSummariesFromOtherReplicas.count(replicaId) == 0)
          ? 0
          : numOfSummariesFromOtherReplicas.at(replicaId);

  // if we have too many messages from the same replica
  if (numOfMsgsFromSender >= (psd_->getMaxNumOfStoredCheckpoints() + 1)) {
    LOG_WARN(STLogger, "Too many messages from replica " << replicaId);
    return false;
  }

  auto p = summariesCerts.find(m->checkpointNum);
  CheckpointSummaryMsgCert* cert = nullptr;

  if (p == summariesCerts.end()) {
    cert = new CheckpointSummaryMsgCert(
        replicaForStateTransfer_, replicas_.size(), fVal_, fVal_ + 1, myId_);
    summariesCerts[m->checkpointNum] = cert;
  } else {
    cert = p->second;
  }

  bool used = cert->addMsg(const_cast<CheckpointSummaryMsg*>(m), replicaId);

  if (used)
    numOfSummariesFromOtherReplicas[replicaId] = numOfMsgsFromSender + 1;

  if (!cert->isComplete()) {
    LOG_INFO(STLogger, "Does not have enough CheckpointSummaryMsg messages");
    return true;
  }

  LOG_INFO(STLogger, "Has enough CheckpointSummaryMsg messages");

  CheckpointSummaryMsg* checkSummary = cert->bestCorrectMsg();

  Assert(checkSummary != nullptr);

  //

  Assert(preferredReplicas_.empty());
  Assert(currentSourceReplica == NO_REPLICA);
  Assert(timeMilliCurrentSourceReplica == 0);
  Assert(nextRequiredBlock == 0);
  Assert(digestOfNextRequiredBlock.isZero());
  Assert(pendingItemDataMsgs.empty());
  Assert(totalSizeOfPendingItemDataMsgs == 0);

  // set the preferred replicas

  for (uint16_t r : replicas_) {  // TODO(GG): can be improved
    CheckpointSummaryMsg* t = cert->getMsgFromReplica(r);
    if (t != nullptr && CheckpointSummaryMsg::equivalent(t, checkSummary))
      preferredReplicas_.insert(r);
  }

  Assert(static_cast<uint16_t>(preferredReplicas_.size()) >= fVal_ + 1);

  // set new checkpoint

  DataStore::CheckpointDesc newCheckpoint;

  newCheckpoint.checkpointNum = checkSummary->checkpointNum;
  newCheckpoint.lastBlock = checkSummary->lastBlock;
  newCheckpoint.digestOfLastBlock = checkSummary->digestOfLastBlock;
  newCheckpoint.digestOfResPagesDescriptor =
      checkSummary->digestOfResPagesDescriptor;

  Assert(!psd_->hasCheckpointBeingFetched());
  psd_->setCheckpointBeingFetched(newCheckpoint);

  LOG_INFO(STLogger,
           "Start fetching checkpoint: "
               << " checkpointNum " << newCheckpoint.checkpointNum
               << " lastBlock " << newCheckpoint.lastBlock
               << " digestOfLastBlock "
               << newCheckpoint.digestOfLastBlock.toString()
               << " digestOfResPagesDescriptor "
               << newCheckpoint.digestOfResPagesDescriptor.toString());

  // clean
  clearInfoAboutGettingCheckpointSummary();
  lastMsgSeqNum_ = 0;

  // check if we need to fetch blocks, or reserved pages

  const uint64_t lastReachableBlockNum = as_->getLastReachableBlockNum();

  if (newCheckpoint.lastBlock > lastReachableBlockNum) {
    psd_->setFirstRequiredBlock(lastReachableBlockNum + 1);
    psd_->setLastRequiredBlock(newCheckpoint.lastBlock);
  } else {
    Assert(newCheckpoint.lastBlock == lastReachableBlockNum);
    Assert(psd_->getFirstRequiredBlock() == 0);
    Assert(psd_->getLastRequiredBlock() == 0);
  }

  LOG_INFO(STLogger, "New state is " << stateName(getFetchingState()));

  processData();

  return true;
}

bool BCStateTran::onMessage(const FetchBlocksMsg* m,
                            uint32_t msgLen,
                            uint16_t replicaId) {
  LOG_INFO(STLogger, "BCStateTran::onMessage - FetchBlocksMsg");

  // if msg is invalid
  if (msgLen < sizeof(FetchBlocksMsg) || m->msgSeqNum == 0 ||
      m->firstRequiredBlock == 0 ||
      m->lastRequiredBlock < m->firstRequiredBlock) {
    LOG_WARN(STLogger, "msg is invalid");
    return false;
  }

  // if msg is not relevant
  if (!checkValidityAndSaveMsgSeqNum(replicaId, m->msgSeqNum)) {
    LOG_WARN(STLogger, "msg is irrelevant");
    return false;
  }

  FetchingState fs = getFetchingState();

  // if msg should be rejected
  if (fs != FetchingState::NotFetching ||
      m->lastRequiredBlock > as_->getLastReachableBlockNum()) {
    RejectFetchingMsg outMsg;
    outMsg.requestMsgSeqNum = m->msgSeqNum;

    LOG_WARN(STLogger,
             "Rejecting msg. Sending RejectFetchingMsg to replica "
                 << replicaId
                 << " with requestMsgSeqNum=" << outMsg.requestMsgSeqNum);

    replicaForStateTransfer_->sendStateTransferMessage(
        reinterpret_cast<char*>(&outMsg), sizeof(RejectFetchingMsg), replicaId);

    return false;
  }

  // compute information about next block and chunk
  uint64_t nextBlock = m->lastRequiredBlock;
  uint32_t sizeOfNextBlock = 0;
  bool tmp = as_->getBlock(nextBlock, buffer_, &sizeOfNextBlock);
  Assert(tmp && sizeOfNextBlock > 0);

  uint32_t sizeOfLastChunk = maxChunkSize_;
  uint32_t numOfChunksInNextBlock = sizeOfNextBlock / maxChunkSize_;
  if (sizeOfNextBlock % maxChunkSize_ != 0) {
    sizeOfLastChunk = sizeOfNextBlock % maxChunkSize_;
    numOfChunksInNextBlock++;
  }

  uint16_t nextChunk = m->lastKnownChunkInLastRequiredBlock + 1;

  // if msg is invalid (lastKnownChunkInLastRequiredBlock+1 does not exist)
  if (nextChunk > numOfChunksInNextBlock) {
    LOG_WARN(STLogger, "msg is invalid (illegal chunk number)");

    memset(buffer_, 0, sizeOfNextBlock);
    return false;
  }

  // send chunks
  uint16_t numOfSentChunks = 0;
  while (true) {
    uint32_t chunkSize =
        (nextChunk < numOfChunksInNextBlock) ? maxChunkSize_ : sizeOfLastChunk;

    Assert(chunkSize > 0);

    char* pRawChunk = buffer_ + (nextChunk - 1) * maxChunkSize_;

    ItemDataMsg* outMsg = ItemDataMsg::alloc(chunkSize);  // TODO(GG): improve

    outMsg->requestMsgSeqNum = m->msgSeqNum;
    outMsg->blockNumber = nextBlock;
    outMsg->totalNumberOfChunksInBlock = numOfChunksInNextBlock;
    outMsg->chunkNumber = nextChunk;
    outMsg->dataSize = chunkSize;
    memcpy(outMsg->data, pRawChunk, chunkSize);

    LOG_INFO(STLogger,
             "Sending ItemDataMsg ("
                 << " destination" << replicaId << " requestMsgSeqNum"
                 << outMsg->requestMsgSeqNum << " blockNumber"
                 << outMsg->blockNumber << " totalNumberOfChunksInBlock"
                 << outMsg->totalNumberOfChunksInBlock << " chunkNumber"
                 << outMsg->chunkNumber << " dataSize" << outMsg->dataSize
                 << " )");

    replicaForStateTransfer_->sendStateTransferMessage(
        reinterpret_cast<char*>(outMsg), outMsg->size(), replicaId);

    ItemDataMsg::free(outMsg);

    numOfSentChunks++;

    // if we've already sent enough chunks
    if (numOfSentChunks >= maxNumberOfChunksInBatch_) {
      break;
    }
    // if we still have chunks in block
    else if (static_cast<uint16_t>(nextChunk + 1) <= numOfChunksInNextBlock) {
      nextChunk++;
    }
    // we sent all relevant blocks
    else if (nextBlock - 1 < m->firstRequiredBlock) {
      break;
      // start sending the next block
    } else {
      nextBlock--;
      memset(buffer_, 0, sizeOfNextBlock);
      sizeOfNextBlock = 0;
      bool tmp2 = as_->getBlock(nextBlock, buffer_, &sizeOfNextBlock);
      Assert(tmp2 && sizeOfNextBlock > 0);

      sizeOfLastChunk = maxChunkSize_;
      numOfChunksInNextBlock = sizeOfNextBlock / maxChunkSize_;
      if (sizeOfNextBlock % maxChunkSize_ != 0) {
        sizeOfLastChunk = sizeOfNextBlock % maxChunkSize_;
        numOfChunksInNextBlock++;
      }

      nextChunk = 1;
    }
  }

  memset(buffer_, 0, sizeOfNextBlock);

  return false;
}

bool BCStateTran::onMessage(const FetchResPagesMsg* m,
                            uint32_t msgLen,
                            uint16_t replicaId) {
  LOG_INFO(STLogger, "BCStateTran::onMessage - FetchResPagesMsg");

  // if msg is invalid
  if (msgLen < sizeof(FetchResPagesMsg) || m->msgSeqNum == 0 ||
      m->requiredCheckpointNum == 0) {
    LOG_WARN(STLogger, "msg is invalid");
    return false;
  }

  // if msg is not relevant
  if (!checkValidityAndSaveMsgSeqNum(replicaId, m->msgSeqNum)) {
    LOG_WARN(STLogger, "msg is irrelevant");
    return false;
  }

  FetchingState fs = getFetchingState();

  // if msg should be rejected
  if (fs != FetchingState::NotFetching ||
      !psd_->hasCheckpointDesc(m->requiredCheckpointNum)) {
    RejectFetchingMsg outMsg;
    outMsg.requestMsgSeqNum = m->msgSeqNum;

    LOG_WARN(STLogger,
             "Rejecting msg. Sending RejectFetchingMsg to replica "
                 << replicaId
                 << " with requestMsgSeqNum=" << outMsg.requestMsgSeqNum);

    replicaForStateTransfer_->sendStateTransferMessage(
        reinterpret_cast<char*>(&outMsg), sizeof(RejectFetchingMsg), replicaId);

    return false;
  }

  // find virtual block
  DescOfVBlockForResPages descOfVBlock;
  descOfVBlock.checkpointNum = m->requiredCheckpointNum;
  descOfVBlock.lastCheckpointKnownToRequester =
      m->lastCheckpointKnownToRequester;
  char* vblock = getVBlockFromCache(descOfVBlock);

  // if we don't have the relevant vblock, create the vblock
  if (vblock == nullptr) {
    LOG_INFO(STLogger,
             "Creating a new vblock: checkpointNum="
                 << descOfVBlock.checkpointNum
                 << " lastCheckpointKnownToRequester="
                 << descOfVBlock.lastCheckpointKnownToRequester);

    // TODO(GG): consider adding protection against bad replicas
    // that lead to unnecessary creations of vblocks
    vblock = createVBlock(descOfVBlock);
    Assert(vblock != nullptr);
    setVBlockInCache(descOfVBlock, vblock);

    Assert(cacheOfVirtualBlockForResPages.size() <= kMaxVBlocksInCache);
  }

  uint32_t vblockSize = getSizeOfVirtualBlock(vblock, kSizeOfReservedPage);

  Assert(vblockSize > sizeof(HeaderOfVirtualBlock));
  Assert(checkStructureOfVirtualBlock(vblock, vblockSize, kSizeOfReservedPage));

  // compute information about next chunk

  uint32_t sizeOfLastChunk = maxChunkSize_;
  uint32_t numOfChunksInVBlock = vblockSize / maxChunkSize_;
  if (vblockSize % maxChunkSize_ != 0) {
    sizeOfLastChunk = vblockSize % maxChunkSize_;
    numOfChunksInVBlock++;
  }

  uint16_t nextChunk = m->lastKnownChunk + 1;

  // if msg is invalid (becuase lastKnownChunk+1 does not exist)
  if (nextChunk > numOfChunksInVBlock) {
    LOG_WARN(STLogger, "msg is invalid (illegal chunk number)");
    return false;
  }

  // send chunks
  uint16_t numOfSentChunks = 0;
  while (true) {
    uint32_t chunkSize =
        (nextChunk < numOfChunksInVBlock) ? maxChunkSize_ : sizeOfLastChunk;
    Assert(chunkSize > 0);

    char* pRawChunk = vblock + (nextChunk - 1) * maxChunkSize_;

    ItemDataMsg* outMsg = ItemDataMsg::alloc(chunkSize);

    outMsg->requestMsgSeqNum = m->msgSeqNum;
    outMsg->blockNumber = ID_OF_VBLOCK_RES_PAGES;
    outMsg->totalNumberOfChunksInBlock = numOfChunksInVBlock;
    outMsg->chunkNumber = nextChunk;
    outMsg->dataSize = chunkSize;
    memcpy(outMsg->data, pRawChunk, chunkSize);

    LOG_INFO(STLogger,
             "Sending ItemDataMsg ("
                 << " destination" << replicaId << " requestMsgSeqNum"
                 << outMsg->requestMsgSeqNum << " blockNumber"
                 << outMsg->blockNumber << " totalNumberOfChunksInBlock"
                 << outMsg->totalNumberOfChunksInBlock << " chunkNumber"
                 << outMsg->chunkNumber << " dataSize" << outMsg->dataSize
                 << " )");

    replicaForStateTransfer_->sendStateTransferMessage(
        reinterpret_cast<char*>(outMsg), outMsg->size(), replicaId);

    ItemDataMsg::free(outMsg);

    numOfSentChunks++;

    // if we've already sent enough chunks
    if (numOfSentChunks >= maxNumberOfChunksInBatch_) {
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

bool BCStateTran::onMessage(const RejectFetchingMsg* m,
                            uint32_t msgLen,
                            uint16_t replicaId) {
  LOG_INFO(STLogger, "BCStateTran::onMessage - RejectFetchingMsg");

  FetchingState fs = getFetchingState();
  Assert(fs == FetchingState::GettingMissingBlocks ||
         fs == FetchingState::GettingMissingResPages);

  Assert(preferredReplicas_.size() > 0);

  // if msg is invalid
  if (msgLen < sizeof(RejectFetchingMsg)) {
    LOG_WARN(STLogger, "msg is invalid");
    return false;
  }

  // if msg is not relevant
  if (currentSourceReplica != replicaId ||
      lastMsgSeqNum_ != m->requestMsgSeqNum) {
    LOG_WARN(STLogger, "msg is irrelevant");
    return false;
  }

  Assert(preferredReplicas_.count(replicaId) != 0);

  LOG_WARN(STLogger,
           "Removing replica " << replicaId << " from preferredReplicas_");

  preferredReplicas_.erase(replicaId);
  currentSourceReplica = NO_REPLICA;
  clearAllPendingItemsData();

  if (preferredReplicas_.size() > 0) {
    processData();
  } else if (fs == FetchingState::GettingMissingBlocks) {
    LOG_INFO(STLogger,
             "Adding all peer replicas to preferredReplicas_"
             "(because preferredReplicas_.size()==0)");

    // in this case, we will try to use all other replicas
    set<uint16_t> tmp = replicas_;
    tmp.erase(myId_);
    preferredReplicas_ = tmp;

    processData();
  } else if (fs == FetchingState::GettingMissingResPages) {
    LOG_INFO(STLogger,
             "Go to state  GettingCheckpointSummaries"
             " (because preferredReplicas_.size()==0)");

    // move to GettingCheckpointSummaries

    preferredReplicas_.clear();
    currentSourceReplica = NO_REPLICA;
    timeMilliCurrentSourceReplica = 0;
    nextRequiredBlock = 0;
    digestOfNextRequiredBlock.makeZero();
    clearAllPendingItemsData();

    psd_->deleteCheckpointBeingFetched();
    Assert(getFetchingState() == FetchingState::GettingCheckpointSummaries);

    verifyEmptyInfoAboutGettingCheckpointSummary();
    sendAskForCheckpointSummariesMsg();
  } else {
    Assert(false);
  }

  return false;
}

bool BCStateTran::onMessage(const ItemDataMsg* m,
                            uint32_t msgLen,
                            uint16_t replicaId) {
  LOG_INFO(STLogger, "BCStateTran::onMessage - ItemDataMsg");

  FetchingState fs = getFetchingState();
  Assert(fs == FetchingState::GettingMissingBlocks ||
         fs == FetchingState::GettingMissingResPages);

  const uint16_t MaxNumOfChunksInBlock =
      (fs == FetchingState::GettingMissingBlocks) ? maxNumOfChunksInAppBlock_
                                                  : maxNumOfChunksInVBlock_;

  LOG_INFO(STLogger,
           "m->blockNumber= " << m->blockNumber
                              << "  m->totalNumberOfChunksInBlock= "
                              << m->totalNumberOfChunksInBlock
                              << "  m->chunkNumber= " << m->chunkNumber
                              << "  m->dataSize= " << m->dataSize);

  // if msg is invalid
  if (msgLen < m->size() || m->requestMsgSeqNum == 0 || m->blockNumber == 0 ||
      m->totalNumberOfChunksInBlock == 0 ||
      m->totalNumberOfChunksInBlock > MaxNumOfChunksInBlock ||
      m->chunkNumber == 0 || m->dataSize == 0) {
    LOG_WARN(STLogger, "msg is invalid");
    return false;
  }

  //  const DataStore::CheckpointDesc fcp = psd_->getCheckpointBeingFetched();
  const uint64_t firstRequiredBlock = psd_->getFirstRequiredBlock();
  const uint64_t lastRequiredBlock = psd_->getLastRequiredBlock();

  if (fs == FetchingState::GettingMissingBlocks) {
    // if msg is not relevant
    if (currentSourceReplica != replicaId ||
        m->requestMsgSeqNum != lastMsgSeqNum_ ||
        m->blockNumber > lastRequiredBlock ||
        m->blockNumber < firstRequiredBlock ||
        (m->blockNumber + maxNumberOfChunksInBatch_ + 1 < lastRequiredBlock) ||
        m->dataSize + totalSizeOfPendingItemDataMsgs >
            maxPendingDataFromSourceReplica_) {
      LOG_WARN(STLogger, "msg is irrelevant");  // TODO(GG)

      /*
            LOG_INFO(STLogger,  endl << "  m->requestMsgSeqNum=" <<
         m->requestMsgSeqNum << "  m->blockNumber=" << m->blockNumber << "
         m->totalNumberOfChunksInBlock=" << m->totalNumberOfChunksInBlock << "
         m->chunkNumber=" << m->chunkNumber << "  m->dataSize=" << m->dataSize);


            LOG_INFO(STLogger,  "COND (currentSourceReplica != replicaId)=" <<
         (currentSourceReplica != replicaId)); LOG_INFO(STLogger,  "COND
         (m->requestMsgSeqNum != lastMsgSeqNum_)=" << (m->requestMsgSeqNum !=
         lastMsgSeqNum_)); LOG_INFO(STLogger,  "COND (m->blockNumber >
         lastRequiredBlock)=" << (m->blockNumber > lastRequiredBlock));
            LOG_INFO(STLogger,  "COND (m->blockNumber < firstRequiredBlock)=" <<
         (m->blockNumber < firstRequiredBlock)); LOG_INFO(STLogger,  "COND
         (m->blockNumber + MAX_NUMBER_OF_CHUNKS_IN_SINGLE_BATCH + 1 <
         lastRequiredBlock)=" << (m->blockNumber +
         MAX_NUMBER_OF_CHUNKS_IN_SINGLE_BATCH + 1 < lastRequiredBlock));
            LOG_INFO(STLogger,  "COND (m->dataSize +
         totalSizeOfPendingItemDataMsgs > maxPendingDataFromSourceReplica_)=" <<
         (m->dataSize + totalSizeOfPendingItemDataMsgs >
         maxPendingDataFromSourceReplica_));
      */

      return false;
    }
  } else {
    Assert(firstRequiredBlock == 0);
    Assert(lastRequiredBlock == 0);

    // if msg is not relevant
    if (currentSourceReplica != replicaId ||
        m->requestMsgSeqNum != lastMsgSeqNum_ ||
        m->blockNumber != ID_OF_VBLOCK_RES_PAGES ||
        m->dataSize + totalSizeOfPendingItemDataMsgs >
            maxPendingDataFromSourceReplica_) {
      LOG_WARN(STLogger, "msg is irrelevant");
      return false;
    }
  }

  Assert(preferredReplicas_.count(replicaId) != 0);

  bool added = false;

  tie(std::ignore, added) =
      pendingItemDataMsgs.insert(const_cast<ItemDataMsg*>(m));

  if (added) {
    LOG_INFO(STLogger, "ItemDataMsg was added to pendingItemDataMsgs");

    totalSizeOfPendingItemDataMsgs += m->dataSize;

    processData();
    return true;
  } else {
    LOG_INFO(STLogger, "ItemDataMsg was NOT added to pendingItemDataMsgs");

    return false;
  }
}

//////////////////////////////////////////////////////////////////////////////
// cache that holds virtual blocks
//////////////////////////////////////////////////////////////////////////////

char* BCStateTran::getVBlockFromCache(
    const DescOfVBlockForResPages& desc) const {
  auto p = cacheOfVirtualBlockForResPages.find(desc);

  if (p == cacheOfVirtualBlockForResPages.end()) return nullptr;

  char* vBlock = p->second;

  Assert(vBlock != nullptr);

  HeaderOfVirtualBlock* header =
      reinterpret_cast<HeaderOfVirtualBlock*>(vBlock);

  Assert(desc.lastCheckpointKnownToRequester ==
         header->lastCheckpointKnownToRequester);

  return vBlock;
}

void BCStateTran::setVBlockInCache(const DescOfVBlockForResPages& desc,
                                   char* vBlock) {
  auto p = cacheOfVirtualBlockForResPages.find(desc);

  Assert(p == cacheOfVirtualBlockForResPages.end());

  if (cacheOfVirtualBlockForResPages.size() >= kMaxVBlocksInCache) {
    auto minItem = cacheOfVirtualBlockForResPages.begin();
    std::free(minItem->second);
    cacheOfVirtualBlockForResPages.erase(minItem);
  }

  cacheOfVirtualBlockForResPages[desc] = vBlock;

  Assert(cacheOfVirtualBlockForResPages.size() <= kMaxVBlocksInCache);
}

char* BCStateTran::createVBlock(const DescOfVBlockForResPages& desc) {
  Assert(psd_->hasCheckpointDesc(desc.checkpointNum));

  // find the updated pages
  std::list<uint32_t> updatedPages;

  for (uint32_t i = 0; i < numberOfReservedPages_; i++) {
    uint64_t actualPageCheckpoint = 0;
    psd_->getResPage(i, desc.checkpointNum, &actualPageCheckpoint);

    // because we have checkpoint desc.checkpointNum
    Assert(actualPageCheckpoint <= desc.checkpointNum);

    if (actualPageCheckpoint > desc.lastCheckpointKnownToRequester)
      updatedPages.push_back(i);
  }

  const uint32_t numberOfUpdatedPages = updatedPages.size();

  // allocate and fill block
  const uint32_t elementSize =
      sizeof(ElementOfVirtualBlock) + kSizeOfReservedPage - 1;
  const uint32_t size =
      sizeof(HeaderOfVirtualBlock) + numberOfUpdatedPages * elementSize;

  char* rawVBlock = reinterpret_cast<char*>(std::malloc(size));

  HeaderOfVirtualBlock* header =
      reinterpret_cast<HeaderOfVirtualBlock*>(rawVBlock);
  header->lastCheckpointKnownToRequester = desc.lastCheckpointKnownToRequester;
  header->numberOfUpdatedPages = numberOfUpdatedPages;

  if (numberOfUpdatedPages == 0) {
    Assert(checkStructureOfVirtualBlock(rawVBlock, size, kSizeOfReservedPage));
    LOG_INFO(
        STLogger,
        "New vblock contains " << 0 << " updated pages , its size is " << size);
    return rawVBlock;
  }

  char* elements = rawVBlock + sizeof(HeaderOfVirtualBlock);

  uint16_t idx = 0;
  for (uint32_t pageId : updatedPages) {
    Assert(idx < numberOfUpdatedPages);

    uint64_t actualPageCheckpoint = 0;
    STDigest pageDigest;

    psd_->getResPage(pageId,
                     desc.checkpointNum,
                     &actualPageCheckpoint,
                     &pageDigest,
                     buffer_,
                     kSizeOfReservedPage);
    Assert(actualPageCheckpoint <= desc.checkpointNum);
    Assert(actualPageCheckpoint > desc.lastCheckpointKnownToRequester);
    Assert(!pageDigest.isZero());

    ElementOfVirtualBlock* currElement =
        reinterpret_cast<ElementOfVirtualBlock*>(elements + idx * elementSize);

    currElement->pageId = pageId;
    currElement->checkpointNumber = actualPageCheckpoint;
    currElement->pageDigest = pageDigest;
    memcpy(currElement->page, buffer_, kSizeOfReservedPage);
    memset(buffer_, 0, kSizeOfReservedPage);

    idx++;
  }

  Assert(idx == numberOfUpdatedPages);

  Assert(!pedanticChecks_ ||
         checkStructureOfVirtualBlock(rawVBlock, size, kSizeOfReservedPage));

  LOG_INFO(STLogger,
           "New vblock contains " << numberOfUpdatedPages
                                  << " updated pages , its size is " << size);

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
  Assert(lastTimeSentAskForCheckpointSummariesMsg == 0);
  Assert(retransmissionNumberOfAskForCheckpointSummariesMsg == 0);
  Assert(summariesCerts.empty());
  Assert(numOfSummariesFromOtherReplicas.empty());
}

///////////////////////////////////////////////////////////////////////////
// for states GettingMissingBlocks or GettingMissingResPages
///////////////////////////////////////////////////////////////////////////

void BCStateTran::clearAllPendingItemsData() {
  LOG_INFO(STLogger, "BCStateTran::clearAllPendingItemsData");

  for (auto i : pendingItemDataMsgs)
    replicaForStateTransfer_->freeStateTransferMsg(reinterpret_cast<char*>(i));

  pendingItemDataMsgs.clear();
  totalSizeOfPendingItemDataMsgs = 0;
}

void BCStateTran::clearPendingItemsData(uint64_t untilBlock) {
  LOG_INFO(STLogger,
           "BCStateTran::clearPendingItemsData - untilBlock=" << untilBlock);

  if (untilBlock == 0) return;

  auto it = pendingItemDataMsgs.begin();

  while (it != pendingItemDataMsgs.end() && (*it)->blockNumber >= untilBlock) {
    Assert(totalSizeOfPendingItemDataMsgs >= (*it)->dataSize);

    totalSizeOfPendingItemDataMsgs -= (*it)->dataSize;

    replicaForStateTransfer_->freeStateTransferMsg(
        reinterpret_cast<char*>(*it));

    it = pendingItemDataMsgs.erase(it);
  }
}

bool BCStateTran::getNextFullBlock(uint64_t requiredBlock,
                                   bool& outBadDataDetected,
                                   int16_t& outLastChunkInRequiredBlock,
                                   char* outBlock,
                                   uint32_t& outBlockSize,
                                   bool isVBLock) {
  Assert(requiredBlock >= 1);

  const uint32_t maxSize = (isVBLock ? maxVBlockSize_ : maxBlockSize_);

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
  while ((it != pendingItemDataMsgs.end()) &&
         ((*it)->blockNumber == requiredBlock)) {
    ItemDataMsg* msg = *it;

    // the conditions of these asserts are checked when receiving  the message
    Assert(msg->totalNumberOfChunksInBlock > 0);
    Assert(msg->chunkNumber >= 1);

    if (totalNumberOfChunks == 0)
      totalNumberOfChunks = msg->totalNumberOfChunksInBlock;

    blockSize += msg->dataSize;

    if (totalNumberOfChunks != msg->totalNumberOfChunksInBlock ||
        msg->chunkNumber > totalNumberOfChunks || blockSize > maxSize) {
      badData = true;
      break;
    }

    if (maxAvailableChunk + 1 < msg->chunkNumber) break;  // we have a hole

    Assert(maxAvailableChunk + 1 == msg->chunkNumber);

    maxAvailableChunk = msg->chunkNumber;

    Assert(maxAvailableChunk <= totalNumberOfChunks);

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
    Assert(it != pendingItemDataMsgs.end());
    Assert((*it)->blockNumber == requiredBlock);

    ItemDataMsg* msg = *it;

    Assert(msg->chunkNumber >= 1);
    Assert(msg->totalNumberOfChunksInBlock == totalNumberOfChunks);

    Assert(currentChunk + 1 == msg->chunkNumber);

    Assert(currentPos + msg->dataSize <= maxSize);

    memcpy(outBlock + currentPos, msg->data, msg->dataSize);

    currentChunk = msg->chunkNumber;
    currentPos += msg->dataSize;

    totalSizeOfPendingItemDataMsgs -= (*it)->dataSize;

    it = pendingItemDataMsgs.erase(it);

    if (currentChunk == totalNumberOfChunks) {
      outBlockSize = currentPos;
      return true;
    }
  }
}

bool BCStateTran::checkBlock(uint64_t blockNum,
                             const STDigest& expectedBlockDigest,
                             char* block,
                             uint32_t blockSize) const {
  STDigest blockDigest;

  computeDigestOfBlock(blockNum, block, blockSize, &blockDigest);

  if (blockDigest != expectedBlockDigest) {
    LOG_WARN(
        STLogger,
        "BCStateTran::checkBlock - incorrect digest: "
            << "  blockDigest= " << blockDigest.toString()
            << "  expectedBlockDigest= " << expectedBlockDigest.toString());

    return false;
  } else {
    return true;
  }
}

bool BCStateTran::checkVirtualBlockOfResPages(
    const STDigest& expectedDigestOfResPagesDescriptor,
    char* vblock,
    uint32_t vblockSize) const {
  LOG_INFO(STLogger, "BCStateTran::checkVirtualBlockOfResPages");

  if (!checkStructureOfVirtualBlock(vblock, vblockSize, kSizeOfReservedPage)) {
    LOG_WARN(STLogger, "vblock has illegal structure");
    return false;
  }

  HeaderOfVirtualBlock* h = reinterpret_cast<HeaderOfVirtualBlock*>(vblock);
  const uint32_t numberOfUpdatedPages = h->numberOfUpdatedPages;
  const uint64_t lastCheckpointKnownToRequester =
      h->lastCheckpointKnownToRequester;

  if (psd_->getLastStoredCheckpoint() != lastCheckpointKnownToRequester) {
    LOG_WARN(STLogger,
             "vblock has irrelevant checkpoint: checkpoint="
                 << lastCheckpointKnownToRequester
                 << " expected=" << psd_->getLastStoredCheckpoint());

    return false;
  }

  // build ResPagesDescriptor
  DataStore::ResPagesDescriptor* pagesDesc =
      psd_->getResPagesDescriptor(lastCheckpointKnownToRequester);

  Assert(pagesDesc->numOfPages == numberOfReservedPages_);

  if (numberOfUpdatedPages > 0) {
    uint32_t nextUpdateIndex = 0;

    ElementOfVirtualBlock* nextUpdate =
        getVirtualElement(0, kSizeOfReservedPage, vblock);

    for (uint32_t i = 0; i < numberOfReservedPages_; i++) {
      Assert(pagesDesc->d[i].pageId == i);
      Assert(pagesDesc->d[i].relevantCheckpoint <=
             lastCheckpointKnownToRequester);

      if (i == nextUpdate->pageId) {
        pagesDesc->d[i].relevantCheckpoint = nextUpdate->checkpointNumber;
        pagesDesc->d[i].pageDigest = nextUpdate->pageDigest;

        nextUpdateIndex++;
        if (nextUpdateIndex < numberOfUpdatedPages) {
          nextUpdate =
              getVirtualElement(nextUpdateIndex, kSizeOfReservedPage, vblock);
        } else {
          nextUpdate = nullptr;
          break;
        }
      }
    }
  }

  STDigest d;
  computeDigestOfPagesDescriptor(pagesDesc, d);

  psd_->free(pagesDesc);

  if (d != expectedDigestOfResPagesDescriptor) {
    LOG_WARN(STLogger,
             "vblock defines invalid digest of pages descriptor: "
             "digest="
                 << d.toString() << " expected="
                 << expectedDigestOfResPagesDescriptor.toString());

    return false;
  }

  // check digests of new pages
  for (uint32_t i = 0; i < numberOfUpdatedPages; i++) {
    ElementOfVirtualBlock* e =
        getVirtualElement(i, kSizeOfReservedPage, vblock);

    // verified in checkStructureOfVirtualBlock
    Assert(e->checkpointNumber > 0);

    STDigest pageDigest;
    computeDigestOfPage(e->pageId, e->checkpointNumber, e->page, pageDigest);

    if (pageDigest != e->pageDigest) {
      LOG_WARN(STLogger,
               "vblock contains invalid digest for page "
                   << e->pageId
                   << " : "
                      "digest="
                   << e->pageDigest.toString()
                   << " expected=" << pageDigest.toString());

      return false;
    }
  }

  return true;
}

uint16_t BCStateTran::selectSourceReplica() {
  const size_t size = preferredReplicas_.size();
  Assert(size > 0);

  auto i = preferredReplicas_.begin();

  if (size > 1) {
    // TODO(GG): can be optimized
    unsigned int c = randomGen_() % size;
    while (c > 0) {
      c--;
      i++;
    }
  }

  LOG_INFO(STLogger, "select new source replica " << (*i));

  return *i;
}

void BCStateTran::processData() {
  LOG_INFO(STLogger, "BCStateTran::processData");

  const FetchingState fs = getFetchingState();
  Assert(fs == FetchingState::GettingMissingBlocks ||
         fs == FetchingState::GettingMissingResPages);
  Assert(preferredReplicas_.size() > 0);

  Assert(totalSizeOfPendingItemDataMsgs <= maxPendingDataFromSourceReplica_);

  const bool isGettingBlocks = (fs == FetchingState::GettingMissingBlocks);

  Assert(!isGettingBlocks || psd_->getLastRequiredBlock() != 0);
  Assert(isGettingBlocks || psd_->getLastRequiredBlock() == 0);

  LOG_INFO(STLogger, "state is " << stateName(fs));

  const uint64_t currTime = getMonotonicTimeMilli();

  bool badDataFromCurrentSourceReplica = false;

  while (true) {
    //////////////////////////////////////////////////////////////////////////
    // if needed, select a source replica
    //////////////////////////////////////////////////////////////////////////

    bool newSourceReplica = false;

    const uint64_t diffMilli = ((currentSourceReplica == NO_REPLICA) ||
                                (currTime < timeMilliCurrentSourceReplica))
                                   ? 0
                                   : (currTime - timeMilliCurrentSourceReplica);

    if ((currentSourceReplica == NO_REPLICA) ||
        (badDataFromCurrentSourceReplica) ||
        // TODO(GG): TBD - compute dynamically
        (diffMilli > sourceReplicaReplacementTimeoutMilli_)) {
      // we want to replace the source replica

      LOG_INFO(STLogger, "replacing source replica");

      if (currentSourceReplica != NO_REPLICA) {
        preferredReplicas_.erase(currentSourceReplica);

        if (preferredReplicas_.size() == 0) {
          if (fs == FetchingState::GettingMissingBlocks) {
            LOG_INFO(STLogger,
                     "Adding all peer replicas to preferredReplicas_"
                     "(because preferredReplicas_.size()==0)");

            // in this case, we will try to use all other replicas
            set<uint16_t> tmp = replicas_;
            tmp.erase(myId_);
            preferredReplicas_ = tmp;
          } else if (fs == FetchingState::GettingMissingResPages) {
            LOG_INFO(STLogger,
                     "Go to state  GettingCheckpointSummaries"
                     " (because preferredReplicas_.size()==0)");

            // move to GettingCheckpointSummaries

            Assert(preferredReplicas_.empty());
            currentSourceReplica = NO_REPLICA;
            timeMilliCurrentSourceReplica = 0;
            nextRequiredBlock = 0;
            digestOfNextRequiredBlock.makeZero();
            clearAllPendingItemsData();

            psd_->deleteCheckpointBeingFetched();
            Assert(getFetchingState() ==
                   FetchingState::GettingCheckpointSummaries);

            verifyEmptyInfoAboutGettingCheckpointSummary();
            sendAskForCheckpointSummariesMsg();

            break;  // get out !!
          }
        }
      }

      newSourceReplica = true;
      currentSourceReplica = selectSourceReplica();
      timeMilliCurrentSourceReplica = currTime;
      badDataFromCurrentSourceReplica = false;
      clearAllPendingItemsData();
    }

    Assert(currentSourceReplica != NO_REPLICA);
    Assert(timeMilliCurrentSourceReplica != 0);
    Assert(badDataFromCurrentSourceReplica == false);

    //////////////////////////////////////////////////////////////////////////
    // if needed, determine the next required block
    //////////////////////////////////////////////////////////////////////////
    if (nextRequiredBlock == 0) {
      Assert(digestOfNextRequiredBlock.isZero());

      DataStore::CheckpointDesc cp = psd_->getCheckpointBeingFetched();

      if (!isGettingBlocks) {
        nextRequiredBlock = ID_OF_VBLOCK_RES_PAGES;
        digestOfNextRequiredBlock = cp.digestOfResPagesDescriptor;
      } else {
        nextRequiredBlock = psd_->getLastRequiredBlock();

        // if this is the last block in this checkpoint
        if (cp.lastBlock == nextRequiredBlock) {
          digestOfNextRequiredBlock = cp.digestOfLastBlock;
        } else {
          // we should already have block number nextRequiredBlock+1
          Assert(as_->hasBlock(nextRequiredBlock + 1));
          as_->getPrevDigestFromBlock(nextRequiredBlock + 1,
                                      reinterpret_cast<StateTransferDigest*>(
                                          &digestOfNextRequiredBlock));
        }
      }
    }

    Assert(nextRequiredBlock != 0);
    Assert(!digestOfNextRequiredBlock.isZero());

    LOG_INFO(STLogger,
             "nextRequiredBlock=" << nextRequiredBlock
                                  << " digestOfNextRequiredBlock="
                                  << digestOfNextRequiredBlock.toString());

    //////////////////////////////////////////////////////////////////////////
    // Process and check the available chunks
    //////////////////////////////////////////////////////////////////////////

    int16_t lastChunkInRequiredBlock = 0;
    uint32_t actualBlockSize = 0;

    const bool newBlock = getNextFullBlock(nextRequiredBlock,
                                           badDataFromCurrentSourceReplica,
                                           lastChunkInRequiredBlock,
                                           buffer_,
                                           actualBlockSize,
                                           !isGettingBlocks);

    bool newBlockIsValid = false;

    if (newBlock && isGettingBlocks) {
      Assert(!badDataFromCurrentSourceReplica);
      newBlockIsValid = checkBlock(nextRequiredBlock,
                                   digestOfNextRequiredBlock,
                                   buffer_,
                                   actualBlockSize);
      badDataFromCurrentSourceReplica = !newBlockIsValid;
    } else if (newBlock && !isGettingBlocks) {
      Assert(!badDataFromCurrentSourceReplica);
      newBlockIsValid = checkVirtualBlockOfResPages(
          digestOfNextRequiredBlock, buffer_, actualBlockSize);
      badDataFromCurrentSourceReplica = !newBlockIsValid;
    } else {
      Assert(!newBlock && actualBlockSize == 0);
    }

    LOG_INFO(STLogger,
             "newBlock=" << newBlock << " newBlockIsValid=" << newBlockIsValid);

    //////////////////////////////////////////////////////////////////////////
    // if we have a new block
    //////////////////////////////////////////////////////////////////////////
    if (newBlockIsValid && isGettingBlocks) {
      timeMilliCurrentSourceReplica = currTime;

      Assert(lastChunkInRequiredBlock >= 1 && actualBlockSize > 0);

      LOG_INFO(STLogger,
               "add block " << nextRequiredBlock << " (size=" << actualBlockSize
                            << " )");

      bool b = as_->putBlock(nextRequiredBlock, buffer_, actualBlockSize);
      Assert(b);
      memset(buffer_, 0, actualBlockSize);

      const uint64_t firstRequiredBlock = psd_->getFirstRequiredBlock();

      if (firstRequiredBlock < nextRequiredBlock) {
        as_->getPrevDigestFromBlock(
            nextRequiredBlock,
            reinterpret_cast<StateTransferDigest*>(&digestOfNextRequiredBlock));

        nextRequiredBlock--;

        psd_->setLastRequiredBlock(nextRequiredBlock);
      } else {
        // this is the last block we need

        psd_->setFirstRequiredBlock(0);
        psd_->setLastRequiredBlock(0);
        clearAllPendingItemsData();
        nextRequiredBlock = 0;
        digestOfNextRequiredBlock.makeZero();

        Assert(getFetchingState() == FetchingState::GettingMissingResPages);

        LOG_INFO(STLogger, "moved to GettingMissingResPages");

        sendFetchResPagesMsg(0);

        break;
      }
    }
    //////////////////////////////////////////////////////////////////////////
    // if we have a new vblock
    //////////////////////////////////////////////////////////////////////////
    else if (newBlockIsValid && !isGettingBlocks) {
      timeMilliCurrentSourceReplica = currTime;

      // set the updated pages

      uint32_t numOfUpdates = getNumberOfElements(buffer_);
      for (uint32_t i = 0; i < numOfUpdates; i++) {
        ElementOfVirtualBlock* e =
            getVirtualElement(i, kSizeOfReservedPage, buffer_);

        psd_->setResPage(
            e->pageId, e->checkpointNumber, e->pageDigest, e->page);

        LOG_INFO(STLogger, "update page " << e->pageId);
      }
      memset(buffer_, 0, actualBlockSize);

      Assert(psd_->hasCheckpointBeingFetched());

      DataStore::CheckpointDesc cp = psd_->getCheckpointBeingFetched();

      // set stored data

      Assert(psd_->getFirstRequiredBlock() == 0);
      Assert(psd_->getLastRequiredBlock() == 0);
      Assert(cp.checkpointNum > psd_->getLastStoredCheckpoint());

      psd_->setCheckpointDesc(cp.checkpointNum, cp);
      psd_->setLastStoredCheckpoint(cp.checkpointNum);

      psd_->deleteCheckpointBeingFetched();
      psd_->setIsFetchingState(false);

      // delete old checkpoints

      uint64_t minRelevantCheckpoint = 0;
      if (cp.checkpointNum >= maxNumOfStoredCheckpoints_)
        minRelevantCheckpoint =
            cp.checkpointNum - maxNumOfStoredCheckpoints_ + 1;

      if (minRelevantCheckpoint > 0) {
        while (minRelevantCheckpoint < cp.checkpointNum &&
               !psd_->hasCheckpointDesc(minRelevantCheckpoint))
          minRelevantCheckpoint++;
      }

      const uint64_t oldFirstStoredCheckpoint =
          psd_->getFirstStoredCheckpoint();

      if (minRelevantCheckpoint >= 2 &&
          minRelevantCheckpoint > oldFirstStoredCheckpoint) {
        psd_->deleteDescOfSmallerCheckpoints(minRelevantCheckpoint);
        psd_->deleteCoveredResPageInSmallerCheckpoints(minRelevantCheckpoint);
      }

      if (minRelevantCheckpoint > oldFirstStoredCheckpoint)
        psd_->setFirstStoredCheckpoint(minRelevantCheckpoint);

      LOG_INFO(STLogger, "minRelevantCheckpoint=" << minRelevantCheckpoint);

      //

      preferredReplicas_.clear();
      currentSourceReplica = NO_REPLICA;
      timeMilliCurrentSourceReplica = 0;
      nextRequiredBlock = 0;
      digestOfNextRequiredBlock.makeZero();
      clearAllPendingItemsData();

      //

      bool tmp = checkConsistency(pedanticChecks_);
      Assert(tmp);

      // Completion

      LOG_INFO(
          STLogger,
          "Calling onTransferringComplete for checkpoint " << cp.checkpointNum);

      replicaForStateTransfer_->onTransferringComplete(cp.checkpointNum);

      break;
    }
    //////////////////////////////////////////////////////////////////////////
    // if we don't have new full block/vblock (but we did not detect a problem)
    //////////////////////////////////////////////////////////////////////////
    else if (!badDataFromCurrentSourceReplica && isGettingBlocks) {
      if (newBlock) memset(buffer_, 0, actualBlockSize);

      // If needed, send messages
      if (newSourceReplica ||
          // TODO(GG): TBD - compute dynamically
          diffMilli > fetchRetransmissionTimeoutMilli_) {
        Assert(psd_->getLastRequiredBlock() == nextRequiredBlock);
        sendFetchBlocksMsg(psd_->getFirstRequiredBlock(),
                           nextRequiredBlock,
                           lastChunkInRequiredBlock);
      }

      break;
    } else if (!badDataFromCurrentSourceReplica && !isGettingBlocks) {
      if (newBlock) memset(buffer_, 0, actualBlockSize);

      if (newSourceReplica ||
          // TODO(GG): TBD - compute dynamically
          diffMilli > fetchRetransmissionTimeoutMilli_) {
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

#define CH(COND)               \
  {                            \
    if (!(COND)) return false; \
  }

bool BCStateTran::checkConsistency(bool checkAllBlocks) {
  CH(psd_->initialized());

  // check configuration

  CH(replicas_ == psd_->getReplicas());
  CH(myId_ == psd_->getMyReplicaId());
  CH(fVal_ == psd_->getFVal());

  CH(maxNumOfStoredCheckpoints_ == psd_->getMaxNumOfStoredCheckpoints());
  CH(numberOfReservedPages_ == psd_->getNumberOfReservedPages());

  // check firstStoredCheckpoint & lastStoredCheckpoint

  const uint64_t firstStoredCheckpoint = psd_->getFirstStoredCheckpoint();
  const uint64_t lastStoredCheckpoint = psd_->getLastStoredCheckpoint();

  CH(lastStoredCheckpoint >= firstStoredCheckpoint);
  CH(lastStoredCheckpoint - firstStoredCheckpoint + 1 <=
     maxNumOfStoredCheckpoints_);

  CH((lastStoredCheckpoint == 0) ||
     psd_->hasCheckpointDesc(lastStoredCheckpoint));
  CH((firstStoredCheckpoint == 0) ||
     (firstStoredCheckpoint == lastStoredCheckpoint) ||
     psd_->hasCheckpointDesc(firstStoredCheckpoint));

  // check reachable blocks

  const uint64_t lastReachableBlockNum = as_->getLastReachableBlockNum();

  if (checkAllBlocks && lastReachableBlockNum > 0) {
    for (uint64_t currBlock = lastReachableBlockNum - 1; currBlock >= 1;
         currBlock--) {
      STDigest currDigest;

      {
        uint32_t blockSize = 0;
        as_->getBlock(currBlock, buffer_, &blockSize);
        computeDigestOfBlock(currBlock, buffer_, blockSize, &currDigest);
        memset(buffer_, 0, blockSize);
      }

      // as_->getBlockDigest(currBlock, currDigest);

      CH(!currDigest.isZero());

      STDigest prevFromNextBlockDigest;
      prevFromNextBlockDigest.makeZero();
      as_->getPrevDigestFromBlock(
          currBlock + 1,
          reinterpret_cast<StateTransferDigest*>(&prevFromNextBlockDigest));

      CH(currDigest == prevFromNextBlockDigest);
    }
  }

  // check unreachable blocks

  const uint64_t lastBlockNum = as_->getLastBlockNum();

  CH(lastBlockNum >= lastReachableBlockNum);

  if (lastBlockNum > lastReachableBlockNum) {
    CH(getFetchingState() == FetchingState::GettingMissingResPages);

    uint64_t x = lastBlockNum - 1;
    while (as_->hasBlock(x)) x--;

    CH(x > lastReachableBlockNum);  // we should have a hole

    // we should have a single hole

    for (uint64_t i = lastReachableBlockNum + 1; i <= x; i++)
      CH(!as_->hasBlock(i));
  }

  // check blocks that are being fetched now

  if (lastBlockNum > lastReachableBlockNum) {
    CH(psd_->getIsFetchingState() && psd_->hasCheckpointBeingFetched());
    CH(psd_->getFirstRequiredBlock() - 1 == as_->getLastReachableBlockNum());
    CH(psd_->getLastRequiredBlock() >= psd_->getFirstRequiredBlock());

    if (checkAllBlocks) {
      uint64_t lastRequiredBlock = psd_->getLastRequiredBlock();

      for (uint64_t currBlock = lastBlockNum - 1;
           currBlock >= lastRequiredBlock + 1;
           currBlock--) {
        STDigest currDigest;

        {
          uint32_t blockSize = 0;
          as_->getBlock(currBlock, buffer_, &blockSize);
          computeDigestOfBlock(currBlock, buffer_, blockSize, &currDigest);
          memset(buffer_, 0, blockSize);
        }

        // as_->getBlockDigest(currBlock, currDigest);

        CH(!currDigest.isZero());

        STDigest prevFromNextBlockDigest;
        prevFromNextBlockDigest.makeZero();
        as_->getPrevDigestFromBlock(
            currBlock + 1,
            reinterpret_cast<StateTransferDigest*>(&prevFromNextBlockDigest));

        CH(currDigest == prevFromNextBlockDigest);
      }
    }
  }

  // check stored checkpoints

  if (lastStoredCheckpoint > 0) {
    uint64_t prevLastBlockNum = 0;
    for (uint64_t i = firstStoredCheckpoint; i <= lastStoredCheckpoint; i++) {
      if (!psd_->hasCheckpointDesc(i)) continue;

      DataStore::CheckpointDesc desc = psd_->getCheckpointDesc(i);
      CH(desc.checkpointNum == i);
      CH(desc.lastBlock <= as_->getLastReachableBlockNum());
      CH(desc.lastBlock >= prevLastBlockNum);
      prevLastBlockNum = desc.lastBlock;

      if (desc.lastBlock > 0) {
        STDigest d;

        {
          uint32_t blockSize = 0;
          as_->getBlock(desc.lastBlock, buffer_, &blockSize);
          computeDigestOfBlock(desc.lastBlock, buffer_, blockSize, &d);
          memset(buffer_, 0, blockSize);
        }

        // as_->getBlockDigest(desc.lastBlock, d);

        CH(d == desc.digestOfLastBlock);
      }

      DataStore::ResPagesDescriptor* allPagesDesc =
          psd_->getResPagesDescriptor(i);
      CH(allPagesDesc->numOfPages == numberOfReservedPages_);

      {
        STDigest d2;
        computeDigestOfPagesDescriptor(allPagesDesc, d2);
        CH(d2 == desc.digestOfResPagesDescriptor);
      }

      // check all pages
      for (uint32_t j = 0; j < numberOfReservedPages_; j++) {
        CH(allPagesDesc->d[j].pageId == j);
        CH(allPagesDesc->d[j].relevantCheckpoint <= i);
        CH(allPagesDesc->d[j].relevantCheckpoint > 0);

        uint64_t actualCheckpoint = 0;
        psd_->getResPage(j, i, &actualCheckpoint, buffer_, kSizeOfReservedPage);

        CH(allPagesDesc->d[j].relevantCheckpoint == actualCheckpoint);

        {
          STDigest d3;
          computeDigestOfPage(j, actualCheckpoint, buffer_, d3);

          CH(d3 == allPagesDesc->d[j].pageDigest);
        }
      }

      memset(buffer_, 0, kSizeOfReservedPage);

      psd_->free(allPagesDesc);
    }
  }

  if (!psd_->getIsFetchingState()) {
    CH(!psd_->hasCheckpointBeingFetched());
    CH(psd_->getFirstRequiredBlock() == 0);
    CH(psd_->getLastRequiredBlock() == 0);
  } else if (!psd_->hasCheckpointBeingFetched()) {
    CH(psd_->getFirstRequiredBlock() == 0);
    CH(psd_->getLastRequiredBlock() == 0);

    CH(psd_->numOfAllPendingResPage() == 0);
  } else if (psd_->getLastRequiredBlock() > 0) {
    CH(psd_->getFirstRequiredBlock() > 0);
    CH(psd_->numOfAllPendingResPage() == 0);
  } else {
    CH(psd_->numOfAllPendingResPage() == 0);
  }

  return true;
}

///////////////////////////////////////////////////////////////////////////
// Compute digests
///////////////////////////////////////////////////////////////////////////

void BCStateTran::computeDigestOfPage(const uint32_t pageId,
                                      const uint64_t checkpointNumber,
                                      const char* page,
                                      STDigest& outDigest) {
  DigestContext c;
  c.update(reinterpret_cast<const char*>(&pageId), sizeof(pageId));
  c.update(reinterpret_cast<const char*>(&checkpointNumber),
           sizeof(checkpointNumber));
  if (checkpointNumber > 0) c.update(page, kSizeOfReservedPage);
  c.writeDigest(reinterpret_cast<char*>(&outDigest));
}

void BCStateTran::computeDigestOfPagesDescriptor(
    const DataStore::ResPagesDescriptor* pagesDesc, STDigest& outDigest) {
  DigestContext c;
  c.update(reinterpret_cast<const char*>(pagesDesc), pagesDesc->size());
  c.writeDigest(reinterpret_cast<char*>(&outDigest));
}

void BCStateTran::computeDigestOfBlock(const uint64_t blockNum,
                                       const char* block,
                                       const uint32_t blockSize,
                                       STDigest* outDigest) {
  /*
  // for debug (the digest will be the block number)
  memset(outDigest, 0, sizeof(STDigest));
  uint64_t* p = (uint64_t*)outDigest;
  *p = blockNum;
  */

  Assert(blockNum > 0);
  Assert(blockSize > 0);
  DigestContext c;
  c.update(reinterpret_cast<const char*>(&blockNum), sizeof(blockNum));
  c.update(block, blockSize);
  c.writeDigest(reinterpret_cast<char*>(outDigest));
}

}  // namespace impl
}  // namespace SimpleBlockchainStateTransfer
}  // namespace bftEngine
