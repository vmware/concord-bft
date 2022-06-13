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

#include <string.h>
#include <map>
#include <set>
#include <utility>
#include <future>

#include "assertUtils.hpp"
#include "SimpleStateTransfer.hpp"
#include "SimpleBCStateTransfer.hpp"
#include "memorydb/client.h"
#include "memorydb/key_comparator.h"
#include "Logger.hpp"
#include "storage/direct_kv_key_manipulator.h"
#include "Timers.hpp"

namespace bftEngine {

namespace SimpleInMemoryStateTransfer {

namespace impl {

logging::Logger STLogger = logging::getLogger("state-transfer");

class SimpleStateTran : public ISimpleInMemoryStateTransfer {
 public:
  SimpleStateTran(
      char* ptrToState, uint32_t sizeOfState, uint16_t myReplicaId, uint16_t fVal, uint16_t cVal, bool pedanticChecks);

  virtual ~SimpleStateTran();

  //////////////////////////////////////////////////////////////////////////
  // IStateTransfer methods
  //////////////////////////////////////////////////////////////////////////

  void init(uint64_t maxNumOfRequiredStoredCheckpoints,
            uint32_t numberOfRequiredReservedPages,
            uint32_t sizeOfReservedPage) override;

  void startRunning(IReplicaForStateTransfer* r) override;

  void stopRunning() override;

  bool isRunning() const override;

  void createCheckpointOfCurrentState(uint64_t checkpointNumber) override;

  void getDigestOfCheckpoint(uint64_t checkpointNumber,
                             uint16_t sizeOfDigestBuffer,
                             uint64_t& outBlockId,
                             char* outStateDigest,
                             char* outResPagesDigest,
                             char* outRBVDataDigest) override;
  void startCollectingState() override;

  bool isCollectingState() const override;

  uint32_t numberOfReservedPages() const override;

  uint32_t sizeOfReservedPage() const override;

  bool loadReservedPage(uint32_t reservedPageId, uint32_t copyLength, char* outReservedPage) const override;

  void saveReservedPage(uint32_t reservedPageId, uint32_t copyLength, const char* inReservedPage) override;

  void zeroReservedPage(uint32_t reservedPageId) override;

  void onTimer() override;

  void handleStateTransferMessage(char* msg, uint32_t msgLen, uint16_t senderId) override;

  void addOnTransferringCompleteCallback(const std::function<void(uint64_t)>& cb,
                                         StateTransferCallBacksPriorities priority) override {}

  void addOnFetchingStateChangeCallback(const std::function<void(uint64_t)>& cb) override {}

  //////////////////////////////////////////////////////////////////////////
  // ISimpleInMemoryStateTransfer methods
  //////////////////////////////////////////////////////////////////////////

  void markUpdate(void* ptrToUpdatedRegion, uint32_t sizeOfUpdatedRegion) override;
  void setEraseMetadataFlag() override {}

  void setReconfigurationEngine(
      std::shared_ptr<concord::client::reconfiguration::ClientReconfigurationEngine>) override {}

  virtual void handleIncomingConsensusMessage(const ConsensusMsg msg) override{};
  void reportLastAgreedPrunableBlockId(uint64_t lastAgreedPrunableBlockId) override{};

 protected:
  //////////////////////////////////////////////////////////////////////////
  // DummyBDState
  //////////////////////////////////////////////////////////////////////////

  class DummyBDState : public bcst::IAppState {
   public:
    ////////////////////////////////////////////////////////////////////////
    // IAppState methods
    ////////////////////////////////////////////////////////////////////////

    bool hasBlock(uint64_t blockId) const override;

    bool getBlock(uint64_t blockId,
                  char* outBlock,
                  uint32_t outBlockMaxSize,
                  uint32_t* outBlockActualSize) const override;

    std::future<bool> getBlockAsync(uint64_t blockId,
                                    char* outBlock,
                                    uint32_t outBlockMaxSize,
                                    uint32_t* outBlockActualSize) override;

    bool getPrevDigestFromBlock(uint64_t blockId, bcst::StateTransferDigest* outPrevBlockDigest) const override;

    void getPrevDigestFromBlock(const char* blockData,
                                const uint32_t blockSize,
                                bcst::StateTransferDigest* outPrevBlockDigest) const override;

    bool putBlock(const uint64_t blockId, const char* block, const uint32_t blockSize, bool lastBlock = true) override;

    std::future<bool> putBlockAsync(uint64_t blockId,
                                    const char* block,
                                    const uint32_t blockSize,
                                    bool lastBlock) override;

    uint64_t getLastReachableBlockNum() const override;
    uint64_t getGenesisBlockNum() const override;
    uint64_t getLastBlockNum() const override;
    size_t postProcessUntilBlockId(uint64_t maxBlockId) override;
  };

  ///////////////////////////////////////////////////////////////////////////
  // ReplicaWrapper
  ///////////////////////////////////////////////////////////////////////////

  struct ReplicaWrapper : public IReplicaForStateTransfer {
   public:
    SimpleStateTran* stObject = nullptr;

    IReplicaForStateTransfer* realInterface_ = nullptr;

    ////////////////////////////////////////////////////////////////////////
    // IReplicaForStateTransfer methods
    ////////////////////////////////////////////////////////////////////////

    void onTransferringComplete(uint64_t checkpointNumberOfNewState) override {
      stObject->onComplete(checkpointNumberOfNewState);

      realInterface_->onTransferringComplete(checkpointNumberOfNewState);
    }

    void freeStateTransferMsg(char* m) override { realInterface_->freeStateTransferMsg(m); }

    void sendStateTransferMessage(char* m, uint32_t size, uint16_t replicaId) override {
      realInterface_->sendStateTransferMessage(m, size, replicaId);
    }

    void changeStateTransferTimerPeriod(uint32_t timerPeriodMilli) override {
      realInterface_->changeStateTransferTimerPeriod(timerPeriodMilli);
    }

    concordUtil::Timers::Handle addOneShotTimer(uint32_t timeoutMilli) override {
      return realInterface_->addOneShotTimer(timeoutMilli);
    }
  };

  friend struct ReplicaWrapper;

  /////////////////////////////////////////////////////////////////////////
  // Data members and types
  /////////////////////////////////////////////////////////////////////////

#pragma pack(push, 1)
  struct MetadataDesc {
    uint64_t checkpointNumberOfLastUpdate;
  };
  static_assert(sizeof(MetadataDesc) == 8, "sizeof(MetadataDesc) != 8 bytes");
#pragma pack(pop)

  // external application state
  char* const ptrToState_;
  const uint32_t sizeOfState_;

  // internal objects
  DummyBDState dummyBDState_;
  ReplicaWrapper replicaWrapper_;

  // the BC state transfer
  IStateTransfer* internalST_;

  // temporary buffer

  char* tempBuffer_ = nullptr;

  // information about pages

  uint32_t pageSize_ = 0;  // size of all pages

  // size of last application page
  // (used to make sure we don't write to
  // locations >= ptrToState_+sizeOfState_)
  uint32_t lastAppPageSize_ = 0;

  uint32_t numberOfResPages_ = 0;
  uint32_t numberOfAppPages_ = 0;
  uint32_t numberOfMetadataPages_ = 0;

  // information about updates

  std::set<uint32_t> updateAppPages_;

  // information about checkpoints

  uint64_t lastKnownCheckpoint = 0;

  ///////////////////////////////////////////////////////////////////////////
  // Internal methods
  ///////////////////////////////////////////////////////////////////////////

  bool isInitialized() const { return (pageSize_ != 0); }

  uint32_t totalNumberOfPages() const { return numberOfResPages_ + numberOfAppPages_ + numberOfMetadataPages_; }

  uint32_t resPageToInternalPage(uint32_t x) const {
    ConcordAssert(x < numberOfResPages_);
    return x;
  }

  uint32_t appPageToInternalPage(uint32_t x) const {
    ConcordAssert(x < numberOfAppPages_);
    return (x + numberOfResPages_);
  }

  uint32_t medataPageToInternalPage(uint32_t x) const {
    ConcordAssert(x < numberOfMetadataPages_);
    return (x + numberOfResPages_ + numberOfAppPages_);
  }

  uint32_t internalPageToResPage(uint32_t x) const {
    uint32_t y = x;
    ConcordAssert(y < numberOfResPages_);
    return y;
  }

  uint32_t internalPageToAppPage(uint32_t x) const {
    uint32_t y = (x - numberOfResPages_);
    ConcordAssert(y < numberOfAppPages_);
    return y;
  }

  uint32_t internalPageToMetadataPage(uint32_t x) const {
    uint32_t y = (x - numberOfResPages_ - numberOfAppPages_);
    ConcordAssert(y < numberOfMetadataPages_);
    return y;
  }

  uint32_t findMetadataPageOfAppPage(uint32_t appPage) const {
    ConcordAssert(appPage < numberOfAppPages_);

    const uint32_t numberOfMetadataDescInPage = pageSize_ / sizeof(MetadataDesc);

    const uint32_t mPage = appPage / numberOfMetadataDescInPage;

    ConcordAssert(mPage < numberOfMetadataPages_);

    return mPage;
  }

  uint32_t findIndexInMetadataPage(uint32_t appPage) const {
    ConcordAssert(appPage < numberOfAppPages_);

    const uint32_t numberOfMetadataDescInPage = pageSize_ / sizeof(MetadataDesc);

    uint32_t idx = appPage % numberOfMetadataDescInPage;

    return idx;
  }

  void onComplete(int64_t checkpointNumberOfNewState);
};

}  // namespace impl

ISimpleInMemoryStateTransfer* create(
    void* ptrToState, uint32_t sizeOfState, uint16_t myReplicaId, uint16_t fVal, uint16_t cVal, bool pedanticChecks) {
  // TODO(GG): check arguments

  ISimpleInMemoryStateTransfer* retVal = new impl::SimpleStateTran(
      reinterpret_cast<char*>(ptrToState), sizeOfState, myReplicaId, fVal, cVal, pedanticChecks);

  return retVal;
}

namespace impl {
SimpleStateTran::SimpleStateTran(
    char* ptrToState, uint32_t sizeOfState, uint16_t myReplicaId, uint16_t fVal, uint16_t cVal, bool pedanticChecks)
    : ptrToState_{ptrToState}, sizeOfState_{sizeOfState} {
  bcst::Config config{
      myReplicaId,                          // myReplicaId
      fVal,                                 // fVal
      cVal,                                 // cVal
      (uint16_t)(3 * fVal + 2 * cVal + 1),  // numReplicas
      0,                                    // numRoReplicas
      pedanticChecks,                       // pedanticChecks
      false,                                // isReadOnly
      128,                                  // maxChunkSize
      256,                                  // maxNumberOfChunksInBatch
      1024,                                 // maxBlockSize
      256 * 1024 * 1024,                    // maxPendingDataFromSourceReplica
      2048,                                 // maxNumOfReservedPages
      4096,                                 // sizeOfReservedPage
      600,                                  // gettingMissingBlocksSummaryWindowSize
      10,                                   // minPrePrepareMsgsForPrimaryAwareness
      256,                                  // fetchRangeSize
      1024,                                 // RVT_K
      300,                                  // refreshTimerMs
      2500,                                 // checkpointSummariesRetransmissionTimeoutMs
      60000,                                // maxAcceptableMsgDelayMs
      0,                                    // sourceReplicaReplacementTimeoutMs
      2000,                                 // fetchRetransmissionTimeoutMs
      2,                                    // maxFetchRetransmissions
      5,                                    // metricsDumpIntervalSec
      5000,                                 // maxTimeSinceLastExecutionInMainWindowMs
      5000,                                 // sourceSessionExpiryDurationMs
      300,                                  // sourcePerformanceSnapshotFrequencySec
      true,                                 // runInSeparateThread
      true,                                 // enableReservedPages
      true,                                 // enableSourceBlocksPreFetch
      true,                                 // enableSourceSelectorPrimaryAwareness
      true                                  // enableStoreRvbDataDuringCheckpointing
  };

  auto comparator = concord::storage::memorydb::KeyComparator();
  concord::storage::IDBClient::ptr db(new concord::storage::memorydb::Client(comparator));
  internalST_ = bcst::create(
      config, &dummyBDState_, db, std::make_shared<concord::storage::v1DirectKeyValue::STKeyManipulator>());

  ConcordAssert(internalST_ != nullptr);

  replicaWrapper_.stObject = this;
}

SimpleStateTran::~SimpleStateTran() {
  ConcordAssert(!internalST_->isRunning());

  delete internalST_;

  if (tempBuffer_ != nullptr) std::free(tempBuffer_);
}

void SimpleStateTran::init(uint64_t maxNumOfRequiredStoredCheckpoints,
                           uint32_t numberOfRequiredReservedPages,
                           uint32_t sizeOfReservedPage) {
  ConcordAssert(maxNumOfRequiredStoredCheckpoints >= 2);
  ConcordAssert(numberOfRequiredReservedPages > 0);
  ConcordAssert(sizeOfReservedPage > sizeof(MetadataDesc));

  ConcordAssert(!isInitialized());

  numberOfResPages_ = numberOfRequiredReservedPages;

  pageSize_ = sizeOfReservedPage;

  ConcordAssert(isInitialized());

  // allocate temporary buffer

  tempBuffer_ = reinterpret_cast<char*>(std::malloc(pageSize_));

  memset(tempBuffer_, 0, pageSize_);

  // calculate number of app pages + size of the last application page

  numberOfAppPages_ = sizeOfState_ / pageSize_;
  lastAppPageSize_ = pageSize_;
  if (sizeOfState_ % pageSize_ != 0) {
    numberOfAppPages_++;
    lastAppPageSize_ = sizeOfState_ % pageSize_;
  }
  ConcordAssert(numberOfAppPages_ > 0);

  // calculate number of metadata pages

  const uint32_t numberOfMetadataDescInPage = pageSize_ / sizeof(MetadataDesc);

  ConcordAssert(numberOfMetadataDescInPage > 0);

  numberOfMetadataPages_ = ((numberOfAppPages_ + numberOfMetadataDescInPage - 1) / numberOfMetadataDescInPage);

  ConcordAssert(numberOfMetadataPages_ > 0);

  ConcordAssert(findMetadataPageOfAppPage(0) == 0);

  ConcordAssert(findMetadataPageOfAppPage(numberOfAppPages_ - 1) == (numberOfMetadataPages_ - 1));

  // init internalST_

  internalST_->init(maxNumOfRequiredStoredCheckpoints, totalNumberOfPages(), pageSize_);

  ConcordAssert(numberOfResPages_ ==
                (internalST_->numberOfReservedPages() - numberOfAppPages_ - numberOfMetadataPages_));

  lastKnownCheckpoint = 0;

  // init the metadata pages

  for (uint32_t i = 0; i < numberOfMetadataPages_; i++) {
    uint32_t p = medataPageToInternalPage(i);
    internalST_->zeroReservedPage(p);
  }

  // NB: we don't have to init the application pages
  // (because the initial application pages should not be transferred)
}

void SimpleStateTran::startRunning(IReplicaForStateTransfer* r) {
  ConcordAssert(isInitialized());
  //  ConcordAssert(!internalST_->isRunning());
  //  ConcordAssert(updateAppPages_.empty());

  //  ConcordAssert(replicaWrapper_.realInterface_ == nullptr);
  replicaWrapper_.realInterface_ = r;

  internalST_->startRunning(&replicaWrapper_);
}

void SimpleStateTran::stopRunning() {
  ConcordAssert(isInitialized());
  ConcordAssert(internalST_->isRunning());

  internalST_->stopRunning();

  replicaWrapper_.realInterface_ = nullptr;
}

bool SimpleStateTran::isRunning() const { return internalST_->isRunning(); }

void SimpleStateTran::createCheckpointOfCurrentState(uint64_t checkpointNumber) {
  ConcordAssert(isInitialized());
  ConcordAssert(internalST_->isRunning());
  ConcordAssert(checkpointNumber > lastKnownCheckpoint);

  lastKnownCheckpoint = checkpointNumber;

  //  map from a metadata page to its set of updated app pages
  std::map<uint32_t, std::set<uint32_t> > pagesMap;

  for (uint32_t appPage : updateAppPages_) {
    uint32_t mPage = findMetadataPageOfAppPage(appPage);

    auto loc = pagesMap.find(mPage);

    if (loc == pagesMap.end()) {
      std::set<uint32_t> s;
      s.insert(appPage);
      pagesMap.insert({mPage, s});
    } else {
      loc->second.insert(appPage);
    }
  }

  for (const auto& g : pagesMap) {
    ConcordAssert(g.first < numberOfMetadataPages_);

    internalST_->loadReservedPage(medataPageToInternalPage(g.first), pageSize_, tempBuffer_);

    MetadataDesc* descsArray = reinterpret_cast<MetadataDesc*>(tempBuffer_);

    for (uint32_t appPage : g.second) {
      ConcordAssert(appPage < numberOfAppPages_);
      ConcordAssert(findMetadataPageOfAppPage(appPage) == g.first);

      const bool isLastPage = (appPage == numberOfAppPages_ - 1);
      const uint32_t copyLength = isLastPage ? lastAppPageSize_ : pageSize_;

      char* beginPage = (reinterpret_cast<char*>(ptrToState_)) + (appPage * pageSize_);

      internalST_->saveReservedPage(appPageToInternalPage(appPage), copyLength, beginPage);

      uint32_t mIndex = findIndexInMetadataPage(appPage);

      ConcordAssert(descsArray[mIndex].checkpointNumberOfLastUpdate < checkpointNumber);

      descsArray[mIndex].checkpointNumberOfLastUpdate = checkpointNumber;
    }

    internalST_->saveReservedPage(medataPageToInternalPage(g.first), pageSize_, tempBuffer_);
  }

  updateAppPages_.clear();

  memset(tempBuffer_, 0, pageSize_);

  internalST_->createCheckpointOfCurrentState(checkpointNumber);
}

void SimpleStateTran::getDigestOfCheckpoint(uint64_t checkpointNumber,
                                            uint16_t sizeOfDigestBuffer,
                                            uint64_t& outBlockId,
                                            char* outStateDigest,
                                            char* outResPagesDigest,
                                            char* outRVBDataDigest) {
  ConcordAssert(isInitialized());
  ConcordAssert(internalST_->isRunning());
  ConcordAssert(checkpointNumber <= lastKnownCheckpoint);

  internalST_->getDigestOfCheckpoint(
      checkpointNumber, sizeOfDigestBuffer, outBlockId, outStateDigest, outResPagesDigest, outRVBDataDigest);
}

void SimpleStateTran::startCollectingState() {
  ConcordAssert(isInitialized());
  ConcordAssert(internalST_->isRunning());

  updateAppPages_.clear();

  internalST_->startCollectingState();
}

bool SimpleStateTran::isCollectingState() const {
  ConcordAssert(isInitialized());
  ConcordAssert(internalST_->isRunning());

  return internalST_->isCollectingState();
}

uint32_t SimpleStateTran::numberOfReservedPages() const {
  ConcordAssert(isInitialized());

  return numberOfResPages_;
}

uint32_t SimpleStateTran::sizeOfReservedPage() const {
  ConcordAssert(isInitialized());

  ConcordAssert(pageSize_ == internalST_->sizeOfReservedPage());

  return pageSize_;
}

bool SimpleStateTran::loadReservedPage(uint32_t reservedPageId, uint32_t copyLength, char* outReservedPage) const {
  ConcordAssert(isInitialized());
  ConcordAssert(reservedPageId < numberOfResPages_);

  return internalST_->loadReservedPage(reservedPageId, copyLength, outReservedPage);
}

void SimpleStateTran::saveReservedPage(uint32_t reservedPageId, uint32_t copyLength, const char* inReservedPage) {
  ConcordAssert(isInitialized());
  ConcordAssert(reservedPageId < numberOfResPages_);

  internalST_->saveReservedPage(reservedPageId, copyLength, inReservedPage);
}

void SimpleStateTran::zeroReservedPage(uint32_t reservedPageId) {
  ConcordAssert(isInitialized());
  ConcordAssert(reservedPageId < numberOfResPages_);

  internalST_->zeroReservedPage(reservedPageId);
}

void SimpleStateTran::onTimer() {
  ConcordAssert(isInitialized());

  internalST_->onTimer();
}

void SimpleStateTran::handleStateTransferMessage(char* msg, uint32_t msgLen, uint16_t senderId) {
  ConcordAssert(isInitialized());
  ConcordAssert(internalST_->isRunning());

  internalST_->handleStateTransferMessage(msg, msgLen, senderId);
}

void SimpleStateTran::markUpdate(void* ptrToUpdatedRegion, uint32_t sizeOfUpdatedRegion) {
  ConcordAssert(isInitialized());
  ConcordAssert(internalST_->isRunning());
  ConcordAssert((reinterpret_cast<char*>(ptrToUpdatedRegion)) >= ptrToState_);

  size_t startLocation = ((reinterpret_cast<char*>(ptrToUpdatedRegion)) - ptrToState_);

  ConcordAssert(startLocation < ((size_t)sizeOfState_));

  uint32_t p = (((uint32_t)startLocation) / pageSize_);

  updateAppPages_.insert(p);
}

void SimpleStateTran::onComplete(int64_t checkpointNumberOfNewState) {
  ConcordAssert((uint64_t)checkpointNumberOfNewState > lastKnownCheckpoint);
  ConcordAssert(updateAppPages_.empty());

  for (uint32_t mPage = 0; mPage < numberOfMetadataPages_; mPage++) {
    internalST_->loadReservedPage(medataPageToInternalPage(mPage), pageSize_, tempBuffer_);

    MetadataDesc* descsArray = reinterpret_cast<MetadataDesc*>(tempBuffer_);

    const uint32_t numberOfMetadataDescInPage = pageSize_ / sizeof(MetadataDesc);

    for (uint32_t i = 0; i < numberOfMetadataDescInPage; i++) {
      if (descsArray[i].checkpointNumberOfLastUpdate > lastKnownCheckpoint) {
        uint32_t appPage = (mPage * numberOfMetadataDescInPage) + i;

        ConcordAssert(appPage < numberOfAppPages_);

        char* pagePtr = ptrToState_ + appPage * pageSize_;

        uint32_t size = (appPage == (numberOfAppPages_ - 1)) ? lastAppPageSize_ : pageSize_;

        internalST_->loadReservedPage(appPageToInternalPage(appPage), size, pagePtr);
      }
    }
  }

  lastKnownCheckpoint = checkpointNumberOfNewState;
}

bool SimpleStateTran::DummyBDState::hasBlock(uint64_t blockId) const { return false; }

bool SimpleStateTran::DummyBDState::getBlock(uint64_t blockId,
                                             char* outBlock,
                                             uint32_t outBlockMaxSize,
                                             uint32_t* outBlockActualSize) const {
  ConcordAssert(false);
  return false;
}

std::future<bool> SimpleStateTran::DummyBDState::getBlockAsync(uint64_t blockId,
                                                               char* outBlock,
                                                               uint32_t outBlockMaxSize,
                                                               uint32_t* outBlockActualSize) {
  ConcordAssert(false);
  return std::async([]() { return false; });
}

bool SimpleStateTran::DummyBDState::getPrevDigestFromBlock(uint64_t blockId,
                                                           bcst::StateTransferDigest* outPrevBlockDigest) const {
  ConcordAssert(false);
  return false;
}

void SimpleStateTran::DummyBDState::getPrevDigestFromBlock(const char* blockData,
                                                           const uint32_t blockSize,
                                                           bcst::StateTransferDigest* outPrevBlockDigest) const {
  ConcordAssert(false);
}

bool SimpleStateTran::DummyBDState::putBlock(const uint64_t blockId,
                                             const char* block,
                                             const uint32_t blockSize,
                                             bool lastBlock) {
  ConcordAssert(false);
  return false;
}

std::future<bool> SimpleStateTran::DummyBDState::putBlockAsync(uint64_t blockId,
                                                               const char* block,
                                                               const uint32_t blockSize,
                                                               bool lastBlock) {
  ConcordAssert(false);
  return std::async([]() { return false; });
}

uint64_t SimpleStateTran::DummyBDState::getLastReachableBlockNum() const { return 0; }

uint64_t SimpleStateTran::DummyBDState::getGenesisBlockNum() const { return 0; }

uint64_t SimpleStateTran::DummyBDState::getLastBlockNum() const { return 0; }

size_t SimpleStateTran::DummyBDState::postProcessUntilBlockId(uint64_t blockId) { return 0; }

}  // namespace impl
}  // namespace SimpleInMemoryStateTransfer
}  // namespace bftEngine
