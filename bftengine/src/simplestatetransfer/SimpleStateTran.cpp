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
#include <cassert>
#include <map>
#include <set>
#include <utility>

#include "SimpleStateTransfer.hpp"
#include "SimpleBCStateTransfer.hpp"
#include "Logging.hpp"

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

namespace SimpleInMemoryStateTransfer {

namespace impl {

concordlogger::Logger STLogger =
    concordlogger::Logger::getLogger("state-transfer");

class SimpleStateTran : public ISimpleInMemoryStateTransfer {
 public:
  SimpleStateTran(char* ptrToState,
                  uint32_t sizeOfState,
                  uint16_t myReplicaId,
                  uint16_t fVal,
                  uint16_t cVal,
                  bool pedanticChecks);

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

  void markCheckpointAsStable(uint64_t checkpointNumber) override;

  void getDigestOfCheckpoint(uint64_t checkpointNumber,
                             uint16_t sizeOfDigestBuffer,
                             char* outDigestBuffer) override;

  void startCollectingState() override;

  bool isCollectingState() const override;

  uint32_t numberOfReservedPages() const override;

  uint32_t sizeOfReservedPage() const override;

  bool loadReservedPage(uint32_t reservedPageId,
                        uint32_t copyLength,
                        char* outReservedPage) const override;

  void saveReservedPage(uint32_t reservedPageId,
                        uint32_t copyLength,
                        const char* inReservedPage) override;

  void zeroReservedPage(uint32_t reservedPageId) override;

  void onTimer() override;

  void handleStateTransferMessage(char* msg,
                                  uint32_t msgLen,
                                  uint16_t senderId) override;

  //////////////////////////////////////////////////////////////////////////
  // ISimpleInMemoryStateTransfer methods
  //////////////////////////////////////////////////////////////////////////

  void markUpdate(void* ptrToUpdatedRegion,
                  uint32_t sizeOfUpdatedRegion) override;

 protected:
  //////////////////////////////////////////////////////////////////////////
  // DummyBDState
  //////////////////////////////////////////////////////////////////////////

  class DummyBDState : public SimpleBlockchainStateTransfer::IAppState {
   public:
    ////////////////////////////////////////////////////////////////////////
    // IAppState methods
    ////////////////////////////////////////////////////////////////////////

    bool hasBlock(uint64_t blockId) override;

    bool getBlock(uint64_t blockId,
                  char* outBlock,
                  uint32_t* outBlockSize) override;

    bool getPrevDigestFromBlock(
        uint64_t blockId,
        SimpleBlockchainStateTransfer::StateTransferDigest* outPrevBlockDigest)
        override;

    bool putBlock(uint64_t blockId, char* block, uint32_t blockSize) override;

    uint64_t getLastReachableBlockNum() override;

    uint64_t getLastBlockNum() override;
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

    void onTransferringComplete(int64_t checkpointNumberOfNewState) override {
      stObject->onComplete(checkpointNumberOfNewState);

      realInterface_->onTransferringComplete(checkpointNumberOfNewState);
    }

    void freeStateTransferMsg(char* m) override {
      realInterface_->freeStateTransferMsg(m);
    }

    void sendStateTransferMessage(char* m,
                                  uint32_t size,
                                  uint16_t replicaId) override {
      realInterface_->sendStateTransferMessage(m, size, replicaId);
    }

    void changeStateTransferTimerPeriod(uint32_t timerPeriodMilli) override {
      realInterface_->changeStateTransferTimerPeriod(timerPeriodMilli);
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

  uint32_t totalNumberOfPages() const {
    return numberOfResPages_ + numberOfAppPages_ + numberOfMetadataPages_;
  }

  uint32_t resPageToInternalPage(uint32_t x) const {
    Assert(x < numberOfResPages_);
    return x;
  }

  uint32_t appPageToInternalPage(uint32_t x) const {
    Assert(x < numberOfAppPages_);
    return (x + numberOfResPages_);
  }

  uint32_t medataPageToInternalPage(uint32_t x) const {
    Assert(x < numberOfMetadataPages_);
    return (x + numberOfResPages_ + numberOfAppPages_);
  }

  uint32_t internalPageToResPage(uint32_t x) const {
    uint32_t y = x;
    Assert(y < numberOfResPages_);
    return y;
  }

  uint32_t internalPageToAppPage(uint32_t x) const {
    uint32_t y = (x - numberOfResPages_);
    Assert(y < numberOfAppPages_);
    return y;
  }

  uint32_t internalPageToMetadataPage(uint32_t x) const {
    uint32_t y = (x - numberOfResPages_ - numberOfAppPages_);
    Assert(y < numberOfMetadataPages_);
    return y;
  }

  uint32_t findMetadataPageOfAppPage(uint32_t appPage) const {
    Assert(appPage < numberOfAppPages_);

    const uint32_t numberOfMetadataDescInPage =
        pageSize_ / sizeof(MetadataDesc);

    const uint32_t mPage = appPage / numberOfMetadataDescInPage;

    Assert(mPage < numberOfMetadataPages_);

    return mPage;
  }

  uint32_t findIndexInMetadataPage(uint32_t appPage) const {
    Assert(appPage < numberOfAppPages_);

    const uint32_t numberOfMetadataDescInPage =
        pageSize_ / sizeof(MetadataDesc);

    uint32_t idx = appPage % numberOfMetadataDescInPage;

    return idx;
  }

  void onComplete(int64_t checkpointNumberOfNewState);
};

}  // namespace impl

ISimpleInMemoryStateTransfer* create(void* ptrToState,
                                     uint32_t sizeOfState,
                                     uint16_t myReplicaId,
                                     uint16_t fVal,
                                     uint16_t cVal,
                                     bool pedanticChecks) {
  // TODO(GG): check arguments

  ISimpleInMemoryStateTransfer* retVal =
      new impl::SimpleStateTran(reinterpret_cast<char*>(ptrToState),
                                sizeOfState,
                                myReplicaId,
                                fVal,
                                cVal,
                                pedanticChecks);

  return retVal;
}

namespace impl {
SimpleStateTran::SimpleStateTran(char* ptrToState,
                                 uint32_t sizeOfState,
                                 uint16_t myReplicaId,
                                 uint16_t fVal,
                                 uint16_t cVal,
                                 bool pedanticChecks)
    : ptrToState_{ptrToState}, sizeOfState_{sizeOfState} {
  SimpleBlockchainStateTransfer::Config config;

  config.myReplicaId = myReplicaId;
  config.fVal = fVal;
  config.cVal = cVal;
  config.pedanticChecks = pedanticChecks;

  internalST_ =
      SimpleBlockchainStateTransfer::create(config, &dummyBDState_, false);

  Assert(internalST_ != nullptr);

  replicaWrapper_.stObject = this;
}

SimpleStateTran::~SimpleStateTran() {
  Assert(!internalST_->isRunning());

  delete internalST_;

  if (tempBuffer_ != nullptr) std::free(tempBuffer_);
}

void SimpleStateTran::init(uint64_t maxNumOfRequiredStoredCheckpoints,
                           uint32_t numberOfRequiredReservedPages,
                           uint32_t sizeOfReservedPage) {
  Assert(maxNumOfRequiredStoredCheckpoints >= 2);
  Assert(numberOfRequiredReservedPages > 0);
  Assert(sizeOfReservedPage > sizeof(MetadataDesc));

  Assert(!isInitialized());

  numberOfResPages_ = numberOfRequiredReservedPages;

  pageSize_ = sizeOfReservedPage;

  Assert(isInitialized());

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
  Assert(numberOfAppPages_ > 0);

  // calculate number of metadata pages

  const uint32_t numberOfMetadataDescInPage = pageSize_ / sizeof(MetadataDesc);

  Assert(numberOfMetadataDescInPage > 0);

  numberOfMetadataPages_ =
      ((numberOfAppPages_ + numberOfMetadataDescInPage - 1) /
       numberOfMetadataDescInPage);

  Assert(numberOfMetadataPages_ > 0);

  Assert(findMetadataPageOfAppPage(0) == 0);

  Assert(findMetadataPageOfAppPage(numberOfAppPages_ - 1) ==
         (numberOfMetadataPages_ - 1));

  // init internalST_

  internalST_->init(
      maxNumOfRequiredStoredCheckpoints, totalNumberOfPages(), pageSize_);

  Assert(numberOfResPages_ == (internalST_->numberOfReservedPages() -
                               numberOfAppPages_ - numberOfMetadataPages_));

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
  Assert(isInitialized());
  Assert(!internalST_->isRunning());
  Assert(updateAppPages_.empty());

  Assert(replicaWrapper_.realInterface_ == nullptr);
  replicaWrapper_.realInterface_ = r;

  internalST_->startRunning(&replicaWrapper_);
}

void SimpleStateTran::stopRunning() {
  Assert(isInitialized());
  Assert(internalST_->isRunning());

  internalST_->stopRunning();

  replicaWrapper_.realInterface_ = nullptr;
}

bool SimpleStateTran::isRunning() const { return internalST_->isRunning(); }

void SimpleStateTran::createCheckpointOfCurrentState(
    uint64_t checkpointNumber) {
  Assert(isInitialized());
  Assert(internalST_->isRunning());
  Assert(checkpointNumber > lastKnownCheckpoint);

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

  for (std::pair<uint32_t, std::set<uint32_t> > g : pagesMap) {
    Assert(g.first < numberOfMetadataPages_);

    internalST_->loadReservedPage(
        medataPageToInternalPage(g.first), pageSize_, tempBuffer_);

    MetadataDesc* descsArray = reinterpret_cast<MetadataDesc*>(tempBuffer_);

    for (uint32_t appPage : g.second) {
      Assert(appPage < numberOfAppPages_);
      Assert(findMetadataPageOfAppPage(appPage) == g.first);

      const bool isLastPage = (appPage == numberOfAppPages_ - 1);
      const uint32_t copyLength = isLastPage ? lastAppPageSize_ : pageSize_;

      char* beginPage =
          (reinterpret_cast<char*>(ptrToState_)) + (appPage * pageSize_);

      internalST_->saveReservedPage(
          appPageToInternalPage(appPage), copyLength, beginPage);

      uint32_t mIndex = findIndexInMetadataPage(appPage);

      Assert(descsArray[mIndex].checkpointNumberOfLastUpdate <
             checkpointNumber);

      descsArray[mIndex].checkpointNumberOfLastUpdate = checkpointNumber;
    }

    internalST_->saveReservedPage(
        medataPageToInternalPage(g.first), pageSize_, tempBuffer_);
  }

  updateAppPages_.clear();

  memset(tempBuffer_, 0, pageSize_);

  internalST_->createCheckpointOfCurrentState(checkpointNumber);
}

void SimpleStateTran::markCheckpointAsStable(uint64_t checkpointNumber) {
  Assert(isInitialized());
  Assert(internalST_->isRunning());

  internalST_->markCheckpointAsStable(checkpointNumber);
}

void SimpleStateTran::getDigestOfCheckpoint(uint64_t checkpointNumber,
                                            uint16_t sizeOfDigestBuffer,
                                            char* outDigestBuffer) {
  Assert(isInitialized());
  Assert(internalST_->isRunning());
  Assert(checkpointNumber <= lastKnownCheckpoint);

  internalST_->getDigestOfCheckpoint(
      checkpointNumber, sizeOfDigestBuffer, outDigestBuffer);
}

void SimpleStateTran::startCollectingState() {
  Assert(isInitialized());
  Assert(internalST_->isRunning());

  updateAppPages_.clear();

  internalST_->startCollectingState();
}

bool SimpleStateTran::isCollectingState() const {
  Assert(isInitialized());
  Assert(internalST_->isRunning());

  return internalST_->isCollectingState();
}

uint32_t SimpleStateTran::numberOfReservedPages() const {
  Assert(isInitialized());

  return numberOfResPages_;
}

uint32_t SimpleStateTran::sizeOfReservedPage() const {
  Assert(isInitialized());

  Assert(pageSize_ == internalST_->sizeOfReservedPage());

  return pageSize_;
}

bool SimpleStateTran::loadReservedPage(uint32_t reservedPageId,
                                       uint32_t copyLength,
                                       char* outReservedPage) const {
  Assert(isInitialized());
  Assert(internalST_->isRunning());
  Assert(reservedPageId < numberOfResPages_);

  return internalST_->loadReservedPage(
      reservedPageId, copyLength, outReservedPage);
}

void SimpleStateTran::saveReservedPage(uint32_t reservedPageId,
                                       uint32_t copyLength,
                                       const char* inReservedPage) {
  Assert(isInitialized());
  Assert(reservedPageId < numberOfResPages_);

  internalST_->saveReservedPage(reservedPageId, copyLength, inReservedPage);
}

void SimpleStateTran::zeroReservedPage(uint32_t reservedPageId) {
  Assert(isInitialized());
  Assert(reservedPageId < numberOfResPages_);

  internalST_->zeroReservedPage(reservedPageId);
}

void SimpleStateTran::onTimer() {
  Assert(isInitialized());

  internalST_->onTimer();
}

void SimpleStateTran::handleStateTransferMessage(char* msg,
                                                 uint32_t msgLen,
                                                 uint16_t senderId) {
  Assert(isInitialized());
  Assert(internalST_->isRunning());

  internalST_->handleStateTransferMessage(msg, msgLen, senderId);
}

void SimpleStateTran::markUpdate(void* ptrToUpdatedRegion,
                                 uint32_t sizeOfUpdatedRegion) {
  Assert(isInitialized());
  Assert(internalST_->isRunning());
  Assert((reinterpret_cast<char*>(ptrToUpdatedRegion)) >= ptrToState_);

  size_t startLocation =
      ((reinterpret_cast<char*>(ptrToUpdatedRegion)) - ptrToState_);

  Assert(startLocation < ((size_t)sizeOfState_));

  uint32_t p = (((uint32_t)startLocation) / pageSize_);

  updateAppPages_.insert(p);
}

void SimpleStateTran::onComplete(int64_t checkpointNumberOfNewState) {
  Assert((uint64_t)checkpointNumberOfNewState > lastKnownCheckpoint);
  Assert(updateAppPages_.empty());

  for (uint32_t mPage = 0; mPage < numberOfMetadataPages_; mPage++) {
    internalST_->loadReservedPage(
        medataPageToInternalPage(mPage), pageSize_, tempBuffer_);

    MetadataDesc* descsArray = reinterpret_cast<MetadataDesc*>(tempBuffer_);

    const uint32_t numberOfMetadataDescInPage =
        pageSize_ / sizeof(MetadataDesc);

    for (uint32_t i = 0; i < numberOfMetadataDescInPage; i++) {
      if (descsArray[i].checkpointNumberOfLastUpdate > lastKnownCheckpoint) {
        uint32_t appPage = (mPage * numberOfMetadataDescInPage) + i;

        Assert(appPage < numberOfAppPages_);

        char* pagePtr = ptrToState_ + appPage * pageSize_;

        uint32_t size =
            (appPage == (numberOfAppPages_ - 1)) ? lastAppPageSize_ : pageSize_;

        internalST_->loadReservedPage(
            appPageToInternalPage(appPage), size, pagePtr);
      }
    }
  }

  lastKnownCheckpoint = checkpointNumberOfNewState;
}

bool SimpleStateTran::DummyBDState::hasBlock(uint64_t blockId) { return false; }

bool SimpleStateTran::DummyBDState::getBlock(uint64_t blockId,
                                             char* outBlock,
                                             uint32_t* outBlockSize) {
  Assert(false);
  return false;
}

bool SimpleStateTran::DummyBDState::getPrevDigestFromBlock(
    uint64_t blockId,
    SimpleBlockchainStateTransfer::StateTransferDigest* outPrevBlockDigest) {
  Assert(false);
  return false;
}

bool SimpleStateTran::DummyBDState::putBlock(uint64_t blockId,
                                             char* block,
                                             uint32_t blockSize) {
  Assert(false);
  return false;
}

uint64_t SimpleStateTran::DummyBDState::getLastReachableBlockNum() { return 0; }

uint64_t SimpleStateTran::DummyBDState::getLastBlockNum() { return 0; }

}  // namespace impl
}  // namespace SimpleInMemoryStateTransfer
}  // namespace bftEngine
