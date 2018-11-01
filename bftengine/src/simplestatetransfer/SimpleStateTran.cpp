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


namespace bftEngine {

namespace SimpleInMemoryStateTransfer {

namespace impl {

class SimpleStateTran : public ISimpleInMemoryStateTransfer {
 public:
  SimpleStateTran(char* ptrToState, uint32_t sizeOfState,
    uint16_t myReplicaId, uint16_t fVal,
    uint16_t cVal, bool pedanticChecks);

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

  void createCheckpointOfCurrentState(
    uint64_t checkpointNumber) override;

  void markCheckpointAsStable(
    uint64_t checkpointNumber) override;

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

    bool getPrevDigestFromBlock(uint64_t blockId,
      SimpleBlockchainStateTransfer::StateTransferDigest* outPrevBlockDigest)
      override;

    bool putBlock(uint64_t blockId,
      char* block,
      uint32_t blockSize) override;

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

    void onTransferringComplete(
      int64_t checkpointNumberOfNewState) override {
      stObject->onComplete(checkpointNumberOfNewState);

      realInterface_->onTransferringComplete(checkpointNumberOfNewState);
    }


    void freeStateTransferMsg(char *m) override {
      realInterface_->freeStateTransferMsg(m);
    }

    void sendStateTransferMessage(char *m,
      uint32_t size,
      uint16_t replicaId) override  {
      realInterface_->sendStateTransferMessage(m, size, replicaId);
    }

    void changeStateTransferTimerPeriod(
      uint32_t timerPeriodMilli) override {
      realInterface_->changeStateTransferTimerPeriod(timerPeriodMilli);
    }
  };

  friend struct ReplicaWrapper;

  /////////////////////////////////////////////////////////////////////////
  // Data members and types
  /////////////////////////////////////////////////////////////////////////

  struct MetadataDesc {
    uint64_t checkpointNumberOfLastUpdate;
  };

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

  bool isInitialized() const {
    return (pageSize_ != 0);
  }

  uint32_t totalNumberOfPages() const {
    return numberOfResPages_ + numberOfAppPages_ + numberOfMetadataPages_;
  }

  uint32_t resPageToInternalPage(uint32_t x) const {
    assert(x < numberOfResPages_);
    return x;
  }

  uint32_t appPageToInternalPage(uint32_t x) const {
    assert(x < numberOfAppPages_);
    return (x + numberOfResPages_);
  }

  uint32_t medataPageToInternalPage(uint32_t x) const {
    assert(x < numberOfMetadataPages_);
    return (x + numberOfResPages_ + numberOfAppPages_);
  }

  uint32_t internalPageToResPage(uint32_t x) const {
    uint32_t y = x;
    assert(y < numberOfResPages_);
    return y;
  }

  uint32_t internalPageToAppPage(uint32_t x) const {
    uint32_t y = (x - numberOfResPages_);
    assert(y < numberOfAppPages_);
    return y;
  }

  uint32_t internalPageToMetadataPage(uint32_t x) const {
    uint32_t y = (x - numberOfResPages_ - numberOfAppPages_);
    assert(y < numberOfMetadataPages_);
    return y;
  }

  uint32_t findMetadataPageOfAppPage(uint32_t appPage) const {
    assert(appPage < numberOfAppPages_);

    const uint32_t numberOfMetadataDescInPage =
      pageSize_ / sizeof(MetadataDesc);

    const uint32_t mPage = appPage / numberOfMetadataDescInPage;

    assert(mPage < numberOfMetadataPages_);

    return mPage;
  }

  uint32_t findIndexInMetadataPage(uint32_t appPage) const {
    assert(appPage < numberOfAppPages_);

    const uint32_t numberOfMetadataDescInPage =
      pageSize_ / sizeof(MetadataDesc);

    uint32_t idx = appPage % numberOfMetadataDescInPage;

    return idx;
  }

  void onComplete(int64_t checkpointNumberOfNewState);
};

}  // namespace impl

ISimpleInMemoryStateTransfer* create(
  void* ptrToState, uint32_t sizeOfState,
  uint16_t myReplicaId, uint16_t fVal,
  uint16_t cVal, bool pedanticChecks) {
  // TODO(GG): check arguments

  ISimpleInMemoryStateTransfer* retVal =
    new impl::SimpleStateTran(reinterpret_cast<char*>(ptrToState), sizeOfState,
      myReplicaId, fVal, cVal, pedanticChecks);

  return retVal;
}


namespace impl {
SimpleStateTran::SimpleStateTran(char* ptrToState,
  uint32_t sizeOfState,
  uint16_t myReplicaId,
  uint16_t fVal,
    uint16_t cVal,
    bool pedanticChecks)
  :
  ptrToState_{ ptrToState },
  sizeOfState_{ sizeOfState }
{
  SimpleBlockchainStateTransfer::Config config;

  config.myReplicaId = myReplicaId;
  config.fVal = fVal;
    config.cVal = cVal;
    config.pedanticChecks = pedanticChecks;

  internalST_ =
    SimpleBlockchainStateTransfer::create(config, &dummyBDState_, false);

  assert(internalST_ != nullptr);

  replicaWrapper_.stObject = this;
}

SimpleStateTran::~SimpleStateTran() {
  assert(!internalST_->isRunning());

  delete internalST_;

  if (tempBuffer_ != nullptr) std::free(tempBuffer_);
}

void SimpleStateTran::init(uint64_t maxNumOfRequiredStoredCheckpoints,
  uint32_t numberOfRequiredReservedPages,
  uint32_t sizeOfReservedPage) {
  assert(maxNumOfRequiredStoredCheckpoints >= 2);
  assert(numberOfRequiredReservedPages > 0);
  assert(sizeOfReservedPage > sizeof(MetadataDesc));

  assert(!isInitialized());

  numberOfResPages_ = numberOfRequiredReservedPages;

  pageSize_ = sizeOfReservedPage;

  assert(isInitialized());

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
  assert(numberOfAppPages_ > 0);

  // calculate number of metadata pages

  const uint32_t numberOfMetadataDescInPage =
    pageSize_ / sizeof(MetadataDesc);

  assert(numberOfMetadataDescInPage > 0);

  numberOfMetadataPages_ =
    ((numberOfAppPages_ + numberOfMetadataDescInPage - 1) /
      numberOfMetadataDescInPage);

  assert(numberOfMetadataPages_ > 0);

  assert(findMetadataPageOfAppPage(0) == 0);

  assert(findMetadataPageOfAppPage(numberOfAppPages_ - 1) ==
    (numberOfMetadataPages_ - 1));

  // init internalST_

  internalST_->init(maxNumOfRequiredStoredCheckpoints,
    totalNumberOfPages(),
    pageSize_);

  assert(numberOfResPages_ ==
    (internalST_->numberOfReservedPages() -
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
  assert(isInitialized());
  assert(!internalST_->isRunning());
  assert(updateAppPages_.empty());

  assert(replicaWrapper_.realInterface_ == nullptr);
  replicaWrapper_.realInterface_ = r;

  internalST_->startRunning(&replicaWrapper_);
}

void SimpleStateTran::stopRunning() {
  assert(isInitialized());
  assert(internalST_->isRunning());

  internalST_->stopRunning();

  replicaWrapper_.realInterface_ = nullptr;
}

bool SimpleStateTran::isRunning() const {
  return internalST_->isRunning();
}

void SimpleStateTran::createCheckpointOfCurrentState(
  uint64_t checkpointNumber) {
  assert(isInitialized());
  assert(internalST_->isRunning());
  assert(checkpointNumber > lastKnownCheckpoint);

  lastKnownCheckpoint = checkpointNumber;

  //  map from a metadata page to its set of updated app pages
  std::map<uint32_t, std::set<uint32_t> > pagesMap;

  for (uint32_t appPage : updateAppPages_) {
    uint32_t mPage = findMetadataPageOfAppPage(appPage);

    auto loc = pagesMap.find(mPage);

    if (loc == pagesMap.end()) {
      std::set<uint32_t> s;
      s.insert(appPage);
      pagesMap.insert({ mPage, s });
    } else {
      loc->second.insert(appPage);
    }
  }

  for (std::pair< uint32_t, std::set<uint32_t> > g : pagesMap) {
    assert(g.first < numberOfMetadataPages_);

    internalST_->loadReservedPage(medataPageToInternalPage(g.first),
      pageSize_, tempBuffer_);

    MetadataDesc* descsArray = reinterpret_cast<MetadataDesc*>(tempBuffer_);

    for (uint32_t appPage : g.second) {
      assert(appPage < numberOfAppPages_);
      assert(findMetadataPageOfAppPage(appPage) == g.first);

      const bool isLastPage = (appPage == numberOfAppPages_ - 1);
      const uint32_t copyLength = isLastPage ?
        lastAppPageSize_ : pageSize_;

      char* beginPage =
                (reinterpret_cast<char*>(ptrToState_)) + (appPage * pageSize_);

      internalST_->saveReservedPage(appPageToInternalPage(appPage),
        copyLength, beginPage);

      uint32_t mIndex = findIndexInMetadataPage(appPage);

      assert(descsArray[mIndex].checkpointNumberOfLastUpdate <
        checkpointNumber);

      descsArray[mIndex].checkpointNumberOfLastUpdate = checkpointNumber;
    }

    internalST_->saveReservedPage(medataPageToInternalPage(g.first),
      pageSize_, tempBuffer_);
  }

  updateAppPages_.clear();

  memset(tempBuffer_, 0, pageSize_);

  internalST_->createCheckpointOfCurrentState(checkpointNumber);
}

void SimpleStateTran::markCheckpointAsStable(uint64_t checkpointNumber) {
  assert(isInitialized());
  assert(internalST_->isRunning());

  internalST_->markCheckpointAsStable(checkpointNumber);
}

void SimpleStateTran::getDigestOfCheckpoint(uint64_t checkpointNumber,
  uint16_t sizeOfDigestBuffer,
  char* outDigestBuffer) {
  assert(isInitialized());
  assert(internalST_->isRunning());
  assert(checkpointNumber <= lastKnownCheckpoint);

  internalST_->getDigestOfCheckpoint(checkpointNumber,
    sizeOfDigestBuffer,
    outDigestBuffer);
}

void SimpleStateTran::startCollectingState() {
  assert(isInitialized());
  assert(internalST_->isRunning());

  updateAppPages_.clear();

  internalST_->startCollectingState();
}

bool SimpleStateTran::isCollectingState() const {
  assert(isInitialized());
  assert(internalST_->isRunning());

  return internalST_->isCollectingState();
}

uint32_t SimpleStateTran::numberOfReservedPages() const {
  assert(isInitialized());

  return numberOfResPages_;
}

uint32_t SimpleStateTran::sizeOfReservedPage() const {
  assert(isInitialized());

  assert(pageSize_ == internalST_->sizeOfReservedPage());

  return pageSize_;
}

bool SimpleStateTran::loadReservedPage(uint32_t reservedPageId,
  uint32_t copyLength,
  char* outReservedPage) const {
  assert(isInitialized());
  assert(internalST_->isRunning());
  assert(reservedPageId < numberOfResPages_);

  return internalST_->loadReservedPage(reservedPageId,
    copyLength, outReservedPage);
}

void SimpleStateTran::saveReservedPage(uint32_t reservedPageId,
  uint32_t copyLength,
  const char* inReservedPage) {
  assert(isInitialized());
  assert(reservedPageId < numberOfResPages_);

  internalST_->saveReservedPage(reservedPageId, copyLength, inReservedPage);
}

void SimpleStateTran::zeroReservedPage(uint32_t reservedPageId) {
  assert(isInitialized());
  assert(reservedPageId < numberOfResPages_);

  internalST_->zeroReservedPage(reservedPageId);
}

void SimpleStateTran::onTimer() {
  assert(isInitialized());

  internalST_->onTimer();
}

void SimpleStateTran::handleStateTransferMessage(char* msg, uint32_t msgLen,
  uint16_t senderId) {
  assert(isInitialized());
  assert(internalST_->isRunning());

  internalST_->handleStateTransferMessage(msg, msgLen, senderId);
}

void SimpleStateTran::markUpdate(void* ptrToUpdatedRegion,
  uint32_t sizeOfUpdatedRegion) {
  assert(isInitialized());
  assert(internalST_->isRunning());
  assert((reinterpret_cast<char*>(ptrToUpdatedRegion)) >= ptrToState_);

  size_t startLocation =
                  ((reinterpret_cast<char*>(ptrToUpdatedRegion)) - ptrToState_);

  assert(startLocation < ((size_t)sizeOfState_));

  uint32_t p = (((uint32_t)startLocation) / pageSize_);

  updateAppPages_.insert(p);
}

void SimpleStateTran::onComplete(int64_t checkpointNumberOfNewState) {
  assert((uint64_t)checkpointNumberOfNewState > lastKnownCheckpoint);
  assert(updateAppPages_.empty());

  for (uint32_t mPage = 0; mPage < numberOfMetadataPages_; mPage++) {
    internalST_->loadReservedPage(medataPageToInternalPage(mPage),
      pageSize_, tempBuffer_);

    MetadataDesc* descsArray = reinterpret_cast<MetadataDesc*>(tempBuffer_);

    const uint32_t numberOfMetadataDescInPage =
      pageSize_ / sizeof(MetadataDesc);

    for (uint32_t i = 0; i < numberOfMetadataDescInPage; i++) {
      if (descsArray[i].checkpointNumberOfLastUpdate > lastKnownCheckpoint) {
        uint32_t appPage = (mPage*numberOfMetadataDescInPage) + i;

        assert(appPage < numberOfAppPages_);

        char* pagePtr = ptrToState_ + appPage * pageSize_;

        uint32_t size = (appPage == (numberOfAppPages_ - 1)) ?
          lastAppPageSize_ : pageSize_;

        internalST_->loadReservedPage(appPageToInternalPage(appPage),
          size, pagePtr);
      }
    }
  }

  lastKnownCheckpoint = checkpointNumberOfNewState;
}


bool SimpleStateTran::DummyBDState::hasBlock(uint64_t blockId) {
  return false;
}

bool SimpleStateTran::DummyBDState::getBlock(
  uint64_t blockId, char* outBlock, uint32_t* outBlockSize) {
  assert(false);
  return false;
}

bool SimpleStateTran::DummyBDState::getPrevDigestFromBlock(
  uint64_t blockId,
  SimpleBlockchainStateTransfer::StateTransferDigest* outPrevBlockDigest) {
  assert(false);
  return false;
}

bool SimpleStateTran::DummyBDState::putBlock(
  uint64_t blockId, char* block, uint32_t blockSize) {
  assert(false);
  return false;
}

uint64_t SimpleStateTran::DummyBDState::getLastReachableBlockNum() {
  return 0;
}

uint64_t SimpleStateTran::DummyBDState::getLastBlockNum() {
  return 0;
}

}  // namespace impl
}  // namespace SimpleInMemoryStateTransfer
}  // namespace bftEngine

