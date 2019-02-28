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

#include <set>

#include "InMemoryDataStore.hpp"

namespace bftEngine {
namespace SimpleBlockchainStateTransfer {
namespace impl {

InMemoryDataStore::InMemoryDataStore(uint32_t sizeOfReservedPage)
    : sizeOfReservedPage_(sizeOfReservedPage) {
  checkpointBeingFetched.checkpointNum = 0;
}

bool InMemoryDataStore::initialized() { return wasInit_; }

void InMemoryDataStore::setAsInitialized() { wasInit_ = true; }

void InMemoryDataStore::setReplicas(const set<uint16_t> replicas) {
  assert(replicas_.empty());
  replicas_ = replicas;
}

set<uint16_t> InMemoryDataStore::getReplicas() {
  assert(!replicas_.empty());
  return replicas_;
}

void InMemoryDataStore::setMyReplicaId(uint16_t id) {
  assert(myReplicaId_ == UINT16_MAX);
  myReplicaId_ = id;
}

uint16_t InMemoryDataStore::getMyReplicaId() {
  assert(myReplicaId_ != UINT16_MAX);
  return myReplicaId_;
}

void InMemoryDataStore::setFVal(uint16_t fVal) {
  assert(fVal_ == UINT16_MAX);
  fVal_ = fVal;
}

uint16_t InMemoryDataStore::getFVal() {
  assert(fVal_ != UINT16_MAX);
  return fVal_;
}

void InMemoryDataStore::setMaxNumOfStoredCheckpoints(uint64_t numChecks) {
  assert(numChecks > 0);

  assert(maxNumOfStoredCheckpoints_ == UINT64_MAX);
  maxNumOfStoredCheckpoints_ = numChecks;
}

uint64_t InMemoryDataStore::getMaxNumOfStoredCheckpoints() {
  assert(maxNumOfStoredCheckpoints_ != UINT64_MAX);
  return maxNumOfStoredCheckpoints_;
}

void InMemoryDataStore::setNumberOfReservedPages(uint32_t numResPages) {
  assert(numResPages > 0);

  assert(numberOfReservedPages_ == UINT32_MAX);
  numberOfReservedPages_ = numResPages;
}

uint32_t InMemoryDataStore::getNumberOfReservedPages() {
  assert(numberOfReservedPages_ != UINT32_MAX);
  return numberOfReservedPages_;
}

void InMemoryDataStore::setLastStoredCheckpoint(uint64_t c) {
  lastStoredCheckpoint = c;
}

uint64_t InMemoryDataStore::getLastStoredCheckpoint() {
  return lastStoredCheckpoint;
}

void InMemoryDataStore::setFirstStoredCheckpoint(uint64_t c) {
  firstStoredCheckpoint = c;
}

uint64_t InMemoryDataStore::getFirstStoredCheckpoint() {
  return firstStoredCheckpoint;
}

void InMemoryDataStore::setCheckpointDesc(uint64_t checkpoint,
                                          const CheckpointDesc& desc) {
  assert(checkpoint == desc.checkpointNum);
  assert(descMap.count(checkpoint) == 0);

  descMap[checkpoint] = desc;

  //  assert(descMap.size() < 21);  // TODO(GG): delete - debug only
}

DataStore::CheckpointDesc InMemoryDataStore::getCheckpointDesc(
    uint64_t checkpoint) {
  auto p = descMap.find(checkpoint);

  assert(p != descMap.end());

  assert(p->first == p->second.checkpointNum);

  return p->second;
}

bool InMemoryDataStore::hasCheckpointDesc(uint64_t checkpoint) {
  auto p = descMap.find(checkpoint);

  return (p != descMap.end());
}

void InMemoryDataStore::deleteDescOfSmallerCheckpoints(uint64_t checkpoint) {
  auto p = descMap.begin();
  while ((p != descMap.end()) && (p->first < checkpoint)) {
    assert(p->first == p->second.checkpointNum);
    p = descMap.erase(p);
  }
}

void InMemoryDataStore::setIsFetchingState(bool b) { fetching = b; }

bool InMemoryDataStore::getIsFetchingState() { return fetching; }

void InMemoryDataStore::setCheckpointBeingFetched(const CheckpointDesc& c) {
  assert(checkpointBeingFetched.checkpointNum == 0);

  checkpointBeingFetched = c;
}

DataStore::CheckpointDesc InMemoryDataStore::getCheckpointBeingFetched() {
  assert(checkpointBeingFetched.checkpointNum != 0);

  return checkpointBeingFetched;
}

bool InMemoryDataStore::hasCheckpointBeingFetched() {
  return (checkpointBeingFetched.checkpointNum != 0);
}

void InMemoryDataStore::deleteCheckpointBeingFetched() {
  memset(&checkpointBeingFetched, 0, sizeof(checkpointBeingFetched));

  assert(checkpointBeingFetched.checkpointNum == 0);
}

void InMemoryDataStore::setFirstRequiredBlock(uint64_t i) {
  firstRequiredBlock = i;
}

uint64_t InMemoryDataStore::getFirstRequiredBlock() {
  return firstRequiredBlock;
}

void InMemoryDataStore::setLastRequiredBlock(uint64_t i) {
  lastRequiredBlock = i;
}

uint64_t InMemoryDataStore::getLastRequiredBlock() { return lastRequiredBlock; }

void InMemoryDataStore::setPendingResPage(uint32_t inPageId,
                                          const char* inPage,
                                          uint32_t inPageLen) {
  assert(inPageLen <= sizeOfReservedPage_);

  auto pos = pendingPages.find(inPageId);

  char* page = nullptr;

  if (pos == pendingPages.end()) {
    page = reinterpret_cast<char*>(std::malloc(sizeOfReservedPage_));
    pendingPages[inPageId] = page;
  } else {
    page = pos->second;
  }

  memcpy(page, inPage, inPageLen);

  if (inPageLen < sizeOfReservedPage_)
    memset(page + inPageLen, 0, (sizeOfReservedPage_ - inPageLen));
}

bool InMemoryDataStore::hasPendingResPage(uint32_t inPageId) {
  return (pendingPages.count(inPageId) > 0);
}

void InMemoryDataStore::getPendingResPage(uint32_t inPageId,
                                          char* outPage,
                                          uint32_t pageLen) {
  assert(pageLen <= sizeOfReservedPage_);

  auto pos = pendingPages.find(inPageId);

  assert(pos != pendingPages.end());

  memcpy(outPage, pos->second, pageLen);
}

uint32_t InMemoryDataStore::numOfAllPendingResPage() {
  return (uint32_t)(pendingPages.size());
}

set<uint32_t> InMemoryDataStore::getNumbersOfPendingResPages() {
  set<uint32_t> retSet;

  for (auto p : pendingPages) retSet.insert(p.first);

  return retSet;
}

void InMemoryDataStore::deleteAllPendingPages() {
  for (auto p : pendingPages) std::free(p.second);

  pendingPages.clear();
}

void InMemoryDataStore::associatePendingResPageWithCheckpoint(
    uint32_t inPageId, uint64_t inCheckpoint, const STDigest& inPageDigest) {
  // find in pendingPages
  auto pendingPos = pendingPages.find(inPageId);
  assert(pendingPos != pendingPages.end());
  assert(pendingPos->second != nullptr);

  // create key, and make sure that we don't already have this element
  ResPageKey key = {inPageId, inCheckpoint};
  assert(pages.count(key) == 0);

  // create value
  ResPageVal val = {inPageDigest, pendingPos->second};

  // add to the pages map
  pages[key] = val;

  // remove from pendingPages map
  pendingPages.erase(pendingPos);
}

void InMemoryDataStore::setResPage(uint32_t inPageId,
                                   uint64_t inCheckpoint,
                                   const STDigest& inPageDigest,
                                   const char* inPage) {
  // create key, and make sure that we don't already have this element
  ResPageKey key = {inPageId, inCheckpoint};
  assert(pages.count(key) == 0);

  // prepare page
  char* page = reinterpret_cast<char*>(std::malloc(sizeOfReservedPage_));
  memcpy(page, inPage, sizeOfReservedPage_);

  // create value
  ResPageVal val = {inPageDigest, page};

  // add to the pages map
  pages[key] = val;
}

void InMemoryDataStore::getResPage(uint32_t inPageId,
                                   uint64_t inCheckpoint,
                                   uint64_t* outActualCheckpoint) {
  getResPage(inPageId, inCheckpoint, outActualCheckpoint, nullptr, nullptr, 0);
}

void InMemoryDataStore::getResPage(uint32_t inPageId,
                                   uint64_t inCheckpoint,
                                   uint64_t* outActualCheckpoint,
                                   char* outPage,
                                   uint32_t copylength) {
  getResPage(inPageId,
             inCheckpoint,
             outActualCheckpoint,
             nullptr,
             outPage,
             copylength);
}

void InMemoryDataStore::getResPage(uint32_t inPageId,
                                   uint64_t inCheckpoint,
                                   uint64_t* outActualCheckpoint,
                                   STDigest* outPageDigest,
                                   char* outPage,
                                   uint32_t copylength) {
  assert(copylength <= sizeOfReservedPage_);

  assert(inCheckpoint <= lastStoredCheckpoint);

  ResPageKey key = {inPageId, inCheckpoint};

  auto p = pages.lower_bound(key);

  assert(p != pages.end());
  assert(p->first.pageId == inPageId);
  assert(p->first.checkpoint <= inCheckpoint);

  if (outActualCheckpoint != nullptr)
    *outActualCheckpoint = p->first.checkpoint;

  if (outPageDigest != nullptr) *outPageDigest = p->second.pageDigest;

  if (outPage != nullptr) {
    assert(copylength > 0);
    memcpy(outPage, p->second.page, copylength);
  }
}

void InMemoryDataStore::deleteCoveredResPageInSmallerCheckpoints(
    uint64_t inMinRelevantCheckpoint) {
  if (inMinRelevantCheckpoint <= 1) return;  //  nothing to delete

  assert(pages.size() >= numberOfReservedPages_);

  auto iter = pages.begin();
  uint32_t prevItemPageId = iter->first.pageId;
  assert(prevItemPageId == 0);
  bool prevItemIsInLastRelevantCheckpoint =
      (iter->first.checkpoint <= inMinRelevantCheckpoint);

  iter++;

  while (iter != pages.end()) {
    if (iter->first.pageId == prevItemPageId &&
        prevItemIsInLastRelevantCheckpoint) {
      // delete
      assert(iter->second.page != nullptr);
      std::free(iter->second.page);
      iter = pages.erase(iter);
    } else {
      prevItemPageId = iter->first.pageId;
      prevItemIsInLastRelevantCheckpoint =
          (iter->first.checkpoint <= inMinRelevantCheckpoint);
      iter++;
    }
  }

  assert(pages.size() >= numberOfReservedPages_);
  assert(pages.size() <=
         (numberOfReservedPages_ * (maxNumOfStoredCheckpoints_ + 1)));
}

DataStore::ResPagesDescriptor* InMemoryDataStore::getResPagesDescriptor(
    uint64_t inCheckpoint) {
  //  printf("\nInMemoryDataStore::getResPagesDescriptor");

  size_t reqSize = DataStore::ResPagesDescriptor::size(numberOfReservedPages_);

  void* p = std::malloc(reqSize);

  memset(p, 0, reqSize);

  DataStore::ResPagesDescriptor* desc = (DataStore::ResPagesDescriptor*)p;

  desc->numOfPages = numberOfReservedPages_;

  if (inCheckpoint >= 1) {
    uint32_t nextPage = 0;

    for (auto iter : pages) {
      if (nextPage == iter.first.pageId &&
          iter.first.checkpoint <= inCheckpoint) {
        SingleResPageDesc& singleDesc = desc->d[nextPage];

        singleDesc.pageId = nextPage;
        singleDesc.relevantCheckpoint = iter.first.checkpoint;
        singleDesc.pageDigest = iter.second.pageDigest;

        /*
                printf("singleDesc.pageId=%d singleDesc.relevantCheckpoint=%d
           ccccc=%s", (int)singleDesc.pageId,
                (int)singleDesc.relevantCheckpoint,
                singleDesc.pageDigest.toString().c_str());
        */

        nextPage++;
      }
    }

    assert(nextPage == numberOfReservedPages_);
  } else {
    // inCheckpoint == 0
    for (uint32_t i = 0; i < numberOfReservedPages_; i++) {
      SingleResPageDesc& singleDesc = desc->d[i];

      singleDesc.pageId = i;
      singleDesc.relevantCheckpoint = 0;
      singleDesc.pageDigest.makeZero();
    }
  }

  return desc;
}

void InMemoryDataStore::free(ResPagesDescriptor* desc) {
  assert(desc->numOfPages == numberOfReservedPages_);
  void* p = desc;
  std::free(p);
}

}  // namespace impl
}  // namespace SimpleBlockchainStateTransfer
}  // namespace bftEngine
