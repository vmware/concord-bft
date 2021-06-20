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
#include "assertUtils.hpp"
#include "Logger.hpp"
#include "kvstream.h"

namespace bftEngine {
namespace bcst {
namespace impl {

InMemoryDataStore::InMemoryDataStore(uint32_t sizeOfReservedPage) : sizeOfReservedPage_(sizeOfReservedPage) {
  checkpointBeingFetched.checkpointNum = 0;
}

bool InMemoryDataStore::initialized() { return wasInit_; }

void InMemoryDataStore::setAsInitialized() { wasInit_ = true; }

void InMemoryDataStore::setReplicas(const set<uint16_t> replicas) {
  ConcordAssert(replicas_.empty());
  replicas_ = replicas;
}

set<uint16_t> InMemoryDataStore::getReplicas() {
  ConcordAssert(!replicas_.empty());
  return replicas_;
}

void InMemoryDataStore::setMyReplicaId(uint16_t id) {
  ConcordAssert(myReplicaId_ == UINT16_MAX);
  myReplicaId_ = id;
}

uint16_t InMemoryDataStore::getMyReplicaId() {
  ConcordAssert(myReplicaId_ != UINT16_MAX);
  return myReplicaId_;
}

void InMemoryDataStore::setFVal(uint16_t fVal) {
  ConcordAssert(fVal_ == UINT16_MAX);
  fVal_ = fVal;
}

uint16_t InMemoryDataStore::getFVal() {
  ConcordAssert(fVal_ != UINT16_MAX);
  return fVal_;
}

void InMemoryDataStore::setMaxNumOfStoredCheckpoints(uint64_t numChecks) {
  ConcordAssert(numChecks > 0);

  ConcordAssert(maxNumOfStoredCheckpoints_ == UINT64_MAX);
  maxNumOfStoredCheckpoints_ = numChecks;
}

uint64_t InMemoryDataStore::getMaxNumOfStoredCheckpoints() const {
  ConcordAssert(maxNumOfStoredCheckpoints_ != UINT64_MAX);
  return maxNumOfStoredCheckpoints_;
}

void InMemoryDataStore::setNumberOfReservedPages(uint32_t numResPages) {
  ConcordAssert(numResPages > 0);

  ConcordAssert(numberOfReservedPages_ == UINT32_MAX);
  numberOfReservedPages_ = numResPages;
}

uint32_t InMemoryDataStore::getNumberOfReservedPages() {
  ConcordAssert(numberOfReservedPages_ != UINT32_MAX);
  return numberOfReservedPages_;
}

void InMemoryDataStore::setLastStoredCheckpoint(uint64_t c) { lastStoredCheckpoint = c; }

uint64_t InMemoryDataStore::getLastStoredCheckpoint() { return lastStoredCheckpoint; }

void InMemoryDataStore::setFirstStoredCheckpoint(uint64_t c) { firstStoredCheckpoint = c; }

uint64_t InMemoryDataStore::getFirstStoredCheckpoint() { return firstStoredCheckpoint; }

void InMemoryDataStore::setCheckpointDesc(uint64_t checkpoint, const CheckpointDesc& desc) {
  ConcordAssert(checkpoint == desc.checkpointNum);
  ConcordAssert(descMap.count(checkpoint) == 0);

  descMap[checkpoint] = desc;

  //  ConcordAssert(descMap.size() < 21);  // TODO(GG): delete - debug only
}

DataStore::CheckpointDesc InMemoryDataStore::getCheckpointDesc(uint64_t checkpoint) {
  auto p = descMap.find(checkpoint);

  ConcordAssert(p != descMap.end());

  ConcordAssert(p->first == p->second.checkpointNum);

  return p->second;
}

bool InMemoryDataStore::hasCheckpointDesc(uint64_t checkpoint) {
  auto p = descMap.find(checkpoint);

  return (p != descMap.end());
}

void InMemoryDataStore::deleteDescOfSmallerCheckpoints(uint64_t checkpoint) {
  auto p = descMap.begin();
  while ((p != descMap.end()) && (p->first < checkpoint)) {
    ConcordAssert(p->first == p->second.checkpointNum);
    p = descMap.erase(p);
  }
}

void InMemoryDataStore::setIsFetchingState(bool b) { fetching = b; }

bool InMemoryDataStore::getIsFetchingState() { return fetching; }

void InMemoryDataStore::setCheckpointBeingFetched(const CheckpointDesc& c) {
  ConcordAssert(checkpointBeingFetched.checkpointNum == 0);

  checkpointBeingFetched = c;
}

DataStore::CheckpointDesc InMemoryDataStore::getCheckpointBeingFetched() {
  ConcordAssert(checkpointBeingFetched.checkpointNum != 0);

  return checkpointBeingFetched;
}

bool InMemoryDataStore::hasCheckpointBeingFetched() { return (checkpointBeingFetched.checkpointNum != 0); }

void InMemoryDataStore::deleteCheckpointBeingFetched() {
  // TODO(DD): Create a ctor for CheckpointDesc?
  // NOLINTNEXTLINE(bugprone-undefined-memory-manipulation)
  memset(&checkpointBeingFetched, 0, sizeof(checkpointBeingFetched));

  ConcordAssert(checkpointBeingFetched.checkpointNum == 0);
}

void InMemoryDataStore::setFirstRequiredBlock(uint64_t i) { firstRequiredBlock = i; }

uint64_t InMemoryDataStore::getFirstRequiredBlock() { return firstRequiredBlock; }

void InMemoryDataStore::setLastRequiredBlock(uint64_t i) { lastRequiredBlock = i; }

uint64_t InMemoryDataStore::getLastRequiredBlock() { return lastRequiredBlock; }

void InMemoryDataStore::setPendingResPage(uint32_t inPageId, const char* inPage, uint32_t inPageLen) {
  LOG_DEBUG(logger(), inPageId);
  ConcordAssert(inPageLen <= sizeOfReservedPage_);

  auto pos = pendingPages.find(inPageId);

  char* page = nullptr;

  if (pos == pendingPages.end()) {
    page = reinterpret_cast<char*>(std::malloc(sizeOfReservedPage_));
    pendingPages[inPageId] = page;
  } else {
    page = pos->second;
  }

  memcpy(page, inPage, inPageLen);

  if (inPageLen < sizeOfReservedPage_) memset(page + inPageLen, 0, (sizeOfReservedPage_ - inPageLen));
}

bool InMemoryDataStore::hasPendingResPage(uint32_t inPageId) { return (pendingPages.count(inPageId) > 0); }

void InMemoryDataStore::getPendingResPage(uint32_t inPageId, char* outPage, uint32_t pageLen) {
  ConcordAssert(pageLen <= sizeOfReservedPage_);

  auto pos = pendingPages.find(inPageId);

  ConcordAssert(pos != pendingPages.end());

  memcpy(outPage, pos->second, pageLen);
}

uint32_t InMemoryDataStore::numOfAllPendingResPage() { return (uint32_t)(pendingPages.size()); }

set<uint32_t> InMemoryDataStore::getNumbersOfPendingResPages() {
  set<uint32_t> retSet;

  for (auto p : pendingPages) retSet.insert(p.first);

  return retSet;
}

void InMemoryDataStore::deleteAllPendingPages() {
  for (auto p : pendingPages) std::free(p.second);

  pendingPages.clear();
}

void InMemoryDataStore::associatePendingResPageWithCheckpoint(uint32_t inPageId,
                                                              uint64_t inCheckpoint,
                                                              const STDigest& inPageDigest) {
  LOG_DEBUG(logger(), "pageId: " << inPageId << " checkpoint: " << inCheckpoint);
  // find in pendingPages
  auto pendingPos = pendingPages.find(inPageId);
  ConcordAssert(pendingPos != pendingPages.end());
  ConcordAssert(pendingPos->second != nullptr);

  // create key, and make sure that we don't already have this element
  ResPageKey key = {inPageId, inCheckpoint};
  ConcordAssert(pages.count(key) == 0);

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
  LOG_DEBUG(logger(), "pageId: " << inPageId << " checkpoint: " << inCheckpoint);
  // create key, and make sure that we don't already have this element
  ResPageKey key = {inPageId, inCheckpoint};
  ConcordAssert(pages.count(key) == 0);

  // prepare page
  char* page = reinterpret_cast<char*>(std::malloc(sizeOfReservedPage_));
  memcpy(page, inPage, sizeOfReservedPage_);

  // create value
  ResPageVal val = {inPageDigest, page};

  // add to the pages map
  pages[key] = val;
}

bool InMemoryDataStore::getResPage(uint32_t inPageId,
                                   uint64_t inCheckpoint,
                                   uint64_t* outActualCheckpoint,
                                   STDigest* outPageDigest,
                                   char* outPage,
                                   uint32_t copylength) {
  ConcordAssert(copylength <= sizeOfReservedPage_);

  ConcordAssert(inCheckpoint <= lastStoredCheckpoint);

  ResPageKey key = {inPageId, inCheckpoint};

  auto p = pages.lower_bound(key);

  if (p == pages.end() || p->first.pageId > inPageId) return false;

  ConcordAssert(p->first.checkpoint <= inCheckpoint);

  if (outActualCheckpoint != nullptr) *outActualCheckpoint = p->first.checkpoint;

  if (outPageDigest != nullptr) *outPageDigest = p->second.pageDigest;

  LOG_DEBUG(logger(),
            "pageId: " << inPageId << " checkpoint: " << inCheckpoint << " actual checkpoint: " << p->first.checkpoint
                       << " digest: " << p->second.pageDigest.toString());

  if (outPage != nullptr) {
    ConcordAssert(copylength > 0);
    memcpy(outPage, p->second.page, copylength);
  }
  return true;
}

void InMemoryDataStore::deleteCoveredResPageInSmallerCheckpoints(uint64_t inMinRelevantCheckpoint) {
  if (inMinRelevantCheckpoint <= 1) return;  //  nothing to delete

  auto iter = pages.begin();
  if (iter == pages.end()) return;
  uint32_t prevItemPageId = iter->first.pageId;
  bool prevItemIsInLastRelevantCheckpoint = (iter->first.checkpoint <= inMinRelevantCheckpoint);

  std::ostringstream oss("deleted: ");
  iter++;

  while (iter != pages.end()) {
    if (iter->first.pageId == prevItemPageId && prevItemIsInLastRelevantCheckpoint) {
      // delete
      ConcordAssert(iter->second.page != nullptr);
      oss << "[" << iter->first.pageId << ":" << iter->first.checkpoint << "] ";
      std::free(iter->second.page);
      iter = pages.erase(iter);
    } else {
      prevItemPageId = iter->first.pageId;
      prevItemIsInLastRelevantCheckpoint = (iter->first.checkpoint <= inMinRelevantCheckpoint);
      iter++;
    }
  }
  LOG_DEBUG(logger(), oss.str());

  ConcordAssert(pages.size() <= (numberOfReservedPages_ * (maxNumOfStoredCheckpoints_ + 1)));
}

DataStore::ResPagesDescriptor* InMemoryDataStore::getResPagesDescriptor(uint64_t inCheckpoint) {
  size_t reqSize = DataStore::ResPagesDescriptor::size(numberOfReservedPages_);

  void* p = std::malloc(reqSize);
  memset(p, 0, reqSize);

  DataStore::ResPagesDescriptor* desc = (DataStore::ResPagesDescriptor*)p;

  desc->numOfPages = numberOfReservedPages_;

  for (const auto& iter : pages) {
    if (iter.first.checkpoint <= inCheckpoint) {
      SingleResPageDesc& singleDesc = desc->d[iter.first.pageId];
      if (singleDesc.relevantCheckpoint > 0) {
        ConcordAssert(singleDesc.relevantCheckpoint > iter.first.checkpoint);
        LOG_TRACE(logger(), "skip: " << KVLOG(inCheckpoint, singleDesc.pageId, iter.first.checkpoint));
        continue;  // we already have a description for this pageId with a greater checkpoint num
      }
      singleDesc.pageId = iter.first.pageId;
      singleDesc.relevantCheckpoint = iter.first.checkpoint;
      singleDesc.pageDigest = iter.second.pageDigest;

      LOG_TRACE(
          logger(),
          KVLOG(inCheckpoint, singleDesc.pageId, singleDesc.relevantCheckpoint, singleDesc.pageDigest.toString()));
    }
  }
  return desc;
}

void InMemoryDataStore::free(ResPagesDescriptor* desc) {
  ConcordAssert(desc->numOfPages == numberOfReservedPages_);
  void* p = desc;
  std::free(p);
}

}  // namespace impl
}  // namespace bcst
}  // namespace bftEngine
