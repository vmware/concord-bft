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

#ifndef BFTENGINE_SRC_BCSTATETRANSFER_DATASTORE_HPP_
#define BFTENGINE_SRC_BCSTATETRANSFER_DATASTORE_HPP_

#include <cassert>
#include <string>
#include <set>

#include "STDigest.hpp"

using std::set;

namespace bftEngine {
namespace SimpleBlockchainStateTransfer {
namespace impl {

class DataStore {
 public:
  virtual ~DataStore() {}

  //////////////////////////////////////////////////////////////////////////
  // config
  //////////////////////////////////////////////////////////////////////////

  virtual bool initialized() = 0;
  virtual void setAsInitialized() = 0;

  virtual void setReplicas(const set<uint16_t> replicas) = 0;
  virtual set<uint16_t> getReplicas() = 0;

  virtual void setMyReplicaId(uint16_t id) = 0;
  virtual uint16_t getMyReplicaId() = 0;

  virtual void setFVal(uint16_t fVal) = 0;
  virtual uint16_t getFVal() = 0;

  virtual void setMaxNumOfStoredCheckpoints(uint64_t numChecks) = 0;
  virtual uint64_t getMaxNumOfStoredCheckpoints() = 0;

  virtual void setNumberOfReservedPages(uint32_t numResPages) = 0;
  virtual uint32_t getNumberOfReservedPages() = 0;

  //////////////////////////////////////////////////////////////////////////
  // first/last checkpoint number which are currently maintained
  //////////////////////////////////////////////////////////////////////////

  virtual void setLastStoredCheckpoint(uint64_t c) = 0;
  virtual uint64_t getLastStoredCheckpoint() = 0;

  virtual void setFirstStoredCheckpoint(uint64_t c) = 0;
  virtual uint64_t getFirstStoredCheckpoint() = 0;

  //////////////////////////////////////////////////////////////////////////
  // Checkpoints
  //////////////////////////////////////////////////////////////////////////

  struct CheckpointDesc {
    uint64_t checkpointNum;
    uint64_t lastBlock;
    STDigest digestOfLastBlock;
    STDigest digestOfResPagesDescriptor;
  };

  virtual void setCheckpointDesc(uint64_t checkpoint,
                                 const CheckpointDesc& desc) = 0;
  virtual CheckpointDesc getCheckpointDesc(uint64_t checkpoint) = 0;
  virtual bool hasCheckpointDesc(uint64_t checkpoint) = 0;
  virtual void deleteDescOfSmallerCheckpoints(uint64_t checkpoint) = 0;

  //////////////////////////////////////////////////////////////////////////
  // reserved pages
  //////////////////////////////////////////////////////////////////////////

  virtual void setPendingResPage(uint32_t inPageId,
                                 const char* inPage,
                                 uint32_t pageLen) = 0;
  virtual bool hasPendingResPage(uint32_t inPageId) = 0;
  virtual void getPendingResPage(uint32_t inPageId,
                                 char* outPage,
                                 uint32_t pageLen) = 0;
  virtual uint32_t numOfAllPendingResPage() = 0;
  virtual set<uint32_t> getNumbersOfPendingResPages() = 0;
  virtual void deleteAllPendingPages() = 0;

  virtual void associatePendingResPageWithCheckpoint(
      uint32_t inPageId,
      uint64_t inCheckpoint,
      const STDigest& inPageDigest) = 0;

  virtual void setResPage(uint32_t inPageId,
                          uint64_t inCheckpoint,
                          const STDigest& inPageDigest,
                          const char* inPage) = 0;
  virtual void getResPage(uint32_t inPageId,
                          uint64_t inCheckpoint,
                          uint64_t* outActualCheckpoint) = 0;
  virtual void getResPage(uint32_t inPageId,
                          uint64_t inCheckpoint,
                          uint64_t* outActualCheckpoint,
                          char* outPage,
                          uint32_t copylength) = 0;
  virtual void getResPage(uint32_t inPageId,
                          uint64_t inCheckpoint,
                          uint64_t* outActualCheckpoint,
                          STDigest* outPageDigest,
                          char* outPage,
                          uint32_t copylength) = 0;

  virtual void deleteCoveredResPageInSmallerCheckpoints(
      uint64_t inCheckpoint) = 0;

  struct SingleResPageDesc {
    uint32_t pageId;
    uint64_t relevantCheckpoint;
    STDigest pageDigest;
  };

  struct ResPagesDescriptor {
    uint32_t numOfPages;
    SingleResPageDesc d[1];  // number of elements in this array == numOfPages

    size_t size() const { return size(numOfPages); }

    static size_t size(uint32_t numberOfPages) {
      assert(numberOfPages > 0);
      return sizeof(ResPagesDescriptor) +
             (numberOfPages - 1) * sizeof(SingleResPageDesc);
    }
  };

  virtual ResPagesDescriptor* getResPagesDescriptor(uint64_t inCheckpoint) = 0;
  virtual void free(ResPagesDescriptor*) = 0;

  //////////////////////////////////////////////////////////////////////////
  // Fetching status
  //////////////////////////////////////////////////////////////////////////

  virtual void setIsFetchingState(bool b) = 0;
  virtual bool getIsFetchingState() = 0;

  virtual void setCheckpointBeingFetched(const CheckpointDesc& c) = 0;
  virtual CheckpointDesc getCheckpointBeingFetched() = 0;
  virtual bool hasCheckpointBeingFetched() = 0;
  virtual void deleteCheckpointBeingFetched() = 0;

  virtual void setFirstRequiredBlock(uint64_t i) = 0;
  virtual uint64_t getFirstRequiredBlock() = 0;

  virtual void setLastRequiredBlock(uint64_t i) = 0;
  virtual uint64_t getLastRequiredBlock() = 0;
};

}  // namespace impl
}  // namespace SimpleBlockchainStateTransfer
}  // namespace bftEngine

#endif  // BFTENGINE_SRC_BCSTATETRANSFER_DATASTORE_HPP_
