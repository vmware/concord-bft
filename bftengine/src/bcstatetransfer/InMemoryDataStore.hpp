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

#ifndef BFTENGINE_SRC_BCSTATETRANSFER_INMEMORYDATASTORE_HPP_
#define BFTENGINE_SRC_BCSTATETRANSFER_INMEMORYDATASTORE_HPP_

#include <map>
#include <set>

#include "DataStore.hpp"
#include "STDigest.hpp"

using std::map;

namespace bftEngine {
namespace SimpleBlockchainStateTransfer {
namespace impl {

class InMemoryDataStore : public DataStore {
 public:
  explicit InMemoryDataStore(uint32_t sizeOfReservedPage);
  ~InMemoryDataStore() override {}

  //////////////////////////////////////////////////////////////////////////
  // config
  //////////////////////////////////////////////////////////////////////////

  bool initialized() override;
  void setAsInitialized() override;

  void setReplicas(const set<uint16_t> replicas) override;
  set<uint16_t> getReplicas() override;

  void setMyReplicaId(uint16_t id) override;
  uint16_t getMyReplicaId() override;

  void setFVal(uint16_t fVal) override;
  uint16_t getFVal() override;

  void setMaxNumOfStoredCheckpoints(uint64_t numChecks) override;
  uint64_t getMaxNumOfStoredCheckpoints() override;

  void setNumberOfReservedPages(uint32_t numResPages) override;
  uint32_t getNumberOfReservedPages() override;

  //////////////////////////////////////////////////////////////////////////
  // first/last checkpoint number which are currently maintained
  //////////////////////////////////////////////////////////////////////////

  void setLastStoredCheckpoint(uint64_t c) override;
  uint64_t getLastStoredCheckpoint() override;

  void setFirstStoredCheckpoint(uint64_t c) override;
  uint64_t getFirstStoredCheckpoint() override;

  //////////////////////////////////////////////////////////////////////////
  // Checkpoints
  //////////////////////////////////////////////////////////////////////////

  void setCheckpointDesc(uint64_t checkpoint,
                         const CheckpointDesc& desc) override;
  CheckpointDesc getCheckpointDesc(uint64_t checkpoint) override;
  bool hasCheckpointDesc(uint64_t checkpoint) override;
  void deleteDescOfSmallerCheckpoints(uint64_t checkpoint) override;

  //////////////////////////////////////////////////////////////////////////
  // Fetching status
  //////////////////////////////////////////////////////////////////////////

  void setIsFetchingState(bool b) override;
  bool getIsFetchingState() override;

  void setCheckpointBeingFetched(const CheckpointDesc& c) override;
  CheckpointDesc getCheckpointBeingFetched() override;
  bool hasCheckpointBeingFetched() override;
  void deleteCheckpointBeingFetched() override;

  void setFirstRequiredBlock(uint64_t i) override;
  uint64_t getFirstRequiredBlock() override;

  void setLastRequiredBlock(uint64_t i) override;
  uint64_t getLastRequiredBlock() override;

  //////////////////////////////////////////////////////////////////////////
  // reserved pages
  //////////////////////////////////////////////////////////////////////////

  void setPendingResPage(uint32_t inPageId,
                         const char* inPage,
                         uint32_t inPageLen) override;
  bool hasPendingResPage(uint32_t inPageId) override;
  void getPendingResPage(uint32_t inPageId,
                         char* outPage,
                         uint32_t pageLen) override;
  uint32_t numOfAllPendingResPage() override;
  set<uint32_t> getNumbersOfPendingResPages() override;
  void deleteAllPendingPages() override;

  void associatePendingResPageWithCheckpoint(
      uint32_t inPageId,
      uint64_t inCheckpoint,
      const STDigest& inPageDigest) override;

  void setResPage(uint32_t inPageId,
                  uint64_t inCheckpoint,
                  const STDigest& inPageDigest,
                  const char* inPage) override;
  void getResPage(uint32_t inPageId,
                  uint64_t inCheckpoint,
                  uint64_t* outActualCheckpoint) override;
  void getResPage(uint32_t inPageId,
                  uint64_t inCheckpoint,
                  uint64_t* outActualCheckpoint,
                  char* outPage,
                  uint32_t copylength) override;
  void getResPage(uint32_t inPageId,
                  uint64_t inCheckpoint,
                  uint64_t* outActualCheckpoint,
                  STDigest* outPageDigest,
                  char* outPage,
                  uint32_t copylength) override;

  void deleteCoveredResPageInSmallerCheckpoints(uint64_t inCheckpoint) override;

  ResPagesDescriptor* getResPagesDescriptor(uint64_t inCheckpoint) override;
  void free(ResPagesDescriptor*) override;

 protected:
  const uint32_t sizeOfReservedPage_;

  bool wasInit_ = false;

  set<uint16_t> replicas_;

  uint16_t myReplicaId_ = UINT16_MAX;

  uint16_t fVal_ = UINT16_MAX;

  uint64_t maxNumOfStoredCheckpoints_ = UINT64_MAX;

  uint32_t numberOfReservedPages_ = UINT32_MAX;

  uint64_t lastStoredCheckpoint = UINT64_MAX;
  uint64_t firstStoredCheckpoint = UINT64_MAX;

  map<uint64_t, CheckpointDesc> descMap;

  bool fetching = false;

  CheckpointDesc checkpointBeingFetched;
  // none if checkpointBeingFetched.checkpointNum == 0

  uint64_t firstRequiredBlock = UINT64_MAX;
  uint64_t lastRequiredBlock = UINT64_MAX;

  map<uint32_t, char*> pendingPages;

  struct ResPageKey {
    uint32_t pageId;
    uint64_t checkpoint;

    bool operator<(const ResPageKey& rhs) const {
      if (pageId != rhs.pageId)
        return pageId < rhs.pageId;
      else
        return rhs.checkpoint < checkpoint;
    }
  };

  struct ResPageVal {
    STDigest pageDigest;
    char* page;
  };

  map<ResPageKey, ResPageVal> pages;
};

}  // namespace impl
}  // namespace SimpleBlockchainStateTransfer
}  // namespace bftEngine

#endif  // BFTENGINE_SRC_BCSTATETRANSFER_INMEMORYDATASTORE_HPP_
