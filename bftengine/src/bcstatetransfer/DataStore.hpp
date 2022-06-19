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

#pragma once

#include <cassert>
#include <string>
#include <set>
#include "assertUtils.hpp"
#include "storage/db_interface.h"
#include "Digest.hpp"
#include "Logger.hpp"

using std::set;
using concord::storage::ITransaction;
using concord::util::digest::Digest;

namespace bftEngine {
namespace bcst {
namespace impl {

using BlockId = uint64_t;
class DataStoreTransaction;

class DataStore : public std::enable_shared_from_this<DataStore> {
 public:
  virtual ~DataStore() {}

  //////////////////////////////////////////////////////////////////////////
  // config
  //////////////////////////////////////////////////////////////////////////

  virtual bool initialized() = 0;
  virtual void setAsInitialized() = 0;

  virtual void setReplicas(const set<uint16_t>& replicas) = 0;
  virtual set<uint16_t> getReplicas() = 0;

  virtual void setMyReplicaId(uint16_t id) = 0;
  virtual uint16_t getMyReplicaId() = 0;

  virtual void setFVal(uint16_t fVal) = 0;
  virtual uint16_t getFVal() = 0;

  virtual void setMaxNumOfStoredCheckpoints(uint64_t numChecks) = 0;
  virtual uint64_t getMaxNumOfStoredCheckpoints() const = 0;

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
  // Range Validation Blocks
  //////////////////////////////////////////////////////////////////////////
  virtual void setPrunedBlocksDigests(const std::vector<std::pair<BlockId, Digest>>& prunedBlocksDigests) = 0;
  virtual std::vector<std::pair<BlockId, Digest>> getPrunedBlocksDigests() = 0;

  //////////////////////////////////////////////////////////////////////////
  // Checkpoints
  //////////////////////////////////////////////////////////////////////////
  struct CheckpointDesc {
    void makeZero() {
      checkpointNum = 0;
      maxBlockId = 0;
      digestOfMaxBlockId.makeZero();
      digestOfResPagesDescriptor.makeZero();
      rvbData.clear();
    }

    uint64_t checkpointNum = 0;
    uint64_t maxBlockId = 0;
    Digest digestOfMaxBlockId;
    Digest digestOfResPagesDescriptor;
    std::vector<char> rvbData{};
  };

  virtual void setCheckpointDesc(uint64_t checkpoint,
                                 const CheckpointDesc& desc,
                                 const bool checkIfAlreadyExists = true) = 0;
  virtual CheckpointDesc getCheckpointDesc(uint64_t checkpoint) = 0;
  virtual bool hasCheckpointDesc(uint64_t checkpoint) = 0;
  virtual void deleteDescOfSmallerCheckpoints(uint64_t checkpoint) = 0;

  //////////////////////////////////////////////////////////////////////////
  // reserved pages
  //////////////////////////////////////////////////////////////////////////

  virtual void setPendingResPage(uint32_t inPageId, const char* inPage, uint32_t pageLen) = 0;
  virtual bool hasPendingResPage(uint32_t inPageId) = 0;
  virtual void getPendingResPage(uint32_t inPageId, char* outPage, uint32_t pageLen) = 0;
  virtual uint32_t numOfAllPendingResPage() = 0;
  virtual set<uint32_t> getNumbersOfPendingResPages() = 0;
  virtual void deleteAllPendingPages() = 0;

  virtual void associatePendingResPageWithCheckpoint(uint32_t inPageId,
                                                     uint64_t inCheckpoint,
                                                     const Digest& inPageDigest) = 0;

  virtual void setResPage(uint32_t inPageId,
                          uint64_t inCheckpoint,
                          const Digest& inPageDigest,
                          const char* inPage,
                          const bool checkIfAlreadyExists = true) = 0;
  virtual bool getResPage(uint32_t inPageId, uint64_t inCheckpoint, uint64_t* outActualCheckpoint) {
    return getResPage(inPageId, inCheckpoint, outActualCheckpoint, nullptr, nullptr, 0);
  }
  virtual bool getResPage(
      uint32_t inPageId, uint64_t inCheckpoint, uint64_t* outActualCheckpoint, char* outPage, uint32_t copylength) {
    return getResPage(inPageId, inCheckpoint, outActualCheckpoint, nullptr, outPage, copylength);
  }
  virtual bool getResPage(uint32_t inPageId,
                          uint64_t inCheckpoint,
                          uint64_t* outActualCheckpoint,
                          Digest* outPageDigest,
                          char* outPage,
                          uint32_t copylength) = 0;

  virtual void deleteCoveredResPageInSmallerCheckpoints(uint64_t inCheckpoint) = 0;

  struct SingleResPageDesc {
    uint32_t pageId;
    uint64_t relevantCheckpoint;
    Digest pageDigest;
  };

  struct ResPagesDescriptor {
    uint32_t numOfPages;
    SingleResPageDesc d[1];  // number of elements in this array == numOfPages

    size_t size() const { return size(numOfPages); }

    static size_t size(uint32_t numberOfPages) {
      ConcordAssert(numberOfPages > 0);
      return sizeof(ResPagesDescriptor) + (numberOfPages - 1) * sizeof(SingleResPageDesc);
    }
    std::string toString(const std::string& digest) const {
      std::ostringstream oss;
      oss << "\nReserved pages descriptor: #pages: " << std::to_string(numOfPages) << "\n";
      oss << "digest\t" << digest << "\n";
      for (uint32_t i = 0; i < numOfPages; ++i)
        if (d[i].relevantCheckpoint > 0)
          oss << "[" << d[i].pageId << ":" << d[i].relevantCheckpoint << "]\t" << d[i].pageDigest.toString() << "\n";
      return oss.str();
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

  // Transaction support
  virtual DataStoreTransaction* beginTransaction() = 0;

  // Sometimes we would want to clear the metadata when the state transfer is shutting down (i.e on various
  // reconfiguration actions)
  virtual void setEraseDataStoreFlag() = 0;
};

/** *******************************************************************************************************************
 *
 */
class DataStoreTransaction : public DataStore, public ITransaction {
 public:
  class Guard : public ITransaction::Guard {
   public:
    explicit Guard(DataStoreTransaction* t) : ITransaction::Guard(t) {}
    DataStoreTransaction* txn() { return static_cast<DataStoreTransaction*>(txn_); }
  };

  DataStoreTransaction(std::shared_ptr<DataStore> ds, ITransaction* txn)
      : ITransaction(txn->getId()), ds_(ds), txn_(txn) {}
  ~DataStoreTransaction() {}
  /**
   * DataStore implementation
   */
  void setAsInitialized() override { ds_->setAsInitialized(); }
  void setReplicas(const set<uint16_t>& replicas) override { ds_->setReplicas(replicas); }
  void setMyReplicaId(uint16_t id) override { ds_->setMyReplicaId(id); }
  void setMaxNumOfStoredCheckpoints(uint64_t numChp) override { ds_->setMaxNumOfStoredCheckpoints(numChp); }
  void setNumberOfReservedPages(uint32_t numResPgs) override { ds_->setNumberOfReservedPages(numResPgs); }
  void setLastStoredCheckpoint(uint64_t c) override { ds_->setLastStoredCheckpoint(c); }
  void setIsFetchingState(bool b) override { ds_->setIsFetchingState(b); }
  void setCheckpointBeingFetched(const CheckpointDesc& c) override { ds_->setCheckpointBeingFetched(c); }
  void setFirstRequiredBlock(uint64_t i) override { ds_->setFirstRequiredBlock(i); }
  void setLastRequiredBlock(uint64_t i) override { ds_->setLastRequiredBlock(i); }
  void setFVal(uint16_t fVal) override { ds_->setFVal(fVal); }
  void free(ResPagesDescriptor* des) override { ds_->free(des); }
  void deleteAllPendingPages() override { ds_->deleteAllPendingPages(); }
  void deleteCheckpointBeingFetched() override { ds_->deleteCheckpointBeingFetched(); }
  void deleteDescOfSmallerCheckpoints(uint64_t chpt) override { ds_->deleteDescOfSmallerCheckpoints(chpt); }

  void deleteCoveredResPageInSmallerCheckpoints(uint64_t inCheckpoint) override {
    return ds_->deleteCoveredResPageInSmallerCheckpoints(inCheckpoint);
  }
  void associatePendingResPageWithCheckpoint(uint32_t inPageId,
                                             uint64_t inCheckpoint,
                                             const Digest& inPageDigest) override {
    ds_->associatePendingResPageWithCheckpoint(inPageId, inCheckpoint, inPageDigest);
  }
  void setCheckpointDesc(uint64_t checkpoint,
                         const CheckpointDesc& desc,
                         const bool checkIfAlreadyExists = true) override {
    ds_->setCheckpointDesc(checkpoint, desc, checkIfAlreadyExists);
  }
  void setFirstStoredCheckpoint(uint64_t c) override { ds_->setFirstStoredCheckpoint(c); }
  void setPendingResPage(uint32_t inPageId, const char* inPage, uint32_t pageLen) override {
    ds_->setPendingResPage(inPageId, inPage, pageLen);
  }
  void setResPage(uint32_t inPageId,
                  uint64_t inCheckpoint,
                  const Digest& inPageDigest,
                  const char* inPage,
                  const bool checkIfAlreadyExists = true) override {
    ds_->setResPage(inPageId, inCheckpoint, inPageDigest, inPage, checkIfAlreadyExists);
  }
  bool initialized() override { return ds_->initialized(); }
  bool getIsFetchingState() override { return ds_->getIsFetchingState(); }
  bool hasCheckpointBeingFetched() override { return ds_->hasCheckpointBeingFetched(); }
  bool hasCheckpointDesc(uint64_t checkpoint) override { return ds_->hasCheckpointDesc(checkpoint); }
  bool hasPendingResPage(uint32_t inPageId) override { return ds_->hasPendingResPage(inPageId); }
  uint16_t getMyReplicaId() override { return ds_->getMyReplicaId(); }
  uint16_t getFVal() override { return ds_->getFVal(); }
  uint64_t getMaxNumOfStoredCheckpoints() const override { return ds_->getMaxNumOfStoredCheckpoints(); }
  uint32_t getNumberOfReservedPages() override { return ds_->getNumberOfReservedPages(); }
  uint64_t getLastStoredCheckpoint() override { return ds_->getLastStoredCheckpoint(); }
  uint64_t getFirstStoredCheckpoint() override { return ds_->getFirstStoredCheckpoint(); }
  uint64_t getFirstRequiredBlock() override { return ds_->getFirstRequiredBlock(); }
  void setPrunedBlocksDigests(const std::vector<std::pair<BlockId, Digest>>& prunedBlocksDigests) override {
    ds_->setPrunedBlocksDigests(prunedBlocksDigests);
  }
  std::vector<std::pair<BlockId, Digest>> getPrunedBlocksDigests() override { return ds_->getPrunedBlocksDigests(); }
  uint64_t getLastRequiredBlock() override { return ds_->getLastRequiredBlock(); }
  uint32_t numOfAllPendingResPage() override { return ds_->numOfAllPendingResPage(); }
  set<uint32_t> getNumbersOfPendingResPages() override { return ds_->getNumbersOfPendingResPages(); }
  set<uint16_t> getReplicas() override { return ds_->getReplicas(); }

  CheckpointDesc getCheckpointDesc(uint64_t checkpoint) override { return ds_->getCheckpointDesc(checkpoint); }
  CheckpointDesc getCheckpointBeingFetched() override { return ds_->getCheckpointBeingFetched(); }

  void getPendingResPage(uint32_t inPageId, char* outPage, uint32_t pageLen) override {
    return ds_->getPendingResPage(inPageId, outPage, pageLen);
  }
  bool getResPage(uint32_t inPageId,
                  uint64_t inCheckpoint,
                  uint64_t* outActualCheckpoint,
                  Digest* outPageDigest,
                  char* outPage,
                  uint32_t copylength) override {
    return ds_->getResPage(inPageId, inCheckpoint, outActualCheckpoint, outPageDigest, outPage, copylength);
  }
  ResPagesDescriptor* getResPagesDescriptor(uint64_t inCheckpoint) override {
    return ds_->getResPagesDescriptor(inCheckpoint);
  }
  /**
   * ITransaction implementation
   */
  void commit() override { txn_->commit(); }
  void rollback() override { txn_->rollback(); };
  void put(const concordUtils::Sliver& key, const concordUtils::Sliver& value) override { txn_->put(key, value); }
  std::string get(const concordUtils::Sliver& key) override { return txn_->get(key); }
  void del(const concordUtils::Sliver& key) override { txn_->del(key); }

  void setEraseDataStoreFlag() override { ds_->setEraseDataStoreFlag(); }

 private:
  // You can't begin a transaction from within a transaction. However, since we
  // inherit from DataStore, we must implement this method.
  DataStoreTransaction* beginTransaction() override {
    ConcordAssert(false);
    return nullptr;
  }
  std::shared_ptr<DataStore> ds_;
  std::shared_ptr<ITransaction> txn_;
};

}  // namespace impl
}  // namespace bcst
}  // namespace bftEngine
