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
#include "storage/db_interface.h"
#include "STDigest.hpp"

using std::set;
using concord::storage::ITransaction;

namespace bftEngine {
namespace SimpleBlockchainStateTransfer {
namespace impl {

class DataStoreTransaction;

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
    uint64_t checkpointNum = 0;
    uint64_t lastBlock     = 0;
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

  virtual void setPendingResPage(
               uint32_t inPageId, const char* inPage, uint32_t pageLen) = 0;
  virtual bool hasPendingResPage(uint32_t inPageId) = 0;
  virtual void getPendingResPage(
                    uint32_t inPageId, char* outPage, uint32_t pageLen) = 0;
  virtual uint32_t numOfAllPendingResPage() = 0;
  virtual set<uint32_t> getNumbersOfPendingResPages() = 0;
  virtual void deleteAllPendingPages() = 0;

  virtual void associatePendingResPageWithCheckpoint(uint32_t inPageId,
                    uint64_t inCheckpoint, const STDigest& inPageDigest) = 0;

  virtual void setResPage(uint32_t inPageId, uint64_t inCheckpoint,
                       const STDigest& inPageDigest, const char* inPage) = 0;
  virtual void getResPage(uint32_t inPageId,
                   uint64_t inCheckpoint, uint64_t* outActualCheckpoint) = 0;
  virtual void getResPage(uint32_t inPageId, uint64_t inCheckpoint,
      uint64_t* outActualCheckpoint, char* outPage, uint32_t copylength) = 0;
  virtual void getResPage(uint32_t inPageId, uint64_t inCheckpoint,
       uint64_t* outActualCheckpoint, STDigest* outPageDigest, char* outPage,
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

    size_t size() const  { return size(numOfPages); }

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

  // Transaction support
  virtual DataStoreTransaction* beginTransaction() = 0;
};

/** *******************************************************************************************************************
 *
 */
class DataStoreTransaction: public DataStore,
                            public ITransaction {
public:
  class Guard: public ITransaction::Guard {
   public:
     Guard(DataStoreTransaction* t): ITransaction::Guard(t){}
     DataStoreTransaction* txn() {return static_cast<DataStoreTransaction*>(txn_);}
  };
  typedef std::shared_ptr<DataStoreTransaction> ptr;
  DataStoreTransaction(DataStore* ds, ITransaction::ptr txn):
    ITransaction(txn->getId()),ds_(ds), txn_(txn){}
  ~DataStoreTransaction(){}
  /**
   * DataStore implementation
   */
  void setAsInitialized()                                 override {ds_->setAsInitialized();}
  void setReplicas(const set<uint16_t> replicas)          override {ds_->setReplicas(replicas);}
  void setMyReplicaId(uint16_t id)                        override {ds_->setMyReplicaId(id);}
  void setMaxNumOfStoredCheckpoints(uint64_t numChp)      override {ds_->setMaxNumOfStoredCheckpoints(numChp);}
  void setNumberOfReservedPages(uint32_t numResPgs)       override {ds_->setNumberOfReservedPages(numResPgs);}
  void setLastStoredCheckpoint(uint64_t c)                override {ds_->setLastStoredCheckpoint(c);}
  void setIsFetchingState(bool b)                         override {ds_->setIsFetchingState(b);}
  void setCheckpointBeingFetched(const CheckpointDesc& c) override {ds_->setCheckpointBeingFetched(c);}
  void setFirstRequiredBlock(uint64_t i)                  override {ds_->setFirstRequiredBlock(i);}
  void setLastRequiredBlock(uint64_t i)                   override {ds_->setLastRequiredBlock(i);}
  void setFVal(uint16_t fVal)                             override {ds_->setFVal(fVal);}
  void free(ResPagesDescriptor* des)                      override {ds_->free(des);}
  void deleteAllPendingPages()                            override {ds_->deleteAllPendingPages();}
  void deleteCheckpointBeingFetched()                     override {ds_->deleteCheckpointBeingFetched();}
  void deleteDescOfSmallerCheckpoints(uint64_t chpt)      override {ds_->deleteDescOfSmallerCheckpoints(chpt);}

  void deleteCoveredResPageInSmallerCheckpoints( uint64_t inCheckpoint) override {
    return ds_->deleteCoveredResPageInSmallerCheckpoints(inCheckpoint);
  }
  void associatePendingResPageWithCheckpoint(uint32_t inPageId,
                                             uint64_t inCheckpoint,
                                             const STDigest& inPageDigest) override {
    ds_->associatePendingResPageWithCheckpoint(inPageId, inCheckpoint, inPageDigest);
  }
  void setCheckpointDesc(uint64_t checkpoint,
                         const CheckpointDesc& desc) override {ds_->setCheckpointDesc(checkpoint, desc);}
  void setFirstStoredCheckpoint(uint64_t c)          override {ds_->setFirstStoredCheckpoint(c);}
  void setPendingResPage(uint32_t inPageId,
                         const char* inPage,
                         uint32_t pageLen) override {ds_->setPendingResPage(inPageId, inPage, pageLen);}
  void setResPage(uint32_t inPageId,
                  uint64_t inCheckpoint,
                  const STDigest& inPageDigest,
                  const char* inPage) override {ds_->setResPage(inPageId, inCheckpoint, inPageDigest, inPage);}
  bool     initialized()                          override {return ds_->initialized();}
  bool     getIsFetchingState()                   override {return ds_->getIsFetchingState();}
  bool     hasCheckpointBeingFetched()            override {return ds_->hasCheckpointBeingFetched();}
  bool     hasCheckpointDesc(uint64_t checkpoint) override {return ds_->hasCheckpointDesc(checkpoint);}
  bool     hasPendingResPage(uint32_t inPageId)   override {return ds_->hasPendingResPage(inPageId);}
  uint16_t getMyReplicaId()                       override {return ds_->getMyReplicaId();}
  uint16_t getFVal()                              override {return ds_->getFVal();}
  uint64_t getMaxNumOfStoredCheckpoints()         override {return ds_->getMaxNumOfStoredCheckpoints();}
  uint32_t getNumberOfReservedPages()             override {return ds_->getNumberOfReservedPages();}
  uint64_t getLastStoredCheckpoint()              override {return ds_->getLastStoredCheckpoint();}
  uint64_t getFirstStoredCheckpoint()             override {return ds_->getFirstStoredCheckpoint();}
  uint64_t getFirstRequiredBlock()                override {return ds_->getFirstRequiredBlock();}
  uint64_t getLastRequiredBlock()                 override {return ds_->getLastRequiredBlock();}
  uint32_t numOfAllPendingResPage()               override {return ds_->numOfAllPendingResPage();}
  set<uint32_t>  getNumbersOfPendingResPages()    override {return ds_->getNumbersOfPendingResPages();}
  set<uint16_t>  getReplicas()                    override {return ds_->getReplicas();}

  CheckpointDesc getCheckpointDesc(uint64_t checkpoint) override {return ds_->getCheckpointDesc(checkpoint);}
  CheckpointDesc getCheckpointBeingFetched()            override {return ds_->getCheckpointBeingFetched();}

  void getPendingResPage(uint32_t inPageId,
                         char* outPage,
                         uint32_t pageLen) override { return ds_->getPendingResPage(inPageId,outPage, pageLen);}
  void getResPage(uint32_t inPageId,
                  uint64_t inCheckpoint,
                  uint64_t* outActualCheckpoint) override {
    return ds_->getResPage(inPageId, inCheckpoint, outActualCheckpoint);
  }
  void getResPage(uint32_t inPageId,
                  uint64_t inCheckpoint,
                  uint64_t* outActualCheckpoint,
                  char* outPage,
                  uint32_t copylength) override {
    return ds_->getResPage(inPageId, inCheckpoint, outActualCheckpoint, outPage, copylength);
  }
  void getResPage( uint32_t inPageId,
                   uint64_t inCheckpoint,
                   uint64_t* outActualCheckpoint,
                   STDigest* outPageDigest,
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
  void commit()   override {txn_->commit();}
  void rollback() override {txn_->rollback();};
  void put(const concordUtils::Sliver& key, const concordUtils::Sliver& value) override {txn_->put(key, value);}
  std::string get(const concordUtils::Sliver& key) override {return txn_->get(key);}
  void del(const concordUtils::Sliver& key) override {txn_->del(key);}

protected:
  DataStoreTransaction* beginTransaction() override {assert(false); return nullptr;}
  DataStore*  ds_; // TODO[TK] different behavior between DBDataStore and InMemoryDataStore
  ITransaction::ptr          txn_;

};

}  // namespace impl
}  // namespace SimpleBlockchainStateTransfer
}  // namespace bftEngine

