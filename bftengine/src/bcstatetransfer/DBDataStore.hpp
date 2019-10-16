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

#include <map>
#include <set>

#include "string.hpp"
#include "STDigest.hpp"
#include "Logger.hpp"
#include "InMemoryDataStore.hpp"
#include "blockchain/db_adapter.h"

namespace bftEngine {
namespace SimpleBlockchainStateTransfer {
namespace impl  {

using concord::storage::IDBClient;
using concord::storage::ITransaction;
using concordUtils::Status;
using concordUtils::Sliver;
using concord::storage::blockchain::EDBKeyType;
using concord::storage::blockchain::ObjectId;
using concord::storage::blockchain::KeyManipulator;
/** *******************************************************************************************************************
 *  This class is used in one of two modes:
 *  1. When ITransaction is not set - works directly through IDBClient instance;
 *  2. When ITransaction is set - is invoked via DBDataStoreTransaction.
 */
class DBDataStore: public DataStore {
public:
  /**
   * C-r for DBDataStore first time initialization
   */
  DBDataStore( concord::storage::IDBClient::ptr dbc, uint32_t sizeOfReservedPage):
                inmem_(sizeOfReservedPage),
                dbc_(dbc){
    load();
  }

  DataStoreTransaction* beginTransaction() override;

  void setAsInitialized()                       override;
  void setReplicas(const set<uint16_t>)         override;
  void setMyReplicaId(uint16_t)                 override;
  void setMaxNumOfStoredCheckpoints(uint64_t)   override;
  void setNumberOfReservedPages(uint32_t)       override;
  void setLastStoredCheckpoint(uint64_t)        override;
  void setFirstStoredCheckpoint(uint64_t)       override;
  void setIsFetchingState(bool)                 override;
  void setFirstRequiredBlock(uint64_t)          override;
  void setLastRequiredBlock(uint64_t)           override;
  void setFVal(uint16_t)                        override;
  void deleteAllPendingPages()                  override;
  void deleteCheckpointBeingFetched()           override;
  void deleteDescOfSmallerCheckpoints(uint64_t) override;
  void deleteCoveredResPageInSmallerCheckpoints( uint64_t)          override;
  void setCheckpointBeingFetched(const CheckpointDesc&)             override;
  void setResPage(uint32_t, uint64_t, const STDigest&, const char*) override;
  void setPendingResPage(uint32_t, const char*, uint32_t)           override;
  void setCheckpointDesc(uint64_t, const CheckpointDesc&)           override;
  void associatePendingResPageWithCheckpoint(uint32_t, uint64_t, const STDigest&) override;

  void           free(ResPagesDescriptor*  desc) override {inmem_.free(desc);}
  bool           initialized() override { return inmem_.initialized();}
  bool           hasCheckpointDesc(uint64_t checkpoint) override;
  bool           hasPendingResPage(uint32_t inPageId) override {return inmem_.hasPendingResPage(inPageId);}
  bool           getIsFetchingState() override { return inmem_.getIsFetchingState(); }
  bool           hasCheckpointBeingFetched() override { return inmem_.hasCheckpointBeingFetched();}
  uint16_t       getMyReplicaId() override { return inmem_.getMyReplicaId();}
  uint16_t       getFVal() override  { return inmem_.getFVal(); }
  uint32_t       numOfAllPendingResPage() override { return inmem_.numOfAllPendingResPage();}
  uint32_t       getNumberOfReservedPages() override {return inmem_.getNumberOfReservedPages();}
  uint64_t       getMaxNumOfStoredCheckpoints() const override { return inmem_.getMaxNumOfStoredCheckpoints(); }
  uint64_t       getLastStoredCheckpoint() override {return inmem_.getLastStoredCheckpoint();}
  uint64_t       getFirstStoredCheckpoint() override {return inmem_.getFirstStoredCheckpoint();}
  uint64_t       getFirstRequiredBlock() override  { return inmem_.getFirstRequiredBlock(); }
  uint64_t       getLastRequiredBlock() override {return inmem_.getLastRequiredBlock();}
  CheckpointDesc getCheckpointDesc(uint64_t checkpoint) override;
  CheckpointDesc getCheckpointBeingFetched() override {return inmem_.getCheckpointBeingFetched();}
  set<uint16_t>  getReplicas() override { return inmem_.getReplicas();}
  set<uint32_t>  getNumbersOfPendingResPages() override { return inmem_.getNumbersOfPendingResPages(); }


  void getPendingResPage( uint32_t inPageId, char* outPage, uint32_t pageLen) override {
    inmem_.getPendingResPage(inPageId, outPage, pageLen);
  }
  ResPagesDescriptor* getResPagesDescriptor(uint64_t inCheckpoint) override {
    return inmem_.getResPagesDescriptor(inCheckpoint);
  }
  void getResPage(uint32_t inPageId, uint64_t inCheckpoint, uint64_t* outActualCheckpoint) override {
     inmem_.getResPage(inPageId, inCheckpoint, outActualCheckpoint);
  }
  void getResPage(uint32_t inPageId, uint64_t inCheckpoint, uint64_t* outActualCheckpoint,
                  char* outPage, uint32_t copylength) override {
     inmem_.getResPage(inPageId, inCheckpoint, outActualCheckpoint, outPage, copylength);
  }
  void getResPage(uint32_t inPageId, uint64_t inCheckpoint, uint64_t* outActualCheckpoint,
                  STDigest* outPageDigest, char* outPage, uint32_t copylength) override {
    inmem_.getResPage(inPageId, inCheckpoint, outActualCheckpoint, outPageDigest, outPage, copylength);
  }
 protected:

  void endTransaction() override;

  enum GeneralIds: ObjectId {
    Initialized = 1,
    MyReplicaId,
    MaxNumOfStoredCheckpoints,
    NumberOfReservedPages,
    LastStoredCheckpoint,
    FirstStoredCheckpoint,
    IsFetchingState,
    fVal,
    FirstRequiredBlock,
    LastRequiredBlock,
    Replicas,
    CheckpointBeingFetched
  };

  void load();
  void loadResPages();
  void loadPendingPages();

  void serializeCheckpoint   (std::ostream& os, const CheckpointDesc& desc) const;
  void deserializeCheckpoint (std::istream& is,       CheckpointDesc& desc) const;

  void serializeResPage      (std::ostream&, uint32_t,  uint64_t, const STDigest&, const char*) const;
  void deserializeResPage    (std::istream&, uint32_t&, uint64_t&,      STDigest&,       char*&) const;

  void deserializePendingPage(std::istream&, char*&, uint32_t&) const;

  /**
   * add to existing transaction
   */
  void setResPageTxn( uint32_t ,uint64_t ,const STDigest&,const char*, ITransaction*);
  void associatePendingResPageWithCheckpointTxn(uint32_t, uint64_t,const STDigest&, ITransaction*);
  void deleteAllPendingPagesTxn(ITransaction*);
  void deleteCoveredResPageInSmallerCheckpointsTxn( uint64_t, ITransaction*);
  void deleteDescOfSmallerCheckpointsTxn(uint64_t, ITransaction*);
  /** *****************************************************************************************************************
   * db layer access
   */
  void put(const GeneralIds& objId, const Sliver& val){
    put(genKey(objId), val);
  }
  void put(const Sliver& key, const Sliver& val){
    if (txn_){
       LOG_TRACE(logger(), "put objId:" << key << " val: " << val << " txn: " + txn_->getIdStr());
       txn_->put(key, val);
    }
    else{
      LOG_TRACE(logger(), "put objId:" << key << " val: " << val);
      dbc_->put(key, val);
    }
  }
  /**
   * @return true if key found, false if key not found
   * @throw  otherwise
   */
  bool get(GeneralIds objId, Sliver& val){ return get(genKey(objId), val);}

  bool get(const Sliver& key, Sliver& val){
    Status s = dbc_->get(key, val);
    if(!(s.isOK() || s.isNotFound()))
        throw std::runtime_error("error get objId: " + key.toString() + std::string(", reason: ") + s.toString());
    if(s.isNotFound()){
      LOG_TRACE(logger(), "not found: key: " <<  key);
      return false;
    }
    LOG_TRACE(logger(), "get objId:" << key << " val: " << val);
    return true;
  }
  /**
   * @return true if key found, false if key not found
   * @throw  otherwise
   */
  bool del(GeneralIds objId){ return del(genKey(objId));}
  bool del(const Sliver& key){
    LOG_TRACE(logger(), "delete k.ey:" <<  key);
    Status s = dbc_->del(key);
    if(!(s.isOK() || s.isNotFound()))
        throw std::runtime_error("error del key: " + key.toString() + std::string(", reason: ") + s.toString());
    if(s.isNotFound()){
      LOG_ERROR(logger(), "not found: key: " <<  key);
      return false;
    }
    return true;
  }
  /**
   * convenience functions for integral types
   */
  template<typename T>
  void putInt(const GeneralIds& objId, T val) { put(genKey(objId), std::to_string(val)); }
  template<typename T>
  T get(const ObjectId& objId){
    concordUtils::Sliver val;
    if(!get(genKey(objId), val))
      return 0;
    std::string s(reinterpret_cast<char*>(val.data()), val.length());
    return concord::util::to<T>(s);
  }
  /** *****************************************************************************************************************
   * keys generation
   */
  Sliver dynamicResPageKey (uint32_t pageid, uint64_t chkp) const {
    return KeyManipulator::generateStateTransferKey(EDBKeyType::E_DB_KEY_TYPE_BFT_ST_TRAN_RES_PAGE_DYN_KEY, pageid, chkp);
  }
  Sliver staticResPageKey  (uint32_t pageid, uint64_t chkp) const {
    static uint64_t maxStored = inmem_.getMaxNumOfStoredCheckpoints();
    return KeyManipulator::generateStateTransferKey(EDBKeyType::E_DB_KEY_TYPE_BFT_ST_TRAN_RES_PAGE_STAT_KEY,
                                                    pageid ,
                                                    (uint64_t)(chkp % maxStored));
  }
  Sliver pendingPageKey(uint32_t pageid) const {
    return KeyManipulator::generateStateTransferKey(EDBKeyType::E_DB_KEY_TYPE_BFT_ST_TRAN_PEN_PAGE_KEY, pageid);
  }
  Sliver chkpDescKey (uint64_t chkp)  const {
    return KeyManipulator::generateStateTransferKey(EDBKeyType::E_DB_KEY_TYPE_BFT_ST_TRAN_CHKP_DESC_KEY, chkp);
  }
  Sliver genKey(const ObjectId& objId) const {
    return KeyManipulator::generateStateTransferKey(objId);
  }
  /** ****************************************************************************************************************/
  concordlogger::Logger& logger(){
      static concordlogger::Logger logger_ = concordlogger::Log::getLogger("DBDataStore");
      return logger_;
  }

protected:
  InMemoryDataStore inmem_;
  ITransaction*     txn_ = nullptr;
  IDBClient::ptr    dbc_;
};

}  // namespace impl
}  // namespace SimpleBlockchainStateTransfer
}  // namespace bftEngine
