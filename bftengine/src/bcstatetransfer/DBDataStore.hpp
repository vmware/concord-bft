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
#include "blockchain/db_types.h"

namespace bftEngine {
namespace SimpleBlockchainStateTransfer {
namespace impl  {

using concord::storage::IDBClient;
using concord::storage::ITransaction;
using concordUtils::Status;
using concordUtils::Sliver;
using concord::storage::blockchain::EDBKeyType;
/** *******************************************************************************************************************
 *  This class is used in one of two modes:
 *  1. When ITransaction is not set - works directly through MetadataStorage instance;
 *  2. When ITransaction is set - is invoked via DBDataStoreTransaction.
 */
class DBDataStore: public DataStore {
public:
  /**
   * C-r for DBDataStore first time initialization
   */
  DBDataStore( std::unique_ptr<concord::storage::IDBClient> dbc, uint32_t sizeOfReservedPage):
                inmem_(InMemoryDataStore(sizeOfReservedPage)), dbc_(std::move(dbc)){
    load();
  }

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
  bool           hasCheckpointDesc(uint64_t checkpoint) override { return inmem_.hasCheckpointDesc(checkpoint);}
  bool           hasPendingResPage(uint32_t inPageId) override {return inmem_.hasPendingResPage(inPageId);}
  bool           getIsFetchingState() const override { return inmem_.getIsFetchingState(); }
  bool           hasCheckpointBeingFetched() override { return inmem_.hasCheckpointBeingFetched();}
  uint16_t       getMyReplicaId() const override { return inmem_.getMyReplicaId();}
  uint16_t       getFVal() const override  { return inmem_.getFVal(); }
  uint32_t       numOfAllPendingResPage() override { return inmem_.numOfAllPendingResPage();}
  uint32_t       getNumberOfReservedPages() const override {return inmem_.getNumberOfReservedPages();}
  uint64_t       getMaxNumOfStoredCheckpoints() const override  { return inmem_.getMaxNumOfStoredCheckpoints(); }
  uint64_t       getLastStoredCheckpoint() const override {return inmem_.getLastStoredCheckpoint();}
  uint64_t       getFirstStoredCheckpoint() const override {return inmem_.getFirstStoredCheckpoint();}
  uint64_t       getFirstRequiredBlock() const override  { return inmem_.getFirstRequiredBlock(); }
  uint64_t       getLastRequiredBlock() const override {return inmem_.getLastRequiredBlock();}
  CheckpointDesc getCheckpointDesc(uint64_t checkpoint) override;
  CheckpointDesc getCheckpointBeingFetched() const override {return inmem_.getCheckpointBeingFetched();}
  set<uint16_t>  getReplicas() const override { return inmem_.getReplicas();}
  set<uint32_t>  getNumbersOfPendingResPages() const override { return inmem_.getNumbersOfPendingResPages(); }


  void getPendingResPage( uint32_t inPageId, char* outPage, uint32_t pageLen) const override {
    inmem_.getPendingResPage(inPageId, outPage, pageLen);
  }
  ResPagesDescriptor* getResPagesDescriptor(uint64_t inCheckpoint) const override {
    return inmem_.getResPagesDescriptor(inCheckpoint);
  }
  void getResPage(uint32_t inPageId, uint64_t inCheckpoint, uint64_t* outActualCheckpoint) const override {
     inmem_.getResPage(inPageId, inCheckpoint, outActualCheckpoint);
  }
  void getResPage(uint32_t inPageId, uint64_t inCheckpoint, uint64_t* outActualCheckpoint,
                  char* outPage, uint32_t copylength) const override {
     inmem_.getResPage(inPageId, inCheckpoint, outActualCheckpoint, outPage, copylength);
  }
  void getResPage(uint32_t inPageId, uint64_t inCheckpoint, uint64_t* outActualCheckpoint,
                  STDigest* outPageDigest, char* outPage, uint32_t copylength) const override {
    inmem_.getResPage(inPageId, inCheckpoint, outActualCheckpoint, outPageDigest, outPage, copylength);
  }
 protected:
  typedef std::uint64_t ObjectId;

  enum class EDBKeySubType: std::uint8_t {
    General = (int)EDBKeyType::E_DB_KEY_TYPE_LAST + 1,
    ReservedPagesDynamicId,
    CheckPointDesc
  };
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
    CheckpointBeingFetched,
    PendingPages,
    ReservedPagesStaticId = PendingPages + 1000,
  };
  /**
   * C-r for use with beginTransaction
   */
  DBDataStore(const DBDataStore& ) = default;

  void load();
  void loadDynamicKeys();
  void loadResPages();
  void loadPendingPages();

  void serializeCheckpoint   (std::ostream& os, const CheckpointDesc& desc) const;
  void deserializeCheckpoint (std::istream& is,       CheckpointDesc& desc) const;

  void serializeResPage      (std::ostream&, uint32_t,  uint64_t, const STDigest&, const char*) const;
  void deserializeResPage    (std::istream&, uint32_t&, uint64_t&,      STDigest&,       char*) const;

  void deserializePendingPage(std::istream&, char*, uint32_t&) const;

  /**
   * adds to existing transaction
   */
  void setResPageTxn( uint32_t ,uint64_t ,const STDigest&,const char*, ITransaction*);
  /** *****************************************************************************************************************
   * db layer access
   */
  void put(const GeneralIds& objId, const std::string& val) {
    put(objId, val, nullptr);
  }
  void put(const GeneralIds& objId, const std::string& val, ITransaction* txn) {
    put(genKey(objId), val, txn);
  }


  void put(const std::string& key, const std::string& val) {
    put(key, val, nullptr);
  }
  void put(const std::string& key, const std::string& val, ITransaction* txn) {
    if (txn){
       LOG_TRACE(logger(), "put objId:" << key << " val:" << val << " txn: " + std::to_string(txn->getId()));
       txn->put(key, val);
    }
    else{
      LOG_TRACE(logger(), "put objId:" << key << " val:" << val);
      dbc_->put(key, val);
    }
  }
  /**
   * @return true if key found, false if key not found
   * @throw  otherwise
   */
  bool get(GeneralIds objId, Sliver& val){
    auto key = genKey(objId);
    return get(key, val);
  }
  bool get(const std::string& key, Sliver& val){
    LOG_TRACE(logger(), "get objId:" << key);
    Status s = dbc_->get(key, val);
    if(!(s.isOK() || s.isNotFound()))
        throw std::runtime_error("error get objId: " + key + std::string(", reason: ") + s.toString());
    if(s.isNotFound()){
      LOG_ERROR(logger(), "not found: key: " + key);
      return false;
    }
    return true;
  }

  /**
   * @return true if key found, false if key not found
   * @throw  otherwise
   */
  bool del(GeneralIds objId){ return del(genKey(objId));}
  bool del(const Sliver& key){
    LOG_TRACE(logger(), "delete k.ey:" <<  key.toString());
    Status s = dbc_->del(key);
    if(!(s.isOK() || s.isNotFound()))
        throw std::runtime_error("error del key: " + key.toString() + std::string(", reason: ") + s.toString());
    if(s.isNotFound()){
      LOG_ERROR(logger(), "not found: key: " + key.toString());
      return false;
    }
    return true;
  }

  /**
   * convenience functions for integral types
   */
  template<typename T>
  void putInt(const GeneralIds& objId, T val) { putInt(objId, val, nullptr); }

  template<typename T>
  void putInt(const GeneralIds& objId, T val, ITransaction* txn) { put(objId, std::to_string(val), txn); }

  template<typename T>
  T get(const ObjectId& objId){
    concordUtils::Sliver val;
    if(!get(genKey(objId), val)) return 0;
    std::string s(reinterpret_cast<char*>(val.data()), val.length());
    return concord::util::to<T>(s);
  }
  /** *****************************************************************************************************************
   * keys generation
   */
  std::string dynamicResPageKey (uint32_t pageid, uint64_t chkp) const {
    return genKey(EDBKeySubType::ReservedPagesDynamicId, pageid, chkp);
  }
  std::string staticResPageKey  (uint32_t pageid, uint64_t chkp) const {
    static uint64_t maxStored = inmem_.getMaxNumOfStoredCheckpoints();
    return genKey(ReservedPagesStaticId + pageid * (chkp % maxStored));
  }
  std::string pendingPageKey (uint32_t pageid) const { return genKey(PendingPages + pageid);}
  std::string chkpDescKey    (uint64_t chkp)   const { return genKey(EDBKeySubType::CheckPointDesc, chkp); }
  const std::string common_prefix  = std::string(1, (char)EDBKeyType::E_DB_KEY_TYPE_BFT_METADATA_KEY);
  const std::string general_prefix = std::string(1, (char)EDBKeySubType::General);

  std::string genKey(const ObjectId& objId) const {
    static std::string prefix = common_prefix + general_prefix;
    return prefix + std::to_string(objId);
  }
  std::string genKey(const EDBKeySubType& subtype, const uint32_t& pageid, const uint64_t& chkp) const {
    return common_prefix + std::string(1, (char)subtype) + std::to_string(pageid) + std::to_string(chkp);
  }
  std::string genKey(const EDBKeySubType& subtype, const uint32_t& pageid) const {
    return common_prefix + std::string(1, (char)subtype) + std::to_string(pageid);
  }
  std::string genKey(const EDBKeySubType& subtype, const uint64_t& chkp) const {
    return common_prefix + std::string(1, (char)subtype) + std::to_string(chkp);
  }
  /** ****************************************************************************************************************/
  concordlogger::Logger& logger(){
      static concordlogger::Logger logger_ = concordlogger::Log::getLogger("DBDataStore");
      return logger_;
  }

protected:
  InMemoryDataStore inmem_;
  std::unique_ptr<concord::storage::IDBClient> dbc_;
  std::map<InMemoryDataStore::ResPageKey, std::string> keysofkeys_;
};

}  // namespace impl
}  // namespace SimpleBlockchainStateTransfer
}  // namespace bftEngine
