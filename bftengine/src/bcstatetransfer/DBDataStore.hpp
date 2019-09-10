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

#include "DataStore.hpp"
#include "storage/db_metadata_storage.h"
#include "string.hpp"
#include "STDigest.hpp"
#include "Logger.hpp"

namespace bftEngine {
namespace SimpleBlockchainStateTransfer {
namespace impl  {

using concord::storage::IDBClient;
using concord::storage::ITransaction;
using concordUtils::Status;
using concord::storage::DBMetadataStorage;

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
  DBDataStore(std::shared_ptr<DBMetadataStorage> mdts,
              concord::storage::IDBClient::ptr dbc,
              uint32_t sizeOfReservedPage):
                  dbc_(dbc), sizeOfReservedPage_(sizeOfReservedPage){}
protected:
  /**
   * C-r for use with beginTransaction
   */
  DBDataStore(concord::storage::ITransaction::ptr txn, DBDataStore* parent):
                 txn_(txn),
                 sizeOfReservedPage_(parent->sizeOfReservedPage_){}
public:
  DataStoreTransaction* beginTransaction() override {
    concord::storage::ITransaction::ptr txn(dbc_->beginTransaction());
    return  new DataStoreTransaction(new DBDataStore(txn, this), txn);
  }

  void setAsInitialized() override;
  void setReplicas(const set<uint16_t> replicas)          override;
  void setMyReplicaId(uint16_t id)                        override;
  void setMaxNumOfStoredCheckpoints(uint64_t numChecks)   override;
  void setNumberOfReservedPages(uint32_t numResPages)     override;
  void setLastStoredCheckpoint(uint64_t c)                override;
  void setCheckpointDesc(uint64_t checkpoint, const CheckpointDesc& desc)      override;
  void setFirstStoredCheckpoint(uint64_t c)               override;
  void setPendingResPage(uint32_t inPageId, const char* inPage, uint32_t pageLen) override;
  void setResPage(uint32_t inPageId, uint64_t inCheckpoint, const STDigest& inPageDigest, const char* inPage) override;
  void setIsFetchingState(bool b)                         override;
  void setCheckpointBeingFetched(const CheckpointDesc& c) override;
  void setFirstRequiredBlock(uint64_t i)                  override;
  void setLastRequiredBlock(uint64_t i)                   override;
  void setFVal(uint16_t fVal) override;
  void associatePendingResPageWithCheckpoint(uint32_t inPageId,
                                           uint64_t inCheckpoint,
                                           const STDigest& inPageDigest) override;
  void deleteAllPendingPages() override;
  void deleteCheckpointBeingFetched() override;
  void deleteDescOfSmallerCheckpoints(uint64_t checkpoint) override;
  void deleteCoveredResPageInSmallerCheckpoints( uint64_t inCheckpoint) override;
  void free(ResPagesDescriptor*) override;
  bool initialized() override;
  set<uint16_t>  getReplicas() override;
  uint16_t       getMyReplicaId() override;
  uint16_t       getFVal() override;
  uint64_t       getMaxNumOfStoredCheckpoints() override;
  uint32_t       getNumberOfReservedPages() override;
  uint64_t       getLastStoredCheckpoint() override;
  uint64_t       getFirstStoredCheckpoint() override;
  CheckpointDesc getCheckpointDesc(uint64_t checkpoint) override;
  CheckpointDesc getCheckpointBeingFetched() override;
  bool           hasCheckpointDesc(uint64_t checkpoint) override;
  bool           hasPendingResPage(uint32_t inPageId) override;
  uint32_t       numOfAllPendingResPage() override;
  set<uint32_t>  getNumbersOfPendingResPages() override;
  void           getPendingResPage( uint32_t inPageId,char* outPage, uint32_t pageLen) override;
  void           getResPage(uint32_t inPageId, uint64_t inCheckpoint, uint64_t* outActualCheckpoint) override;
  void           getResPage(uint32_t inPageId, uint64_t inCheckpoint, uint64_t* outActualCheckpoint,
                            char* outPage, uint32_t copylength) override;
  void           getResPage(uint32_t inPageId, uint64_t inCheckpoint, uint64_t* outActualCheckpoint,
                            STDigest* outPageDigest, char* outPage, uint32_t copylength) override;
  ResPagesDescriptor* getResPagesDescriptor(uint64_t inCheckpoint) override;
  bool           getIsFetchingState() override;
  bool           hasCheckpointBeingFetched() override;
  uint64_t       getFirstRequiredBlock() override;
  uint64_t       getLastRequiredBlock() override;

 protected:
  void serializeCheckpoint  (std::ostream& os, const CheckpointDesc& desc);
  void deserializeCheckpoint(std::istream& is, CheckpointDesc& desc);
  /**
   * ResPage key layout: ResPage:inPageId:inChekpoint
   */
  std::string resPageKeyPrefix(uint32_t pageId) const { return "ResPage" + KEY_DELIM + std::to_string(pageId);}
  std::string resPageKey(uint64_t checkpoint, uint32_t pageId) const {
    return resPageKeyPrefix(pageId) + KEY_DELIM + std::to_string(checkpoint);
  }
  std::string pendingPageKey(uint32_t inPageId) const {
    return "PendingResPage" + KEY_DELIM + std::to_string(inPageId);
  }
  // concord::storage::blockchain::EDBKeyType::E_DB_KEY_TYPE_BFT_METADATA_KEY = 3
  const std::string KEY_PREFIX = "\3:STATE_TRANSFER:";
  const std::string KEY_DELIM = ":";
  void put(const std::string& key, const std::string& val){
    LOG_TRACE(logger(), "put key:" <<  KEY_PREFIX + key << " val:" << val << txn_? "txn: " + std::to_string(txn_->getId()):"");
    if (txn_)
       txn_->put(KEY_PREFIX + key, val);
    else
       dbc_->put(KEY_PREFIX + key, val);
  }
  /**
   * @return true if key found, false if key not found
   * @throw  otherwise
   */
  bool get(const std::string& key, concordUtils::Sliver& val){
    LOG_TRACE(logger(), "get key:" <<  KEY_PREFIX + key );
    Status s = dbc_->get(KEY_PREFIX + key, val);
    if(!(s.isOK() || s.isNotFound()))
        throw std::runtime_error("error get key: " + key + std::string(", reason: ") + s.toString());
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
  bool del(const std::string& key){
    LOG_TRACE(logger(), "delete key:" <<  KEY_PREFIX + key );
    Status s = dbc_->del(KEY_PREFIX + key);
    if(!(s.isOK() || s.isNotFound()))
        throw std::runtime_error("error del key: " + key + std::string(", reason: ") + s.toString());
    if(s.isNotFound()){
      LOG_ERROR(logger(), "not found: key: " + key);
      return false;
    }
    return true;
  }
  /**
   * convenience functions for integral types
   */
  template<typename T>
  void putInt(const std::string& key, T val) { put(key, std::to_string(val)); }
  template<typename T>
  T get(const std::string& key){
    concordUtils::Sliver val;
    get(key, val);
    std::string s(reinterpret_cast<char*>(val.data()), val.length());
    return concord::util::to<T>(s);
  }

  IDBClient::IDBClientIterator* getIterator(const std::string& key){
    IDBClient::IDBClientIterator* it = dbc_->getIterator();
    it->seekAtLeast(KEY_PREFIX + key);
    return it;
  }

  static concordlogger::Logger& logger(){
      static concordlogger::Logger logger_ = concordlogger::Log::getLogger("DBDataStore");
      return logger_;
    }

  ITransaction::ptr txn_;
  IDBClient::ptr    dbc_;
  uint32_t          sizeOfReservedPage_;
};

}  // namespace impl
}  // namespace SimpleBlockchainStateTransfer
}  // namespace bftEngine
