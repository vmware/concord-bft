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
#include <memory>

#include "KVBCInterfaces.h"
#include "blockchain/db_interfaces.h"
#include "blockchain/db_adapter.h"
#include "SimpleBCStateTransfer.hpp"
#include "Replica.hpp"
#include "Metrics.hpp"

using namespace bftEngine::SimpleBlockchainStateTransfer;

namespace SimpleKVBC {

class RequestsHandlerImp;

class ReplicaImp : public IReplica,
                   public concord::storage::blockchain::ILocalKeyValueStorageReadOnly,
                   public concord::storage::blockchain::IBlocksAppender,
                   public bftEngine::SimpleBlockchainStateTransfer::IAppState {
 public:
  // IReplica methods

  virtual Status start() override;
  virtual Status stop() override;
  virtual RepStatus getReplicaStatus() const override;
  virtual bool isRunning() const override;
  virtual void monitor() const override {}

  // ILocalKeyValueStorageReadOnly methods

  virtual Status get(Sliver key, Sliver& outValue) const override;
  virtual Status get(concordUtils::BlockId readVersion,
                     Sliver key,
                     Sliver& outValue,
                     concordUtils::BlockId& outBlock) const override;
  virtual concordUtils::BlockId getLastBlock() const override;
  virtual Status getBlockData(concordUtils::BlockId blockId,
                              concordUtils::SetOfKeyValuePairs& outBlockData) const override;
  Status mayHaveConflictBetween(Sliver key,
                                concordUtils::BlockId fromBlock,
                                concordUtils::BlockId toBlock,
                                bool& outRes) const override;
  virtual concord::storage::blockchain::ILocalKeyValueStorageReadOnlyIterator* getSnapIterator() const override;
  virtual Status freeSnapIterator(
      concord::storage::blockchain::ILocalKeyValueStorageReadOnlyIterator* iter) const override;

  // IBlocksAppender

  virtual Status addBlock(const concordUtils::SetOfKeyValuePairs& updates, concordUtils::BlockId& outBlockId) override;

  // IAppState

  virtual uint64_t getLastReachableBlockNum() override;
  virtual uint64_t getLastBlockNum() override;
  virtual bool hasBlock(uint64_t blockId) override;
  virtual bool getBlock(uint64_t blockId, char* outBlock, uint32_t* outBlockSize) override;
  virtual bool getPrevDigestFromBlock(uint64_t blockId, StateTransferDigest* outPrevBlockDigest) override;
  virtual bool putBlock(uint64_t blockId, char* block, uint32_t blockSize) override;

 protected:
  ReplicaImp();
  ~ReplicaImp();

  // methods
  Status addBlockInternal(const concordUtils::SetOfKeyValuePairs& updates, concordUtils::BlockId& outBlockId);
  Status getInternal(concordUtils::BlockId readVersion,
                     Sliver key,
                     Sliver& outValue,
                     concordUtils::BlockId& outBlock) const;
  void insertBlockInternal(concordUtils::BlockId blockId, Sliver block);
  Sliver getBlockInternal(concordUtils::BlockId blockId) const;
  concord::storage::blockchain::DBAdapter* getBcDbAdapter() const { return m_bcDbAdapter; }
  bool executeCommand(uint16_t clientId,
                      bool readOnly,
                      uint32_t requestSize,
                      const char* request,
                      uint32_t maxReplySize,
                      char* outReply,
                      uint32_t& outActualReplySize);

  // consts
  const ICommandsHandler* m_cmdHandler;

  // internal types
  class KeyIDPair  // represents <key,blockId>
  {
   public:
    const Sliver key;
    const concordUtils::BlockId blockId;

    KeyIDPair(Sliver s, concordUtils::BlockId i) : key(s), blockId(i) {}

    bool operator<(const KeyIDPair& k) const {
      int c = this->key.compare(k.key);
      if (c == 0)
        return this->blockId > k.blockId;
      else
        return c < 0;
    }

    bool operator==(const KeyIDPair& k) const {
      if (this->blockId != k.blockId) return false;
      return (this->key.compare(k.key) == 0);
    }
  };

  class StorageWrapperForIdleMode
      : public concord::storage::blockchain::ILocalKeyValueStorageReadOnly  // TODO(GG): do we want synchronization here
                                                                            // ?
  {
   private:
    const ReplicaImp* rep;

   public:
    StorageWrapperForIdleMode(const ReplicaImp* r);
    Status get(Sliver key, Sliver& outValue) const override;
    Status get(concordUtils::BlockId readVersion,
               Sliver key,
               Sliver& outValue,
               concordUtils::BlockId& outBlock) const override;
    concordUtils::BlockId getLastBlock() const override;
    Status getBlockData(concordUtils::BlockId blockId, concordUtils::SetOfKeyValuePairs& outBlockData) const override;
    Status mayHaveConflictBetween(Sliver key,
                                  concordUtils::BlockId fromBlock,
                                  concordUtils::BlockId toBlock,
                                  bool& outRes) const override;
    concord::storage::blockchain::ILocalKeyValueStorageReadOnlyIterator* getSnapIterator() const override;
    Status freeSnapIterator(concord::storage::blockchain::ILocalKeyValueStorageReadOnlyIterator* iter) const override;
    void monitor() const override {}
  };

  class StorageIterator : public concord::storage::blockchain::ILocalKeyValueStorageReadOnlyIterator {
   private:
    const ReplicaImp* rep;
    concordUtils::BlockId readVersion;
    concordUtils::KeyValuePair m_current;
    concordUtils::BlockId m_currentBlock;
    bool m_isEnd;
    concord::storage::IDBClient::IDBClientIterator* m_iter;

   public:
    StorageIterator(const ReplicaImp* r);
    virtual ~StorageIterator() {}
    virtual void setReadVersion(concordUtils::BlockId _readVersion) { readVersion = _readVersion; }
    virtual concordUtils::KeyValuePair first(concordUtils::BlockId readVersion,
                                             concordUtils::BlockId& actualVersion,
                                             bool& isEnd) override;
    virtual concordUtils::KeyValuePair first() override {
      concordUtils::BlockId block = m_currentBlock;
      concordUtils::BlockId dummy;
      bool dummy2;
      return first(block, dummy, dummy2);
    }  // TODO(SG): Not implemented originally!
    virtual concordUtils::KeyValuePair seekAtLeast(
        concordUtils::BlockId readVersion,
        concordUtils::Key key,
        concordUtils::BlockId& actualVersion,
        bool& isEnd) override;  // Assumes lexicographical ordering of the keys, seek the first element k >= key
    virtual concordUtils::KeyValuePair seekAtLeast(concordUtils::Key key) override {
      concordUtils::BlockId block = m_currentBlock;
      concordUtils::BlockId dummy;
      bool dummy2;
      return seekAtLeast(block, key, dummy, dummy2);
    }  // TODO(SG): Not implemented originally!
    virtual concordUtils::KeyValuePair next(concordUtils::BlockId readVersion,
                                            concordUtils::Key key,
                                            concordUtils::BlockId& actualVersion,
                                            bool& isEnd) override;  // Proceed to next element and return it
    virtual concordUtils::KeyValuePair next() override {
      concordUtils::BlockId block = m_currentBlock;
      concordUtils::BlockId dummy;
      bool dummy2;
      return next(block, getCurrent().first, dummy, dummy2);
    }                                                          // TODO(SG): Not implemented originally!
    virtual concordUtils::KeyValuePair getCurrent() override;  // Return current element without moving
    virtual bool isEnd() override;
    virtual Status freeInternalIterator();
  };

  bftEngine::Replica* m_replica;

  uint32_t maxBlockSize = 0;

  // data
  bool m_running;
  RepStatus m_currentRepStatus;
  StorageWrapperForIdleMode m_InternalStorageWrapperForIdleMode;

  concord::storage::blockchain::DBAdapter* m_bcDbAdapter;
  concordUtils::BlockId lastBlock = 0;

  // static methods
  static Sliver createBlockFromUpdates(const concordUtils::SetOfKeyValuePairs& updates,
                                       concordUtils::SetOfKeyValuePairs& outUpdatesInNewBlock,
                                       bftEngine::SimpleBlockchainStateTransfer::StateTransferDigest digestOfPrev);
  static concordUtils::SetOfKeyValuePairs fetchBlockData(Sliver block);

  // friends
  friend IReplica* createReplica(const ReplicaConfig& conf,
                                 bftEngine::ICommunication* comm,
                                 ICommandsHandler* _cmdHandler,
                                 std::shared_ptr<concordMetrics::Aggregator> aggregator);

  friend RequestsHandlerImp;
};

class RequestsHandlerImp : public bftEngine::RequestsHandler {
 public:
  ReplicaImp* m_Executor;
  int execute(uint16_t clientId,
              uint64_t sequenceNum,
              bool readOnly,
              uint32_t requestSize,
              const char* request,
              uint32_t maxReplySize,
              char* outReply,
              uint32_t& outActualReplySize) override;
};

}  // namespace SimpleKVBC
