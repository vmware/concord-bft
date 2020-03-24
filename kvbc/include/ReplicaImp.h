// Copyright 2018-2019 VMware, all rights reserved
//
// KV Blockchain replica definition.

#pragma once

#include <functional>
#include <map>
#include <string>
#include <atomic>

#include "bftengine/ICommunication.hpp"
#include "communication/CommFactory.hpp"
#include "bftengine/Replica.hpp"
#include "bftengine/ReplicaConfig.hpp"
#include "bcstatetransfer/SimpleBCStateTransfer.hpp"
#include "communication/StatusInfo.h"
#include "Logger.hpp"
#include "KVBCInterfaces.h"
#include "replica_state_sync_imp.hpp"
#include "db_adapter.h"
#include "db_interfaces.h"
#include "memorydb/client.h"
#include "bftengine/DbMetadataStorage.hpp"

namespace concord {
namespace kvbc {

class ReplicaInitException : public std::exception {
 public:
  explicit ReplicaInitException(const std::string &what) : msg(what){};

  virtual const char *what() const noexcept override { return msg.c_str(); }

 private:
  std::string msg;
};

class ReplicaImp : public IReplica,
                   public concord::storage::blockchain::ILocalKeyValueStorageReadOnly,
                   public concord::storage::blockchain::IBlocksAppender {
 public:
  // concord::kvbc::IReplica methods
  virtual Status start() override;
  virtual Status stop() override;

  virtual RepStatus getReplicaStatus() const override;

  virtual const concord::storage::blockchain::ILocalKeyValueStorageReadOnly &getReadOnlyStorage() override;

  virtual Status addBlockToIdleReplica(const concord::storage::SetOfKeyValuePairs &updates) override;

  virtual void set_command_handler(ICommandsHandler *handler) override;

  // concord::storage::ILocalKeyValueStorageReadOnly methods
  virtual Status get(const Sliver &key, Sliver &outValue) const override;

  virtual Status get(concord::storage::blockchain::BlockId readVersion,
                     const Sliver &key,
                     Sliver &outValue,
                     concord::storage::blockchain::BlockId &outBlock) const override;

  virtual concord::storage::blockchain::BlockId getLastBlock() const override;

  virtual Status getBlockData(concord::storage::blockchain::BlockId blockId,
                              concord::storage::SetOfKeyValuePairs &outBlockData) const override;

  virtual Status mayHaveConflictBetween(const Sliver &key,
                                        concord::storage::blockchain::BlockId fromBlock,
                                        concord::storage::blockchain::BlockId toBlock,
                                        bool &outRes) const override;

  virtual void monitor() const override;

  // concord::storage::IBlocksAppender
  virtual Status addBlock(const concord::storage::SetOfKeyValuePairs &updates,
                          concord::storage::blockchain::BlockId &outBlockId) override;

  bool isRunning() const override { return (m_currentRepStatus == RepStatus::Running); }

  ReplicaImp(bftEngine::ICommunication *comm,
             bftEngine::ReplicaConfig &config,
             concord::storage::blockchain::DBAdapter *dbAdapter,
             std::shared_ptr<concordMetrics::Aggregator> aggregator);

  void setReplicaStateSync(ReplicaStateSync *rss) { replicaStateSync_.reset(rss); }

  ~ReplicaImp() override;

 protected:
  // METHODS

  Status addBlockInternal(const concord::storage::SetOfKeyValuePairs &updates,
                          concord::storage::blockchain::BlockId &outBlockId);
  Status getInternal(concord::storage::blockchain::BlockId readVersion,
                     Sliver key,
                     Sliver &outValue,
                     concord::storage::blockchain::BlockId &outBlock) const;
  void insertBlockInternal(concord::storage::blockchain::BlockId blockId, Sliver block);
  Sliver getBlockInternal(concord::storage::blockchain::BlockId blockId) const;
  concord::storage::blockchain::DBAdapter *getBcDbAdapter() const { return m_bcDbAdapter; }

 private:
  void createReplicaAndSyncState();

  // INTERNAL TYPES

  // represents <key,blockId>
  class KeyIDPair {
   public:
    const Sliver key;
    const concord::storage::blockchain::BlockId blockId;

    KeyIDPair(Sliver s, concord::storage::blockchain::BlockId i) : key(s), blockId(i) {}

    bool operator<(const KeyIDPair &k) const {
      int c = this->key.compare(k.key);
      if (c == 0) {
        return this->blockId > k.blockId;
      } else {
        return c < 0;
      }
    }

    bool operator==(const KeyIDPair &k) const {
      if (this->blockId != k.blockId) {
        return false;
      }
      return (this->key.compare(k.key) == 0);
    }
  };

  // TODO(GG): do we want synchronization here ?
  class StorageWrapperForIdleMode : public concord::storage::blockchain::ILocalKeyValueStorageReadOnly {
   private:
    const ReplicaImp *rep;

   public:
    StorageWrapperForIdleMode(const ReplicaImp *r);

    virtual Status get(const Sliver &key, Sliver &outValue) const override;

    virtual Status get(concord::storage::blockchain::BlockId readVersion,
                       const Sliver &key,
                       Sliver &outValue,
                       concord::storage::blockchain::BlockId &outBlock) const override;
    virtual concord::storage::blockchain::BlockId getLastBlock() const override;

    virtual Status getBlockData(concord::storage::blockchain::BlockId blockId,
                                concord::storage::SetOfKeyValuePairs &outBlockData) const override;

    virtual Status mayHaveConflictBetween(const Sliver &key,
                                          concord::storage::blockchain::BlockId fromBlock,
                                          concord::storage::blockchain::BlockId toBlock,
                                          bool &outRes) const override;

    virtual void monitor() const override;
  };

  class BlockchainAppState : public bftEngine::SimpleBlockchainStateTransfer::IAppState {
   public:
    BlockchainAppState(ReplicaImp *const parent);

    virtual bool hasBlock(uint64_t blockId) override;
    virtual bool getBlock(uint64_t blockId, char *outBlock, uint32_t *outBlockSize) override;
    virtual bool getPrevDigestFromBlock(
        uint64_t blockId, bftEngine::SimpleBlockchainStateTransfer::StateTransferDigest *outPrevBlockDigest) override;
    virtual bool putBlock(uint64_t blockId, char *block, uint32_t blockSize) override;
    virtual uint64_t getLastReachableBlockNum() override;
    virtual uint64_t getLastBlockNum() override;
    virtual void wait() override;

   private:
    ReplicaImp *const m_ptrReplicaImpl = nullptr;
    concordlogger::Logger m_logger;

    // from IAppState. represents maximal block number n such that all
    // blocks 1 <= i <= n exist
    std::atomic<concord::storage::blockchain::BlockId> m_lastReachableBlock{0};

    friend class ReplicaImp;
  };

  // DATA
 private:
  concordlogger::Logger logger;
  RepStatus m_currentRepStatus;
  StorageWrapperForIdleMode m_InternalStorageWrapperForIdleMode;

  concord::storage::blockchain::DBAdapter *m_bcDbAdapter = nullptr;
  concord::storage::blockchain::BlockId m_lastBlock = 0;
  bftEngine::ICommunication *m_ptrComm = nullptr;
  bftEngine::ReplicaConfig m_replicaConfig;
  bftEngine::IReplica *m_replicaPtr = nullptr;
  ICommandsHandler *m_cmdHandler = nullptr;
  bftEngine::IStateTransfer *m_stateTransfer = nullptr;
  std::unique_ptr<BlockchainAppState> m_appState;
  concord::storage::DBMetadataStorage *m_metadataStorage = nullptr;
  std::unique_ptr<ReplicaStateSync> replicaStateSync_;
  std::shared_ptr<concordMetrics::Aggregator> aggregator_;
};

}  // namespace kvbc
}  // namespace concord
