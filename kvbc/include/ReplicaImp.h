// Copyright 2018-2020 VMware, all rights reserved
//
// KV Blockchain replica definition.

#pragma once

#include <functional>
#include <map>
#include <string>
#include <atomic>

#include "OpenTracing.hpp"
#include "communication/ICommunication.hpp"
#include "communication/CommFactory.hpp"
#include "bftengine/Replica.hpp"
#include "bftengine/ReplicaConfig.hpp"
#include "bcstatetransfer/SimpleBCStateTransfer.hpp"
#include "communication/StatusInfo.h"
#include "Logger.hpp"
#include "KVBCInterfaces.h"
#include "replica_state_sync_imp.hpp"
#include "db_adapter_interface.h"
#include "db_interfaces.h"
#include "memorydb/client.h"
#include "bftengine/DbMetadataStorage.hpp"
#include "storage_factory_interface.h"
#include "ControlStateManager.hpp"
#include "diagnostics.h"
#include "performance_handler.h"

namespace concord::kvbc {

class ReplicaImp : public IReplica,
                   public ILocalKeyValueStorageReadOnly,
                   public IBlocksAppender,
                   public IBlocksDeleter,
                   public bftEngine::bcst::IAppState {
 public:
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // IReplica implementation
  Status start() override;
  Status stop() override;
  bool isRunning() const override { return (m_currentRepStatus == RepStatus::Running); }
  RepStatus getReplicaStatus() const override;
  const ILocalKeyValueStorageReadOnly &getReadOnlyStorage() override;
  Status addBlockToIdleReplica(const concord::storage::SetOfKeyValuePairs &updates) override;
  void set_command_handler(ICommandsHandler *handler) override;

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // ILocalKeyValueStorageReadOnly implementation
  Status get(const Sliver &key, Sliver &outValue) const override;
  Status get(BlockId readVersion, const Sliver &key, Sliver &outValue, BlockId &outBlock) const override;
  BlockId getGenesisBlock() const override { return m_bcDbAdapter->getGenesisBlockId(); }
  BlockId getLastBlock() const override { return getLastBlockNum(); }
  Status getBlockData(BlockId blockId, concord::storage::SetOfKeyValuePairs &outBlockData) const override;
  Status mayHaveConflictBetween(const Sliver &key, BlockId fromBlock, BlockId toBlock, bool &outRes) const override;
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // IBlocksAppender implementation
  Status addBlock(const concord::storage::SetOfKeyValuePairs &updates,
                  BlockId &outBlockId,
                  const concordUtils::SpanWrapper &parent_span) override;
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // IBlocksDeleter implementation
  void deleteGenesisBlock() override;
  BlockId deleteBlocksUntil(BlockId until) override;
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // IAppState implementation
  bool hasBlock(BlockId blockId) const override;
  bool getBlock(uint64_t blockId, char *outBlock, uint32_t *outBlockSize) override;
  bool getPrevDigestFromBlock(uint64_t blockId, bftEngine::bcst::StateTransferDigest *) override;
  bool putBlock(const uint64_t blockId, const char *block, const uint32_t blockSize) override;
  uint64_t getLastReachableBlockNum() const override { return m_bcDbAdapter->getLastReachableBlockId(); }
  uint64_t getLastBlockNum() const override { return m_bcDbAdapter->getLatestBlockId(); }
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  ReplicaImp(bft::communication::ICommunication *comm,
             const bftEngine::ReplicaConfig &config,
             std::unique_ptr<IStorageFactory> storageFactory,
             std::shared_ptr<concordMetrics::Aggregator> aggregator);

  void setReplicaStateSync(ReplicaStateSync *rss) { replicaStateSync_.reset(rss); }

  bftEngine::IStateTransfer &getStateTransfer() { return *m_stateTransfer; }

  ~ReplicaImp() override;

 protected:
  Status addBlockInternal(const concord::storage::SetOfKeyValuePairs &updates, BlockId &outBlockId);
  Status getInternal(BlockId readVersion, const Key &key, Sliver &outValue, BlockId &outBlock) const;
  void insertBlockInternal(BlockId blockId, Sliver block);
  RawBlock getBlockInternal(BlockId blockId) const;

 private:
  friend class StorageWrapperForIdleMode;

  void createReplicaAndSyncState();

  // INTERNAL TYPES

  // represents <key,blockId>
  class KeyIDPair {
   public:
    const Sliver key;
    const BlockId blockId;

    KeyIDPair(Sliver s, BlockId i) : key(s), blockId(i) {}

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

 private:
  logging::Logger logger;
  RepStatus m_currentRepStatus;

  std::unique_ptr<IDbAdapter> m_bcDbAdapter;
  std::shared_ptr<storage::IDBClient> m_metadataDBClient;
  bft::communication::ICommunication *m_ptrComm = nullptr;
  const bftEngine::ReplicaConfig &replicaConfig_;
  bftEngine::IReplica::IReplicaPtr m_replicaPtr = nullptr;
  ICommandsHandler *m_cmdHandler = nullptr;
  bftEngine::IStateTransfer *m_stateTransfer = nullptr;
  concord::storage::DBMetadataStorage *m_metadataStorage = nullptr;
  std::unique_ptr<ReplicaStateSync> replicaStateSync_;
  std::shared_ptr<concordMetrics::Aggregator> aggregator_;
  std::shared_ptr<bftEngine::ControlStateManager> controlStateManager_;

  // 5 Minutes
  static constexpr int64_t MAX_VALUE_MICROSECONDS = 1000 * 1000 * 60 * 5;
  // 1 second
  static constexpr int64_t MAX_VALUE_NANOSECONDS = 1000 * 1000 * 1000;
  using Recorder = concord::diagnostics::Recorder;
  struct Recorders {
    Recorders() {
      auto &registrar = concord::diagnostics::RegistrarSingleton::getInstance();
      registrar.perf.registerComponent("kvbc",
                                       {{"get_value", get_value},
                                        {"get_block", get_block},
                                        {"get_block_data", get_block_data},
                                        {"may_have_conflict_between", may_have_conflict_between},
                                        {"add_block", add_block}});
    }
    std::shared_ptr<Recorder> get_value =
        std::make_shared<Recorder>(1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    std::shared_ptr<Recorder> get_block =
        std::make_shared<Recorder>(1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    std::shared_ptr<Recorder> get_block_data =
        std::make_shared<Recorder>(1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    std::shared_ptr<Recorder> may_have_conflict_between =
        std::make_shared<Recorder>(1, MAX_VALUE_NANOSECONDS, 3, concord::diagnostics::Unit::NANOSECONDS);
    std::shared_ptr<Recorder> add_block =
        std::make_shared<Recorder>(1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
  };

  Recorders histograms_;
};

}  // namespace concord::kvbc
