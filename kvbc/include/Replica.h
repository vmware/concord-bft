// Copyright 2018-2020 VMware, all rights reserved
//
// KV Blockchain replica definition.

#pragma once

#include <functional>
#include <map>
#include <string>
#include <atomic>

#include "st_reconfiguraion_sm.hpp"
#include "OpenTracing.hpp"
#include "kvbc_adapter/replica_adapter.hpp"
#include "categorization/db_categories.h"
#include "communication/ICommunication.hpp"
#include "communication/CommFactory.hpp"
#include "bftengine/Replica.hpp"
#include "bftengine/ReplicaConfig.hpp"
#include "bcstatetransfer/SimpleBCStateTransfer.hpp"
#include "communication/StatusInfo.h"
#include "KVBCInterfaces.h"
#include "replica_state_sync_imp.hpp"
#include "db_adapter_interface.h"
#include "db_interfaces.h"
#include "memorydb/client.h"
#include "bftengine/DbMetadataStorage.hpp"
#include "storage_factory_interface.h"
#include "ControlStateManager.hpp"
#include "thread_pool.hpp"
#include "client/reconfiguration/client_reconfiguration_engine.hpp"
#include <ccron/cron_table_registry.hpp>
#include <ccron/ticks_generator.hpp>
#include "log/logger.hpp"
#include "kvbc_app_filter/value_from_kvbc_proto.h"
#include "newest_public_event_group_record_time.h"
#include "ReplicaFactory.hpp"

namespace concord::kvbc {

class Replica : public IReplica,
                public IBlocksDeleter,
                public IReader,
                public IBlockAdder,
                public bftEngine::bcst::IAppState {
 public:
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // IReplica implementation
  Status start() override final;
  Status stop() override final;
  bool isRunning() const override final { return (m_currentRepStatus == RepStatus::Running); }
  RepStatus getReplicaStatus() const override final;
  const IReader &getReadOnlyStorage() const override final;
  BlockId addBlockToIdleReplica(categorization::Updates &&updates) override final;
  void set_command_handler(std::shared_ptr<ICommandsHandler> handler) override final;

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // IBlocksDeleter implementation
  void deleteGenesisBlock() override final;
  BlockId deleteBlocksUntil(BlockId until, bool delete_files_in_range) override final;
  void deleteLastReachableBlock() override final;
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // IReader
  std::optional<categorization::Value> get(const std::string &category_id,
                                           const std::string &key,
                                           BlockId block_id) const override final;

  std::optional<categorization::Value> getLatest(const std::string &category_id,
                                                 const std::string &key) const override final;

  void multiGet(const std::string &category_id,
                const std::vector<std::string> &keys,
                const std::vector<BlockId> &versions,
                std::vector<std::optional<categorization::Value>> &values) const override final;

  void multiGetLatest(const std::string &category_id,
                      const std::vector<std::string> &keys,
                      std::vector<std::optional<categorization::Value>> &values) const override final;

  std::optional<categorization::TaggedVersion> getLatestVersion(const std::string &category_id,
                                                                const std::string &key) const override final;

  void multiGetLatestVersion(const std::string &category_id,
                             const std::vector<std::string> &keys,
                             std::vector<std::optional<categorization::TaggedVersion>> &versions) const override final;

  std::optional<categorization::Updates> getBlockUpdates(BlockId block_id) const override final;

  // Get the current genesis block ID in the system.
  BlockId getGenesisBlockId() const override final;

  // Get the last block ID in the system.
  BlockId getLastBlockId() const override final;
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // checkpoint
  void checkpointInProcess(bool flag, kvbc::BlockId);
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // IBlockAdder
  BlockId add(categorization::Updates &&) override final;
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // IAppState implementation
  bool hasBlock(BlockId blockId) const override final;
  bool getBlock(uint64_t blockId,
                char *outBlock,
                uint32_t outBlockMaxSize,
                uint32_t *outBlockActualSize) const override final;
  std::future<bool> getBlockAsync(uint64_t blockId,
                                  char *outBlock,
                                  uint32_t outBlockMaxSize,
                                  uint32_t *outBlockActualSize) override final;
  bool getPrevDigestFromBlock(uint64_t blockId, bftEngine::bcst::StateTransferDigest *) const override final;
  void getPrevDigestFromBlock(const char *blockData,
                              const uint32_t blockSize,
                              bftEngine::bcst::StateTransferDigest *outPrevBlockDigest) const override final;
  bool putBlock(const uint64_t blockId,
                const char *blockData,
                const uint32_t blockSize,
                bool lastBlock = true) override final;
  std::future<bool> putBlockAsync(uint64_t blockId,
                                  const char *block,
                                  const uint32_t blockSize,
                                  bool lastblock) override final;
  uint64_t getLastReachableBlockNum() const override final;
  uint64_t getGenesisBlockNum() const override final;
  // This method is used by state-transfer in order to find the latest block id in either the state-transfer chain or
  // the main blockchain
  uint64_t getLastBlockNum() const override final;
  size_t postProcessUntilBlockId(uint64_t max_block_id) override final;
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  bool getBlockFromObjectStore(uint64_t blockId,
                               char *outBlock,
                               uint32_t outblockMaxSize,
                               uint32_t *outBlockSize) const;
  bool getPrevDigestFromObjectStoreBlock(uint64_t blockId, bftEngine::bcst::StateTransferDigest *) const;
  void getPrevDigestFromObjectStoreBlock(const char *blockData,
                                         const uint32_t blockSize,
                                         bftEngine::bcst::StateTransferDigest *outPrevBlockDigest) const;
  bool putBlockToObjectStore(const uint64_t blockId,
                             const char *blockData,
                             const uint32_t blockSize,
                             bool lastBlock = false);

  Replica(bft::communication::ICommunication *comm,
          const bftEngine::ReplicaConfig &config,
          std::unique_ptr<IStorageFactory> storageFactory,
          std::shared_ptr<concordMetrics::Aggregator> aggregator,
          const std::shared_ptr<concord::performance::PerformanceManager> &pm,
          std::map<std::string, categorization::CATEGORY_TYPE> kvbc_categories,
          const std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> &secretsManager);

  // Initialize replica internals, before start(). Used as a workaround, because doing init in the constructor wouldn't
  // work in all cases due to some external components being set via set* methods. Additionally, some of these external
  // components depend on the replica itself being constructed.
  // If not explicitly called by the user, start() will call it automatically.
  Status initInternals();

  void setReplicaStateSync(ReplicaStateSync *rss) { replicaStateSync_.reset(rss); }

  bftEngine::IStateTransfer &getStateTransfer() { return *m_stateTransfer; }

  std::shared_ptr<cron::CronTableRegistry> cronTableRegistry() const { return cronTableRegistry_; }
  std::shared_ptr<cron::TicksGenerator> ticksGenerator() const { return m_replicaPtr->ticksGenerator(); }
  void registerStBasedReconfigurationHandler(std::shared_ptr<concord::client::reconfiguration::IStateHandler>);

  std::shared_ptr<concord::storage::rocksdb::NativeClient> nativeDbClient() {
    return storage::rocksdb::NativeClient::fromIDBClient(m_dbSet.dataDBClient);
  }

  std::optional<adapter::ReplicaBlockchain> &kvBlockchain() { return op_kvBlockchain; }

  void setStateSnapshotValueConverter(const Converter &c) { m_stateSnapshotValueConverter = c; }

  void setLastApplicationTransactionTimeCallback(const LastApplicationTransactionTimeCallback &cb) {
    m_lastAppTxnCallback = cb;
  }

  virtual ~Replica();
  std::shared_ptr<MsgsCommunicator> getMsgsCommunicator() const { return m_replicaPtr->getMsgsCommunicator(); }
  std::shared_ptr<MsgHandlersRegistrator> getMsgHandlersRegistrator() const {
    return m_replicaPtr->getMsgHandlersRegistrator();
  }
  concordUtil::Timers *getTimers() { return m_replicaPtr->getTimers(); }

 protected:
  RawBlock getBlockInternal(BlockId blockId) const;

 private:
  friend class StorageWrapperForIdleMode;

  void createReplicaAndSyncState();
  void registerReconfigurationHandlers(std::shared_ptr<bftEngine::IRequestsHandler> requestHandler);
  void handleNewEpochEvent();
  template <typename T>
  void saveReconfigurationCmdToResPages(const std::string &);
  void handleWedgeEvent();
  uint64_t getStoredReconfigData(const std::string &kCategory, const std::string &key, const kvbc::BlockId &bid);
  void startRoReplicaCreEngine();
  BlockId getLastKnownReconfigCmdBlockNum() const;
  void setLastKnownReconfigCmdBlock(const std::vector<uint8_t> &);
  // INTERNAL TYPES

  // represents <key,blockId>
  class KeyIDPair {
   public:
    const Sliver key;
    const BlockId blockId;

    KeyIDPair(const Sliver &s, BlockId i) : key(s), blockId(i) {}

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

  concord::kvbc::v4blockchain::KeyValueBlockchain::BlockchainRecovery blockchain_recovery;
  concord::kvbc::IStorageFactory::DatabaseSet m_dbSet;
  // The ReplicaBlockchain is used for a normal read-write replica.
  std::optional<adapter::ReplicaBlockchain> op_kvBlockchain;
  adapter::ReplicaBlockchain *m_kvBlockchain{nullptr};
  Converter m_stateSnapshotValueConverter{concord::kvbc::valueFromKvbcProto};
  kvbc::LastApplicationTransactionTimeCallback m_lastAppTxnCallback{newestPublicEventGroupRecordTime};
  // The IdbAdapter instance is used for a read-only replica.
  std::unique_ptr<IDbAdapter> m_bcDbAdapter;
  std::shared_ptr<storage::IDBClient> m_metadataDBClient;
  std::unique_ptr<bft::communication::ICommunication> m_ptrComm;
  const bftEngine::ReplicaConfig &replicaConfig_;
  bftEngine::ReplicaFactory::IReplicaPtr m_replicaPtr;
  std::shared_ptr<ICommandsHandler> m_cmdHandler = nullptr;
  bftEngine::IStateTransfer *m_stateTransfer = nullptr;
  concord::storage::DBMetadataStorage *m_metadataStorage = nullptr;
  std::unique_ptr<ReplicaStateSync> replicaStateSync_;
  std::shared_ptr<concordMetrics::Aggregator> aggregator_;
  std::shared_ptr<concord::performance::PerformanceManager> pm_;
  // secretsManager_ can be nullptr. This means that encrypted configuration is not enabled
  // and there is no instance of SecretsManagerEnc available
  const std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> secretsManager_;
  std::unique_ptr<concord::kvbc::StReconfigurationHandler> stReconfigurationSM_;
  std::shared_ptr<cron::CronTableRegistry> cronTableRegistry_{std::make_shared<cron::CronTableRegistry>()};
  std::unique_ptr<concord::client::reconfiguration::ClientReconfigurationEngine> creEngine_;
  std::shared_ptr<concord::client::reconfiguration::IStateClient> creClient_;
  concord::util::ThreadPool blocks_io_workers_pool;
  struct Recorders {
    static constexpr uint64_t MAX_VALUE_MICROSECONDS = 2ULL * 1000ULL * 1000ULL;  // 2 seconds
    const std::string component_ = "iappstate";
    Recorders() {
      auto &registrar = concord::diagnostics::RegistrarSingleton::getInstance();
      if (!registrar.perf.isRegisteredComponent(component_)) {
        registrar.perf.registerComponent(component_, {get_block_duration, put_block_duration});
      }
    }

    ~Recorders() {}

    DEFINE_SHARED_RECORDER(get_block_duration, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
    DEFINE_SHARED_RECORDER(put_block_duration, 1, MAX_VALUE_MICROSECONDS, 3, concord::diagnostics::Unit::MICROSECONDS);
  };
  // All these recorders need to be shared
  Recorders histograms_;
};  // namespace concord::kvbc

}  // namespace concord::kvbc
