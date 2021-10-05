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
#include "categorization/kv_blockchain.h"
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
namespace concord::kvbc {

class Replica : public IReplica,
                public IBlocksDeleter,
                public IReader,
                public IBlockAdder,
                public bftEngine::bcst::IAppState {
 public:
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // IReplica implementation
  Status start() override;
  Status stop() override;
  bool isRunning() const override { return (m_currentRepStatus == RepStatus::Running); }
  RepStatus getReplicaStatus() const override;
  const IReader &getReadOnlyStorage() const override;
  BlockId addBlockToIdleReplica(categorization::Updates &&updates) override;
  void set_command_handler(std::shared_ptr<ICommandsHandler> handler) override;

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // IBlocksDeleter implementation
  void deleteGenesisBlock() override;
  BlockId deleteBlocksUntil(BlockId until) override;
  concordUtils::Status reclaimDiskSpace() override { return m_kvBlockchain->db()->asIDBClient()->reclaimDiskSpace(); }
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // IReader
  std::optional<categorization::Value> get(const std::string &category_id,
                                           const std::string &key,
                                           BlockId block_id) const override;

  std::optional<categorization::Value> getLatest(const std::string &category_id, const std::string &key) const override;

  void multiGet(const std::string &category_id,
                const std::vector<std::string> &keys,
                const std::vector<BlockId> &versions,
                std::vector<std::optional<categorization::Value>> &values) const override;

  void multiGetLatest(const std::string &category_id,
                      const std::vector<std::string> &keys,
                      std::vector<std::optional<categorization::Value>> &values) const override;

  std::optional<categorization::TaggedVersion> getLatestVersion(const std::string &category_id,
                                                                const std::string &key) const override;

  void multiGetLatestVersion(const std::string &category_id,
                             const std::vector<std::string> &keys,
                             std::vector<std::optional<categorization::TaggedVersion>> &versions) const override;

  std::optional<categorization::Updates> getBlockUpdates(BlockId block_id) const override;

  // Get the current genesis block ID in the system.
  BlockId getGenesisBlockId() const override;

  // Get the last block ID in the system.
  BlockId getLastBlockId() const override;
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // IBlockAdder
  BlockId add(categorization::Updates &&) override;
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // IAppState implementation
  bool hasBlock(BlockId blockId) const override;
  bool getBlock(uint64_t blockId, char *outBlock, uint32_t *outBlockSize) override;
  bool getPrevDigestFromBlock(uint64_t blockId, bftEngine::bcst::StateTransferDigest *) override;
  bool putBlock(const uint64_t blockId, const char *blockData, const uint32_t blockSize) override;
  uint64_t getLastReachableBlockNum() const override;
  uint64_t getGenesisBlockNum() const override;
  // This method is used by state-transfer in order to find the latest block id in either the state-transfer chain or
  // the main blockchain
  uint64_t getLastBlockNum() const override;
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  bool getBlockFromObjectStore(uint64_t blockId, char *outBlock, uint32_t *outBlockSize);
  bool getPrevDigestFromObjectStoreBlock(uint64_t blockId, bftEngine::bcst::StateTransferDigest *);
  bool putBlockToObjectStore(const uint64_t blockId, const char *blockData, const uint32_t blockSize);

  Replica(bft::communication::ICommunication *comm,
          const bftEngine::ReplicaConfig &config,
          std::unique_ptr<IStorageFactory> storageFactory,
          std::shared_ptr<concordMetrics::Aggregator> aggregator,
          const std::shared_ptr<concord::performance::PerformanceManager> &pm,
          std::map<std::string, categorization::CATEGORY_TYPE> kvbc_categories,
          const std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> &secretsManager);

  void setReplicaStateSync(ReplicaStateSync *rss) { replicaStateSync_.reset(rss); }

  bftEngine::IStateTransfer &getStateTransfer() { return *m_stateTransfer; }

  ~Replica() override;

 protected:
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

  concord::kvbc::IStorageFactory::DatabaseSet m_dbSet;
  // The categorization KeyValueBlockchain is used for a normal read-write replica.
  std::optional<categorization::KeyValueBlockchain> m_kvBlockchain;
  // The IdbAdapter instance is used for a read-only replica.
  std::unique_ptr<IDbAdapter> m_bcDbAdapter;
  std::shared_ptr<storage::IDBClient> m_metadataDBClient;
  bft::communication::ICommunication *m_ptrComm = nullptr;
  const bftEngine::ReplicaConfig &replicaConfig_;
  bftEngine::IReplica::IReplicaPtr m_replicaPtr = nullptr;
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

};  // namespace concord::kvbc

}  // namespace concord::kvbc
