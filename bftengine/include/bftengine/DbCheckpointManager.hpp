// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <vector>
#include <chrono>
#include <optional>
#include <string>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <algorithm>
#include <thread>
#include <atomic>
#include <PrimitiveTypes.hpp>
#include "callback_registry.hpp"
#include "status.hpp"
#include "Serializable.h"
#include "PersistentStorage.hpp"
#include "DbCheckpointMetadata.hpp"
#include "Metrics.hpp"
#include "InternalBFTClient.hpp"
#include "storage/db_interface.h"
#include "util/filesystem.hpp"
#include "kv_types.hpp"
namespace _fs = fs;
namespace bftEngine::impl {
using std::chrono::duration_cast;
using Status = concordUtils::Status;
using SystemClock = std::chrono::system_clock;
using Seconds = std::chrono::seconds;
using InternalBftClient = bftEngine::impl::InternalBFTClient;
using DbCheckpointId = uint64_t;
using BlockId = uint64_t;
class DbCheckpointManager {
 public:
  /***************************************************
   *@Input parameter1: Request sequnce number
   *@Input parameter2: Optional request timestamp
   *@Input parameter3: Optional blockId
   *@Description: Creates rocksdb checkpoint asynchronously if database has new state.
   *  If we already have a checkpoint with same state as lastBlock in the DB, we reject the request.
   *  Creating another db checkpoint with the same state as the previous one has no use.
   *  Note1: if ReplicaConfig::maxNumberOfDbCheckpoints is set to zero, then also we do not create rocksDb
   *  and return std::nullopt in that case
   *  Note2: if there is no state in the blockchain, we don't create a snapshot and return std::nullopt.
   *  Note3: We create db-snapshot only on stable seq number. But, we need same snapshot id for all
   *  replicas. All replicas handle db-snapshot create request at same seq num and snapshot id is equal to
   *  the last reachable block number at that time. Some replicas may not have reached stable sequence number while
   *  handling the request, in that case it waits to reach the sable sequence number before creating snapshot. In
   *  such a case, we pass block id as input paramter, and it is used to trim extra blocks that might be present in
   *  the db snapshot if required.
   *@Return: returns a unique db checkpoint id. Else return std::nullopt
   ***************************************************/
  std::optional<CheckpointId> createDbCheckpointAsync(const SeqNum& seqNum,
                                                      const std::optional<Timestamp>& timestamp,
                                                      const std::optional<DbCheckpointId>& blockId);

  /***************************************************
   *@Description: Returns last created db checkpoint metadata. If there is no db-checkpoint created then it returns
   *nullopt
   *@Return: returns the metadata of last created checkpoint. Else return std::nullopt
   *  Note: if ReplicaConfig::maxNumberOfDbCheckpoints is set to zero, then also this api retrun std::nullopt
   ***************************************************/
  std::optional<DbCheckpointMetadata::DbCheckPointDescriptor> getLastCreatedDbCheckpointMetadata();

  enum class CheckpointState {
    kCreated,  // Note that even though the current state of a checkpoint might be `kCreated`, it might be deleted at
               // any future point in time by the checkpoint manager.

    kPending,  // It is expected that if a checkpoint is `kPending`, it will become `kCreated` at a future point in
               // time.

    kNonExistent
  };
  CheckpointState getCheckpointState(CheckpointId id) const;

  std::string getPathForCheckpoint(CheckpointId id) const { return dbClient_->getPathForCheckpoint(id); }

  // A callback that is called after creating a checkpoint. Its purpose is to prepare it for use. It receives as
  // parameters:
  //  * the block ID at which the checkpoint was created (it might be less than the last block ID in the checkpoint
  //  itself)
  //  * the path to the checkpoint
  using PrepareCheckpointCallback = std::function<void(BlockId, const std::string&)>;

  Seconds getLastCheckpointCreationTime() const { return lastCheckpointCreationTime_; }
  void initializeDbCheckpointManager(std::shared_ptr<concord::storage::IDBClient> dbClient,
                                     std::shared_ptr<bftEngine::impl::PersistentStorage> p,
                                     std::shared_ptr<concordMetrics::Aggregator> aggregator,
                                     const std::function<BlockId()>& getLastBlockIdCb,
                                     const PrepareCheckpointCallback& prepareCheckpointCb,
                                     const std::function<void(bool, concord::kvbc::BlockId)>& checkpointInProcessCb);
  std::map<CheckpointId, DbCheckpointMetadata::DbCheckPointDescriptor> getListOfDbCheckpoints() const {
    return dbCheckptMetadata_.dbCheckPoints_;
  }

  /***
   * The operator command uses this function to find the next immediate stable seq number
   * with checkpointWindowSize(150) where dbCheckpoint/snapshot will be created.
   ***/
  void setNextStableSeqNumToCreateSnapshot(const std::optional<SeqNum>& seqNum);
  std::optional<SeqNum> getNextStableSeqNumToCreateSnapshot() const { return nextSeqNumToCreateCheckPt_; }

  static DbCheckpointManager& instance(InternalBftClient* client = nullptr) {
    static DbCheckpointManager instance(client);
    return instance;
  }
  ~DbCheckpointManager() {
    stopped_ = true;
    if (monitorThread_.joinable()) monitorThread_.join();
  }
  void sendInternalCreateDbCheckpointMsg(const SeqNum& seqNum, bool noop);
  BlockId getLastReachableBlock() const;
  SeqNum getLastStableSeqNum() const;
  void setCheckpointInProcess(bool, concord::kvbc::BlockId) const;
  void setOnStableSeqNumCb_(std::function<void(SeqNum)> cb) { onStableSeqNumCb_ = cb; }
  void onStableSeqNum(SeqNum s) {
    if (onStableSeqNumCb_) onStableSeqNumCb_(s);
  }
  void setGetLastStableSeqNumCb(std::function<SeqNum()> cb) { getLastStableSeqNumCb_ = cb; }
  inline void checkAndCreateDbSnapshot(SeqNum seqNum) {
    if (ReplicaConfig::instance().dbCheckpointFeatureEnabled)
      createDbCheckpointAsync(seqNum, std::nullopt, std::nullopt);
  }
  void addOnDbCheckpointCreatedCb(std::function<void(SeqNum)> cb) {
    if (cb) onDbCheckpointCreated_.push_back(cb);
  }
  inline auto getLastDbCheckpointSeqNum() const { return lastCheckpointSeqNum_; }
  std::string getDiskUsageInfo();
  // return a map of pair<checkpoint_id, size_on_disk>
  // checkpoint_id = 0 indicates rocksdb size
  // only used for apollo test
  std::map<uint64_t, uint64_t> getDbSize();

 private:
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("concord.bft.db_checkpoint_manager"));
    return logger_;
  }
  DbCheckpointManager(InternalBftClient* client)
      : client_(client),
        metrics_{concordMetrics::Component("rocksdbCheckpoint", std::make_shared<concordMetrics::Aggregator>())},
        maxDbCheckpointCreationTimeMsec_(metrics_.RegisterGauge("maxDbCheckpointCreationTimeInMsecSoFar", 0)),
        lastDbCheckpointSizeInMb_(metrics_.RegisterGauge("lastDbCheckpointSizeInMb", 0)),
        lastDbCheckpointBlockId_(metrics_.RegisterGauge("lastDbCheckpointBlockId", 0)),
        numOfDbCheckpointsCreated_(metrics_.RegisterCounter("numOfDbCheckpointsCreated", 0)) {
    metrics_.Register();
  }
  void init();
  Status createDbCheckpoint(const DbCheckpointId& checkPointId,
                            const BlockId& lastBlockId,
                            const SeqNum& seqNum,
                            const std::optional<Timestamp>& timestamp);
  void removeCheckpoint(const DbCheckpointId& checkPointId);
  void removeAllCheckpoints() const;
  void cleanUp();
  std::function<BlockId()> getLastBlockIdCb_;
  PrepareCheckpointCallback prepareCheckpointCb_;
  std::function<void(bool, concord::kvbc::BlockId)> checkpointInProcessCb_;
  // get total size recursively
  uint64_t directorySize(const _fs::path& directory, const bool& excludeHardLinks, bool recursive);
  // get checkpoint metadata
  void loadCheckpointDataFromPersistence();
  void checkforCleanup();
  void checkAndRemove();
  void removeOldestDbCheckpoint();
  void updateDbCheckpointMetadata();
  void updateLastCmdInfo(const SeqNum&, const std::optional<Timestamp>&);
  void removeDbCheckpointFuture(CheckpointId);
  void updateMetrics();
  InternalBftClient* client_{nullptr};
  std::atomic<bool> stopped_ = false;
  DbCheckpointMetadata dbCheckptMetadata_;
  std::map<CheckpointId, std::future<void>> dbCreateCheckPtFuture_;
  std::future<void> cleanUpFuture_;
  std::shared_ptr<concord::storage::IDBClient> dbClient_;
  std::shared_ptr<bftEngine::impl::PersistentStorage> ps_;
  // this thread minitors if we over use the disk space
  // for rocksDb checkpooints then, it logs error and
  // cleans up oldest checkpoint
  std::thread monitorThread_;
  mutable std::mutex lock_;
  mutable std::mutex lockDbCheckPtFuture_;
  std::mutex lockLastDbCheckpointDesc_;
  uint32_t maxNumOfCheckpoints_{0};  // 0-disabled
  SeqNum lastCheckpointSeqNum_{0};
  std::optional<DbCheckpointMetadata::DbCheckPointDescriptor> lastCreatedCheckpointMetadata_{std::nullopt};
  std::optional<SeqNum> nextSeqNumToCreateCheckPt_{std::nullopt};
  std::chrono::seconds lastCheckpointCreationTime_{duration_cast<Seconds>(SystemClock::now().time_since_epoch())};
  std::function<void(SeqNum)> onStableSeqNumCb_;
  std::function<SeqNum()> getLastStableSeqNumCb_;
  std::vector<std::function<void(SeqNum)>> onDbCheckpointCreated_;
  std::string dbCheckPointDirPath_;
  concordMetrics::Component metrics_;
  concordMetrics::GaugeHandle maxDbCheckpointCreationTimeMsec_;
  concordMetrics::GaugeHandle lastDbCheckpointSizeInMb_;
  concordMetrics::GaugeHandle lastDbCheckpointBlockId_;
  concordMetrics::CounterHandle numOfDbCheckpointsCreated_;
};

}  // namespace bftEngine::impl
