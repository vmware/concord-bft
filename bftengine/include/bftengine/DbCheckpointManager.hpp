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
#include "callback_registry.hpp"
#include "status.hpp"
#include "storage/db_interface.h"
#include "Serializable.h"
#include "bftengine/PersistentStorage.hpp"
#include <optional>
#include <string>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <bftengine/DbCheckpointMetadata.hpp>
#include "Metrics.hpp"
#include <algorithm>
#include <thread>
#include <atomic>
#include "InternalBFTClient.hpp"
#if __has_include(<filesystem>)
#include <filesystem>
namespace fs = std::filesystem;
#elif __has_include(<experimental/filesystem>)
#include <experimental/filesystem>
namespace _fs = std::experimental::filesystem;
#else
#error "Missing filesystem support"
#endif
namespace bftEngine::impl {
using std::chrono::duration_cast;
using Status = concordUtils::Status;
using SystemClock = std::chrono::system_clock;
using Seconds = std::chrono::seconds;
class DbCheckpointManager {
 public:
  void createDbCheckpoint(const SeqNum& seqNum);
  std::chrono::seconds getLastCheckpointCreationTime() const { return lastCheckpointCreationTime_; }
  void initializeDbCheckpointManager(std::shared_ptr<concord::storage::IDBClient> dbClient,
                                     std::shared_ptr<bftEngine::impl::PersistentStorage> p,
                                     std::shared_ptr<concordMetrics::Aggregator> aggregator,
                                     const std::function<uint64_t()>& getLastBlockIdCb);
  std::map<CheckpointId, DbCheckpointMetadata::DbCheckPointDescriptor> getListOfDbCheckpoints() const {
    return dbCheckptMetadata_.dbCheckPoints_;
  }

  /***
   * The operator command uses this function to find the next immediate stable seq number
   * with checkpointWindowSize(150) where dbCheckpoint/snapshot will be created.
   ***/
  void setNextStableSeqNumToCreateSnapshot(const std::optional<SeqNum>& seqNum);
  std::optional<SeqNum> getNextStableSeqNumToCreateSnapshot() const {
    if (nextStableSeqNum_ == std::nullopt) return std::nullopt;
    return nextStableSeqNum_;
  }

  /***
   * Function is used to send empty client request msg to achieve stable seq num.
   * This function is used by createDbCheckpoint operator command to create db snapshot.
   ***/
  void sendInternalClientRequestMsg(const SeqNum& seqNum);

 public:
  static DbCheckpointManager& instance(bftEngine::impl::InternalBFTClient* client = nullptr) {
    static DbCheckpointManager instance_(client);
    return instance_;
  }
  ~DbCheckpointManager() {
    if (!stopped_) {
      stopped_ = true;
      cv_.notify_one();
      if (cleanupThread_.joinable()) cleanupThread_.join();
    }
  }

 private:
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("concord.bft.db_checkpoint_manager"));
    return logger_;
  }
  DbCheckpointManager(bftEngine::impl::InternalBFTClient* client)
      : client_(client),
        metrics_{concordMetrics::Component("rocksdbCheckpoint", std::make_shared<concordMetrics::Aggregator>())},
        maxDbCheckpointCreationTimeMsec_(metrics_.RegisterGauge("maxDbCheckpointCreationTimeInMsecSoFar", 0)),
        dbCheckpointCreationAverageTimeMsec_(metrics_.RegisterGauge("dbCheckpointCreationAverageTimeMsec", 0)),
        lastDbCheckpointSizeInMb_(metrics_.RegisterGauge("lastDbCheckpointSizeInMb", 0)),
        lastDbCheckpointBlockId_(metrics_.RegisterGauge("lastDbCheckpointBlockId", 0)),
        numOfDbCheckpointsCreated_(metrics_.RegisterCounter("numOfDbCheckpointsCreated", 0)) {
    metrics_.Register();
  }
  void init();
  Status createDbCheckpoint(const uint64_t& checkPointId, const uint64_t& lastBlockId, const SeqNum& seqNum);
  void removeCheckpoint(const uint64_t& checkPointId);
  void removeAllCheckpoints() const;
  void cleanUp();
  std::function<uint64_t()> getLastBlockIdCb_;
  // get total size recursively
  uint64_t directorySize(const _fs::path& directory, const bool& excludeHardLinks);
  // get checkpoint metadata
  void loadCheckpointDataFromPersistence();
  void checkforCleanup();
  std::atomic<bool> stopped_ = false;
  DbCheckpointMetadata dbCheckptMetadata_;
  std::shared_ptr<concord::storage::IDBClient> dbClient_;
  std::shared_ptr<bftEngine::impl::PersistentStorage> ps_;
  std::queue<CheckpointId> checkpointToBeRemoved_;
  std::thread cleanupThread_;
  std::condition_variable cv_;
  std::mutex lock_;
  uint32_t maxNumOfCheckpoints_{0};  // 0-disabled
  SeqNum lastCheckpointSeqNum_{0};
  bftEngine::impl::InternalBFTClient* client_{nullptr};
  std::optional<SeqNum> nextStableSeqNum_ = std::nullopt;
  std::chrono::seconds lastCheckpointCreationTime_{duration_cast<Seconds>(SystemClock::now().time_since_epoch())};
  std::string dbCheckPointDirPath_;
  concordMetrics::Component metrics_;
  concordMetrics::GaugeHandle maxDbCheckpointCreationTimeMsec_;
  concordMetrics::GaugeHandle dbCheckpointCreationAverageTimeMsec_;
  concordMetrics::GaugeHandle lastDbCheckpointSizeInMb_;
  concordMetrics::GaugeHandle lastDbCheckpointBlockId_;
  concordMetrics::CounterHandle numOfDbCheckpointsCreated_;
};

}  // namespace bftEngine::impl
