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

#include "status.hpp"
#include "db_adapter_interface.h"
#include "db_interfaces.h"
#include "Serializable.h"
#include "PersistentStorage.hpp"
#include <optional>
#include <string>
#include <vector>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <chrono>
#include <bftengine/DbCheckpointMetadata.hpp>
#include "Metrics.hpp"
#include <algorithm>
#if __has_include(<filesystem>)
#include <filesystem>
namespace fs = std::filesystem;
#elif __has_include(<experimental/filesystem>)
#include <experimental/filesystem>
namespace _fs = std::experimental::filesystem;
#else
#error "Missing filesystem support"
#endif
namespace concord::kvbc {

using CheckpointId = bftEngine::impl::CheckpointId;
using DbCheckpointMetadata = bftEngine::impl::DbCheckpointMetadata;
using Status = concordUtils::Status;

class IDbCheckPointManager {
 public:
  // create checkpoint
  virtual Status createDbCheckpoint(const uint64_t& checkPointId, const uint64_t& lastBlockId, const uint64_t&) = 0;
  // set max num of checkpoints
  virtual void setMaxNumOfAllowedCheckpoints(const uint32_t& maxNum) = 0;
  virtual uint32_t getMaxNumOfCheckpoints() const = 0;
  // set metadata storage ptr
  virtual void setPersistentStorage(std::shared_ptr<bftEngine::impl::PersistentStorage>) = 0;
  // load stored db checkpoints data from persistence and
  virtual void init() = 0;
  // remove checkpoint
  virtual void removeCheckpoint(const uint64_t& checkPointId) = 0;
  // remove all existing checkpoints
  virtual void removeAllCheckpoints() const = 0;
  // cleanup if db checkpoint creation is dsabled
  virtual void cleanUp() = 0;
  virtual void setAggregator(std::shared_ptr<concordMetrics::Aggregator>) = 0;

  virtual ~IDbCheckPointManager() = default;
};

class RocksDbCheckPointManager : public IDbCheckPointManager {
 public:
  RocksDbCheckPointManager(const std::shared_ptr<storage::IDBClient>& dbClient,
                           const uint32_t& maxNumOfChkPt,
                           const std::string& dbCheckpointDir)
      : rocksDbClient_{dbClient},
        ps_{nullptr},
        maxNumOfCheckpoints_{maxNumOfChkPt},
        dbCheckPointDirPath_{dbCheckpointDir},
        metrics_{concordMetrics::Component("rocksdbCheckpoint", std::make_shared<concordMetrics::Aggregator>())},
        maxDbCheckpointCreationTimeMsec_(metrics_.RegisterGauge("maxDbCheckpointCreationTimeInMsecSoFar", 0)),
        dbCheckpointCreationAverageTimeMsec_(metrics_.RegisterGauge("dbCheckpointCreationAverageTimeMsec", 0)),
        lastDbCheckpointSizeInMb_(metrics_.RegisterGauge("lastDbCheckpointSizeInMb", 0)),
        lastDbCheckpointBlockId_(metrics_.RegisterGauge("lastDbCheckpointBlockId", 0)),
        numOfDbCheckpointsCreated_(metrics_.RegisterCounter("numOfDbCheckpointsCreated", 0)) {
    rocksDbClient_->setCheckpointPath(dbCheckPointDirPath_);
    maxNumOfCheckpoints_ = std::min(maxNumOfCheckpoints_, bftEngine::impl::MAX_ALLOWED_CHECKPOINTS);
    metrics_.Register();
  }
  void init() override;
  Status createDbCheckpoint(const uint64_t& checkPointId, const uint64_t& lastBlockId, const uint64_t& seqNum) override;

  void removeCheckpoint(const uint64_t& checkPointId) override;
  void setMaxNumOfAllowedCheckpoints(const uint32_t& maxNumDbCheckpoint) override {
    if (maxNumDbCheckpoint > bftEngine::impl::MAX_ALLOWED_CHECKPOINTS) {
      LOG_ERROR(getLogger(),
                "setting maxNumDbCheckpoint checkpoint failed"
                    << KVLOG(maxNumDbCheckpoint, bftEngine::impl::MAX_ALLOWED_CHECKPOINTS));
      return;
    }
    maxNumOfCheckpoints_ = maxNumDbCheckpoint;
  }
  uint32_t getMaxNumOfCheckpoints() const override { return maxNumOfCheckpoints_; }
  void setPersistentStorage(std::shared_ptr<bftEngine::impl::PersistentStorage> p) override { ps_ = p; }
  void removeAllCheckpoints() const override;
  void cleanUp() override;
  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) override {
    metrics_.SetAggregator(aggregator);
  }
  ~RocksDbCheckPointManager() {
    if (cleanupThread_.joinable()) cleanupThread_.join();
  }

 private:
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("concord.kvbc.db_checkpoint"));
    return logger_;
  }
  // get total size recursively
  uint64_t directorySize(const _fs::path& directory, const bool& excludeHardLinks);
  // get checkpoint metadata
  void loadCheckpointDataFromPersistence();
  void checkforCleanup();
  DbCheckpointMetadata dbCheckptMetadata_;
  const std::shared_ptr<storage::IDBClient>& rocksDbClient_;
  std::shared_ptr<bftEngine::impl::PersistentStorage> ps_;
  std::queue<CheckpointId> checkpointToBeRemoved_;
  std::thread cleanupThread_;
  std::condition_variable cv_;
  std::mutex lock_;
  uint32_t maxNumOfCheckpoints_{0};  // 0-disabled
  uint64_t lastCheckpointSeqNum_{0};
  std::string dbCheckPointDirPath_;
  concordMetrics::Component metrics_;
  concordMetrics::GaugeHandle maxDbCheckpointCreationTimeMsec_;
  concordMetrics::GaugeHandle dbCheckpointCreationAverageTimeMsec_;
  concordMetrics::GaugeHandle lastDbCheckpointSizeInMb_;
  concordMetrics::GaugeHandle lastDbCheckpointBlockId_;
  concordMetrics::CounterHandle numOfDbCheckpointsCreated_;
};
}  // namespace concord::kvbc