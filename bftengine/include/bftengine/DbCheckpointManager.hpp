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
using SeqNum = bftEngine::impl::SeqNum;
using Status = concordUtils::Status;
using SystemClock = std::chrono::system_clock;
using Seconds = std::chrono::seconds;
using InternalBftClient = bftEngine::impl::InternalBFTClient;
class DbCheckpointManager {
 public:
  // void createDbCheckpoint(const SeqNum& seqNum);
  void createDbCheckpointAsync(const SeqNum& seqNum, const std::optional<Timestamp>& timestamp);
  Seconds getLastCheckpointCreationTime() const { return lastCheckpointCreationTime_; }
  void initializeDbCheckpointManager(std::shared_ptr<concord::storage::IDBClient> dbClient,
                                     std::shared_ptr<bftEngine::impl::PersistentStorage> p,
                                     std::shared_ptr<concordMetrics::Aggregator> aggregator,
                                     const std::function<uint64_t()>& getLastBlockIdCb);

  static DbCheckpointManager& instance(InternalBftClient* client = nullptr) {
    static DbCheckpointManager instance(client);
    return instance;
  }
  ~DbCheckpointManager() {
    stopped_ = true;
    if (monitorThread_.joinable()) monitorThread_.join();
  }
  void sendInternalCreateDbCheckpointMsg(const SeqNum& seqNum);

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
  Status createDbCheckpoint(const uint64_t& checkPointId,
                            const uint64_t& lastBlockId,
                            const uint64_t& seqNum,
                            const std::optional<Timestamp>& timestamp);
  void removeCheckpoint(const uint64_t& checkPointId);
  void removeAllCheckpoints() const;
  void cleanUp();
  std::function<uint64_t()> getLastBlockIdCb_;
  // get total size recursively
  uint64_t directorySize(const _fs::path& directory, const bool& excludeHardLinks, bool recursive);
  // get checkpoint metadata
  void loadCheckpointDataFromPersistence();
  void checkforCleanup();
  void checkAndRemove();
  void removeOldestDbCheckpoint();
  void updateDbCheckpointMetadata();
  InternalBftClient* client_;
  std::atomic<bool> stopped_ = false;
  DbCheckpointMetadata dbCheckptMetadata_;
  std::shared_ptr<concord::storage::IDBClient> dbClient_;
  std::shared_ptr<bftEngine::impl::PersistentStorage> ps_;
  // this thread minitors if we over use the disk space
  // for rocksDb checkpooints then, it logs error and
  // cleans up oldest checkpoint
  std::thread monitorThread_;
  std::mutex lock_;
  uint32_t maxNumOfCheckpoints_{0};  // 0-disabled
  uint64_t lastCheckpointSeqNum_{0};
  std::chrono::seconds lastCheckpointCreationTime_{duration_cast<Seconds>(SystemClock::now().time_since_epoch())};
  std::string dbCheckPointDirPath_;
  concordMetrics::Component metrics_;
  concordMetrics::GaugeHandle maxDbCheckpointCreationTimeMsec_;
  concordMetrics::GaugeHandle lastDbCheckpointSizeInMb_;
  concordMetrics::GaugeHandle lastDbCheckpointBlockId_;
  concordMetrics::CounterHandle numOfDbCheckpointsCreated_;
};

}  // namespace bftEngine::impl
