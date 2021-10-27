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

namespace concord::kvbc {
constexpr int MAX_ALLOWED_CHECKPOINTS{100};
using CheckpointId = uint64_t;
using Time = std::chrono::duration<long>;
using Status = concordUtils::Status;
struct DbCheckpointMetadata : public concord::serialize::SerializableFactory<DbCheckpointMetadata> {
  struct DbCheckPointDescriptor : public concord::serialize::SerializableFactory<DbCheckPointDescriptor> {
    // unique id for the checkpoint
    CheckpointId checkPointId_{0};
    // number of seconds since epoch
    Time creationTimeSinceEpoch_;
    // last block Id/ For now checkPointId = lastBlockId
    uint64_t lastBlockId_{0};

    DbCheckPointDescriptor(const CheckpointId& id = 0, const Time& t = Time{0}, const uint64_t lastBlockId = 0)
        : checkPointId_{id}, creationTimeSinceEpoch_{t}, lastBlockId_{lastBlockId} {}
    void serializeDataMembers(std::ostream& outStream) const override {
      serialize(outStream, checkPointId_);
      serialize(outStream, creationTimeSinceEpoch_);
      serialize(outStream, lastBlockId_);
    }
    void deserializeDataMembers(std::istream& inStream) override {
      deserialize(inStream, checkPointId_);
      deserialize(inStream, creationTimeSinceEpoch_);
      deserialize(inStream, lastBlockId_);
    }
  };
  std::map<CheckpointId, DbCheckPointDescriptor> dbCheckPoints_;
  DbCheckpointMetadata() = default;
  void serializeDataMembers(std::ostream& outStream) const override { serialize(outStream, dbCheckPoints_); }
  void deserializeDataMembers(std::istream& inStream) override { deserialize(inStream, dbCheckPoints_); }
};

class IDbCheckPointManager {
 public:
  // create checkpoint
  virtual Status createDbCheckpoint(const uint64_t& checkPointId, const uint64_t& lastBlockId) = 0;
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
  virtual void removeAllCheckpoint() = 0;

  virtual ~IDbCheckPointManager() = default;
};

class RocksDbCheckPointManager : public IDbCheckPointManager {
 public:
  RocksDbCheckPointManager(const std::shared_ptr<storage::IDBClient>& dbClient, const uint32_t& maxNumOfChkPt = 10)
      : rocksDbClient_{dbClient}, ps_{nullptr}, maxNumOfCheckpoints_{maxNumOfChkPt} {
    if (maxNumOfChkPt > MAX_ALLOWED_CHECKPOINTS) maxNumOfCheckpoints_ = MAX_ALLOWED_CHECKPOINTS;
  }
  void init() override;
  Status createDbCheckpoint(const uint64_t& checkPointId, const uint64_t& lastBlockId) override;

  void removeCheckpoint(const uint64_t& checkPointId) override;
  void setMaxNumOfAllowedCheckpoints(const uint32_t& maxNumDbCheckpoint) override {
    if (maxNumDbCheckpoint > MAX_ALLOWED_CHECKPOINTS) {
      LOG_ERROR(getLogger(),
                "setting maxNumDbCheckpoint checkpoint failed" << KVLOG(maxNumDbCheckpoint, MAX_ALLOWED_CHECKPOINTS));
      return;
    }
    maxNumOfCheckpoints_ = maxNumDbCheckpoint;
  }
  uint32_t getMaxNumOfCheckpoints() const override { return maxNumOfCheckpoints_; }
  void setPersistentStorage(std::shared_ptr<bftEngine::impl::PersistentStorage> p) override { ps_ = p; }
  void removeAllCheckpoint() override;
  ~RocksDbCheckPointManager() { cleanupThread_.join(); }

 private:
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("concord.kvbc.db_checkpoint"));
    return logger_;
  }
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
};
}  // namespace concord::kvbc