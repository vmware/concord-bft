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
#include "assertUtils.hpp"
#include "db_checkpoint.h"
#include <utility>

namespace concord::kvbc {
using Clock = std::chrono::steady_clock;
using SystemClock = std::chrono::system_clock;
Status RocksDbCheckPointManager::createDbCheckpoint(const CheckpointId& checkPointId, const uint64_t& lastBlockId) {
  if (!maxNumOfCheckpoints_) return Status::OK();
  if (dbCheckptMetadata_.dbCheckPoints_.find(checkPointId) == dbCheckptMetadata_.dbCheckPoints_.end()) {
    auto start = Clock::now();
    auto status = rocksDbClient_->createCheckpoint(checkPointId);
    if (!status.isOK()) {
      LOG_ERROR(getLogger(), "Failed to create rocksdb checkpoint: " << KVLOG(checkPointId));
      return status;
    }
    auto end = Clock::now();
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    LOG_INFO(getLogger(), "rocksdb checkpoint created: " << KVLOG(checkPointId, duration_ms.count()));
    dbCheckptMetadata_.dbCheckPoints_.insert(
        {checkPointId,
         {checkPointId,
          std::chrono::duration_cast<std::chrono::seconds>(SystemClock::now().time_since_epoch()),
          lastBlockId}});
    while (dbCheckptMetadata_.dbCheckPoints_.size() > maxNumOfCheckpoints_) {
      auto it = dbCheckptMetadata_.dbCheckPoints_.begin();
      {
        std::lock_guard<std::mutex> lg(lock_);
        checkpointToBeRemoved_.push(it->first);
      }
      dbCheckptMetadata_.dbCheckPoints_.erase(it);
    }
    // write metadata to persistence
    // atomicWrite
    std::ostringstream outStream;
    concord::serialize::Serializable::serialize(outStream, dbCheckptMetadata_);
    auto data = outStream.str();
    std::vector<uint8_t> v(data.begin(), data.end());
    ps_->setDbCheckpointMetadata(v);
    {
      std::lock_guard<std::mutex> lg(lock_);
      if (!checkpointToBeRemoved_.empty()) {
        cv_.notify_one();
      }
    }
  }
  return Status::OK();
}
void RocksDbCheckPointManager::loadCheckpointDataFromPersistence() {
  auto max_buff_size = maxNumOfCheckpoints_ * sizeof(std::pair<CheckpointId, DbCheckpointMetadata>);
  std::optional<std::vector<std::uint8_t>> db_checkpoint_metadata = ps_->getDbCheckpointMetadata(max_buff_size);
  if (db_checkpoint_metadata.has_value()) {
    std::string data(db_checkpoint_metadata.value().begin(), db_checkpoint_metadata.value().end());
    std::istringstream inStream;
    inStream.str(data);
    concord::serialize::Serializable::deserialize(inStream, dbCheckptMetadata_);
  }
}
void RocksDbCheckPointManager::checkforCleanup() {
  std::unique_lock<std::mutex> lk(lock_);
  while (checkpointToBeRemoved_.empty()) {
    cv_.wait_for(lk, std::chrono::seconds(2), [this]() { return !checkpointToBeRemoved_.empty(); });
  }
  auto id = std::move(checkpointToBeRemoved_.front());
  checkpointToBeRemoved_.pop();
  removeCheckpoint(id);
}
void RocksDbCheckPointManager::init() {
  // check if there is chkpt data in persistence
  loadCheckpointDataFromPersistence();

  // if db checkpoint creation is disabled then remove all checkpoints
  if (!maxNumOfCheckpoints_ && !dbCheckptMetadata_.dbCheckPoints_.empty()) {
    dbCheckptMetadata_.dbCheckPoints_.clear();
    std::ostringstream outStream;
    concord::serialize::Serializable::serialize(outStream, dbCheckptMetadata_);
    auto data = outStream.str();
    std::vector<uint8_t> v(data.begin(), data.end());
    ps_->setDbCheckpointMetadata(v);
    removeAllCheckpoint();
    return;
  }
  // start cleanup thread if checkpoint collection is enabled
  cleanupThread_ = std::thread([this]() {
    while (maxNumOfCheckpoints_) {
      checkforCleanup();
    }
  });

  // if there is a checkpoint created in database but entry corresponding to that is not found in persistence
  // then probably, its a partially created checkpoint and we want to remove it
  auto listOfCheckpointsCreated = rocksDbClient_->getListOfCreatedCheckpoints();
  for (auto& cp : listOfCheckpointsCreated) {
    if (dbCheckptMetadata_.dbCheckPoints_.find(cp) == dbCheckptMetadata_.dbCheckPoints_.end()) {
      std::lock_guard<std::mutex> lg(lock_);
      checkpointToBeRemoved_.push(cp);
    }
  }
  {
    std::lock_guard<std::mutex> lg(lock_);
    if (!checkpointToBeRemoved_.empty()) {
      cv_.notify_one();
    }
  }
}
void RocksDbCheckPointManager::removeCheckpoint(const uint64_t& checkPointId) {
  return rocksDbClient_->removeCheckpoint(checkPointId);
}
void RocksDbCheckPointManager::removeAllCheckpoint() {
  auto listOfCheckpointsCreated = rocksDbClient_->getListOfCreatedCheckpoints();
  for (auto& cp : listOfCheckpointsCreated) {
    removeCheckpoint(cp);
  }
}
}  // namespace concord::kvbc
