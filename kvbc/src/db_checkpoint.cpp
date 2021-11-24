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
#include "db_checkpoint.h"
#include <utility>
#include <cmath>

namespace concord::kvbc {
using Clock = std::chrono::steady_clock;
using SystemClock = std::chrono::system_clock;
struct HumanReadable {
  std::uintmax_t size{};

  template <typename Os>
  friend Os& operator<<(Os& os, HumanReadable hr) {
    int i{};
    double mantissa = hr.size;
    for (; mantissa >= 1024.; ++i) {
      mantissa /= 1024.;
    }
    mantissa = std::ceil(mantissa * 10.) / 10.;
    os << mantissa << "BKMGTPE"[i];
    return i == 0 ? os : os << "B (" << hr.size << ')';
  }
};
Status RocksDbCheckPointManager::createDbCheckpoint(const CheckpointId& checkPointId,
                                                    const uint64_t& lastBlockId,
                                                    const uint64_t& seqNum) {
  if (!maxNumOfCheckpoints_) return Status::OK();
  if (seqNum <= lastCheckpointSeqNum_) return Status::OK();
  if (dbCheckptMetadata_.dbCheckPoints_.find(checkPointId) == dbCheckptMetadata_.dbCheckPoints_.end()) {
    auto start = Clock::now();
    auto status = rocksDbClient_->createCheckpoint(checkPointId);
    if (!status.isOK()) {
      LOG_ERROR(getLogger(), "Failed to create rocksdb checkpoint: " << KVLOG(checkPointId));
      return status;
    }
    auto end = Clock::now();
    lastCheckpointCreationTime_ =
        std::chrono::duration_cast<std::chrono::seconds>(SystemClock::now().time_since_epoch());
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // update metrics
    lastDbCheckpointBlockId_.Get().Set(lastBlockId);
    numOfDbCheckpointsCreated_++;
    auto maxSoFar = maxDbCheckpointCreationTimeMsec_.Get().Get();
    maxDbCheckpointCreationTimeMsec_.Get().Set(std::max(maxSoFar, static_cast<uint64_t>(duration_ms.count())));
    auto count = numOfDbCheckpointsCreated_.Get().Get();
    auto averageSoFar =
        (dbCheckpointCreationAverageTimeMsec_.Get().Get() * (count - 1) + static_cast<uint64_t>(duration_ms.count())) /
        count;
    dbCheckpointCreationAverageTimeMsec_.Get().Set(averageSoFar);
    metrics_.UpdateAggregator();
    LOG_INFO(getLogger(), "rocksdb checkpoint created: " << KVLOG(checkPointId, duration_ms.count()));

    lastCheckpointSeqNum_ = seqNum;
    dbCheckptMetadata_.dbCheckPoints_.insert(
        {checkPointId, {checkPointId, lastCheckpointCreationTime_, lastBlockId, lastCheckpointSeqNum_}});
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
    // update metrics
    auto ret = std::async(std::launch::async, [this]() {
      const auto& checkpointDir = rocksDbClient_->getCheckpointPath();
      if (auto it = dbCheckptMetadata_.dbCheckPoints_.rbegin(); it != dbCheckptMetadata_.dbCheckPoints_.rend()) {
        _fs::path path(checkpointDir);
        _fs::path chkptIdPath = path / std::to_string(it->first);
        auto lastDbCheckpointSize = directorySize(chkptIdPath, false);
        lastDbCheckpointSizeInMb_.Get().Set(lastDbCheckpointSize / (1024 * 1024));
        metrics_.UpdateAggregator();
        LOG_INFO(getLogger(),
                 "rocksdb check point id:" << it->first << ", size: " << HumanReadable{lastDbCheckpointSize});
        LOG_INFO(GL, "-- RocksDb checkpoint metrics dump--" + metrics_.ToJson());
      }
    });
  }

  return Status::OK();
}
void RocksDbCheckPointManager::loadCheckpointDataFromPersistence() {
  auto max_buff_size = bftEngine::impl::MAX_ALLOWED_CHECKPOINTS * sizeof(std::pair<CheckpointId, DbCheckpointMetadata>);
  std::optional<std::vector<std::uint8_t>> db_checkpoint_metadata = ps_->getDbCheckpointMetadata(max_buff_size);
  if (db_checkpoint_metadata.has_value()) {
    std::string data(db_checkpoint_metadata.value().begin(), db_checkpoint_metadata.value().end());
    std::istringstream inStream;
    inStream.str(data);
    concord::serialize::Serializable::deserialize(inStream, dbCheckptMetadata_);
    LOG_INFO(getLogger(),
             "Num of db_checkpoints loaded from persistence " << KVLOG(dbCheckptMetadata_.dbCheckPoints_.size()));
    for (const auto& [db_chkpt_id, db_chk_pt_val] : dbCheckptMetadata_.dbCheckPoints_) {
      LOG_INFO(
          getLogger(),
          KVLOG(db_chkpt_id, db_chk_pt_val.creationTimeSinceEpoch_.count(), db_chk_pt_val.lastDbCheckpointSeqNum_));
    }
    if (auto it = dbCheckptMetadata_.dbCheckPoints_.rbegin(); it != dbCheckptMetadata_.dbCheckPoints_.rend()) {
      lastCheckpointSeqNum_ = it->second.lastDbCheckpointSeqNum_;
      lastDbCheckpointBlockId_.Get().Set(it->second.lastBlockId_);
      lastCheckpointCreationTime_ = it->second.creationTimeSinceEpoch_;
      metrics_.UpdateAggregator();
      LOG_INFO(getLogger(), KVLOG(lastCheckpointSeqNum_));
    }
  }
}
void RocksDbCheckPointManager::checkforCleanup() {
  std::unique_lock<std::mutex> lk(lock_);
  while (checkpointToBeRemoved_.empty()) {
    cv_.wait_for(lk, std::chrono::seconds(2), [this]() { return !checkpointToBeRemoved_.empty(); });
  }
  auto id = checkpointToBeRemoved_.front();
  checkpointToBeRemoved_.pop();
  removeCheckpoint(id);
}
void RocksDbCheckPointManager::init() {
  // check if there is chkpt data in persistence
  loadCheckpointDataFromPersistence();
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
void RocksDbCheckPointManager::removeAllCheckpoints() const { return rocksDbClient_->removeAllCheckpoints(); }
void RocksDbCheckPointManager::cleanUp() {
  // check if there is chkpt data in persistence
  loadCheckpointDataFromPersistence();
  // if db checkpoint creation is disabled then remove all checkpoints
  if (!maxNumOfCheckpoints_) {
    if (!dbCheckptMetadata_.dbCheckPoints_.empty()) {
      dbCheckptMetadata_.dbCheckPoints_.clear();
      std::ostringstream outStream;
      concord::serialize::Serializable::serialize(outStream, dbCheckptMetadata_);
      auto data = outStream.str();
      std::vector<uint8_t> v(data.begin(), data.end());
      ps_->setDbCheckpointMetadata(v);
    }
    removeAllCheckpoints();
  }
}
uint64_t RocksDbCheckPointManager::directorySize(const _fs::path& directory, const bool& excludeHardLinks) {
  uint64_t size{0};
  try {
    if (_fs::exists(directory)) {
      for (const auto& entry : _fs::recursive_directory_iterator(directory)) {
        if (_fs::is_regular_file(entry) && !_fs::is_symlink(entry)) {
          if (_fs::hard_link_count(entry) > 1 && excludeHardLinks) continue;
          size += _fs::file_size(entry);
        } else if (_fs::is_directory(entry)) {
          size += directorySize(entry.path(), excludeHardLinks);
        }
      }
    }
  } catch (std::exception& e) {
    LOG_ERROR(getLogger(), "Failed to find size of the checkpoint dir: " << e.what());
  }
  return size;
}
}  // namespace concord::kvbc
