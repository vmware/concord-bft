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

#include "DbCheckpointManager.hpp"
#include <future>
#include <cmath>
#include <assertUtils.hpp>
#include "Serializable.h"
#include "concord.cmf.hpp"
#include "SigManager.hpp"
#include "Replica.hpp"

namespace bftEngine::impl {
using Clock = std::chrono::steady_clock;
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
Status DbCheckpointManager::createDbCheckpoint(const CheckpointId& checkPointId,
                                               const uint64_t& lastBlockId,
                                               const SeqNum& seqNum) {
  if (maxNumOfCheckpoints_ == 0) return Status::OK();
  if (seqNum <= lastCheckpointSeqNum_) return Status::OK();
  ConcordAssert(dbClient_.get() != nullptr);
  ConcordAssert(ps_.get() != nullptr);
  if (dbCheckptMetadata_.dbCheckPoints_.find(checkPointId) == dbCheckptMetadata_.dbCheckPoints_.end()) {
    auto start = Clock::now();
    auto status = dbClient_->createCheckpoint(checkPointId);
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
    LOG_INFO(getLogger(), "rocksdb checkpoint created: " << KVLOG(checkPointId, duration_ms.count(), seqNum));

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
      const auto& checkpointDir = dbClient_->getCheckpointPath();
      if (auto it = dbCheckptMetadata_.dbCheckPoints_.rbegin(); it != dbCheckptMetadata_.dbCheckPoints_.rend()) {
        _fs::path path(checkpointDir);
        _fs::path chkptIdPath = path / std::to_string(it->first);
        auto lastDbCheckpointSize = directorySize(chkptIdPath, false);
        lastDbCheckpointSizeInMb_.Get().Set(lastDbCheckpointSize / (1024 * 1024));
        metrics_.UpdateAggregator();
        LOG_INFO(getLogger(),
                 "rocksdb check point id:" << it->first << ", size: " << HumanReadable{lastDbCheckpointSize});
        LOG_DEBUG(GL, "-- RocksDb checkpoint metrics dump--" + metrics_.ToJson());
      }
    });
  }

  return Status::OK();
}
void DbCheckpointManager::loadCheckpointDataFromPersistence() {
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
void DbCheckpointManager::checkforCleanup() {
  std::unique_lock<std::mutex> lk(lock_);
  while (!stopped_ && checkpointToBeRemoved_.empty()) {
    cv_.wait(lk, [this]() { return (!checkpointToBeRemoved_.empty() || stopped_); });
  }
  if (!checkpointToBeRemoved_.empty()) {
    auto id = checkpointToBeRemoved_.front();
    checkpointToBeRemoved_.pop();
    removeCheckpoint(id);
  }
}
void DbCheckpointManager::init() {
  // check if there is chkpt data in persistence
  loadCheckpointDataFromPersistence();
  // start cleanup thread if checkpoint collection is enabled
  cleanupThread_ = std::thread([this]() {
    while (maxNumOfCheckpoints_ && !stopped_) {
      checkforCleanup();
    }
  });

  // if there is a checkpoint created in database but entry corresponding to that is not found in persistence
  // then probably, its a partially created checkpoint and we want to remove it
  auto listOfCheckpointsCreated = dbClient_->getListOfCreatedCheckpoints();
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
void DbCheckpointManager::removeCheckpoint(const uint64_t& checkPointId) {
  return dbClient_->removeCheckpoint(checkPointId);
}
void DbCheckpointManager::removeAllCheckpoints() const { return dbClient_->removeAllCheckpoints(); }
void DbCheckpointManager::cleanUp() {
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
uint64_t DbCheckpointManager::directorySize(const _fs::path& directory, const bool& excludeHardLinks) {
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

void DbCheckpointManager::initializeDbCheckpointManager(std::shared_ptr<concord::storage::IDBClient> dbClient,
                                                        std::shared_ptr<bftEngine::impl::PersistentStorage> p,
                                                        std::shared_ptr<concordMetrics::Aggregator> aggregator,
                                                        const std::function<uint64_t()>& getLastBlockIdCb) {
  dbClient_ = dbClient;
  ps_ = p;
  dbCheckPointDirPath_ = ReplicaConfig::instance().getdbCheckpointDirPath();
  dbClient_->setCheckpointPath(dbCheckPointDirPath_);
  maxNumOfCheckpoints_ =
      std::min(ReplicaConfig::instance().maxNumberOfDbCheckpoints, bftEngine::impl::MAX_ALLOWED_CHECKPOINTS);
  metrics_.SetAggregator(aggregator);
  if (getLastBlockIdCb) getLastBlockIdCb_ = getLastBlockIdCb;
  if (maxNumOfCheckpoints_) {
    init();
  } else {
    // db checkpoint is disabled. Cleanup metadata and checkpoints created if any
    auto ret = std::async(std::launch::async, [this]() { cleanUp(); });
  }
}

void DbCheckpointManager::createDbCheckpoint(const SeqNum& seqNum) {
  if (getLastBlockIdCb_) {
    const auto& lastBlockid = getLastBlockIdCb_();
    createDbCheckpoint(lastBlockid, lastBlockid, seqNum);  // checkpoint id and last block id is same
    return;
  }
  LOG_ERROR(getLogger(), "Failed to create db checkpoint. getLastBlockId cb is not set");
}

void DbCheckpointManager::setNextStableSeqNumToCreateSnapshot(const std::optional<SeqNum>& seqNum) {
  if (seqNum == std::nullopt) {
    nextStableSeqNum_ = std::nullopt;
    return;
  }
  SeqNum seq_num_to_create_snapshot = (seqNum.value() + checkpointWindowSize);
  seq_num_to_create_snapshot = seq_num_to_create_snapshot - (seq_num_to_create_snapshot % checkpointWindowSize);
  nextStableSeqNum_ = seq_num_to_create_snapshot;
  LOG_INFO(getLogger(), "setNextStableSeqNumToCreateSnapshot, nextStableSeqNum_: " << nextStableSeqNum_.value());
}

void DbCheckpointManager::sendInternalClientRequestMsg(const SeqNum& seqNum) {
  // send empty req to achieve next stable seq number
  LOG_INFO(getLogger(), "sending dummy client Requests to bring the system into stable checkpoint");
  auto replica_id = bftEngine::ReplicaConfig::instance().getreplicaId();
  concord::messages::ReconfigurationRequest req;
  req.sender = replica_id;
  req.command = concord::messages::CreateDbCheckpointCommand{replica_id};
  std::vector<uint8_t> data_vec;
  concord::messages::serialize(data_vec, req);
  std::string sig(SigManager::instance()->getMySigLength(), '\0');
  uint16_t sig_length{0};
  SigManager::instance()->sign(reinterpret_cast<char*>(data_vec.data()), data_vec.size(), sig.data(), sig_length);
  req.signature = std::vector<uint8_t>(sig.begin(), sig.end());
  data_vec.clear();
  concord::messages::serialize(data_vec, req);
  std::string strMsg(data_vec.begin(), data_vec.end());
  std::string cid = "dbCheckpoint_" + std::to_string(seqNum) + "_" + std::to_string(replica_id);
  if (client_) client_->sendRequest(bftEngine::RECONFIG_FLAG, strMsg.size(), strMsg.c_str(), cid);
}
}  // namespace bftEngine::impl