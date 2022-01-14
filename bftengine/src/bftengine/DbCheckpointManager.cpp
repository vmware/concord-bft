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
#include "db_checkpoint_msg.cmf.hpp"
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
                                               const BlockId& lastBlockId,
                                               const SeqNum& seqNum,
                                               const std::optional<Timestamp>& timestamp) {
  ConcordAssert(dbClient_.get() != nullptr);
  ConcordAssert(ps_.get() != nullptr);
  {
    std::scoped_lock lock(lock_);
    if (dbCheckptMetadata_.dbCheckPoints_.find(checkPointId) == dbCheckptMetadata_.dbCheckPoints_.end()) {
      auto start = Clock::now();
      auto status = dbClient_->createCheckpoint(checkPointId);
      if (!status.isOK()) {
        LOG_ERROR(getLogger(), "Failed to create rocksdb checkpoint: " << KVLOG(checkPointId));
        return status;
      }
      prepareCheckpointCb_(lastBlockId, dbClient_->getPathForCheckpoint(checkPointId));
      auto end = Clock::now();

      auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
      lastDbCheckpointBlockId_.Get().Set(lastBlockId);
      numOfDbCheckpointsCreated_++;
      auto maxSoFar = maxDbCheckpointCreationTimeMsec_.Get().Get();
      maxDbCheckpointCreationTimeMsec_.Get().Set(std::max(maxSoFar, static_cast<uint64_t>(duration_ms.count())));
      auto count = numOfDbCheckpointsCreated_.Get().Get();
      (void)count;
      metrics_.UpdateAggregator();
      LOG_INFO(getLogger(), "rocksdb checkpoint created: " << KVLOG(checkPointId, duration_ms.count(), seqNum));
      lastCheckpointSeqNum_ = seqNum;
      {
        std::scoped_lock lock(lockLastDbCheckpointDesc_);
        lastCreatedCheckpointMetadata_.emplace(DbCheckpointMetadata::DbCheckPointDescriptor{
            checkPointId, lastCheckpointCreationTime_, lastBlockId, lastCheckpointSeqNum_});
        dbCheckptMetadata_.dbCheckPoints_.insert({checkPointId, lastCreatedCheckpointMetadata_.value()});
      }
      updateDbCheckpointMetadata();
    }
    while (dbCheckptMetadata_.dbCheckPoints_.size() > maxNumOfCheckpoints_) {
      removeOldestDbCheckpoint();
    }
  }
  updateMetrics();
  return Status::OK();
}
void DbCheckpointManager::loadCheckpointDataFromPersistence() {
  constexpr auto max_buff_size = DB_CHECKPOINT_METADATA_MAX_SIZE;
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
      lastCreatedCheckpointMetadata_ = it->second;
      lastCheckpointSeqNum_ = it->second.lastDbCheckpointSeqNum_;
      lastDbCheckpointBlockId_.Get().Set(it->second.lastBlockId_);
      metrics_.UpdateAggregator();
      LOG_INFO(getLogger(), KVLOG(lastCheckpointSeqNum_));
    }
    lastCheckpointCreationTime_ = dbCheckptMetadata_.lastCmdTimestamp_;
  }
}

void DbCheckpointManager::init() {
  // check if there is chkpt data in persistence
  loadCheckpointDataFromPersistence();
  // if there is a checkpoint created in database but entry corresponding to that is not found in persistence
  // then probably, its a partially created checkpoint and we want to remove it
  auto listOfCheckpointsCreated = dbClient_->getListOfCreatedCheckpoints();
  for (auto& cp : listOfCheckpointsCreated) {
    if (dbCheckptMetadata_.dbCheckPoints_.find(cp) == dbCheckptMetadata_.dbCheckPoints_.end()) {
      removeCheckpoint(cp);
    }
  }

  monitorThread_ = std::thread([this]() {
    while (maxNumOfCheckpoints_ && !stopped_) {
      checkAndRemove();
      std::this_thread::sleep_for(std::chrono::seconds{60});
    }
  });
}
void DbCheckpointManager::removeCheckpoint(const CheckpointId& checkPointId) {
  return dbClient_->removeCheckpoint(checkPointId);
}
void DbCheckpointManager::removeAllCheckpoints() const { return dbClient_->removeAllCheckpoints(); }
void DbCheckpointManager::cleanUp() {
  // this gets called when db checkpoint is disabled
  // check if there is chkpt data in persistence
  loadCheckpointDataFromPersistence();
  if (!maxNumOfCheckpoints_) {
    if (!dbCheckptMetadata_.dbCheckPoints_.empty()) {
      dbCheckptMetadata_.dbCheckPoints_.clear();
      updateDbCheckpointMetadata();
    }
    removeAllCheckpoints();
  }
}
uint64_t DbCheckpointManager::directorySize(const _fs::path& directory, const bool& excludeHardLinks, bool recursive) {
  uint64_t size{0};
  try {
    if (_fs::exists(directory)) {
      for (const auto& entry : _fs::recursive_directory_iterator(directory)) {
        if (_fs::is_regular_file(entry) && !_fs::is_symlink(entry)) {
          if (_fs::hard_link_count(entry) > 1 && excludeHardLinks) continue;
          size += _fs::file_size(entry);
        } else if (_fs::is_directory(entry) && recursive) {
          size += directorySize(entry.path(), excludeHardLinks, recursive);
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
                                                        const std::function<BlockId()>& getLastBlockIdCb,
                                                        const PrepareCheckpointCallback& prepareCheckpointCb) {
  dbClient_ = dbClient;
  ps_ = p;
  dbCheckPointDirPath_ = ReplicaConfig::instance().getdbCheckpointDirPath();
  dbClient_->setCheckpointPath(dbCheckPointDirPath_);
  maxNumOfCheckpoints_ =
      std::min(ReplicaConfig::instance().maxNumberOfDbCheckpoints, bftEngine::impl::MAX_ALLOWED_CHECKPOINTS);
  metrics_.SetAggregator(aggregator);
  if (getLastBlockIdCb) getLastBlockIdCb_ = getLastBlockIdCb;
  prepareCheckpointCb_ = prepareCheckpointCb;
  if (ReplicaConfig::instance().dbCheckpointFeatureEnabled) {
    init();
  } else {
    // db checkpoint is disabled. Cleanup metadata and checkpoints created if any
    cleanUpFuture_ = std::async(std::launch::async, [this]() { cleanUp(); });
  }
}
std::optional<DbCheckpointMetadata::DbCheckPointDescriptor> DbCheckpointManager::getLastCreatedDbCheckpointMetadata() {
  std::scoped_lock lock(lockLastDbCheckpointDesc_);
  return lastCreatedCheckpointMetadata_;
}
std::optional<CheckpointId> DbCheckpointManager::createDbCheckpointAsync(const SeqNum& seqNum,
                                                                         const std::optional<Timestamp>& timestamp) {
  if (seqNum <= lastCheckpointSeqNum_) {
    LOG_ERROR(getLogger(), "createDb checkpoint failed." << KVLOG(seqNum, lastCheckpointSeqNum_));
    return std::nullopt;
  }
  // We can have some replicas configured to not create db checkpoint. This is because,
  // backup functionality can be supported with a minimum of (f+1) replicas also
  // However, we need to store the last request seqNum and timestamp because, all replicas do consensus
  // to start the command execution at some seqNum but only replicas with non-zero numOfDbCheckpoints config
  // actually creates db checkpoint. Hence, primary needs to know what was the last cmd seqNum and timestamp
  updateLastCmdInfo(seqNum, timestamp);
  if (ReplicaConfig::instance().getmaxNumberOfDbCheckpoints() == 0) {
    LOG_WARN(getLogger(),
             "createDb checkpoint failed. Max allowed db checkpoint is configured to "
                 << ReplicaConfig::instance().getmaxNumberOfDbCheckpoints());
    return std::nullopt;
  }
  // Get last blockId from rocksDb
  const auto lastBlockid = getLastBlockIdCb_();
  std::scoped_lock lock(lockDbCheckPtFuture_);
  if (dbCreateCheckPtFuture_.find(lastBlockid) != dbCreateCheckPtFuture_.end()) {
    // rocksDb checkpoint with this lastBlock Id is already created or in progress
    // request to create rocksDb checkpoint is rejected in this case
    return std::nullopt;
  }
  // start async create db checkpoint operation
  auto ret = std::async(std::launch::async, [this, seqNum, timestamp, lastBlockid]() -> void {
    createDbCheckpoint(lastBlockid, lastBlockid, seqNum, timestamp);
  });
  // store the future for the async task
  dbCreateCheckPtFuture_.emplace(std::make_pair(lastBlockid, std::move(ret)));
  return lastBlockid;
}

void DbCheckpointManager::checkAndRemove() {
  // get current db size
  _fs::path dbPath(dbClient_->getPath());
  const auto& dbCurrentSize = directorySize(dbPath, false, false);
  try {
    const _fs::space_info diskSpace = _fs::space(dbPath);
    // make sure that we have at least equal amount of free storage available
    // all the time to avoid any failure dure to low disk space
    if (diskSpace.available < dbCurrentSize) {
      LOG_WARN(getLogger(),
               "low disk space. Removing oldest db checkpoint. " << KVLOG(dbCurrentSize, diskSpace.available));
      {
        std::scoped_lock lock(lock_);
        removeOldestDbCheckpoint();
      }
    }
  } catch (std::exception& e) {
    LOG_FATAL(getLogger(), "Failed to get the available db size on disk: " << e.what());
  }
}

void DbCheckpointManager::removeOldestDbCheckpoint() {
  if (auto it = dbCheckptMetadata_.dbCheckPoints_.begin(); it != dbCheckptMetadata_.dbCheckPoints_.end()) {
    removeCheckpoint(it->second.checkPointId_);
    dbCheckptMetadata_.dbCheckPoints_.erase(it);
    LOG_INFO(getLogger(), "removed db checkpoint, id: " << it->second.checkPointId_);
    updateDbCheckpointMetadata();
    removeDbCheckpointFuture(it->second.checkPointId_);
    if (dbCheckptMetadata_.dbCheckPoints_.size() == 0) {
      std::scoped_lock lock(lockLastDbCheckpointDesc_);
      lastCreatedCheckpointMetadata_ = std::nullopt;
    }
  }
}
void DbCheckpointManager::removeDbCheckpointFuture(CheckpointId id) {
  std::scoped_lock lock(lockDbCheckPtFuture_);
  dbCreateCheckPtFuture_.erase(id);
}
void DbCheckpointManager::updateDbCheckpointMetadata() {
  std::ostringstream outStream;
  concord::serialize::Serializable::serialize(outStream, dbCheckptMetadata_);
  auto data = outStream.str();
  std::vector<uint8_t> v(data.begin(), data.end());
  ps_->setDbCheckpointMetadata(v);
}
void DbCheckpointManager::sendInternalCreateDbCheckpointMsg(const SeqNum& seqNum, bool noop) {
  auto replica_id = bftEngine::ReplicaConfig::instance().getreplicaId();
  concord::messages::db_checkpoint_msg::CreateDbCheckpoint req;
  req.sender = replica_id;
  req.seqNum = seqNum;
  req.noop = noop;
  std::vector<uint8_t> data_vec;
  concord::messages::db_checkpoint_msg::serialize(data_vec, req);
  std::string sig(SigManager::instance()->getMySigLength(), '\0');
  uint16_t sig_length{0};
  SigManager::instance()->sign(reinterpret_cast<char*>(data_vec.data()), data_vec.size(), sig.data(), sig_length);
  req.signature = std::vector<uint8_t>(sig.begin(), sig.end());
  data_vec.clear();
  concord::messages::db_checkpoint_msg::serialize(data_vec, req);
  std::string strMsg(data_vec.begin(), data_vec.end());
  std::string cid = "replicaDbCheckpoint_" + std::to_string(seqNum) + "_" + std::to_string(replica_id);
  if (client_) client_->sendRequest(bftEngine::DB_CHECKPOINT_FLAG, strMsg.size(), strMsg.c_str(), cid);
}

void DbCheckpointManager::setNextStableSeqNumToCreateSnapshot(const std::optional<SeqNum>& seqNum) {
  if (seqNum == std::nullopt) {
    nextSeqNumToCreateCheckPt_ = std::nullopt;
    return;
  }
  SeqNum seq_num_to_create_snapshot = (seqNum.value() + checkpointWindowSize);
  seq_num_to_create_snapshot = seq_num_to_create_snapshot - (seq_num_to_create_snapshot % checkpointWindowSize);
  nextSeqNumToCreateCheckPt_ = seq_num_to_create_snapshot;
  LOG_INFO(getLogger(), "setNextStableSeqNumToCreateSnapshot " << KVLOG(nextSeqNumToCreateCheckPt_.value()));
}
void DbCheckpointManager::updateMetrics() {
  const auto& checkpointDir = dbClient_->getCheckpointPath();
  if (const auto it = dbCheckptMetadata_.dbCheckPoints_.crbegin(); it != dbCheckptMetadata_.dbCheckPoints_.crend()) {
    _fs::path path(checkpointDir);
    _fs::path chkptIdPath = path / std::to_string(it->first);
    auto lastDbCheckpointSize = directorySize(chkptIdPath, false, true);
    lastDbCheckpointSizeInMb_.Get().Set(lastDbCheckpointSize / (1024 * 1024));
    metrics_.UpdateAggregator();
    LOG_INFO(getLogger(), "rocksdb check point id:" << it->first << ", size: " << HumanReadable{lastDbCheckpointSize});
    LOG_INFO(GL, "-- RocksDb checkpoint metrics dump--" + metrics_.ToJson());
  }
}
void DbCheckpointManager::updateLastCmdInfo(const SeqNum& seqNum, const std::optional<Timestamp>& timestamp) {
  dbCheckptMetadata_.lastCmdSeqNum_ = seqNum;
  if (timestamp.has_value()) {
    dbCheckptMetadata_.lastCmdTimestamp_ = std::chrono::duration_cast<Seconds>(timestamp.value().time_since_epoch);
  } else {
    dbCheckptMetadata_.lastCmdTimestamp_ = std::chrono::duration_cast<Seconds>(SystemClock::now().time_since_epoch());
  }
  lastCheckpointCreationTime_ = dbCheckptMetadata_.lastCmdTimestamp_;
  if (maxNumOfCheckpoints_ == 0) {
    // update lastCmdSeqNum and timestamp
    std::scoped_lock lock(lock_);
    updateDbCheckpointMetadata();
  }
}
}  // namespace bftEngine::impl
