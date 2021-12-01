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

void DbCheckpointManager::initializeDbCheckpointHanlder(const std::shared_ptr<concord::storage::IDBClient>& dbClient,
                                                        std::shared_ptr<bftEngine::impl::PersistentStorage> p,
                                                        std::shared_ptr<concordMetrics::Aggregator> aggregator,
                                                        const std::function<uint64_t()>& getLastBlockIdCb) {
  dbCheckpointHandler_ = std::make_unique<concord::storage::RocksDbCheckPointHandler>(
      dbClient,
      ReplicaConfig::instance().getmaxNumberOfDbCheckpoints(),
      ReplicaConfig::instance().getdbCheckpointDirPath(),
      aggregator);
  if (getLastBlockIdCb) getLastBlockIdCb_ = getLastBlockIdCb;
  dbCheckpointHandler_->setPersistentStorage(p);
  if (ReplicaConfig::instance().getmaxNumberOfDbCheckpoints()) {
    dbCheckpointHandler_->init();
    lastCheckpointCreationTime_ = dbCheckpointHandler_->getLastCheckpointCreationTime();
  } else {
    // db checkpoint is disabled. Cleanup metadata and checkpoints created if any
    auto ret = std::async(std::launch::async, [this]() { dbCheckpointHandler_->cleanUp(); });
  }
}

void DbCheckpointManager::createDbCheckpoint(const SeqNum& seqNum) {
  if (getLastBlockIdCb_) {
    const auto& lastBlockid = getLastBlockIdCb_();
    dbCheckpointHandler_->createDbCheckpoint(
        lastBlockid, lastBlockid, seqNum);  // checkpoint id and last block id is same
    lastCheckpointCreationTime_ = dbCheckpointHandler_->getLastCheckpointCreationTime();
    return;
  }
  LOG_ERROR(getLogger(), "Failed to create db checkpoint. getLastBlockId cb is not set");
}