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

#include "Serializable.h"
#include "DbCheckpointHandler.hpp"
#include <vector>
#include <chrono>
#include <PrimitiveTypes.hpp>
#include "callback_registry.hpp"

namespace bftEngine::impl {
using SeqNum = bftEngine::impl::SeqNum;
class DbCheckpointManager {
 public:
  void createDbCheckpoint(const SeqNum& seqNum);
  void setLastCheckpointCreationTime(const std::chrono::seconds& lastTime) { lastCheckpointCreationTime_ = lastTime; }
  std::chrono::seconds getLastCheckpointCreationTime() const { return lastCheckpointCreationTime_; }
  void initializeDbCheckpointHanlder(const std::shared_ptr<concord::storage::IDBClient>& dbClient,
                                     std::shared_ptr<bftEngine::impl::PersistentStorage> p,
                                     std::shared_ptr<concordMetrics::Aggregator> aggregator,
                                     const std::function<uint64_t()>& getLastBlockIdCb);

 public:
  static DbCheckpointManager& instance() {
    static DbCheckpointManager instance_;
    return instance_;
  }

 private:
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("concord.bft.db_checkpoint_manager"));
    return logger_;
  }
  DbCheckpointManager() = default;
  std::chrono::seconds lastCheckpointCreationTime_{0};
  std::unique_ptr<concord::storage::IDbCheckPointHandler> dbCheckpointHandler_;
  std::function<uint64_t()> getLastBlockIdCb_;
};

}  // namespace bftEngine::impl
