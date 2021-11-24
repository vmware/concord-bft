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

#include <vector>
#include <chrono>
#include <PrimitiveTypes.hpp>

namespace bftEngine::impl {
using SeqNum = bftEngine::impl::SeqNum;
class DbCheckpointManager {
 public:
  void addCreateDbCheckpointCb(const std::function<void(SeqNum)>& cb) {
    if (cb) createDbChecheckpointCb_ = cb;
  }
  void onCreateDbCheckpoint(const SeqNum& seqNum) {
    if (createDbChecheckpointCb_) createDbChecheckpointCb_(seqNum);
  }
  void onStableCheckPoint(const SeqNum& seqNum) const {
    if (onSatbleCheckpointCb_) onSatbleCheckpointCb_(seqNum);
  }
  void addOnStableSeqNum(std::function<void(const SeqNum&)> cb) { onSatbleCheckpointCb_ = cb; }
  void setLastCheckpointCreationTime(const std::chrono::seconds& lastTime) { lastCheckpointCreationTime_ = lastTime; }
  std::chrono::seconds getLastCheckpointCreationTime() const { return lastCheckpointCreationTime_; }

 public:
  static DbCheckpointManager& Instance() {
    static DbCheckpointManager instance_;
    return instance_;
  }

 private:
  DbCheckpointManager() = default;
  std::function<void(SeqNum)> createDbChecheckpointCb_;
  std::function<void(SeqNum)> onSatbleCheckpointCb_;
  std::chrono::seconds lastCheckpointCreationTime_{0};
};

}  // namespace bftEngine::impl
