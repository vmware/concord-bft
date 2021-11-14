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
#include "InternalBFTClient.hpp"

namespace bftEngine::impl {
using SeqNum = bftEngine::impl::SeqNum;
using InternalBftClient = bftEngine::impl::InternalBFTClient;
class DbCheckpointManager {
 public:
  void sendInternalCreateDbCheckpointMsg(const SeqNum& seqNum, bool noop);
  void enableDbCheckpoint(bool enable) { enableDbCheckpoint_ = enable; }
  bool isDbCheckpointEnabled() { return enableDbCheckpoint_; }

  void addCreateDbCheckpointCb(const std::function<void(SeqNum)>& cb) {
    if (cb) createDbChecheckpointCb_ = cb;
  }
  void onCreateDbCheckpointMsg(const SeqNum& seqNum) {
    if (createDbChecheckpointCb_) createDbChecheckpointCb_(seqNum);
  }
  void setNextSeqNumToCreateCheckpoint(SeqNum s) { nextSeqNumToCreateCheckpoint_ = s; }
  SeqNum getNextSeqNumToCreateCheckpoint() const { return nextSeqNumToCreateCheckpoint_; }

 public:
  static DbCheckpointManager& Instance(InternalBftClient* client_ = nullptr) {
    static DbCheckpointManager instance_(client_);
    return instance_;
  }

 private:
  DbCheckpointManager(InternalBftClient* client) : client_(client) {}
  bool enableDbCheckpoint_{false};
  SeqNum nextSeqNumToCreateCheckpoint_{0};
  InternalBftClient* client_;
  std::function<void(SeqNum)> createDbChecheckpointCb_;
};

}  // namespace bftEngine::impl
