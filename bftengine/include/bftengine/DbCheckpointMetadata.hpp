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
#include <PrimitiveTypes.hpp>
#include <vector>
#include <chrono>

namespace bftEngine::impl {
constexpr uint32_t MAX_ALLOWED_CHECKPOINTS{100};
using CheckpointId = uint64_t;
using TimeDuration = std::chrono::duration<long>;
using SeqNum = bftEngine::impl::SeqNum;

struct DbCheckpointMetadata : public concord::serialize::SerializableFactory<DbCheckpointMetadata> {
  struct DbCheckPointDescriptor : public concord::serialize::SerializableFactory<DbCheckPointDescriptor> {
    // unique id for the checkpoint
    CheckpointId checkPointId_{0};
    // number of seconds since epoch
    TimeDuration creationTimeSinceEpoch_;
    // last block Id/ For now checkPointId = lastBlockId
    uint64_t lastBlockId_{0};
    // last SeqNum at which db_checkpoint is created
    SeqNum lastDbCheckpointSeqNum_{0};

    DbCheckPointDescriptor(const CheckpointId& id = 0,
                           const TimeDuration& t = TimeDuration{0},
                           const uint64_t& lastBlockId = 0,
                           const SeqNum& seq = 0)
        : checkPointId_{id}, creationTimeSinceEpoch_{t}, lastBlockId_{lastBlockId}, lastDbCheckpointSeqNum_{seq} {}
    void serializeDataMembers(std::ostream& outStream) const override {
      serialize(outStream, checkPointId_);
      serialize(outStream, creationTimeSinceEpoch_);
      serialize(outStream, lastBlockId_);
      serialize(outStream, lastDbCheckpointSeqNum_);
    }
    void deserializeDataMembers(std::istream& inStream) override {
      deserialize(inStream, checkPointId_);
      deserialize(inStream, creationTimeSinceEpoch_);
      deserialize(inStream, lastBlockId_);
      deserialize(inStream, lastDbCheckpointSeqNum_);
    }
  };
  std::map<CheckpointId, DbCheckPointDescriptor> dbCheckPoints_;
  uint64_t lastCmdSeqNum_{0};
  TimeDuration lastCmdTimestamp_{0};
  DbCheckpointMetadata() = default;
  void serializeDataMembers(std::ostream& outStream) const override {
    serialize(outStream, lastCmdSeqNum_);
    serialize(outStream, lastCmdTimestamp_);
    serialize(outStream, dbCheckPoints_);
  }
  void deserializeDataMembers(std::istream& inStream) override {
    deserialize(inStream, lastCmdSeqNum_);
    deserialize(inStream, lastCmdTimestamp_);
    deserialize(inStream, dbCheckPoints_);
  }
};
constexpr size_t DB_CHECKPOINT_METADATA_MAX_SIZE{
    MAX_ALLOWED_CHECKPOINTS * sizeof(std::pair<CheckpointId, DbCheckpointMetadata::DbCheckPointDescriptor>) +
    sizeof(DbCheckpointMetadata::lastCmdSeqNum_) + sizeof(DbCheckpointMetadata::lastCmdTimestamp_)};
}  // namespace bftEngine::impl
