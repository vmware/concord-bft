// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "PrimitiveTypes.hpp"
#include "TimeUtils.hpp"
#include "messages/CheckpointMsg.hpp"
#include "messages/MsgsCertificate.hpp"
#include "InternalReplicaApi.hpp"
#include "assertUtils.hpp"

namespace bftEngine::impl {

class CheckpointMsg;

template <bool Self = true>
class CheckpointInfo {
 protected:
  struct CheckpointMsgCmp {
    static bool equivalent(CheckpointMsg* a, CheckpointMsg* b);
  };

  MsgsCertificate<CheckpointMsg, Self, Self, true, CheckpointMsg>* checkpointCertificate = nullptr;

  bool sentToAllOrApproved;

  Time executed;  // if != MinTime, represents the execution time of the corresponding sequnce number

 public:
  CheckpointInfo() : sentToAllOrApproved{false}, executed{MinTime} {
    auto& config = ReplicaConfig::instance();
    checkpointCertificate = new MsgsCertificate<CheckpointMsg, Self, Self, true, CheckpointMsg>(
        config.numReplicas, config.fVal, 2 * config.fVal + config.cVal + 1, config.replicaId);
  }

  ~CheckpointInfo() {
    resetAndFree();
    delete checkpointCertificate;
  }

  void resetAndFree() {
    checkpointCertificate->resetAndFree();
    sentToAllOrApproved = false;
    executed = MinTime;
  }

  bool addCheckpointMsg(CheckpointMsg* msg, ReplicaId replicaId) {
    LOG_DEBUG(GL, "Adding checkpoint message");
    return checkpointCertificate->addMsg(msg, replicaId);
  }

  bool isCheckpointCertificateComplete() const {
    LOG_DEBUG(GL, "Checkpoint certificate completed!");
    return checkpointCertificate->isComplete();
  }

  // A replica considers a checkpoint to be super stable if it knows that all n/n replicas have reached to this
  // checkpoint. This is in contrary to stable checkpoint which means that the replica knows that a byzantine quorum of
  // replicas have reached to this checkpoint.
  bool isCheckpointSuperStable() const { return checkpointCertificate->isFull(); }

  CheckpointMsg* selfCheckpointMsg() const { return checkpointCertificate->selfMsg(); }

  const auto& getAllCheckpointMsgs() const { return checkpointCertificate->getAllMsgs(); }

  void tryToMarkCheckpointCertificateCompleted() { checkpointCertificate->tryToMarkComplete(); }

  bool checkpointSentAllOrApproved() const { return sentToAllOrApproved; }

  Time selfExecutionTime() const { return executed; }

  void setSelfExecutionTime(Time t) {
    ConcordAssert(executed == MinTime);
    executed = t;
  }

  void setCheckpointSentAllOrApproved() { sentToAllOrApproved = true; }

  // methods for SequenceWithActiveWindow
  static void init(CheckpointInfo& i, void* d) {
    void* context = d;
    InternalReplicaApi* r = (InternalReplicaApi*)context;

    const ReplicasInfo& info = r->getReplicasInfo();

    const uint16_t myId = info.myId();

    const uint16_t numOfReps = info.numberOfReplicas();
    const uint16_t C = info.cVal();
    const uint16_t F = info.fVal();
    ConcordAssert(numOfReps == 3 * F + 2 * C + 1);
    if (i.checkpointCertificate) delete i.checkpointCertificate;
    i.checkpointCertificate =
        new MsgsCertificate<CheckpointMsg, Self, Self, true, CheckpointMsg>(numOfReps, F, 2 * F + C + 1, myId);
  }

  static void free(CheckpointInfo& i) { i.resetAndFree(); }

  static void reset(CheckpointInfo& i) { i.resetAndFree(); }
};
}  // namespace bftEngine::impl
