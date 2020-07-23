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

#include "CheckpointInfo.hpp"
#include "messages/CheckpointMsg.hpp"
#include "InternalReplicaApi.hpp"
#include "assertUtils.hpp"

namespace bftEngine {
namespace impl {
bool CheckpointInfo::CheckpointMsgCmp::equivalent(CheckpointMsg* a, CheckpointMsg* b) {
  return (a->seqNumber() == b->seqNumber()) && (a->digestOfState() == b->digestOfState());
}

CheckpointInfo::CheckpointInfo() : checkpointCertificate{nullptr}, sentToAllOrApproved{false}, executed{MinTime} {}

CheckpointInfo::~CheckpointInfo() {
  resetAndFree();
  delete checkpointCertificate;
}

void CheckpointInfo::resetAndFree() {
  checkpointCertificate->resetAndFree();
  sentToAllOrApproved = false;
  executed = MinTime;
}

bool CheckpointInfo::addCheckpointMsg(CheckpointMsg* msg, ReplicaId replicaId) {
  return checkpointCertificate->addMsg(msg, replicaId);
}

bool CheckpointInfo::isCheckpointCertificateComplete() const { return checkpointCertificate->isComplete(); }

CheckpointMsg* CheckpointInfo::selfCheckpointMsg() const { return checkpointCertificate->selfMsg(); }

void CheckpointInfo::tryToMarkCheckpointCertificateCompleted() { checkpointCertificate->tryToMarkComplete(); }

bool CheckpointInfo::checkpointSentAllOrApproved() const { return sentToAllOrApproved; }

Time CheckpointInfo::selfExecutionTime() const { return executed; }

void CheckpointInfo::setSelfExecutionTime(Time t) {
  ConcordAssert(executed == MinTime);
  executed = t;
}

void CheckpointInfo::setCheckpointSentAllOrApproved() { sentToAllOrApproved = true; }

void CheckpointInfo::init(CheckpointInfo& i, void* d) {
  void* context = d;
  InternalReplicaApi* r = (InternalReplicaApi*)context;

  const ReplicasInfo& info = r->getReplicasInfo();

  const uint16_t myId = info.myId();

  const uint16_t numOfReps = info.numberOfReplicas();
  const uint16_t C = info.cVal();
  const uint16_t F = info.fVal();
  ConcordAssert(numOfReps == 3 * F + 2 * C + 1);

  i.checkpointCertificate =
      new MsgsCertificate<CheckpointMsg, true, true, true, CheckpointMsgCmp>(numOfReps, F, 2 * F + C + 1, myId);
}

void CheckpointInfo::free(CheckpointInfo& i) { i.resetAndFree(); }

void CheckpointInfo::reset(CheckpointInfo& i) { i.resetAndFree(); }
bool CheckpointInfo::isCheckpointSuperStable() const { return checkpointCertificate->isFull(); }

}  // namespace impl
}  // namespace bftEngine
