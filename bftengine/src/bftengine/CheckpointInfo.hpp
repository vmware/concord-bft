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
#include "messages/MsgsCertificate.hpp"

namespace bftEngine {
namespace impl {
class CheckpointMsg;

class CheckpointInfo {
 protected:
  struct CheckpointMsgCmp {
    static bool equivalent(CheckpointMsg* a, CheckpointMsg* b);
  };

  MsgsCertificate<CheckpointMsg, true, true, true, CheckpointMsgCmp>* checkpointCertificate = nullptr;

  bool sentToAllOrApproved;

  Time executed;  // if != MinTime, represents the execution time of the corresponding sequnce number

 public:
  CheckpointInfo();

  ~CheckpointInfo();

  void resetAndFree();

  bool addCheckpointMsg(CheckpointMsg* msg, ReplicaId replicaId);

  bool isCheckpointCertificateComplete() const;

  // A replica considers a checkpoint to be super stable if it knows that all n/n replicas have reached to this
  // checkpoint. This is in contrary to stable checkpoint which means that the replica knows that a byzantine quorum of
  // replicas have reached to this checkpoint.
  bool isCheckpointSuperStable() const;

  CheckpointMsg* selfCheckpointMsg() const;

  const auto& getAllCheckpointMsgs() const { return checkpointCertificate->getAllMsgs(); }

  void tryToMarkCheckpointCertificateCompleted();

  bool checkpointSentAllOrApproved() const;

  Time selfExecutionTime() const;

  void setSelfExecutionTime(Time t);

  void setCheckpointSentAllOrApproved();

  // methods for SequenceWithActiveWindow
  static void init(CheckpointInfo& i, void* d);

  static void free(CheckpointInfo& i);

  static void reset(CheckpointInfo& i);

  static void acquire(CheckpointInfo& to, CheckpointInfo& from) {}
};
}  // namespace impl
}  // namespace bftEngine
