// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <cstdint>
#include <sstream>

namespace bftEngine::impl {

class MsgCode {
 public:
  enum Type : uint16_t {
    None = 0,

    PrePrepare = 100,
    PreparePartial,
    PrepareFull,
    CommitPartial,
    CommitFull,
    StartSlowCommit,
    PartialCommitProof,
    FullCommitProof,
    PartialExecProof,
    FullExecProof,
    SimpleAck,
    ViewChange,
    NewView,
    Checkpoint,
    AskForCheckpoint,
    ReplicaStatus,
    ReqMissingData,
    StateTransfer,
    ReplicaAsksToLeaveView,
    ReplicaRestartReady,
    ReplicasRestartReadyProof,
    InstallReady,

    ClientPreProcessRequest = 500,
    PreProcessRequest,
    PreProcessReply,
    PreProcessBatchRequest,
    PreProcessBatchReply,
    PreProcessResult,

    ClientRequest = 700,
    ClientBatchRequest = 750,
    ClientReply = 800,

  };
};

inline std::ostream& operator<<(std::ostream& os, const MsgCode::Type& c) {
  switch (c) {
    case MsgCode::None:
      os << "None";
      break;
    case MsgCode::PrePrepare:
      os << "PrePrepare";
      break;
    case MsgCode::PreparePartial:
      os << "PreparePartial";
      break;
    case MsgCode::PrepareFull:
      os << "PrepareFull";
      break;
    case MsgCode::CommitPartial:
      os << "CommitPartial";
      break;
    case MsgCode::CommitFull:
      os << "CommitFull";
      break;
    case MsgCode::StartSlowCommit:
      os << "StartSlowCommit";
      break;
    case MsgCode::PartialCommitProof:
      os << "PartialCommitProof";
      break;
    case MsgCode::FullCommitProof:
      os << "FullCommitProof";
      break;
    case MsgCode::PartialExecProof:
      os << "PartialExecProof";
      break;
    case MsgCode::FullExecProof:
      os << "FullExecProof";
      break;
    case MsgCode::SimpleAck:
      os << "SimpleAck";
      break;
    case MsgCode::ViewChange:
      os << "ViewChange";
      break;
    case MsgCode::NewView:
      os << "NewView";
      break;
    case MsgCode::Checkpoint:
      os << "Checkpoint";
      break;
    case MsgCode::AskForCheckpoint:
      os << "AskForCheckpoint";
      break;
    case MsgCode::ReplicaStatus:
      os << "ReplicaStatus";
      break;
    case MsgCode::ReqMissingData:
      os << "ReqMissingData";
      break;
    case MsgCode::StateTransfer:
      os << "StateTransfer";
      break;
    case MsgCode::ReplicaAsksToLeaveView:
      os << "ReplicaAsksToLeaveView";
      break;
    case MsgCode::ReplicaRestartReady:
      os << "ReplicaRestartReady";
      break;
    case MsgCode::ReplicasRestartReadyProof:
      os << "ReplicasRestartReadyProof";
      break;
    case MsgCode::InstallReady:
      os << "InstallReady";
      break;
    case MsgCode::ClientPreProcessRequest:
      os << "ClientPreProcessRequest";
      break;
    case MsgCode::PreProcessRequest:
      os << "PreProcessRequest";
      break;
    case MsgCode::PreProcessReply:
      os << "PreProcessReply";
      break;
    case MsgCode::ClientRequest:
      os << "ClientRequest";
      break;
    case MsgCode::ClientReply:
      os << "ClientReply";
      break;
    default:
      os << "UNKNOWN";
  }
  return os;
}

}  // namespace bftEngine::impl
