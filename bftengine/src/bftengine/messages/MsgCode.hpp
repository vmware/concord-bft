// Concord
//
// Copyright (c) 2018-2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <cstdint>

namespace bftEngine::impl {

class MsgCode {
 public:
  enum : uint16_t {
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

    ClientPreProcessRequest = 500,
    PreProcessRequest,
    PreProcessReply,

    ClientRequest = 700,
    ClientReply = 800,

  };
};

}  // namespace bftEngine::impl
