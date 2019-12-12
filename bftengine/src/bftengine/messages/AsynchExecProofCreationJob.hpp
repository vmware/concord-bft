// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include "InternalReplicaApi.hpp"
#include "SimpleThreadPool.hpp"
#include "Digest.hpp"

namespace bftEngine::impl {

class AsynchExecProofCreationJob : public util::SimpleThreadPool::Job {
 public:
  AsynchExecProofCreationJob(InternalReplicaApi* internalReplicaApi,
                             IThresholdVerifier* verifier,
                             IThresholdAccumulator* accumulator,
                             Digest& expectedDigest,
                             SeqNum seqNumber,
                             ViewNum view);

  virtual ~AsynchExecProofCreationJob() = default;

  void execute() override;
  void release() override {}

 private:
  InternalReplicaApi* const replicaApi_;
  IThresholdAccumulator* accumulator_;
  Digest expectedDigest_;
  SeqNum seqNumber_;
  ViewNum view_;
  IThresholdVerifier* verifier_;
};

}  // namespace bftEngine::impl
