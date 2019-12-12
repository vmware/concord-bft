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

#include "AsynchExecProofCreationJob.hpp"
#include "PartialExecProofsSet.hpp"
#include "FullExecProofMsg.hpp"
#include "PartialExecProofMsg.hpp"
#include "Crypto.hpp"
#include "assertUtils.hpp"
#include "Logger.hpp"
#include "MerkleExecSignatureInternalMsg.hpp"

namespace bftEngine::impl {

AsynchExecProofCreationJob::AsynchExecProofCreationJob(InternalReplicaApi* const internalReplicaApi,
                                                       IThresholdVerifier* verifier,
                                                       IThresholdAccumulator* accumulator,
                                                       Digest& expectedDigest,
                                                       SeqNum seqNumber,
                                                       ViewNum view)
    : replicaApi_(internalReplicaApi) {
  accumulator_ = accumulator;
  expectedDigest_ = expectedDigest;
  seqNumber_ = seqNumber;
  view_ = view;
  verifier_ = verifier;
}

void AsynchExecProofCreationJob::execute() {
  LOG_DEBUG_F(
      GL, "PartialExecProofsSet::AsynchProofCreationJob::execute - begin (for seqNumber %" PRId64 ")", seqNumber_);

  auto bufferSize = (uint16_t)verifier_->requiredLengthForSignedData();
  char* const bufferForSigComputations = (char*)alloca(bufferSize);
  size_t sigLength = verifier_->requiredLengthForSignedData();

  if (sigLength > UINT16_MAX || sigLength == 0) {
    LOG_WARN_F(GL, "Unable to create FullProof for seqNumber %" PRId64 "", seqNumber_);
    return;
  }

  accumulator_->getFullSignedData(bufferForSigComputations, sigLength);
  bool succeeded = verifier_->verify((char*)&expectedDigest_, sizeof(Digest), bufferForSigComputations, sigLength);

  if (!succeeded) {
    LOG_WARN_F(GL, "Failed to create execution proof for seqNumber %" PRId64 "", seqNumber_);
    LOG_DEBUG_F(
        GL, "PartialExecProofsSet::AsynchProofCreationJob::execute - end (for seqNumber %" PRId64 ")", seqNumber_);
    return;
  } else {
    std::unique_ptr<InternalMessage> pInMsg(new MerkleExecSignatureInternalMsg(
        replicaApi_, view_, seqNumber_, (uint16_t)sigLength, bufferForSigComputations));
    replicaApi_->getMsgsCommunicator()->pushInternalMsg(std::move(pInMsg));
  }
  LOG_DEBUG_F(
      GL, "PartialExecProofsSet::AsynchProofCreationJob::execute - end (for seqNumber %" PRId64 ")", seqNumber_);
}

}  // namespace bftEngine::impl
