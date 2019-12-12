
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

// TODO(GG): this class/file should be replaced by an instance of CollectorOfThresholdSignatures

#include "AsynchProofCreationJob.hpp"
#include "Logger.hpp"
#include "FullCommitProofMsg.hpp"
#include "Crypto.hpp"
#include "FullCommitProofInternalMsg.hpp"

namespace bftEngine::impl {

void AsynchProofCreationJob::execute() {
  LOG_DEBUG_F(GL, "PartialProofsSet::AsynchProofCreationJob::execute - begin (for seqNumber %" PRId64 ")", seqNumber_);

  auto bufferSize = verifier_->requiredLengthForSignedData();
  char* const bufferForSigComputations = (char*)alloca(bufferSize);
  size_t sigLength = verifier_->requiredLengthForSignedData();

  if (sigLength > UINT16_MAX || sigLength == 0) {
    LOG_WARN_F(GL, "Unable to create FullProof for seqNumber %" PRId64 "", seqNumber_);
    return;
  }

  accumulator_->getFullSignedData(bufferForSigComputations, sigLength);
  bool succeeded =
      verifier_->verify((char*)&expectedDigest_, sizeof(Digest), bufferForSigComputations, (uint16_t)sigLength);

  if (!succeeded) {
    LOG_WARN_F(GL, "Failed to create FullProof for seqNumber %" PRId64 "", seqNumber_);
    LOG_DEBUG_F(GL, "PartialProofsSet::AsynchProofCreationJob::execute - end (for seqNumber %" PRId64 ")", seqNumber_);
    return;
  } else {
    auto* fcpMsg = new FullCommitProofMsg(
        replica_->getReplicasInfo().myId(), view_, seqNumber_, bufferForSigComputations, (uint16_t)sigLength);

    std::unique_ptr<InternalMessage> p(new FullCommitProofInternalMsg(replica_, fcpMsg));
    replica_->getMsgsCommunicator()->pushInternalMsg(std::move(p));
  }

  LOG_DEBUG_F(GL, "PartialProofsSet::AsynchProofCreationJob::execute - end (for seqNumber %" PRId64 ")", seqNumber_);
}

}  // namespace bftEngine::impl
