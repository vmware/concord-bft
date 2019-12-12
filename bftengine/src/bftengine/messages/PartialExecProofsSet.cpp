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

// TODO(GG): this class/file should be replaced by an instance of CollectorOfThresholdSignatures (or a similar module)

#include "PartialExecProofsSet.hpp"
#include "FullExecProofMsg.hpp"
#include "PartialExecProofMsg.hpp"
#include "Crypto.hpp"
#include "assertUtils.hpp"
#include "Logger.hpp"
#include "SimpleThreadPool.hpp"
#include "MerkleExecSignatureInternalMsg.hpp"
#include "AsynchExecProofCreationJob.hpp"

namespace bftEngine::impl {

PartialExecProofsSet::PartialExecProofsSet(InternalReplicaApi* internalReplicaApi)
    : replicaApi_(internalReplicaApi),
      replicasInfo_(internalReplicaApi->getReplicasInfo()),
      numOfRequiredPartialProofs_(internalReplicaApi->getReplicasInfo().fVal() + 1),
      seqNumber_(0),
      myPartialExecProof_(nullptr),
      accumulator_(nullptr) {
  expectedDigest_.makeZero();
}

PartialExecProofsSet::~PartialExecProofsSet() { resetAndFree(); }

void PartialExecProofsSet::resetAndFree() {
  if ((myPartialExecProof_ == nullptr) && participatingReplicas_.empty())  // if already empty
    return;

  seqNumber_ = 0;
  delete myPartialExecProof_;
  myPartialExecProof_ = nullptr;
  participatingReplicas_.clear();
  expectedDigest_.makeZero();
  if (accumulator_) thresholdVerifier()->release(accumulator_);
  accumulator_ = nullptr;
  if (!setOfFullExecProofs_.empty()) {
    for (auto item : setOfFullExecProofs_) delete item;
    setOfFullExecProofs_.clear();
  }
}

bool PartialExecProofsSet::addMsg(PartialExecProofMsg* msg) {
  const ReplicaId repId = msg->senderId();

  Assert(msg != nullptr && repId != replicasInfo_.myId());
  Assert((seqNumber_ == 0) || (seqNumber_ == msg->seqNumber()));
  Assert(replicasInfo_.isIdOfReplica(repId));

  if ((participatingReplicas_.count(repId) == 0) && (participatingReplicas_.size() < numOfRequiredPartialProofs_ - 1)) {
    // we don't have enough partial proofs
    participatingReplicas_.insert(repId);
    addImp(msg);
    delete msg;
    return true;
  } else
    return false;
}

void PartialExecProofsSet::addImp(PartialExecProofMsg* msg) {
  thresholdAccumulator()->add(msg->thresholSignature(), msg->thresholSignatureLength());

  if ((participatingReplicas_.size() == (numOfRequiredPartialProofs_ - 1)) && (myPartialExecProof_ != nullptr)) {
    tryToCreateFullProof();
  }
}

void PartialExecProofsSet::tryToCreateFullProof() {
  Assert(accumulator_ != nullptr);

  if ((participatingReplicas_.size() == (numOfRequiredPartialProofs_ - 1)) && (myPartialExecProof_ != nullptr)) {
    IThresholdAccumulator* acc = accumulator_->clone();
    PartialExecProofMsg* myPEP = myPartialExecProof_;

    auto* j = new AsynchExecProofCreationJob(
        replicaApi_, thresholdVerifier(), acc, expectedDigest_, myPEP->seqNumber(), myPEP->viewNumber());

    replicaApi_->getInternalThreadPool().add(j);
    LOG_DEBUG_F(GL, "PartialExecProofsSet - send to BK thread (for seqNumber %" PRId64 ")", seqNumber_);
  }
}

void PartialExecProofsSet::setMerkleSignature(const char* sig, uint16_t sigLength) {
  for (auto item : setOfFullExecProofs_) {
    FullExecProofMsg* fep = item;
    fep->setSignature(sig, sigLength);
  }
}

IThresholdVerifier* PartialExecProofsSet::thresholdVerifier() {
  IThresholdVerifier* verifier = replicaApi_->getThresholdVerifierForExecution();
  Assert(verifier != nullptr);
  return verifier;
}

IThresholdAccumulator* PartialExecProofsSet::thresholdAccumulator() {
  if (accumulator_ == nullptr) accumulator_ = thresholdVerifier()->newAccumulator(false);
  return accumulator_;
}

}  // namespace bftEngine::impl
