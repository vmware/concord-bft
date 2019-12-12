
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

#include "PartialProofsSet.hpp"
#include "Logger.hpp"
#include "PartialCommitProofMsg.hpp"
#include "FullCommitProofMsg.hpp"
#include "Crypto.hpp"
#include "assertUtils.hpp"
#include "InternalReplicaApi.hpp"
#include "FullCommitProofInternalMsg.hpp"
#include "SimpleThreadPool.hpp"
#include "AsynchProofCreationJob.hpp"

namespace bftEngine::impl {

PartialProofsSet::PartialProofsSet(InternalReplicaApi* const rep)
    : replica_(rep),
      numOfRequiredPartialProofsForFast_((3 * rep->getReplicasInfo().fVal()) + rep->getReplicasInfo().cVal() + 1),
      numOfRequiredPartialProofsForOptimisticFast_((3 * rep->getReplicasInfo().fVal()) +
                                                   (2 * rep->getReplicasInfo().cVal()) + 1),
      seqNumber_(0),
      fullCommitProof_(nullptr),
      selfPartialCommitProof_(nullptr) {
  expectedDigest_.makeZero();
  timeOfSelfPartialProof_ = MinTime;
  thresholdAccumulatorForFast_ = nullptr;
  thresholdAccumulatorForOptimisticFast_ = nullptr;
}

PartialProofsSet::~PartialProofsSet() { resetAndFree(); }

void PartialProofsSet::resetAndFree() {
  seqNumber_ = 0;
  delete fullCommitProof_;
  fullCommitProof_ = nullptr;
  delete selfPartialCommitProof_;
  selfPartialCommitProof_ = nullptr;
  participatingReplicasInFast_.clear();
  participatingReplicasInOptimisticFast_.clear();
  expectedDigest_.makeZero();
  timeOfSelfPartialProof_ = MinTime;
  if (thresholdAccumulatorForFast_)
    thresholdVerifier(CommitPath::FAST_WITH_THRESHOLD)->release(thresholdAccumulatorForFast_);
  thresholdAccumulatorForFast_ = nullptr;
  if (thresholdAccumulatorForOptimisticFast_)
    thresholdVerifier(CommitPath::OPTIMISTIC_FAST)->release(thresholdAccumulatorForOptimisticFast_);
  thresholdAccumulatorForOptimisticFast_ = nullptr;
}

void PartialProofsSet::addSelfMsgAndPPDigest(PartialCommitProofMsg* msg, Digest& digest) {
  const ReplicaId myId = msg->senderId();

  Assert(msg != nullptr && myId == replica_->getReplicasInfo().myId());
  Assert(seqNumber_ == 0);
  Assert(expectedDigest_.isZero());
  Assert(selfPartialCommitProof_ == nullptr);
  Assert(fullCommitProof_ == nullptr);

  seqNumber_ = msg->seqNumber();
  expectedDigest_ = digest;
  // NOTE: Accumulator might not be created yet, but the thresholdAccumulator() method creates it for us transparently
  // if needed
  thresholdAccumulator(msg->commitPath())
      ->setExpectedDigest(reinterpret_cast<unsigned char*>(digest.content()), DIGEST_SIZE);
  selfPartialCommitProof_ = msg;
  addImp(msg, msg->commitPath());
}

bool PartialProofsSet::addMsg(PartialCommitProofMsg* m) {
  const ReplicaId repId = m->senderId();

  Assert(m != nullptr && repId != replica_->getReplicasInfo().myId());
  Assert((seqNumber_ == 0) || (seqNumber_ == m->seqNumber()));
  Assert(replica_->getReplicasInfo().isIdOfReplica(repId));

  CommitPath cPath = m->commitPath();
  if (fullCommitProof_ != nullptr) return false;

  if ((selfPartialCommitProof_ != nullptr) && (selfPartialCommitProof_->commitPath() != cPath)) {
    // TODO(GG): TBD - print warning
    return false;
  }

  if (cPath == CommitPath::OPTIMISTIC_FAST) {
    if (participatingReplicasInOptimisticFast_.count(repId) > 0)
      return false;  // check that replica id has not been added yet

    if (participatingReplicasInOptimisticFast_.size() < numOfRequiredPartialProofsForOptimisticFast_ - 1) {
      participatingReplicasInOptimisticFast_.insert(repId);
      addImp(m, cPath);
      delete m;
      return true;
    }
  } else {
    if (participatingReplicasInFast_.count(repId) > 0) return false;  // check that replica id has not been added yet

    if (participatingReplicasInFast_.size() < numOfRequiredPartialProofsForFast_ - 1) {
      participatingReplicasInFast_.insert(repId);
      addImp(m, cPath);
      delete m;
      return true;
    }
  }

  return false;
}

bool PartialProofsSet::hasPartialProofFromReplica(ReplicaId repId) const {
  if (participatingReplicasInOptimisticFast_.count(repId) > 0) return true;
  return !participatingReplicasInFast_.empty();
}

void PartialProofsSet::addImp(PartialCommitProofMsg* m, CommitPath cPath) {
  if (cPath == CommitPath::OPTIMISTIC_FAST) {
    thresholdAccumulator(cPath)->add(m->thresholSignature(), m->thresholSignatureLength());

    if ((participatingReplicasInOptimisticFast_.size() == (numOfRequiredPartialProofsForOptimisticFast_ - 1)) &&
        (selfPartialCommitProof_ != nullptr)) {
      tryToCreateFullProof();
    }
  } else {
    thresholdAccumulator(cPath)->add(m->thresholSignature(), m->thresholSignatureLength());

    if ((participatingReplicasInFast_.size() == (numOfRequiredPartialProofsForFast_ - 1)) &&
        (selfPartialCommitProof_ != nullptr)) {
      tryToCreateFullProof();
    }
  }
}

void PartialProofsSet::setTimeOfSelfPartialProof(const Time& t) { timeOfSelfPartialProof_ = t; }

Time PartialProofsSet::getTimeOfSelfPartialProof() { return timeOfSelfPartialProof_; }

bool PartialProofsSet::addMsg(FullCommitProofMsg* msg) {
  Assert(msg != nullptr);

  if (fullCommitProof_ != nullptr) return false;

  PartialCommitProofMsg* myPCP = selfPartialCommitProof_;

  if (myPCP == nullptr) {
    // TODO(GG): can be improved (we can keep the FullCommitProof  message until myPCP!=nullptr
    LOG_WARN_F(GL, "FullCommitProofMsg arrived before PrePrepare. TODO(GG): should be handled to avoid delays. ");
    return false;
  }

  if (msg->seqNumber() != myPCP->seqNumber() || msg->viewNumber() != myPCP->viewNumber()) {
    LOG_WARN_F(GL, "Received unexpected FullCommitProofMsg");
    return false;
  }

  bool succeeded =
      thresholdVerifier(myPCP->commitPath())
          ->verify(
              (const char*)&expectedDigest_, sizeof(Digest), msg->thresholSignature(), msg->thresholSignatureLength());

  if (succeeded) {
    fullCommitProof_ = msg;
    return true;
  } else {
    LOG_INFO_F(GL, "Unable to verify FullCommitProofMsg message for seqNumber %" PRId64 "", msg->seqNumber());
    return false;
  }
}

PartialCommitProofMsg* PartialProofsSet::getSelfPartialCommitProof() { return selfPartialCommitProof_; }

bool PartialProofsSet::hasFullProof() { return (fullCommitProof_ != nullptr); }

void PartialProofsSet::tryToCreateFullProof() {
  Assert(fullCommitProof_ == nullptr);

  if (selfPartialCommitProof_ == nullptr) return;

  CommitPath cPath = selfPartialCommitProof_->commitPath();
  bool ready = false;
  IThresholdAccumulator* thresholdAccumulator = nullptr;

  if (cPath == CommitPath::OPTIMISTIC_FAST) {
    ready = (participatingReplicasInOptimisticFast_.size() == (numOfRequiredPartialProofsForOptimisticFast_ - 1));
    thresholdAccumulator = thresholdAccumulatorForOptimisticFast_;
  } else {
    ready = (participatingReplicasInFast_.size() == (numOfRequiredPartialProofsForFast_ - 1));
    thresholdAccumulator = thresholdAccumulatorForFast_;
  }

  if (!ready) return;
  Assert(thresholdAccumulator != nullptr);

  {
    IThresholdAccumulator* acc = thresholdAccumulator->clone();
    PartialCommitProofMsg* myPCP = selfPartialCommitProof_;

    auto* j = new AsynchProofCreationJob(
        replica_, thresholdVerifier(cPath), acc, expectedDigest_, myPCP->seqNumber(), myPCP->viewNumber());

    replica_->getInternalThreadPool().add(j);
    LOG_DEBUG_F(GL, "PartialProofsSet - send to BK thread (for seqNumber %" PRId64 ")", seqNumber_);
  }
}

FullCommitProofMsg* PartialProofsSet::getFullProof() { return fullCommitProof_; }

IThresholdVerifier* PartialProofsSet::thresholdVerifier(CommitPath cPath) {
  // TODO: Not thread-safe?
  IThresholdVerifier* verifier;

  if (cPath == CommitPath::OPTIMISTIC_FAST) {
    verifier = replica_->getThresholdVerifierForOptimisticCommit();
  } else /* if (cPath == CommitPath::FAST_WITH_THRESHOLD) */ {
    verifier = replica_->getThresholdVerifierForCommit();
  }

  Assert(verifier != nullptr);
  return verifier;
}

IThresholdAccumulator* PartialProofsSet::thresholdAccumulator(CommitPath cPath) {
  IThresholdAccumulator* accumulator;
  IThresholdVerifier* verifier = thresholdVerifier(cPath);

  if (cPath == CommitPath::OPTIMISTIC_FAST) {
    if (thresholdAccumulatorForOptimisticFast_ == nullptr)
      thresholdAccumulatorForOptimisticFast_ = verifier->newAccumulator(false);

    accumulator = thresholdAccumulatorForOptimisticFast_;
  } else /*if (cPath == CommitPath::FAST_WITH_THRESHOLD)*/ {
    if (thresholdAccumulatorForFast_ == nullptr) thresholdAccumulatorForFast_ = verifier->newAccumulator(false);

    accumulator = thresholdAccumulatorForFast_;
  }

  return accumulator;
}

}  // namespace bftEngine::impl
