
// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

// TODO(GG): this class/file should be replaced by an instance of
// CollectorOfThresholdSignatures

#include "PartialProofsSet.hpp"
#include "Logger.hpp"
#include "PartialCommitProofMsg.hpp"
#include "FullCommitProofMsg.hpp"
#include "Crypto.hpp"
#include "assertUtils.hpp"
#include "InternalReplicaApi.hpp"

namespace bftEngine {
namespace impl {

PartialProofsSet::PartialProofsSet(InternalReplicaApi* const rep)
    : replica(rep),
      numOfRquiredPartialProofsForFast((3 * rep->getReplicasInfo().fVal()) +
                                       rep->getReplicasInfo().cVal() + 1),
      numOfRquiredPartialProofsForOptimisticFast(
          (3 * rep->getReplicasInfo().fVal()) +
          (2 * rep->getReplicasInfo().cVal()) + 1),
      seqNumber(0),
      fullCommitProof(nullptr),
      selfPartialCommitProof(nullptr) {
  expectedDigest.makeZero();
  timeOfSelfPartialProof = MinTime;
  thresholdAccumulatorForFast = nullptr;
  thresholdAccumulatorForOptimisticFast = nullptr;
}

PartialProofsSet::~PartialProofsSet() { resetAndFree(); }

void PartialProofsSet::resetAndFree() {
  seqNumber = 0;
  if (fullCommitProof) delete fullCommitProof;
  fullCommitProof = nullptr;
  if (selfPartialCommitProof) delete selfPartialCommitProof;
  selfPartialCommitProof = nullptr;
  participatingReplicasInFast.clear();
  participatingReplicasInOptimisticFast.clear();
  expectedDigest.makeZero();
  timeOfSelfPartialProof = MinTime;
  if (thresholdAccumulatorForFast)
    thresholdVerifier(CommitPath::FAST_WITH_THRESHOLD)
        ->release(thresholdAccumulatorForFast);
  thresholdAccumulatorForFast = nullptr;
  if (thresholdAccumulatorForOptimisticFast)
    thresholdVerifier(CommitPath::OPTIMISTIC_FAST)
        ->release(thresholdAccumulatorForOptimisticFast);
  thresholdAccumulatorForOptimisticFast = nullptr;
}

void PartialProofsSet::addSelfMsgAndPPDigest(PartialCommitProofMsg* m,
                                             Digest& digest) {
  const ReplicaId myId = m->senderId();

  Assert(m != nullptr && myId == replica->getReplicasInfo().myId());
  Assert(seqNumber == 0);
  Assert(expectedDigest.isZero());
  Assert(selfPartialCommitProof == nullptr);
  Assert(fullCommitProof == nullptr);

  seqNumber = m->seqNumber();
  expectedDigest = digest;
  // NOTE: Accumulator might not be created yet, but the thresholdAccumulator()
  // method creates it for us transparently if needed
  thresholdAccumulator(m->commitPath())
      ->setExpectedDigest(reinterpret_cast<unsigned char*>(digest.content()),
                          DIGEST_SIZE);
  selfPartialCommitProof = m;

  addImp(m, m->commitPath());
}

bool PartialProofsSet::addMsg(PartialCommitProofMsg* m) {
  const ReplicaId repId = m->senderId();

  Assert(m != nullptr && repId != replica->getReplicasInfo().myId());
  Assert((seqNumber == 0) || (seqNumber == m->seqNumber()));
  Assert(replica->getReplicasInfo().isIdOfReplica(repId));

  CommitPath cPath = m->commitPath();

  if (fullCommitProof != nullptr) return false;

  if ((selfPartialCommitProof != nullptr) &&
      (selfPartialCommitProof->commitPath() != cPath)) {
    // TODO(GG): TBD - print warning
    return false;
  }

  if (cPath == CommitPath::OPTIMISTIC_FAST) {
    if (participatingReplicasInOptimisticFast.count(repId) > 0)
      return false;  // check that replica id has not been added yet

    if (participatingReplicasInOptimisticFast.size() <
        numOfRquiredPartialProofsForOptimisticFast - 1) {
      participatingReplicasInOptimisticFast.insert(repId);
      addImp(m, cPath);
      delete m;
      return true;
    }
  } else {
    if (participatingReplicasInFast.count(repId) > 0)
      return false;  // check that replica id has not been added yet

    if (participatingReplicasInFast.size() <
        numOfRquiredPartialProofsForFast - 1) {
      participatingReplicasInFast.insert(repId);
      addImp(m, cPath);
      delete m;
      return true;
    }
  }

  return false;
}

bool PartialProofsSet::hasPartialProofFromReplica(ReplicaId repId) const {
  if (participatingReplicasInOptimisticFast.count(repId) > 0) return true;
  if (participatingReplicasInFast.count(repId) > 0) return true;
  return false;
}

void PartialProofsSet::addImp(PartialCommitProofMsg* m, CommitPath cPath) {
  if (cPath == CommitPath::OPTIMISTIC_FAST) {
    thresholdAccumulator(cPath)->add(m->thresholSignature(),
                                     m->thresholSignatureLength());

    if ((participatingReplicasInOptimisticFast.size() ==
         (numOfRquiredPartialProofsForOptimisticFast - 1)) &&
        (selfPartialCommitProof != nullptr)) {
      tryToCreateFullProof();
    }
  } else {
    thresholdAccumulator(cPath)->add(m->thresholSignature(),
                                     m->thresholSignatureLength());

    if ((participatingReplicasInFast.size() ==
         (numOfRquiredPartialProofsForFast - 1)) &&
        (selfPartialCommitProof != nullptr)) {
      tryToCreateFullProof();
    }
  }
}

void PartialProofsSet::setTimeOfSelfPartialProof(const Time& t) {
  timeOfSelfPartialProof = t;
}

Time PartialProofsSet::getTimeOfSelfPartialProof() {
  return timeOfSelfPartialProof;
}

bool PartialProofsSet::addMsg(FullCommitProofMsg* m) {
  Assert(m != nullptr);

  if (fullCommitProof != nullptr) return false;

  PartialCommitProofMsg* myPCP = selfPartialCommitProof;

  if (myPCP == nullptr) {
    // TODO(GG): can be improved (we can keep the FullCommitProof  message until
    // myPCP!=nullptr
    LOG_WARN_F(GL,
               "FullCommitProofMsg arrived before PrePrepare. TODO(GG): should "
               "be handled to avoid delays. ");
    return false;
  }

  if (m->seqNumber() != myPCP->seqNumber() ||
      m->viewNumber() != myPCP->viewNumber()) {
    LOG_WARN_F(GL, "Received unexpected FullCommitProofMsg");
    return false;
  }

  bool succ = thresholdVerifier(myPCP->commitPath())
                  ->verify((const char*)&expectedDigest,
                           sizeof(Digest),
                           m->thresholSignature(),
                           m->thresholSignatureLength());

  if (succ) {
    fullCommitProof = m;
    return true;
  } else {
    LOG_INFO_F(
        GL,
        "Unable to verify FullCommitProofMsg message for seqNumber %" PRId64 "",
        m->seqNumber());
    return false;
  }
}

PartialCommitProofMsg* PartialProofsSet::getSelfPartialCommitProof() {
  return selfPartialCommitProof;
}

bool PartialProofsSet::hasFullProof() { return (fullCommitProof != nullptr); }

FullCommitProofMsg* PartialProofsSet::getFullProof() { return fullCommitProof; }

class PassFullCommitProofAsInternalMsg : public InternalMessage {
 protected:
  FullCommitProofMsg* selfFcp;
  InternalReplicaApi* r;

 public:
  PassFullCommitProofAsInternalMsg(InternalReplicaApi* replica,
                                   FullCommitProofMsg* selfFcpMsg)
      : selfFcp(selfFcpMsg), r(replica) {}

  virtual ~PassFullCommitProofAsInternalMsg() override {}

  virtual void handle() override { r->onInternalMsg(selfFcp); }
};

// NB: the following class is part of a patch
class AsynchProofCreationJob : public SimpleThreadPool::Job {
 public:
  AsynchProofCreationJob(InternalReplicaApi* myReplica,
                         IThresholdVerifier* verifier,
                         IThresholdAccumulator* acc,
                         Digest& expectedDigest,
                         SeqNum seqNumber,
                         ViewNum viewNumber) {
    this->me = myReplica;
    this->acc = acc;
    this->expectedDigest = expectedDigest;
    this->seqNumber = seqNumber;
    this->view = viewNumber;
    this->verifier = verifier;
  }

  virtual ~AsynchProofCreationJob(){};

  virtual void execute() {
    LOG_INFO_F(GL,
               "PartialProofsSet::AsynchProofCreationJob::execute - begin (for "
               "seqNumber %" PRId64 ")",
               seqNumber);

    const uint16_t bufferSize =
        (uint16_t)verifier->requiredLengthForSignedData();
    char* const bufferForSigComputations = (char*)alloca(bufferSize);

    // char bufferForSigComputations[2048];

    size_t sigLength = verifier->requiredLengthForSignedData();

    //		if (sigLength > sizeof(bufferForSigComputations) || sigLength >
    // UINT16_MAX || sigLength == 0)
    if (sigLength > UINT16_MAX || sigLength == 0) {
      LOG_WARN_F(GL,
                 "Unable to create FullProof for seqNumber %" PRId64 "",
                 seqNumber);
      return;
    }

    acc->getFullSignedData(bufferForSigComputations, sigLength);

    bool succ = verifier->verify((char*)&expectedDigest,
                                 sizeof(Digest),
                                 bufferForSigComputations,
                                 (uint16_t)sigLength);

    if (!succ) {
      LOG_WARN_F(GL,
                 "Failed to create FullProof for seqNumber %" PRId64 "",
                 seqNumber);
      LOG_INFO_F(GL,
                 "PartialProofsSet::AsynchProofCreationJob::execute - end (for "
                 "seqNumber %" PRId64 ")",
                 seqNumber);
      return;
    } else {
      FullCommitProofMsg* fcpMsg =
          new FullCommitProofMsg(me->getReplicasInfo().myId(),
                                 view,
                                 seqNumber,
                                 bufferForSigComputations,
                                 (uint16_t)sigLength);

      //			me->sendToAllOtherReplicas(fcpMsg);

      PassFullCommitProofAsInternalMsg* p =
          new PassFullCommitProofAsInternalMsg(me, fcpMsg);
      me->getIncomingMsgsStorage().pushInternalMsg(p);
    }

    LOG_INFO_F(GL,
               "PartialProofsSet::AsynchProofCreationJob::execute - end (for "
               "seqNumber %" PRId64 ")",
               seqNumber);
  }

  virtual void release() {}

 private:
  InternalReplicaApi* me;
  IThresholdAccumulator* acc;
  Digest expectedDigest;
  SeqNum seqNumber;
  ViewNum view;
  IThresholdVerifier* verifier;
};

void PartialProofsSet::tryToCreateFullProof() {
  Assert(fullCommitProof == nullptr);

  if (selfPartialCommitProof == nullptr) return;

  CommitPath cPath = selfPartialCommitProof->commitPath();

  bool ready = false;
  IThresholdAccumulator* thresholdAccumulator = nullptr;

  if (cPath == CommitPath::OPTIMISTIC_FAST) {
    ready = (participatingReplicasInOptimisticFast.size() ==
             (numOfRquiredPartialProofsForOptimisticFast - 1));
    thresholdAccumulator = thresholdAccumulatorForOptimisticFast;
  } else {
    ready = (participatingReplicasInFast.size() ==
             (numOfRquiredPartialProofsForFast - 1));
    thresholdAccumulator = thresholdAccumulatorForFast;
  }

  if (!ready) return;

  Assert(thresholdAccumulator != nullptr);

  {
    IThresholdAccumulator* acc = thresholdAccumulator->clone();

    PartialCommitProofMsg* myPCP = selfPartialCommitProof;

    AsynchProofCreationJob* j =
        new AsynchProofCreationJob(replica,
                                   thresholdVerifier(cPath),
                                   acc,
                                   expectedDigest,
                                   myPCP->seqNumber(),
                                   myPCP->viewNumber());

    replica->getInternalThreadPool().add(j);

    LOG_INFO_F(GL,
               "PartialProofsSet - send to BK thread (for seqNumber %" PRId64
               ")",
               seqNumber);
  }
}

IThresholdVerifier* PartialProofsSet::thresholdVerifier(CommitPath cPath) {
  // TODO: Not thread-safe?
  IThresholdVerifier* verifier;

  // TODO: ALIN: Not sure if the commented code below would be the desired
  // behavior
  if (cPath == CommitPath::OPTIMISTIC_FAST) {
    verifier = replica->getThresholdVerifierForOptimisticCommit();
  } else /* if (cPath == CommitPath::FAST_WITH_THRESHOLD) */ {
    verifier = replica->getThresholdVerifierForCommit();
    //} else {
  }

  Assert(verifier != nullptr);

  return verifier;
}

IThresholdAccumulator* PartialProofsSet::thresholdAccumulator(
    CommitPath cPath) {
  IThresholdAccumulator* acc;
  IThresholdVerifier* v = thresholdVerifier(cPath);

  if (cPath == CommitPath::OPTIMISTIC_FAST) {
    if (thresholdAccumulatorForOptimisticFast == nullptr)
      thresholdAccumulatorForOptimisticFast = v->newAccumulator(false);

    acc = thresholdAccumulatorForOptimisticFast;
  } else /*if (cPath == CommitPath::FAST_WITH_THRESHOLD)*/ {
    if (thresholdAccumulatorForFast == nullptr)
      thresholdAccumulatorForFast = v->newAccumulator(false);

    acc = thresholdAccumulatorForFast;
    // TODO: ALIN: Not sure if the commented code below would be the desired
    // behavior
    //} else {
  }

  return acc;
}

}  // namespace impl
}  // namespace bftEngine
