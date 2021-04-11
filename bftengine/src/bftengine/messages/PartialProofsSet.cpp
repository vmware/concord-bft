
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

// TODO(GG): this class/file should be replaced by an instance of CollectorOfThresholdSignatures

#include "PartialProofsSet.hpp"
#include <type_traits>
#include "Logger.hpp"
#include "OpenTracing.hpp"
#include "PartialCommitProofMsg.hpp"
#include "FullCommitProofMsg.hpp"
#include "Crypto.hpp"
#include "assertUtils.hpp"
#include "InternalReplicaApi.hpp"
#include "SimpleThreadPool.hpp"
#include "ReplicaConfig.hpp"
#include "CryptoManager.hpp"

namespace bftEngine {
namespace impl {

PartialProofsSet::PartialProofsSet(InternalReplicaApi* const rep)
    : replica(rep),
      numOfRequiredPartialProofsForFast((3 * rep->getReplicasInfo().fVal()) + rep->getReplicasInfo().cVal() + 1),
      numOfRequiredPartialProofsForOptimisticFast((3 * rep->getReplicasInfo().fVal()) +
                                                  (2 * rep->getReplicasInfo().cVal()) + 1),
      seqNumber(0),
      fullCommitProof(nullptr),
      selfPartialCommitProof(nullptr) {
  expectedDigest.makeZero();
  timeOfSelfPartialProof = MinTime;
}

PartialProofsSet::~PartialProofsSet() { resetAndFree(); }

void PartialProofsSet::acquire(PartialProofsSet* rhs) {
  fullCommitProof = rhs->fullCommitProof;
  rhs->fullCommitProof = nullptr;
  selfPartialCommitProof = rhs->selfPartialCommitProof;
  rhs->selfPartialCommitProof = nullptr;
  participatingReplicasInFast = rhs->participatingReplicasInFast;
  participatingReplicasInOptimisticFast = rhs->participatingReplicasInOptimisticFast;
  expectedDigest = rhs->expectedDigest;
}

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
  thresholdAccumulatorForFast = nullptr;
  thresholdAccumulatorForOptimisticFast = nullptr;
}

void PartialProofsSet::addSelfMsgAndPPDigest(PartialCommitProofMsg* m, Digest& digest) {
  const ReplicaId myId = m->senderId();

  ConcordAssert(m != nullptr && myId == replica->getReplicasInfo().myId());
  ConcordAssert(seqNumber == 0);
  ConcordAssert(expectedDigest.isZero());
  ConcordAssert(selfPartialCommitProof == nullptr);
  ConcordAssert(fullCommitProof == nullptr);

  seqNumber = m->seqNumber();
  expectedDigest = digest;
  // NOTE: Accumulator might not be created yet, but the thresholdAccumulator() method creates it for us transparently
  // if needed
  thresholdAccumulator(m->commitPath())
      ->setExpectedDigest(reinterpret_cast<unsigned char*>(digest.content()), DIGEST_SIZE);
  selfPartialCommitProof = m;

  addImp(m, m->commitPath());
}

bool PartialProofsSet::addMsg(PartialCommitProofMsg* m) {
  const ReplicaId repId = m->senderId();

  ConcordAssert(m != nullptr && repId != replica->getReplicasInfo().myId());
  ConcordAssert((seqNumber == 0) || (seqNumber == m->seqNumber()));
  ConcordAssert(replica->getReplicasInfo().isIdOfReplica(repId));

  CommitPath cPath = m->commitPath();

  if (fullCommitProof != nullptr) return false;

  if ((selfPartialCommitProof != nullptr) && (selfPartialCommitProof->commitPath() != cPath)) {
    // TODO(GG): TBD - print warning
    return false;
  }

  if (cPath == CommitPath::OPTIMISTIC_FAST) {
    if (participatingReplicasInOptimisticFast.count(repId) > 0)
      return false;  // check that replica id has not been added yet

    if (participatingReplicasInOptimisticFast.size() < numOfRequiredPartialProofsForOptimisticFast - 1) {
      participatingReplicasInOptimisticFast.insert(repId);
      addImp(m, cPath);
      delete m;
      return true;
    }
  } else {
    if (participatingReplicasInFast.count(repId) > 0) return false;  // check that replica id has not been added yet

    if (participatingReplicasInFast.size() < numOfRequiredPartialProofsForFast - 1) {
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
    thresholdAccumulator(cPath)->add(m->thresholSignature(), m->thresholSignatureLength());

    if ((participatingReplicasInOptimisticFast.size() == (numOfRequiredPartialProofsForOptimisticFast - 1)) &&
        (selfPartialCommitProof != nullptr)) {
      tryToCreateFullProof();
    }
  } else {
    thresholdAccumulator(cPath)->add(m->thresholSignature(), m->thresholSignatureLength());

    if ((participatingReplicasInFast.size() == (numOfRequiredPartialProofsForFast - 1)) &&
        (selfPartialCommitProof != nullptr)) {
      tryToCreateFullProof();
    }
  }
}

void PartialProofsSet::setTimeOfSelfPartialProof(const Time& t) { timeOfSelfPartialProof = t; }

Time PartialProofsSet::getTimeOfSelfPartialProof() { return timeOfSelfPartialProof; }

bool PartialProofsSet::addMsg(FullCommitProofMsg* m) {
  ConcordAssert(m != nullptr);

  if (fullCommitProof != nullptr) return false;

  PartialCommitProofMsg* myPCP = selfPartialCommitProof;

  if (myPCP == nullptr) {
    // TODO(GG): can be improved (we can keep the FullCommitProof  message until myPCP!=nullptr
    LOG_WARN(GL, "FullCommitProofMsg arrived before PrePrepare. TODO(GG): should be handled to avoid delays. ");
    return false;
  }

  if (m->seqNumber() != myPCP->seqNumber() || m->viewNumber() != myPCP->viewNumber()) {
    LOG_WARN(GL, "Received unexpected FullCommitProofMsg");
    return false;
  }

  bool succ =
      thresholdVerifier(myPCP->commitPath())
          ->verify((const char*)&expectedDigest, sizeof(Digest), m->thresholSignature(), m->thresholSignatureLength());

  if (succ) {
    fullCommitProof = m;
    return true;
  } else {
    LOG_INFO(GL, "Unable to verify FullCommitProofMsg message for seqNumber " << m->seqNumber());
    return false;
  }
}

PartialCommitProofMsg* PartialProofsSet::getSelfPartialCommitProof() { return selfPartialCommitProof; }

bool PartialProofsSet::hasFullProof() { return (fullCommitProof != nullptr); }

FullCommitProofMsg* PartialProofsSet::getFullProof() { return fullCommitProof; }

// NB: the following class is part of a patch
class AsynchProofCreationJob : public util::SimpleThreadPool::Job {
 public:
  AsynchProofCreationJob(InternalReplicaApi* myReplica,
                         std::shared_ptr<IThresholdVerifier> verifier,
                         std::shared_ptr<IThresholdAccumulator> acc,
                         Digest& expectedDigest,
                         SeqNum seqNumber,
                         ViewNum viewNumber,
                         const concordUtils::SpanContext& span_context) {
    this->me = myReplica;
    this->acc = acc;
    this->expectedDigest = expectedDigest;
    this->seqNumber = seqNumber;
    this->view = viewNumber;
    this->verifier = verifier;
    span_context_ = span_context;
  }

  virtual ~AsynchProofCreationJob(){};

  virtual void execute() {
    SCOPED_MDC(MDC_REPLICA_ID_KEY, std::to_string(me->getReplicaConfig().replicaId));
    SCOPED_MDC_SEQ_NUM(std::to_string(seqNumber));
    SCOPED_MDC_PATH(CommitPathToMDCString(CommitPath::OPTIMISTIC_FAST));
    LOG_DEBUG(GL, "begin...");

    auto span = concordUtils::startChildSpanFromContext(span_context_, "bft_create_FullCommitProofMsg");
    const uint16_t bufferSize = (uint16_t)verifier->requiredLengthForSignedData();
    std::vector<char> bufferForSigComputations(bufferSize);

    // char bufferForSigComputations[2048];

    size_t sigLength = verifier->requiredLengthForSignedData();

    //		if (sigLength > sizeof(bufferForSigComputations) || sigLength > UINT16_MAX || sigLength == 0)
    if (sigLength > UINT16_MAX || sigLength == 0) {
      LOG_WARN(GL, "Unable to create FullProof for seqNumber " << seqNumber);
      return;
    }

    acc->getFullSignedData(bufferForSigComputations.data(), sigLength);

    bool succ =
        verifier->verify((char*)&expectedDigest, sizeof(Digest), bufferForSigComputations.data(), (uint16_t)sigLength);

    if (!succ) {
      LOG_WARN(GL, "Failed to create FullProof for seqNumber " << seqNumber);
      LOG_DEBUG(GL, "PartialProofsSet::AsynchProofCreationJob::execute - end (for seqNumber " << seqNumber);
      return;
    } else {
      LOG_DEBUG(CNSUS, "Created FullProof, sending full commit proof");
      FullCommitProofMsg* fcpMsg = new FullCommitProofMsg(me->getReplicasInfo().myId(),
                                                          view,
                                                          seqNumber,
                                                          bufferForSigComputations.data(),
                                                          (uint16_t)sigLength,
                                                          span_context_);
      me->getIncomingMsgsStorage().pushInternalMsg(fcpMsg);
    }

    LOG_DEBUG(GL, "end...");
  }

  virtual void release() { delete this; }

 private:
  InternalReplicaApi* me;
  Digest expectedDigest;
  SeqNum seqNumber;
  ViewNum view;
  std::shared_ptr<IThresholdAccumulator> acc;
  std::shared_ptr<IThresholdVerifier> verifier;
  concordUtils::SpanContext span_context_;
};

void PartialProofsSet::tryToCreateFullProof() {
  ConcordAssert(fullCommitProof == nullptr);

  if (selfPartialCommitProof == nullptr) return;

  CommitPath cPath = selfPartialCommitProof->commitPath();

  bool ready = false;
  std::shared_ptr<IThresholdAccumulator> thresholdAccumulator;

  if (cPath == CommitPath::OPTIMISTIC_FAST) {
    ready = (participatingReplicasInOptimisticFast.size() == (numOfRequiredPartialProofsForOptimisticFast - 1));
    thresholdAccumulator = thresholdAccumulatorForOptimisticFast;
  } else {
    ready = (participatingReplicasInFast.size() == (numOfRequiredPartialProofsForFast - 1));
    thresholdAccumulator = thresholdAccumulatorForFast;
  }

  if (!ready) return;

  ConcordAssert(thresholdAccumulator != nullptr);
  {
    PartialCommitProofMsg* myPCP = selfPartialCommitProof;

    const auto& span_context = myPCP->spanContext<std::remove_pointer<decltype(myPCP)>::type>();
    AsynchProofCreationJob* j = new AsynchProofCreationJob(replica,
                                                           thresholdVerifier(cPath),
                                                           thresholdAccumulator,
                                                           expectedDigest,
                                                           myPCP->seqNumber(),
                                                           myPCP->viewNumber(),
                                                           span_context);

    replica->getInternalThreadPool().add(j);

    LOG_TRACE(GL, "send to BK thread (for seqNumber " << seqNumber << ")");
  }
}

std::shared_ptr<IThresholdVerifier> PartialProofsSet::thresholdVerifier(CommitPath cPath) {
  // TODO: Not thread-safe?
  // TODO: ALIN: Not sure if the commented code below would be the desired behavior
  if (cPath == CommitPath::OPTIMISTIC_FAST) {
    return CryptoManager::instance().thresholdVerifierForOptimisticCommit(seqNumber);
  } else /* if (cPath == CommitPath::FAST_WITH_THRESHOLD) */ {
    return CryptoManager::instance().thresholdVerifierForCommit(seqNumber);
  }
}

std::shared_ptr<IThresholdAccumulator> PartialProofsSet::thresholdAccumulator(CommitPath cPath) {
  if (cPath == CommitPath::OPTIMISTIC_FAST) {
    if (!thresholdAccumulatorForOptimisticFast)
      thresholdAccumulatorForOptimisticFast.reset(thresholdVerifier(cPath)->newAccumulator(false));

    return thresholdAccumulatorForOptimisticFast;
  } else /*if (cPath == CommitPath::FAST_WITH_THRESHOLD)*/ {
    if (!thresholdAccumulatorForFast)
      thresholdAccumulatorForFast.reset(thresholdVerifier(cPath)->newAccumulator(false));

    return thresholdAccumulatorForFast;
    // TODO: ALIN: Not sure if the commented code below would be the desired behavior
    //} else {
  }
}

}  // namespace impl
}  // namespace bftEngine
