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

// TODO(GG): clean/move 'include' statements
#include "SeqNumInfo.hpp"
#include "InternalReplicaApi.hpp"

namespace bftEngine {
namespace impl {

SeqNumInfo::SeqNumInfo()
    : replica(nullptr),
      prePrepareMsg(nullptr),
      prepareSigCollector(nullptr),
      commitMsgsCollector(nullptr),
      partialProofsSet(nullptr),
      partialExecProofsSet(nullptr),
      primary(false),
      forcedCompleted(false),
      slowPathHasStarted(false),
      firstSeenFromPrimary(MinTime),
      timeOfLastInfoRequest(MinTime),
      commitUpdateTime(MinTime) {}

SeqNumInfo::~SeqNumInfo() {
  resetAndFree();

  delete prepareSigCollector;
  delete commitMsgsCollector;
  delete partialProofsSet;
  delete partialExecProofsSet;
}

void SeqNumInfo::resetAndFree() {
  delete prePrepareMsg;
  prePrepareMsg = nullptr;

  prepareSigCollector->resetAndFree();
  commitMsgsCollector->resetAndFree();
  partialProofsSet->resetAndFree();
  partialExecProofsSet->resetAndFree();

  primary = false;

  forcedCompleted = false;

  slowPathHasStarted = false;

  firstSeenFromPrimary = MinTime;
  timeOfLastInfoRequest = MinTime;
  commitUpdateTime = getMonotonicTime();  // TODO(GG): TBD
}

void SeqNumInfo::getAndReset(PrePrepareMsg*& outPrePrepare,
                             PrepareFullMsg*& outcombinedValidSignatureMsg) {
  outPrePrepare = prePrepareMsg;
  prePrepareMsg = nullptr;

  prepareSigCollector->getAndReset(outcombinedValidSignatureMsg);

  resetAndFree();
}

bool SeqNumInfo::addMsg(PrePrepareMsg* m) {
  if (prePrepareMsg != nullptr) return false;

  Assert(primary == false);
  Assert(!forcedCompleted);
  Assert(!prepareSigCollector->hasPartialMsgFromReplica(
      replica->getReplicasInfo().myId()));

  prePrepareMsg = m;

  // set expected
  Digest tmpDigest;
  Digest::calcCombination(
      m->digestOfRequests(), m->viewNumber(), m->seqNumber(), tmpDigest);
  prepareSigCollector->setExpected(m->seqNumber(), m->viewNumber(), tmpDigest);

  if (firstSeenFromPrimary == MinTime)  // TODO(GG): remove condition - TBD
    firstSeenFromPrimary = getMonotonicTime();

  return true;
}

bool SeqNumInfo::addSelfMsg(PrePrepareMsg* m) {
  Assert(primary == false);
  Assert(replica->getReplicasInfo().myId() ==
         replica->getReplicasInfo().primaryOfView(m->viewNumber()));
  Assert(!forcedCompleted);
  Assert(prePrepareMsg == nullptr);

  // Assert(me->id() == m->senderId()); // GG: incorrect assert - becuase after
  // a view change it may has been sent by another replica

  prePrepareMsg = m;
  primary = true;

  // set expected
  Digest tmpDigest;
  Digest::calcCombination(
      m->digestOfRequests(), m->viewNumber(), m->seqNumber(), tmpDigest);
  prepareSigCollector->setExpected(m->seqNumber(), m->viewNumber(), tmpDigest);

  if (firstSeenFromPrimary == MinTime)  // TODO(GG): remove condition - TBD
    firstSeenFromPrimary = getMonotonicTime();

  return true;
}

bool SeqNumInfo::addMsg(PreparePartialMsg* m) {
  Assert(replica->getReplicasInfo().myId() != m->senderId());
  Assert(!forcedCompleted);

  bool retVal =
      prepareSigCollector->addMsgWithPartialSignature(m, m->senderId());

  return retVal;
}

bool SeqNumInfo::addSelfMsg(PreparePartialMsg* m) {
  Assert(replica->getReplicasInfo().myId() == m->senderId());
  Assert(!forcedCompleted);

  bool r = prepareSigCollector->addMsgWithPartialSignature(m, m->senderId());
  Assert(r);

  return true;
}

bool SeqNumInfo::addMsg(PrepareFullMsg* m) {
  Assert(replica->getReplicasInfo().myId() != m->senderId());  // TODO(GG): TBD
  Assert(!forcedCompleted);

  bool retVal = prepareSigCollector->addMsgWithCombinedSignature(m);

  return retVal;
}

bool SeqNumInfo::addMsg(CommitPartialMsg* m) {
  Assert(replica->getReplicasInfo().myId() != m->senderId());  // TODO(GG): TBD
  Assert(!forcedCompleted);

  bool r = commitMsgsCollector->addMsgWithPartialSignature(m, m->senderId());

  if (r) commitUpdateTime = getMonotonicTime();

  return r;
}

bool SeqNumInfo::addSelfCommitPartialMsgAndDigest(CommitPartialMsg* m,
                                                  Digest& commitDigest) {
  Assert(replica->getReplicasInfo().myId() == m->senderId());
  Assert(!forcedCompleted);

  // set expected
  Digest tmpDigest;
  Digest::calcCombination(
      commitDigest, m->viewNumber(), m->seqNumber(), tmpDigest);
  commitMsgsCollector->setExpected(m->seqNumber(), m->viewNumber(), tmpDigest);

  // add msg
  bool r = commitMsgsCollector->addMsgWithPartialSignature(m, m->senderId());
  Assert(r);

  commitUpdateTime = getMonotonicTime();

  return true;
}

bool SeqNumInfo::addMsg(CommitFullMsg* m) {
  Assert(replica->getReplicasInfo().myId() != m->senderId());  // TODO(GG): TBD
  Assert(!forcedCompleted);

  bool r = commitMsgsCollector->addMsgWithCombinedSignature(m);

  if (r) commitUpdateTime = getMonotonicTime();

  return r;
}

void SeqNumInfo::forceComplete() {
  Assert(!forcedCompleted);
  Assert(hasPrePrepareMsg());
  Assert(this->partialProofsSet->hasFullProof());

  forcedCompleted = true;
  commitUpdateTime = getMonotonicTime();
}

PrePrepareMsg* SeqNumInfo::getPrePrepareMsg() const { return prePrepareMsg; }

PrePrepareMsg* SeqNumInfo::getSelfPrePrepareMsg() const {
  if (primary) {
    return prePrepareMsg;
  }
  return nullptr;
}

PreparePartialMsg* SeqNumInfo::getSelfPreparePartialMsg() const {
  PreparePartialMsg* p = prepareSigCollector->getPartialMsgFromReplica(
      replica->getReplicasInfo().myId());
  return p;
}

PrepareFullMsg* SeqNumInfo::getValidPrepareFullMsg() const {
  return prepareSigCollector->getMsgWithValidCombinedSignature();
}

CommitPartialMsg* SeqNumInfo::getSelfCommitPartialMsg() const {
  CommitPartialMsg* p = commitMsgsCollector->getPartialMsgFromReplica(
      replica->getReplicasInfo().myId());
  return p;
}

CommitFullMsg* SeqNumInfo::getValidCommitFullMsg() const {
  return commitMsgsCollector->getMsgWithValidCombinedSignature();
}

bool SeqNumInfo::hasPrePrepareMsg() const { return (prePrepareMsg != nullptr); }

bool SeqNumInfo::isPrepared() const {
  return forcedCompleted ||
         ((prePrepareMsg != nullptr) && prepareSigCollector->isComplete());
}

bool SeqNumInfo::isCommitted__gg() const {
  // TODO(GG): TBD - asserts on 'prepared'

  bool retVal = forcedCompleted || commitMsgsCollector->isComplete();
  return retVal;
}

bool SeqNumInfo::preparedOrHasPreparePartialFromReplica(ReplicaId repId) const {
  return isPrepared() || prepareSigCollector->hasPartialMsgFromReplica(repId);
}

bool SeqNumInfo::committedOrHasCommitPartialFromReplica(ReplicaId repId) const {
  return isCommitted__gg() ||
         commitMsgsCollector->hasPartialMsgFromReplica(repId);
}

Time SeqNumInfo::getTimeOfFisrtRelevantInfoFromPrimary() const {
  return firstSeenFromPrimary;
}

Time SeqNumInfo::getTimeOfLastInfoRequest() const {
  return timeOfLastInfoRequest;
}

PartialProofsSet& SeqNumInfo::partialProofs() { return *partialProofsSet; }

PartialExecProofsSet& SeqNumInfo::partialExecProofs() {
  return *partialExecProofsSet;
}

void SeqNumInfo::startSlowPath() { slowPathHasStarted = true; }

bool SeqNumInfo::slowPathStarted() { return slowPathHasStarted; }

void SeqNumInfo::setTimeOfLastInfoRequest(Time t) { timeOfLastInfoRequest = t; }

void SeqNumInfo::onCompletionOfPrepareSignaturesProcessing(
    SeqNum seqNumber,
    ViewNum viewNumber,
    const std::set<ReplicaId>& replicasWithBadSigs) {
  prepareSigCollector->onCompletionOfSignaturesProcessing(
      seqNumber, viewNumber, replicasWithBadSigs);
}

void SeqNumInfo::onCompletionOfPrepareSignaturesProcessing(
    SeqNum seqNumber,
    ViewNum viewNumber,
    const char* combinedSig,
    uint16_t combinedSigLen) {
  prepareSigCollector->onCompletionOfSignaturesProcessing(
      seqNumber, viewNumber, combinedSig, combinedSigLen);
}

void SeqNumInfo::onCompletionOfCombinedPrepareSigVerification(
    SeqNum seqNumber, ViewNum viewNumber, bool isValid) {
  prepareSigCollector->onCompletionOfCombinedSigVerification(
      seqNumber, viewNumber, isValid);
}

///////////////////////////////////////////////////////////////////////////////
// Internal message classes
///////////////////////////////////////////////////////////////////////////////

class CombinedSigFailedInternalMsg : public InternalMessage {
 protected:
  InternalReplicaApi* const replica;
  const SeqNum seqNumber;
  const ViewNum view;
  const std::set<uint16_t> replicasWithBadSigs;

 public:
  CombinedSigFailedInternalMsg(InternalReplicaApi* rep,
                               SeqNum s,
                               ViewNum v,
                               std::set<uint16_t>& repsWithBadSigs)
      : replica{rep},
        seqNumber{s},
        view{v},
        replicasWithBadSigs{repsWithBadSigs} {}

  virtual void handle() override {
    replica->onPrepareCombinedSigFailed(seqNumber, view, replicasWithBadSigs);
  }
};

class CombinedSigSucceededInternalMsg : public InternalMessage {
 protected:
  InternalReplicaApi* const replica;
  const SeqNum seqNumber;
  const ViewNum view;
  const char* combinedSig;
  const uint16_t combinedSigLen;

 public:
  CombinedSigSucceededInternalMsg(InternalReplicaApi* rep,
                                  SeqNum s,
                                  ViewNum v,
                                  const char* sig,
                                  uint16_t sigLen)
      : replica{rep}, seqNumber{s}, view{v}, combinedSigLen{sigLen} {
    char* p = (char*)std::malloc(sigLen);
    memcpy(p, sig, sigLen);
    combinedSig = p;
  }

  virtual ~CombinedSigSucceededInternalMsg() override {
    std::free((void*)combinedSig);
  }

  virtual void handle() override {
    replica->onPrepareCombinedSigSucceeded(
        seqNumber, view, combinedSig, combinedSigLen);
  }
};

class VerifyCombinedSigResultInternalMsg : public InternalMessage {
 protected:
  InternalReplicaApi* const replica;
  const SeqNum seqNumber;
  const ViewNum view;
  const bool isValid;

 public:
  VerifyCombinedSigResultInternalMsg(InternalReplicaApi* rep,
                                     SeqNum s,
                                     ViewNum v,
                                     bool result)
      : replica{rep}, seqNumber{s}, view{v}, isValid{result} {}

  virtual void handle() override {
    replica->onPrepareVerifyCombinedSigResult(seqNumber, view, isValid);
  }
};

class CombinedCommitSigSucceededInternalMsg : public InternalMessage {
 protected:
  InternalReplicaApi* const replica;
  const SeqNum seqNumber;
  const ViewNum view;
  const char* combinedSig;
  const uint16_t combinedSigLen;

 public:
  CombinedCommitSigSucceededInternalMsg(InternalReplicaApi* rep,
                                        SeqNum s,
                                        ViewNum v,
                                        const char* sig,
                                        uint16_t sigLen)
      : replica{rep}, seqNumber{s}, view{v}, combinedSigLen{sigLen} {
    char* p = (char*)std::malloc(sigLen);
    memcpy(p, sig, sigLen);
    combinedSig = p;
  }

  virtual ~CombinedCommitSigSucceededInternalMsg() override {
    std::free((void*)combinedSig);
  }

  virtual void handle() override {
    replica->onCommitCombinedSigSucceeded(
        seqNumber, view, combinedSig, combinedSigLen);
  }
};

class CombinedCommitSigFailedInternalMsg : public InternalMessage {
 protected:
  InternalReplicaApi* const replica;
  const SeqNum seqNumber;
  const ViewNum view;
  const std::set<uint16_t> replicasWithBadSigs;

 public:
  CombinedCommitSigFailedInternalMsg(InternalReplicaApi* rep,
                                     SeqNum s,
                                     ViewNum v,
                                     std::set<uint16_t>& repsWithBadSigs)
      : replica{rep},
        seqNumber{s},
        view{v},
        replicasWithBadSigs{repsWithBadSigs} {}

  virtual void handle() override {
    replica->onCommitCombinedSigFailed(seqNumber, view, replicasWithBadSigs);
  }
};

class VerifyCombinedCommitSigResultInternalMsg : public InternalMessage {
 protected:
  InternalReplicaApi* const replica;
  const SeqNum seqNumber;
  const ViewNum view;
  const bool isValid;

 public:
  VerifyCombinedCommitSigResultInternalMsg(InternalReplicaApi* rep,
                                           SeqNum s,
                                           ViewNum v,
                                           bool result)
      : replica{rep}, seqNumber{s}, view{v}, isValid{result} {}

  virtual void handle() override {
    replica->onCommitVerifyCombinedSigResult(seqNumber, view, isValid);
  }
};

///////////////////////////////////////////////////////////////////////////////
// class SeqNumInfo::ExFuncForPrepareCollector
///////////////////////////////////////////////////////////////////////////////

PrepareFullMsg*
SeqNumInfo::ExFuncForPrepareCollector::createCombinedSignatureMsg(
    void* context,
    SeqNum seqNumber,
    ViewNum viewNumber,
    const char* const combinedSig,
    uint16_t combinedSigLen) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  return PrepareFullMsg::create(viewNumber,
                                seqNumber,
                                r->getReplicasInfo().myId(),
                                combinedSig,
                                combinedSigLen);
}

InternalMessage*
SeqNumInfo::ExFuncForPrepareCollector::createInterCombinedSigFailed(
    void* context,
    SeqNum seqNumber,
    ViewNum viewNumber,
    std::set<uint16_t> replicasWithBadSigs) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  return new CombinedSigFailedInternalMsg(
      r, seqNumber, viewNumber, replicasWithBadSigs);
}

InternalMessage*
SeqNumInfo::ExFuncForPrepareCollector::createInterCombinedSigSucceeded(
    void* context,
    SeqNum seqNumber,
    ViewNum viewNumber,
    const char* combinedSig,
    uint16_t combinedSigLen) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  return new CombinedSigSucceededInternalMsg(
      r, seqNumber, viewNumber, combinedSig, combinedSigLen);
}

InternalMessage*
SeqNumInfo::ExFuncForPrepareCollector::createInterVerifyCombinedSigResult(
    void* context, SeqNum seqNumber, ViewNum viewNumber, bool isValid) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  return new VerifyCombinedSigResultInternalMsg(
      r, seqNumber, viewNumber, isValid);
}

uint16_t SeqNumInfo::ExFuncForPrepareCollector::numberOfRequiredSignatures(
    void* context) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  const ReplicasInfo& info = r->getReplicasInfo();
  return (uint16_t)((info.fVal() * 2) + info.cVal() + 1);
}

IThresholdVerifier* SeqNumInfo::ExFuncForPrepareCollector::thresholdVerifier(
    void* context) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  return r->getThresholdVerifierForSlowPathCommit();
}

SimpleThreadPool& SeqNumInfo::ExFuncForPrepareCollector::threadPool(
    void* context) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  return r->getInternalThreadPool();
}

IncomingMsgsStorage& SeqNumInfo::ExFuncForPrepareCollector::incomingMsgsStorage(
    void* context) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  return r->getIncomingMsgsStorage();
}

///////////////////////////////////////////////////////////////////////////////
// class SeqNumInfo::ExFuncForCommitCollector
///////////////////////////////////////////////////////////////////////////////

CommitFullMsg* SeqNumInfo::ExFuncForCommitCollector::createCombinedSignatureMsg(
    void* context,
    SeqNum seqNumber,
    ViewNum viewNumber,
    const char* const combinedSig,
    uint16_t combinedSigLen) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  return CommitFullMsg::create(viewNumber,
                               seqNumber,
                               r->getReplicasInfo().myId(),
                               combinedSig,
                               combinedSigLen);
}

InternalMessage*
SeqNumInfo::ExFuncForCommitCollector::createInterCombinedSigFailed(
    void* context,
    SeqNum seqNumber,
    ViewNum viewNumber,
    std::set<uint16_t> replicasWithBadSigs) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  return new CombinedCommitSigFailedInternalMsg(
      r, seqNumber, viewNumber, replicasWithBadSigs);
}

InternalMessage*
SeqNumInfo::ExFuncForCommitCollector::createInterCombinedSigSucceeded(
    void* context,
    SeqNum seqNumber,
    ViewNum viewNumber,
    const char* combinedSig,
    uint16_t combinedSigLen) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  return new CombinedCommitSigSucceededInternalMsg(
      r, seqNumber, viewNumber, combinedSig, combinedSigLen);
}

InternalMessage*
SeqNumInfo::ExFuncForCommitCollector::createInterVerifyCombinedSigResult(
    void* context, SeqNum seqNumber, ViewNum viewNumber, bool isValid) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  return new VerifyCombinedCommitSigResultInternalMsg(
      r, seqNumber, viewNumber, isValid);
}

uint16_t SeqNumInfo::ExFuncForCommitCollector::numberOfRequiredSignatures(
    void* context) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  const ReplicasInfo& info = r->getReplicasInfo();
  return (uint16_t)((info.fVal() * 2) + info.cVal() + 1);
}

IThresholdVerifier* SeqNumInfo::ExFuncForCommitCollector::thresholdVerifier(
    void* context) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  return r->getThresholdVerifierForSlowPathCommit();
}

SimpleThreadPool& SeqNumInfo::ExFuncForCommitCollector::threadPool(
    void* context) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  return r->getInternalThreadPool();
}

IncomingMsgsStorage& SeqNumInfo::ExFuncForCommitCollector::incomingMsgsStorage(
    void* context) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  return r->getIncomingMsgsStorage();
}

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

void SeqNumInfo::init(SeqNumInfo& i, void* d) {
  void* context = d;
  InternalReplicaApi* r = (InternalReplicaApi*)context;

  i.replica = r;

  i.prepareSigCollector =
      new CollectorOfThresholdSignatures<PreparePartialMsg,
                                         PrepareFullMsg,
                                         ExFuncForPrepareCollector>(context);
  i.commitMsgsCollector =
      new CollectorOfThresholdSignatures<CommitPartialMsg,
                                         CommitFullMsg,
                                         ExFuncForCommitCollector>(context);
  i.partialProofsSet = new PartialProofsSet((InternalReplicaApi*)r);
  i.partialExecProofsSet = new PartialExecProofsSet((InternalReplicaApi*)r);
}

}  // namespace impl
}  // namespace bftEngine
