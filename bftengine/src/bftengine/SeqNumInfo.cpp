// Concord
//
// Copyright (c) 2018-2023 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "SeqNumInfo.hpp"
#include "InternalReplicaApi.hpp"
#include "OpenTracing.hpp"
#include "messages/SignatureInternalMsgs.hpp"
#include "CryptoManager.hpp"

namespace bftEngine {
namespace impl {

SeqNumInfo::SeqNumInfo()
    : replica_(nullptr),
      prePrepareMsg_(nullptr),
      prepareSigCollector_(nullptr),
      commitMsgsCollector_(nullptr),
      fastPathOptimisticCollector_(nullptr),
      fastPathThresholdCollector_(nullptr),
      fastPathTimeOfSelfPartialProof_(MinTime),
      primary_(false),
      forcedCompleted_(false),
      slowPathHasStarted_(false),
      firstSeenFromPrimary_(MinTime),
      timeOfLastInfoRequest_(MinTime),
      commitUpdateTime_(MinTime) {}

SeqNumInfo::~SeqNumInfo() {
  resetAndFree();

  delete prepareSigCollector_;
  delete commitMsgsCollector_;
  delete fastPathOptimisticCollector_;
  delete fastPathThresholdCollector_;
}

void SeqNumInfo::resetCommitSignatures(CommitPath cPath) {
  switch (cPath) {
    case CommitPath::SLOW:
      commitMsgsCollector_->resetAndFree();
      break;
    case CommitPath::OPTIMISTIC_FAST:
      fastPathOptimisticCollector_->resetAndFree();
      break;
    case CommitPath::FAST_WITH_THRESHOLD:
      fastPathThresholdCollector_->resetAndFree();
      break;
    default:
      LOG_ERROR(CNSUS, "Invalid CommitPath value: " << (int)cPath);
      ConcordAssert(false);
  }
}

void SeqNumInfo::resetPrepareSignatures() { prepareSigCollector_->resetAndFree(); }

void SeqNumInfo::resetAndFree() {
  delete prePrepareMsg_;
  prePrepareMsg_ = nullptr;

  prepareSigCollector_->resetAndFree();
  commitMsgsCollector_->resetAndFree();
  fastPathOptimisticCollector_->resetAndFree();
  fastPathThresholdCollector_->resetAndFree();
  fastPathTimeOfSelfPartialProof_ = MinTime;

  primary_ = false;
  forcedCompleted_ = false;
  slowPathHasStarted_ = false;
  firstSeenFromPrimary_ = MinTime;
  timeOfLastInfoRequest_ = MinTime;
  commitUpdateTime_ = getMonotonicTime();  // TODO(GG): TBD
}

void SeqNumInfo::getAndReset(PrePrepareMsg*& outPrePrepare, PrepareFullMsg*& outCombinedValidSignatureMsg) {
  outPrePrepare = prePrepareMsg_;
  prePrepareMsg_ = nullptr;

  prepareSigCollector_->getAndReset(outCombinedValidSignatureMsg);
  resetAndFree();
}

bool SeqNumInfo::addMsg(PrePrepareMsg* m, bool directAdd, bool isTimeCorrect) {
  if (prePrepareMsg_ != nullptr) return false;

  ConcordAssert(primary_ == false);
  ConcordAssert(!forcedCompleted_);
  ConcordAssert(!prepareSigCollector_->hasPartialMsgFromReplica(replica_->getReplicasInfo().myId()));

  prePrepareMsg_ = m;
  isTimeCorrect_ = isTimeCorrect;

  // set expected
  Digest tmpDigest;
  m->digestOfRequests().calcCombination(m->viewNumber(), m->seqNumber(), tmpDigest);
  if (!directAdd)
    prepareSigCollector_->setExpected(m->seqNumber(), m->viewNumber(), tmpDigest);
  else
    prepareSigCollector_->initExpected(m->seqNumber(), m->viewNumber(), tmpDigest);

  if (firstSeenFromPrimary_ == MinTime)  // TODO(GG): remove condition - TBD
    firstSeenFromPrimary_ = getMonotonicTime();
  return true;
}

bool SeqNumInfo::addSelfMsg(PrePrepareMsg* m, bool directAdd) {
  ConcordAssert(primary_ == false);
  ConcordAssert(replica_->getReplicasInfo().myId() == replica_->getReplicasInfo().primaryOfView(m->viewNumber()));
  ConcordAssert(!forcedCompleted_);
  ConcordAssert(prePrepareMsg_ == nullptr);

  prePrepareMsg_ = m;
  primary_ = true;

  // set expected
  Digest tmpDigest;
  m->digestOfRequests().calcCombination(m->viewNumber(), m->seqNumber(), tmpDigest);
  if (!directAdd)
    prepareSigCollector_->setExpected(m->seqNumber(), m->viewNumber(), tmpDigest);
  else
    prepareSigCollector_->initExpected(m->seqNumber(), m->viewNumber(), tmpDigest);

  if (firstSeenFromPrimary_ == MinTime)  // TODO(GG): remove condition - TBD
    firstSeenFromPrimary_ = getMonotonicTime();
  return true;
}

bool SeqNumInfo::addMsg(PreparePartialMsg* m) {
  ConcordAssert(replica_->getReplicasInfo().myId() != m->senderId());
  ConcordAssert(!forcedCompleted_);

  bool retVal = prepareSigCollector_->addMsgWithPartialSignature(m, m->senderId());
  return retVal;
}

bool SeqNumInfo::addSelfMsg(PreparePartialMsg* m, bool directAdd) {
  ConcordAssert(replica_->getReplicasInfo().myId() == m->senderId());
  ConcordAssert(!forcedCompleted_);

  bool r;
  if (!directAdd)
    r = prepareSigCollector_->addMsgWithPartialSignature(m, m->senderId());
  else
    r = prepareSigCollector_->initMsgWithPartialSignature(m, m->senderId());

  ConcordAssert(r);
  return true;
}

bool SeqNumInfo::addMsg(PrepareFullMsg* m, bool directAdd) {
  ConcordAssert(directAdd || replica_->getReplicasInfo().myId() != m->senderId());  // TODO(GG): TBD
  ConcordAssert(!forcedCompleted_);

  bool retVal;
  if (!directAdd)
    retVal = prepareSigCollector_->addMsgWithCombinedSignature(m);
  else
    retVal = prepareSigCollector_->initMsgWithCombinedSignature(m);
  return retVal;
}

bool SeqNumInfo::addMsg(CommitPartialMsg* m) {
  ConcordAssert(replica_->getReplicasInfo().myId() != m->senderId());  // TODO(GG): TBD
  ConcordAssert(!forcedCompleted_);

  bool r = commitMsgsCollector_->addMsgWithPartialSignature(m, m->senderId());
  if (r) commitUpdateTime_ = getMonotonicTime();
  return r;
}

bool SeqNumInfo::addSelfCommitPartialMsgAndDigest(CommitPartialMsg* m, Digest& commitDigest, bool directAdd) {
  ConcordAssert(replica_->getReplicasInfo().myId() == m->senderId());
  ConcordAssert(!forcedCompleted_);

  Digest tmpDigest;
  commitDigest.calcCombination(m->viewNumber(), m->seqNumber(), tmpDigest);
  bool r;
  if (!directAdd) {
    commitMsgsCollector_->setExpected(m->seqNumber(), m->viewNumber(), tmpDigest);
    r = commitMsgsCollector_->addMsgWithPartialSignature(m, m->senderId());
  } else {
    commitMsgsCollector_->initExpected(m->seqNumber(), m->viewNumber(), tmpDigest);
    r = commitMsgsCollector_->initMsgWithPartialSignature(m, m->senderId());
  }
  ConcordAssert(r);
  commitUpdateTime_ = getMonotonicTime();
  return true;
}

bool SeqNumInfo::addMsg(CommitFullMsg* m, bool directAdd) {
  ConcordAssert(directAdd || replica_->getReplicasInfo().myId() != m->senderId());  // TODO(GG): TBD
  ConcordAssert(!forcedCompleted_);

  bool r;
  if (!directAdd)
    r = commitMsgsCollector_->addMsgWithCombinedSignature(m);
  else
    r = commitMsgsCollector_->initMsgWithCombinedSignature(m);

  if (r) commitUpdateTime_ = getMonotonicTime();
  return r;
}

void SeqNumInfo::forceComplete() {
  ConcordAssert(!forcedCompleted_);
  ConcordAssert(hasPrePrepareMsg());
  ConcordAssert(hasFastPathFullCommitProof());

  forcedCompleted_ = true;
  commitUpdateTime_ = getMonotonicTime();
}

PrePrepareMsg* SeqNumInfo::getPrePrepareMsg() const { return prePrepareMsg_; }

PrePrepareMsg* SeqNumInfo::getSelfPrePrepareMsg() const {
  if (primary_) {
    return prePrepareMsg_;
  }
  return nullptr;
}

PreparePartialMsg* SeqNumInfo::getSelfPreparePartialMsg() const {
  PreparePartialMsg* p = prepareSigCollector_->getPartialMsgFromReplica(replica_->getReplicasInfo().myId());
  return p;
}

PrepareFullMsg* SeqNumInfo::getValidPrepareFullMsg() const {
  return prepareSigCollector_->getMsgWithValidCombinedSignature();
}

CommitPartialMsg* SeqNumInfo::getSelfCommitPartialMsg() const {
  CommitPartialMsg* p = commitMsgsCollector_->getPartialMsgFromReplica(replica_->getReplicasInfo().myId());
  return p;
}

CommitFullMsg* SeqNumInfo::getValidCommitFullMsg() const {
  return commitMsgsCollector_->getMsgWithValidCombinedSignature();
}

bool SeqNumInfo::hasPrePrepareMsg() const { return (prePrepareMsg_ != nullptr); }

bool SeqNumInfo::hasMatchingPrePrepare(SeqNum seqNum) const {
  return (prePrepareMsg_ != nullptr) && prePrepareMsg_->seqNumber() == seqNum;
}

bool SeqNumInfo::isPrepared() const {
  return forcedCompleted_ || ((prePrepareMsg_ != nullptr) && prepareSigCollector_->isComplete());
}

bool SeqNumInfo::isCommitted__gg() const {
  // TODO(GG): TBD - asserts on 'prepared'

  bool retVal = forcedCompleted_ || commitMsgsCollector_->isComplete();
  return retVal;
}

bool SeqNumInfo::preparedOrHasPreparePartialFromReplica(ReplicaId repId) const {
  return isPrepared() || prepareSigCollector_->hasPartialMsgFromReplica(repId);
}

bool SeqNumInfo::committedOrHasCommitPartialFromReplica(ReplicaId repId) const {
  return isCommitted__gg() || commitMsgsCollector_->hasPartialMsgFromReplica(repId);
}

Time SeqNumInfo::getTimeOfFirstRelevantInfoFromPrimary() const { return firstSeenFromPrimary_; }

Time SeqNumInfo::getTimeOfLastInfoRequest() const { return timeOfLastInfoRequest_; }

bool SeqNumInfo::hasFastPathFullCommitProof() const {
  auto optimistic = fastPathOptimisticCollector_->getMsgWithValidCombinedSignature();
  auto threshold = fastPathThresholdCollector_->getMsgWithValidCombinedSignature();
  return optimistic || threshold;
}

bool SeqNumInfo::hasFastPathPartialCommitProofFromReplica(ReplicaId repId) const {
  auto optimistic = fastPathOptimisticCollector_->getPartialMsgFromReplica(repId);
  auto threshold = fastPathThresholdCollector_->getPartialMsgFromReplica(repId);
  return optimistic || threshold;
}

PartialCommitProofMsg* SeqNumInfo::getFastPathSelfPartialCommitProofMsg() const {
  const auto myReplicaId = replica_->getReplicasInfo().myId();
  auto optimistic = fastPathOptimisticCollector_->getPartialMsgFromReplica(myReplicaId);
  auto threshold = fastPathThresholdCollector_->getPartialMsgFromReplica(myReplicaId);
  ConcordAssert(!(optimistic && threshold));
  return optimistic ? optimistic : threshold;
}

FullCommitProofMsg* SeqNumInfo::getFastPathFullCommitProofMsg() const {
  auto optimistic = fastPathOptimisticCollector_->getMsgWithValidCombinedSignature();
  auto threshold = fastPathThresholdCollector_->getMsgWithValidCombinedSignature();
  ConcordAssert(!(optimistic && threshold));
  return optimistic ? optimistic : threshold;
}

void SeqNumInfo::setFastPathTimeOfSelfPartialProof(const Time& t) { fastPathTimeOfSelfPartialProof_ = t; }

Time SeqNumInfo::getFastPathTimeOfSelfPartialProof() const { return fastPathTimeOfSelfPartialProof_; }

bool SeqNumInfo::addFastPathSelfPartialCommitMsgAndDigest(PartialCommitProofMsg* m, Digest& commitDigest) {
  ConcordAssert(m != nullptr);
  const ReplicaId myId = m->senderId();
  ConcordAssert(myId == replica_->getReplicasInfo().myId());
  ConcordAssert(hasMatchingPrePrepare(m->seqNumber()));
  ConcordAssert(!hasFastPathFullCommitProof());
  ConcordAssert(getSelfCommitPartialMsg() == nullptr);

  bool result = false;
  switch (m->commitPath()) {
    case CommitPath::OPTIMISTIC_FAST:
      fastPathOptimisticCollector_->setExpected(m->seqNumber(), m->viewNumber(), commitDigest);
      result = fastPathOptimisticCollector_->addMsgWithPartialSignature(m, myId);
      break;
    case CommitPath::FAST_WITH_THRESHOLD:
      fastPathThresholdCollector_->setExpected(m->seqNumber(), m->viewNumber(), commitDigest);
      result = fastPathThresholdCollector_->addMsgWithPartialSignature(m, myId);
      break;
    default:
      LOG_ERROR(CNSUS, "Invalid CommitPath value: " << (int)m->commitPath());
      ConcordAssert(false);
  }
  return result;
}

bool SeqNumInfo::addFastPathPartialCommitMsg(PartialCommitProofMsg* m) {
  ConcordAssert(m != nullptr);

  const ReplicaId repId = m->senderId();
  ConcordAssert(repId != replica_->getReplicasInfo().myId());
  ConcordAssert(replica_->getReplicasInfo().isIdOfReplica(repId));
  // PartialCommitProofMsg is allowed before a prePrepare is received
  ConcordAssert(!prePrepareMsg_ || hasMatchingPrePrepare(m->seqNumber()));

  if (hasFastPathFullCommitProof()) return false;

  const auto* selfPartialCommitProof = getFastPathSelfPartialCommitProofMsg();
  const CommitPath cPath = m->commitPath();

  // Check different fast commit path
  if (selfPartialCommitProof && selfPartialCommitProof->commitPath() != cPath) {
    LOG_WARN(CNSUS,
             "Ignoring PartialCommitProofMsg (" << CommitPathToStr(cPath) << "). Current path is "
                                                << CommitPathToStr(selfPartialCommitProof->commitPath()));
    return false;
  }

  auto result = false;
  switch (cPath) {
    case CommitPath::OPTIMISTIC_FAST:
      result = fastPathOptimisticCollector_->addMsgWithPartialSignature(m, repId);
      break;
    case CommitPath::FAST_WITH_THRESHOLD:
      result = fastPathThresholdCollector_->addMsgWithPartialSignature(m, repId);
      break;
    default:
      LOG_ERROR(CNSUS, "Invalid CommitPath value: " << (int)cPath);
      ConcordAssert(false);
  }
  return result;
}

bool SeqNumInfo::addFastPathFullCommitMsg(FullCommitProofMsg* m, bool directAdd) {
  ConcordAssert(m != nullptr);
  if (hasFastPathFullCommitProof()) return false;

  PartialCommitProofMsg* myPCP = getFastPathSelfPartialCommitProofMsg();

  if (myPCP == nullptr) {
    // TODO(GG): can be improved (we can keep the FullCommitProof  message until myPCP!=nullptr
    LOG_WARN(CNSUS,
             "FullCommitProofMsg arrived before PartialCommitProofMsg. TODO(GG): should be handled to avoid delays. ");
    return false;
  }

  if (m->seqNumber() != myPCP->seqNumber() || m->viewNumber() != myPCP->viewNumber()) {
    LOG_WARN(CNSUS, "Received unexpected FullCommitProofMsg");
    return false;
  }

  // ToDo--EDJ: FullCommitProofMsg doesn't say to which fast path it belongs
  const auto cPath = myPCP->commitPath();
  bool result = false;
  switch (cPath) {
    case CommitPath::OPTIMISTIC_FAST:
      result = directAdd ? fastPathOptimisticCollector_->initMsgWithCombinedSignature(m)
                         : fastPathOptimisticCollector_->addMsgWithCombinedSignature(m);
      break;
    case CommitPath::FAST_WITH_THRESHOLD:
      result = directAdd ? fastPathThresholdCollector_->initMsgWithCombinedSignature(m)
                         : fastPathThresholdCollector_->addMsgWithCombinedSignature(m);
      break;
    default:
      LOG_ERROR(CNSUS, "Invalid CommitPath value: " << (int)cPath);
      ConcordAssert(false);
  }
  return result;
}

void SeqNumInfo::startSlowPath() { slowPathHasStarted_ = true; }

bool SeqNumInfo::slowPathStarted() { return slowPathHasStarted_; }

void SeqNumInfo::setTimeOfLastInfoRequest(Time t) { timeOfLastInfoRequest_ = t; }

void SeqNumInfo::onCompletionOfPrepareSignaturesProcessing(SeqNum seqNumber,
                                                           ViewNum viewNumber,
                                                           const std::set<ReplicaId>& replicasWithBadSigs) {
  prepareSigCollector_->onCompletionOfSignaturesProcessing(seqNumber, viewNumber, replicasWithBadSigs);
}

void SeqNumInfo::onCompletionOfPrepareSignaturesProcessing(SeqNum seqNumber,
                                                           ViewNum viewNumber,
                                                           const char* combinedSig,
                                                           uint16_t combinedSigLen,
                                                           const concordUtils::SpanContext& span_context) {
  prepareSigCollector_->onCompletionOfSignaturesProcessing(
      seqNumber, viewNumber, combinedSig, combinedSigLen, span_context);
}

void SeqNumInfo::onCompletionOfCombinedPrepareSigVerification(SeqNum seqNumber, ViewNum viewNumber, bool isValid) {
  prepareSigCollector_->onCompletionOfCombinedSigVerification(seqNumber, viewNumber, isValid);
}

void SeqNumInfo::onCompletionOfCommitSignaturesProcessing(SeqNum seqNumber,
                                                          ViewNum viewNumber,
                                                          CommitPath cPath,
                                                          const std::set<uint16_t>& replicasWithBadSigs) {
  switch (cPath) {
    case CommitPath::SLOW:
      commitMsgsCollector_->onCompletionOfSignaturesProcessing(seqNumber, viewNumber, replicasWithBadSigs);
      break;
    case CommitPath::OPTIMISTIC_FAST:
      fastPathOptimisticCollector_->onCompletionOfSignaturesProcessing(seqNumber, viewNumber, replicasWithBadSigs);
      break;
    case CommitPath::FAST_WITH_THRESHOLD:
      fastPathThresholdCollector_->onCompletionOfSignaturesProcessing(seqNumber, viewNumber, replicasWithBadSigs);
      break;
    default:
      LOG_ERROR(CNSUS, "Invalid CommitPath value: " << (int)cPath);
      ConcordAssert(false);
  }
}

void SeqNumInfo::onCompletionOfCommitSignaturesProcessing(SeqNum seqNumber,
                                                          ViewNum viewNumber,
                                                          CommitPath cPath,
                                                          const char* combinedSig,
                                                          uint16_t combinedSigLen,
                                                          const concordUtils::SpanContext& span_context) {
  switch (cPath) {
    case CommitPath::SLOW:
      commitMsgsCollector_->onCompletionOfSignaturesProcessing(
          seqNumber, viewNumber, combinedSig, combinedSigLen, span_context);
      break;
    case CommitPath::OPTIMISTIC_FAST:
      fastPathOptimisticCollector_->onCompletionOfSignaturesProcessing(
          seqNumber, viewNumber, combinedSig, combinedSigLen, span_context);
      break;
    case CommitPath::FAST_WITH_THRESHOLD:
      fastPathThresholdCollector_->onCompletionOfSignaturesProcessing(
          seqNumber, viewNumber, combinedSig, combinedSigLen, span_context);
      break;
    default:
      LOG_ERROR(CNSUS, "Invalid CommitPath value: " << (int)cPath);
      ConcordAssert(false);
  }
}

void SeqNumInfo::onCompletionOfCombinedCommitSigVerification(SeqNum seqNumber,
                                                             ViewNum viewNumber,
                                                             CommitPath cPath,
                                                             bool isValid) {
  switch (cPath) {
    case CommitPath::SLOW:
      commitMsgsCollector_->onCompletionOfCombinedSigVerification(seqNumber, viewNumber, isValid);
      break;
    case CommitPath::OPTIMISTIC_FAST:
      fastPathOptimisticCollector_->onCompletionOfCombinedSigVerification(seqNumber, viewNumber, isValid);
      break;
    case CommitPath::FAST_WITH_THRESHOLD:
      fastPathThresholdCollector_->onCompletionOfCombinedSigVerification(seqNumber, viewNumber, isValid);
      break;
    default:
      LOG_ERROR(CNSUS, "Invalid CommitPath value: " << (int)cPath);
      ConcordAssert(false);
  }
}

///////////////////////////////////////////////////////////////////////////////
// class SeqNumInfo::ExFuncForPrepareCollector
///////////////////////////////////////////////////////////////////////////////

PrepareFullMsg* SeqNumInfo::ExFuncForPrepareCollector::createCombinedSignatureMsg(
    void* context,
    SeqNum seqNumber,
    ViewNum viewNumber,
    const char* const combinedSig,
    uint16_t combinedSigLen,
    const concordUtils::SpanContext& span_context) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  return PrepareFullMsg::create(
      viewNumber, seqNumber, r->getReplicasInfo().myId(), combinedSig, combinedSigLen, span_context);
}

InternalMessage SeqNumInfo::ExFuncForPrepareCollector::createInterCombinedSigFailed(
    SeqNum seqNumber, ViewNum viewNumber, const std::set<uint16_t>& replicasWithBadSigs) {
  return CombinedSigFailedInternalMsg(seqNumber, viewNumber, replicasWithBadSigs);
}

InternalMessage SeqNumInfo::ExFuncForPrepareCollector::createInterCombinedSigSucceeded(
    SeqNum seqNumber,
    ViewNum viewNumber,
    const char* combinedSig,
    uint16_t combinedSigLen,
    const concordUtils::SpanContext& span_context) {
  return CombinedSigSucceededInternalMsg(seqNumber, viewNumber, combinedSig, combinedSigLen, span_context);
}

InternalMessage SeqNumInfo::ExFuncForPrepareCollector::createInterVerifyCombinedSigResult(SeqNum seqNumber,
                                                                                          ViewNum viewNumber,
                                                                                          bool isValid) {
  return VerifyCombinedSigResultInternalMsg(seqNumber, viewNumber, isValid);
}

uint16_t SeqNumInfo::ExFuncForPrepareCollector::numberOfRequiredSignatures(void* context) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  const ReplicasInfo& info = r->getReplicasInfo();
  return (uint16_t)((info.fVal() * 2) + info.cVal() + 1);
}

std::shared_ptr<IThresholdVerifier> SeqNumInfo::ExFuncForPrepareCollector::thresholdVerifier(SeqNum seqNumber) {
  return CryptoManager::instance().thresholdVerifierForSlowPathCommit(seqNumber);
}

concord::util::SimpleThreadPool& SeqNumInfo::ExFuncForPrepareCollector::threadPool(void* context) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  return r->getInternalThreadPool();
}

IncomingMsgsStorage& SeqNumInfo::ExFuncForPrepareCollector::incomingMsgsStorage(void* context) {
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
    uint16_t combinedSigLen,
    const concordUtils::SpanContext& span_context) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  return CommitFullMsg::create(
      viewNumber, seqNumber, r->getReplicasInfo().myId(), combinedSig, combinedSigLen, span_context);
}

InternalMessage SeqNumInfo::ExFuncForCommitCollector::createInterCombinedSigFailed(
    SeqNum seqNumber, ViewNum viewNumber, const std::set<uint16_t>& replicasWithBadSigs) {
  return CombinedCommitSigFailedInternalMsg(seqNumber, viewNumber, replicasWithBadSigs);
}

InternalMessage SeqNumInfo::ExFuncForCommitCollector::createInterCombinedSigSucceeded(
    SeqNum seqNumber,
    ViewNum viewNumber,
    const char* combinedSig,
    uint16_t combinedSigLen,
    const concordUtils::SpanContext& span_context) {
  return CombinedCommitSigSucceededInternalMsg(seqNumber, viewNumber, combinedSig, combinedSigLen, span_context);
}

InternalMessage SeqNumInfo::ExFuncForCommitCollector::createInterVerifyCombinedSigResult(SeqNum seqNumber,
                                                                                         ViewNum viewNumber,
                                                                                         bool isValid) {
  return VerifyCombinedCommitSigResultInternalMsg(seqNumber, viewNumber, isValid);
}

uint16_t SeqNumInfo::ExFuncForCommitCollector::numberOfRequiredSignatures(void* context) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  const ReplicasInfo& info = r->getReplicasInfo();
  return (uint16_t)((info.fVal() * 2) + info.cVal() + 1);
}

std::shared_ptr<IThresholdVerifier> SeqNumInfo::ExFuncForCommitCollector::thresholdVerifier(SeqNum seqNumber) {
  return CryptoManager::instance().thresholdVerifierForSlowPathCommit(seqNumber);
}

concord::util::SimpleThreadPool& SeqNumInfo::ExFuncForCommitCollector::threadPool(void* context) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  return r->getInternalThreadPool();
}

IncomingMsgsStorage& SeqNumInfo::ExFuncForCommitCollector::incomingMsgsStorage(void* context) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  return r->getIncomingMsgsStorage();
}

///////////////////////////////////////////////////////////////////////////////
// class SeqNumInfo::ExFuncForFastPathOptimisticCollector
///////////////////////////////////////////////////////////////////////////////

FullCommitProofMsg* SeqNumInfo::ExFuncForFastPathOptimisticCollector::createCombinedSignatureMsg(
    void* context,
    SeqNum seqNumber,
    ViewNum viewNumber,
    const char* const combinedSig,
    uint16_t combinedSigLen,
    const concordUtils::SpanContext& span_context) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;

  return new FullCommitProofMsg(
      r->getReplicasInfo().myId(), viewNumber, seqNumber, combinedSig, combinedSigLen, span_context);
}

InternalMessage SeqNumInfo::ExFuncForFastPathOptimisticCollector::createInterCombinedSigFailed(
    SeqNum seqNumber, ViewNum viewNumber, const std::set<uint16_t>& replicasWithBadSigs) {
  return FastPathCombinedCommitSigFailedInternalMsg(
      seqNumber, viewNumber, CommitPath::OPTIMISTIC_FAST, replicasWithBadSigs);
}

InternalMessage SeqNumInfo::ExFuncForFastPathOptimisticCollector::createInterCombinedSigSucceeded(
    SeqNum seqNumber,
    ViewNum viewNumber,
    const char* combinedSig,
    uint16_t combinedSigLen,
    const concordUtils::SpanContext& span_context) {
  return FastPathCombinedCommitSigSucceededInternalMsg(
      seqNumber, viewNumber, CommitPath::OPTIMISTIC_FAST, combinedSig, combinedSigLen, span_context);
}

InternalMessage SeqNumInfo::ExFuncForFastPathOptimisticCollector::createInterVerifyCombinedSigResult(SeqNum seqNumber,
                                                                                                     ViewNum viewNumber,
                                                                                                     bool isValid) {
  return FastPathVerifyCombinedCommitSigResultInternalMsg(seqNumber, viewNumber, CommitPath::OPTIMISTIC_FAST, isValid);
}

uint16_t SeqNumInfo::ExFuncForFastPathOptimisticCollector::numberOfRequiredSignatures(void* context) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  const ReplicasInfo& info = r->getReplicasInfo();
  return 3 * info.fVal() + 2 * info.cVal() + 1;
}

std::shared_ptr<IThresholdVerifier> SeqNumInfo::ExFuncForFastPathOptimisticCollector::thresholdVerifier(
    SeqNum seqNumber) {
  return CryptoManager::instance().thresholdVerifierForOptimisticCommit(seqNumber);
}

concord::util::SimpleThreadPool& SeqNumInfo::ExFuncForFastPathOptimisticCollector::threadPool(void* context) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  return r->getInternalThreadPool();
}

IncomingMsgsStorage& SeqNumInfo::ExFuncForFastPathOptimisticCollector::incomingMsgsStorage(void* context) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  return r->getIncomingMsgsStorage();
}

///////////////////////////////////////////////////////////////////////////////
// class SeqNumInfo::ExFuncForFastPathThresholdCollector
///////////////////////////////////////////////////////////////////////////////

FullCommitProofMsg* SeqNumInfo::ExFuncForFastPathThresholdCollector::createCombinedSignatureMsg(
    void* context,
    SeqNum seqNumber,
    ViewNum viewNumber,
    const char* const combinedSig,
    uint16_t combinedSigLen,
    const concordUtils::SpanContext& span_context) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;

  return new FullCommitProofMsg(
      r->getReplicasInfo().myId(), viewNumber, seqNumber, combinedSig, combinedSigLen, span_context);
}

InternalMessage SeqNumInfo::ExFuncForFastPathThresholdCollector::createInterCombinedSigFailed(
    SeqNum seqNumber, ViewNum viewNumber, const std::set<uint16_t>& replicasWithBadSigs) {
  return FastPathCombinedCommitSigFailedInternalMsg(
      seqNumber, viewNumber, CommitPath::FAST_WITH_THRESHOLD, replicasWithBadSigs);
}

InternalMessage SeqNumInfo::ExFuncForFastPathThresholdCollector::createInterCombinedSigSucceeded(
    SeqNum seqNumber,
    ViewNum viewNumber,
    const char* combinedSig,
    uint16_t combinedSigLen,
    const concordUtils::SpanContext& span_context) {
  return FastPathCombinedCommitSigSucceededInternalMsg(
      seqNumber, viewNumber, CommitPath::FAST_WITH_THRESHOLD, combinedSig, combinedSigLen, span_context);
}

InternalMessage SeqNumInfo::ExFuncForFastPathThresholdCollector::createInterVerifyCombinedSigResult(SeqNum seqNumber,
                                                                                                    ViewNum viewNumber,
                                                                                                    bool isValid) {
  return FastPathVerifyCombinedCommitSigResultInternalMsg(
      seqNumber, viewNumber, CommitPath::FAST_WITH_THRESHOLD, isValid);
}

uint16_t SeqNumInfo::ExFuncForFastPathThresholdCollector::numberOfRequiredSignatures(void* context) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  const ReplicasInfo& info = r->getReplicasInfo();
  return 3 * info.fVal() + info.cVal() + 1;
}

std::shared_ptr<IThresholdVerifier> SeqNumInfo::ExFuncForFastPathThresholdCollector::thresholdVerifier(
    SeqNum seqNumber) {
  return CryptoManager::instance().thresholdVerifierForCommit(seqNumber);
}

concord::util::SimpleThreadPool& SeqNumInfo::ExFuncForFastPathThresholdCollector::threadPool(void* context) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  return r->getInternalThreadPool();
}

IncomingMsgsStorage& SeqNumInfo::ExFuncForFastPathThresholdCollector::incomingMsgsStorage(void* context) {
  InternalReplicaApi* r = (InternalReplicaApi*)context;
  return r->getIncomingMsgsStorage();
}

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

void SeqNumInfo::init(SeqNumInfo& i, void* d) {
  void* context = d;
  InternalReplicaApi* r = (InternalReplicaApi*)context;

  i.replica_ = r;
  i.prepareSigCollector_ =
      new CollectorOfThresholdSignatures<PreparePartialMsg, PrepareFullMsg, ExFuncForPrepareCollector>(context);
  i.commitMsgsCollector_ =
      new CollectorOfThresholdSignatures<CommitPartialMsg, CommitFullMsg, ExFuncForCommitCollector>(context);
  i.fastPathOptimisticCollector_ = new FastPathOptimisticCollector(context);
  i.fastPathThresholdCollector_ = new FastPathThresholdCollector(context);
}

}  // namespace impl
}  // namespace bftEngine
