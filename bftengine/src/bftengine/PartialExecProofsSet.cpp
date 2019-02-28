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
// CollectorOfThresholdSignatures (or a similar module)

#include "PartialExecProofsSet.hpp"
#include "FullExecProofMsg.hpp"
#include "PartialExecProofMsg.hpp"
#include "Crypto.hpp"
#include "assertUtils.hpp"
#include "Logger.hpp"
#include "SimpleThreadPool.hpp"

namespace bftEngine {
namespace impl {

PartialExecProofsSet::PartialExecProofsSet(
    InternalReplicaApi* internalReplicaApi)
    : replicaApi(internalReplicaApi),
      replicasInfo(internalReplicaApi->getReplicasInfo()),
      numOfRquiredPartialProofs(internalReplicaApi->getReplicasInfo().fVal() +
                                1),
      seqNumber(0),
      myPartialExecProof(nullptr),
      accumulator(nullptr) {
  expectedDigest.makeZero();
}

PartialExecProofsSet::~PartialExecProofsSet() { resetAndFree(); }

void PartialExecProofsSet::resetAndFree() {
  if ((myPartialExecProof == nullptr) &&
      (participatingReplicas.size() == 0))  // if already empty
    return;

  seqNumber = 0;
  if (myPartialExecProof) delete myPartialExecProof;
  myPartialExecProof = nullptr;
  participatingReplicas.clear();
  expectedDigest.makeZero();
  if (accumulator) thresholdVerifier()->release(accumulator);
  accumulator = nullptr;
  if (setOfFullExecProofs.size() > 0) {
    for (std::set<FullExecProofMsg*>::iterator it = setOfFullExecProofs.begin();
         it != setOfFullExecProofs.end();
         ++it) {
      FullExecProofMsg* p = *it;
      delete p;
    }

    setOfFullExecProofs.clear();
  }
}

void PartialExecProofsSet::addSelf(PartialExecProofMsg* m,
                                   Digest& merkleRoot,
                                   std::set<FullExecProofMsg*> fullExecProofs) {
  const ReplicaId repId = m->senderId();

  Assert(m != nullptr && repId == replicasInfo.myId());
  Assert(seqNumber == 0);
  Assert(myPartialExecProof == nullptr);

  seqNumber = m->seqNumber();
  expectedDigest = merkleRoot;
  // NOTE: Accumulator might not be created yet, but the thresholdAccumulator()
  // method creates it for us transparently if needed
  thresholdAccumulator()->setExpectedDigest(
      reinterpret_cast<unsigned char*>(merkleRoot.content()), DIGEST_SIZE);
  myPartialExecProof = m;
  setOfFullExecProofs = fullExecProofs;

  addImp(m);
}

bool PartialExecProofsSet::addMsg(PartialExecProofMsg* m) {
  const ReplicaId repId = m->senderId();

  Assert(m != nullptr && repId != replicasInfo.myId());
  Assert((seqNumber == 0) || (seqNumber == m->seqNumber()));
  Assert(replicasInfo.isIdOfReplica(repId));

  if ((participatingReplicas.count(repId) == 0) &&
      (participatingReplicas.size() <
       numOfRquiredPartialProofs - 1)  // we don't have enough partial proofs
  ) {
    participatingReplicas.insert(repId);
    addImp(m);
    delete m;
    return true;
  } else {
    return false;
  }
}

void PartialExecProofsSet::addImp(PartialExecProofMsg* m) {
  thresholdAccumulator()->add(m->thresholSignature(),
                              m->thresholSignatureLength());

  if ((participatingReplicas.size() == (numOfRquiredPartialProofs - 1)) &&
      (myPartialExecProof != nullptr)) {
    tryToCreateFullProof();
  }
}

class MerkleExecSignatureInternalMsg : public InternalMessage {
 protected:
  InternalReplicaApi* const replicaApi;
  ViewNum viewNum;
  SeqNum seqNum;
  uint16_t signatureLength;
  const char* signature;

 public:
  MerkleExecSignatureInternalMsg(InternalReplicaApi* const internalReplicaApi,
                                 ViewNum viewNumber,
                                 SeqNum seqNumber,
                                 uint16_t signatureLength,
                                 const char* signature)
      : replicaApi(internalReplicaApi) {
    this->viewNum = viewNumber;
    this->seqNum = seqNumber;
    this->signatureLength = signatureLength;
    char* p = (char*)std::malloc(signatureLength);
    memcpy(p, signature, signatureLength);
    this->signature = p;
  }

  virtual ~MerkleExecSignatureInternalMsg() override {
    std::free((void*)signature);
  }

  virtual void handle() override {
    replicaApi->onMerkleExecSignature(
        viewNum, seqNum, signatureLength, signature);
  }
};

class AsynchExecProofCreationJob : public SimpleThreadPool::Job {
 public:
  AsynchExecProofCreationJob(InternalReplicaApi* const internalReplicaApi,
                             IThresholdVerifier* verifier,
                             IThresholdAccumulator* acc,
                             Digest& expectedDigest,
                             SeqNum seqNumber,
                             ViewNum view)
      : replicaApi(internalReplicaApi) {
    this->acc = acc;
    this->expectedDigest = expectedDigest;
    this->seqNumber = seqNumber;
    this->view = view;
    this->verifier = verifier;
  }

  virtual ~AsynchExecProofCreationJob(){};

  virtual void execute() {
    LOG_INFO_F(GL,
               "PartialExecProofsSet::AsynchProofCreationJob::execute - begin "
               "(for seqNumber %" PRId64 ")",
               seqNumber);

    const uint16_t bufferSize =
        (uint16_t)verifier->requiredLengthForSignedData();
    char* const bufferForSigComputations = (char*)alloca(bufferSize);

    //		char bufferForSigComputations[2048];

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
                                 sigLength);

    if (!succ) {
      LOG_WARN_F(GL,
                 "Failed to create execution proof for seqNumber %" PRId64 "",
                 seqNumber);
      LOG_INFO_F(GL,
                 "PartialExecProofsSet::AsynchProofCreationJob::execute - end "
                 "(for seqNumber %" PRId64 ")",
                 seqNumber);
      return;
    } else {
      MerkleExecSignatureInternalMsg* pInMsg =
          new MerkleExecSignatureInternalMsg(replicaApi,
                                             view,
                                             seqNumber,
                                             (uint16_t)sigLength,
                                             bufferForSigComputations);
      replicaApi->getIncomingMsgsStorage().pushInternalMsg(pInMsg);
    }
    LOG_INFO_F(GL,
               "PartialExecProofsSet::AsynchProofCreationJob::execute - end "
               "(for seqNumber %" PRId64 ")",
               seqNumber);
  }

  virtual void release() {}

 private:
  InternalReplicaApi* const replicaApi;
  IThresholdAccumulator* acc;
  Digest expectedDigest;
  SeqNum seqNumber;
  ViewNum view;
  IThresholdVerifier* verifier;
};

void PartialExecProofsSet::tryToCreateFullProof() {
  Assert(accumulator != nullptr);

  if ((participatingReplicas.size() == (numOfRquiredPartialProofs - 1)) &&
      (myPartialExecProof != nullptr)) {
    IThresholdAccumulator* acc = accumulator->clone();

    PartialExecProofMsg* myPEP = myPartialExecProof;

    AsynchExecProofCreationJob* j =
        new AsynchExecProofCreationJob(replicaApi,
                                       thresholdVerifier(),
                                       acc,
                                       expectedDigest,
                                       myPEP->seqNumber(),
                                       myPEP->viewNumber());

    replicaApi->getInternalThreadPool().add(j);

    LOG_INFO_F(
        GL,
        "PartialExecProofsSet - send to BK thread (for seqNumber %" PRId64 ")",
        seqNumber);
  }
}

void PartialExecProofsSet::setMerkleSignature(const char* sig,
                                              uint16_t sigLength) {
  for (set<FullExecProofMsg*>::iterator it = setOfFullExecProofs.begin();
       it != setOfFullExecProofs.end();
       ++it) {
    FullExecProofMsg* fep = *it;
    fep->setSignature(sig, sigLength);
  }
}

IThresholdVerifier* PartialExecProofsSet::thresholdVerifier() {
  IThresholdVerifier* verifier = replicaApi->getThresholdVerifierForExecution();
  Assert(verifier != nullptr);
  return verifier;
}

IThresholdAccumulator* PartialExecProofsSet::thresholdAccumulator() {
  if (accumulator == nullptr)
    accumulator = thresholdVerifier()->newAccumulator(false);

  return accumulator;
}

}  // namespace impl
}  // namespace bftEngine
