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

#pragma once

#include <type_traits>
#include <unordered_map>
#include <set>
#include <iterator>

#include "OpenTracing.hpp"
#include "PrimitiveTypes.hpp"
#include "Digest.hpp"
#include "SimpleThreadPool.hpp"
#include "InternalReplicaApi.hpp"
#include "IncomingMsgsStorage.hpp"
#include "assertUtils.hpp"
#include "messages/SignedShareMsgs.hpp"
#include "Logger.hpp"
#include "kvstream.h"
#include "demangle.hpp"
#include "threshsign/IThresholdVerifier.h"
#include "threshsign/IThresholdAccumulator.h"

namespace bftEngine {
namespace impl {

template <typename PART, typename FULL, typename ExternalFunc>
// TODO(GG): consider to enforce the requirements from PART, FULL and ExternalFunc
// TODO(GG): add ConcordAssert(ExternalFunc::numberOfRequiredSignatures(context) > 1);
class CollectorOfThresholdSignatures {
 public:
  CollectorOfThresholdSignatures(void* cnt) { this->context = cnt; }

  ~CollectorOfThresholdSignatures() { resetAndFree(); }

  bool addMsgWithPartialSignature(PART* partialSigMsg, ReplicaId repId) {
    ConcordAssert(partialSigMsg != nullptr);

    if ((combinedValidSignatureMsg != nullptr) || (replicasInfo.count(repId) > 0)) return false;

    // add partialSigMsg to replicasInfo
    RepInfo info = {partialSigMsg, SigState::Unknown};
    replicasInfo[repId] = info;

    numberOfUnknownSignatures++;
    LOG_TRACE(THRESHSIGN_LOG,
              KVLOG(partialSigMsg->seqNumber(), partialSigMsg->viewNumber(), numberOfUnknownSignatures));
    trySendToBkThread();

    return true;
  }

  bool addMsgWithCombinedSignature(FULL* combinedSigMsg) {
    if (combinedValidSignatureMsg != nullptr || candidateCombinedSignatureMsg != nullptr) return false;

    candidateCombinedSignatureMsg = combinedSigMsg;
    LOG_TRACE(THRESHSIGN_LOG, KVLOG(combinedSigMsg->seqNumber(), combinedSigMsg->viewNumber()));

    trySendToBkThread();

    return true;
  }

  void setExpected(SeqNum seqNumber, ViewNum view, Digest& digest) {
    ConcordAssert(seqNumber != 0);
    ConcordAssert(expectedSeqNumber == 0);
    ConcordAssert(!processingSignaturesInTheBackground);
    LOG_TRACE(THRESHSIGN_LOG, KVLOG(seqNumber, view));
    expectedSeqNumber = seqNumber;
    expectedView = view;
    expectedDigest = digest;

    trySendToBkThread();
  }

  void resetAndFree() {
    if ((replicasInfo.size() == 0) && (combinedValidSignatureMsg == nullptr) &&
        (candidateCombinedSignatureMsg == nullptr) && (expectedSeqNumber == 0))
      return;  // already empty

    resetContent();
  }

  void getAndReset(FULL*& outcombinedValidSignatureMsg) {
    outcombinedValidSignatureMsg = combinedValidSignatureMsg;

    if ((replicasInfo.size() == 0) && (combinedValidSignatureMsg == nullptr) &&
        (candidateCombinedSignatureMsg == nullptr) && (expectedSeqNumber == 0))
      return;  // already empty

    combinedValidSignatureMsg = nullptr;

    resetContent();
  }

  bool hasPartialMsgFromReplica(ReplicaId repId) const { return (replicasInfo.count(repId) > 0); }

  PART* getPartialMsgFromReplica(ReplicaId repId) const {
    if (replicasInfo.count(repId) == 0) return nullptr;

    const RepInfo& r = replicasInfo.at(repId);
    return r.partialSigMsg;
  }

  FULL* getMsgWithValidCombinedSignature() const { return combinedValidSignatureMsg; }

  bool isComplete() const { return (combinedValidSignatureMsg != nullptr); }

  void onCompletionOfSignaturesProcessing(SeqNum seqNumber,
                                          ViewNum view,
                                          const std::set<ReplicaId>& replicasWithBadSigs)  // if we found bad signatures
  {
    ConcordAssert(expectedSeqNumber == seqNumber);
    ConcordAssert(expectedView == view);
    ConcordAssert(processingSignaturesInTheBackground);
    ConcordAssert(combinedValidSignatureMsg == nullptr);

    processingSignaturesInTheBackground = false;

    for (const ReplicaId repId : replicasWithBadSigs) {
      LOG_TRACE(THRESHSIGN_LOG, "replica with bad signature: " << repId);
      RepInfo& repInfo = replicasInfo[repId];
      repInfo.state = SigState::Invalid;
      numberOfUnknownSignatures--;
    }
    LOG_TRACE(THRESHSIGN_LOG, KVLOG(seqNumber, view, replicasWithBadSigs.size()));
    trySendToBkThread();
  }

  void onCompletionOfSignaturesProcessing(
      SeqNum seqNumber,
      ViewNum view,
      const char* combinedSig,
      uint16_t combinedSigLen,
      const concordUtils::SpanContext& span_context)  // if we compute a valid combined signature
  {
    ConcordAssert(expectedSeqNumber == seqNumber);
    ConcordAssert(expectedView == view);
    ConcordAssert(processingSignaturesInTheBackground);
    ConcordAssert(combinedValidSignatureMsg == nullptr);

    processingSignaturesInTheBackground = false;

    if (candidateCombinedSignatureMsg != nullptr) {
      delete candidateCombinedSignatureMsg;
      candidateCombinedSignatureMsg = nullptr;
    }
    LOG_TRACE(THRESHSIGN_LOG, KVLOG(seqNumber, view));
    combinedValidSignatureMsg =
        ExternalFunc::createCombinedSignatureMsg(context, seqNumber, view, combinedSig, combinedSigLen, span_context);
  }

  void onCompletionOfCombinedSigVerification(SeqNum seqNumber, ViewNum view, bool isValid) {
    ConcordAssert(expectedSeqNumber == seqNumber);
    ConcordAssert(expectedView == view);
    ConcordAssert(processingSignaturesInTheBackground);
    ConcordAssert(combinedValidSignatureMsg == nullptr);
    ConcordAssert(candidateCombinedSignatureMsg != nullptr);

    processingSignaturesInTheBackground = false;
    LOG_TRACE(THRESHSIGN_LOG, KVLOG(seqNumber, view, isValid));
    if (isValid) {
      combinedValidSignatureMsg = candidateCombinedSignatureMsg;
      candidateCombinedSignatureMsg = nullptr;
    } else {
      delete candidateCombinedSignatureMsg;
      candidateCombinedSignatureMsg = nullptr;

      trySendToBkThread();
    }
  }

  // init the expected values (seqNum, view and digest) directly (without sending to a background thread)
  void initExpected(SeqNum seqNumber, ViewNum view, Digest& digest) {
    ConcordAssert(seqNumber != 0);
    ConcordAssert(expectedSeqNumber == 0);
    ConcordAssert(!processingSignaturesInTheBackground);

    expectedSeqNumber = seqNumber;
    expectedView = view;
    expectedDigest = digest;
  }

  // init the PART message directly (without sending to a background thread)
  bool initMsgWithPartialSignature(PART* partialSigMsg, ReplicaId repId) {
    ConcordAssert(partialSigMsg != nullptr);

    ConcordAssert(!processingSignaturesInTheBackground);
    ConcordAssert(expectedSeqNumber != 0);
    ConcordAssert(combinedValidSignatureMsg == nullptr);
    ConcordAssert(candidateCombinedSignatureMsg == nullptr);
    ConcordAssert(replicasInfo.count(repId) == 0);
    ConcordAssert(numberOfUnknownSignatures == 0);  // we can use this method to add at most one PART message

    // add partialSigMsg to replicasInfo
    RepInfo info = {partialSigMsg, SigState::Unknown};
    replicasInfo[repId] = info;

    // TODO(GG): do we want to verify the partial signature here?

    numberOfUnknownSignatures++;

    if (numOfRequiredSigs == 0)  // init numOfRequiredSigs
      numOfRequiredSigs = ExternalFunc::numberOfRequiredSignatures(context);

    ConcordAssert(numberOfUnknownSignatures < numOfRequiredSigs);  // because numOfRequiredSigs > 1

    return true;
  }

  // init the FULL message directly (without sending to a background thread)
  bool initMsgWithCombinedSignature(FULL* combinedSigMsg) {
    ConcordAssert(!processingSignaturesInTheBackground);
    ConcordAssert(expectedSeqNumber != 0);
    ConcordAssert(combinedValidSignatureMsg == nullptr);
    ConcordAssert(candidateCombinedSignatureMsg == nullptr);

    auto verifier = ExternalFunc::thresholdVerifier(expectedSeqNumber);
    bool succ = verifier->verify(
        (char*)&expectedDigest, sizeof(Digest), combinedSigMsg->signatureBody(), combinedSigMsg->signatureLen());
    if (!succ) throw std::runtime_error("failed to verify");

    combinedValidSignatureMsg = combinedSigMsg;

    return true;
  }

 protected:
  void trySendToBkThread() {
    ConcordAssert(combinedValidSignatureMsg == nullptr);

    if (numOfRequiredSigs == 0)  // init numOfRequiredSigs
      numOfRequiredSigs = ExternalFunc::numberOfRequiredSignatures(context);

    if (processingSignaturesInTheBackground || expectedSeqNumber == 0) return;

    LOG_TRACE(THRESHSIGN_LOG, KVLOG(expectedSeqNumber, expectedView, numOfRequiredSigs));
    if (candidateCombinedSignatureMsg != nullptr) {
      processingSignaturesInTheBackground = true;

      CombinedSigVerificationJob* bkJob =
          new CombinedSigVerificationJob(ExternalFunc::thresholdVerifier(expectedSeqNumber),
                                         &ExternalFunc::incomingMsgsStorage(context),
                                         expectedSeqNumber,
                                         expectedView,
                                         expectedDigest,
                                         candidateCombinedSignatureMsg->signatureBody(),
                                         candidateCombinedSignatureMsg->signatureLen(),
                                         context);

      ExternalFunc::threadPool(context).add(bkJob);
    } else if (numberOfUnknownSignatures >= numOfRequiredSigs) {
      processingSignaturesInTheBackground = true;

      SignaturesProcessingJob* bkJob = new SignaturesProcessingJob(ExternalFunc::thresholdVerifier(expectedSeqNumber),
                                                                   &ExternalFunc::incomingMsgsStorage(context),
                                                                   expectedSeqNumber,
                                                                   expectedView,
                                                                   expectedDigest,
                                                                   numOfRequiredSigs,
                                                                   context);

      uint16_t numOfPartSigsInJob = 0;
      for (std::pair<uint16_t, RepInfo>&& info : replicasInfo) {
        if (info.second.state != SigState::Invalid) {
          auto msg = info.second.partialSigMsg;
          auto sig = msg->signatureBody();
          auto len = msg->signatureLen();
          const auto& span_context = msg->template spanContext<PART>();
          bkJob->add(info.first, sig, len, span_context);
          numOfPartSigsInJob++;
        }

        if (numOfPartSigsInJob == numOfRequiredSigs) break;
      }

      ConcordAssert(numOfPartSigsInJob == numOfRequiredSigs);

      ExternalFunc::threadPool(context).add(bkJob);
    }
  }

  class SignaturesProcessingJob : public concord::util::SimpleThreadPool::Job {
   private:
    struct SigData {
      ReplicaId srcRepId;
      char* sigBody;
      uint16_t sigLength;
      concordUtils::SpanContext span_context;
    };

    std::shared_ptr<IThresholdVerifier> verifier;
    IncomingMsgsStorage* const repMsgsStorage;
    const SeqNum expectedSeqNumber;
    const ViewNum expectedView;
    const Digest expectedDigest;
    const uint16_t reqDataItems;
    SigData* const sigDataItems;

    uint16_t numOfDataItems;

    void* context;

    virtual ~SignaturesProcessingJob() {}

   public:
    SignaturesProcessingJob(std::shared_ptr<IThresholdVerifier> thresholdVerifier,
                            IncomingMsgsStorage* const replicaMsgsStorage,
                            SeqNum seqNum,
                            ViewNum view,
                            Digest& digest,
                            uint16_t numOfRequired,
                            void* cnt)
        : verifier{thresholdVerifier},
          repMsgsStorage{replicaMsgsStorage},
          expectedSeqNumber{seqNum},
          expectedView{view},
          expectedDigest{digest},
          reqDataItems{numOfRequired},
          sigDataItems{new SigData[numOfRequired]},
          numOfDataItems(0) {
      this->context = cnt;
      LOG_TRACE(THRESHSIGN_LOG, KVLOG(expectedSeqNumber, expectedView, reqDataItems));
    }

    void add(ReplicaId srcRepId,
             const char* sigBody,
             uint16_t sigLength,
             const concordUtils::SpanContext& span_context) {
      ConcordAssert(numOfDataItems < reqDataItems);

      SigData d;
      d.srcRepId = srcRepId;
      d.sigLength = sigLength;
      d.sigBody = (char*)std::malloc(sigLength);
      d.span_context = span_context;
      memcpy(d.sigBody, sigBody, sigLength);

      sigDataItems[numOfDataItems] = d;
      numOfDataItems++;
      LOG_TRACE(THRESHSIGN_LOG, KVLOG(srcRepId, numOfDataItems));
    }

    void release() override {
      for (uint16_t i = 0; i < numOfDataItems; i++) {
        SigData& d = sigDataItems[i];
        std::free(d.sigBody);
      }

      delete[] sigDataItems;

      delete this;
    }

    void execute() override {
      ConcordAssert(numOfDataItems == reqDataItems);
      MDC_PUT(MDC_REPLICA_ID_KEY, std::to_string(((InternalReplicaApi*)this->context)->getReplicaConfig().replicaId));
      SCOPED_MDC_SEQ_NUM(std::to_string(expectedSeqNumber));
      MDC_PUT(MDC_THREAD_KEY, demangler::demangle<FULL>());
      // TODO(GG): can utilize several threads (discuss with Alin)

      const uint16_t bufferSize = (uint16_t)verifier->requiredLengthForSignedData();
      size_t fullSignedDataLength = bufferSize;
      std::vector<char> bufferForSigComputations(bufferSize);

      const auto& span_context_of_last_message =
          (reqDataItems - 1) ? sigDataItems[reqDataItems - 1].span_context : concordUtils::SpanContext{};
      {
        // optimistically, don't use share verification
        std::unique_ptr<IThresholdAccumulator> acc{verifier->newAccumulator(false)};
        acc->setExpectedDigest(reinterpret_cast<unsigned char*>(expectedDigest.content()), DIGEST_SIZE);
        for (uint16_t i = 0; i < reqDataItems; i++) acc->add(sigDataItems[i].sigBody, sigDataItems[i].sigLength);
        fullSignedDataLength = acc->getFullSignedData(bufferForSigComputations.data(), bufferSize);
      }

      if (!verifier->verify(
              (char*)&expectedDigest, sizeof(Digest), bufferForSigComputations.data(), fullSignedDataLength)) {
        // if verification failed, use accumulator with share verification enabled.
        // this still can succeed if there're enough valid shares.
        // at least replica with bad   signatures will be identified.
        std::unique_ptr<IThresholdAccumulator> acc{verifier->newAccumulator(true)};
        acc->setExpectedDigest(reinterpret_cast<unsigned char*>(expectedDigest.content()), DIGEST_SIZE);
        for (uint16_t i = 0; i < reqDataItems; i++) acc->add(sigDataItems[i].sigBody, sigDataItems[i].sigLength);
        fullSignedDataLength = acc->getFullSignedData(bufferForSigComputations.data(), bufferSize);
        if (!verifier->verify(
                (char*)&expectedDigest, sizeof(Digest), bufferForSigComputations.data(), fullSignedDataLength)) {
          // if verification failed again
          // signer index starts with 1, therefore shareId-1
          std::set<uint16_t> replicasWithBadSigs;
          for (const auto& shareId : acc->getInvalidShareIds()) replicasWithBadSigs.insert((uint16_t)shareId - 1);
          // send failed message
          auto iMsg(ExternalFunc::createInterCombinedSigFailed(expectedSeqNumber, expectedView, replicasWithBadSigs));
          repMsgsStorage->pushInternalMsg(std::move(iMsg));
          return;
        }
      }
      // send success message
      auto iMsg(ExternalFunc::createInterCombinedSigSucceeded(
          expectedSeqNumber, expectedView, bufferForSigComputations.data(), bufferSize, span_context_of_last_message));
      repMsgsStorage->pushInternalMsg(std::move(iMsg));
    }
  };

  class CombinedSigVerificationJob : public concord::util::SimpleThreadPool::Job {
   private:
    std::shared_ptr<IThresholdVerifier> verifier;
    IncomingMsgsStorage* const repMsgsStorage;
    const SeqNum expectedSeqNumber;
    const ViewNum expectedView;
    const Digest expectedDigest;
    char* const combinedSig;
    uint16_t combinedSigLen;
    void* context;

    virtual ~CombinedSigVerificationJob() {}

   public:
    CombinedSigVerificationJob(std::shared_ptr<IThresholdVerifier> thresholdVerifier,
                               IncomingMsgsStorage* const replicaMsgsStorage,
                               SeqNum seqNum,
                               ViewNum view,
                               Digest& digest,
                               const char* const combinedSigBody,
                               uint16_t combinedSigLength,
                               void* cnt)
        : verifier{thresholdVerifier},
          repMsgsStorage{replicaMsgsStorage},
          expectedSeqNumber{seqNum},
          expectedView{view},
          expectedDigest{digest},
          combinedSig{(char*)std::malloc(combinedSigLength)},
          combinedSigLen{combinedSigLength} {
      memcpy(combinedSig, combinedSigBody, combinedSigLen);
      this->context = cnt;
      LOG_TRACE(THRESHSIGN_LOG, KVLOG(expectedSeqNumber, expectedView, combinedSigLen));
    }

    void release() override {
      std::free(combinedSig);

      delete this;
    }

    void execute() override {
      MDC_PUT(MDC_REPLICA_ID_KEY, std::to_string(((InternalReplicaApi*)this->context)->getReplicaConfig().replicaId));
      SCOPED_MDC_SEQ_NUM(std::to_string(expectedSeqNumber));
      MDC_PUT(MDC_THREAD_KEY, demangler::demangle<FULL>());
      bool succ = verifier->verify((char*)&expectedDigest, sizeof(Digest), combinedSig, combinedSigLen);
      auto iMsg(ExternalFunc::createInterVerifyCombinedSigResult(expectedSeqNumber, expectedView, succ));
      repMsgsStorage->pushInternalMsg(std::move(iMsg));
    }
  };

 protected:
  void* context;

  uint16_t numOfRequiredSigs = 0;

  void resetContent() {
    processingSignaturesInTheBackground = false;

    numberOfUnknownSignatures = 0;

    for (auto&& m : replicasInfo) {
      RepInfo& repInfo = m.second;
      delete repInfo.partialSigMsg;
    }
    replicasInfo.clear();

    if (combinedValidSignatureMsg != nullptr) {
      delete combinedValidSignatureMsg;
      combinedValidSignatureMsg = nullptr;
    }

    if (candidateCombinedSignatureMsg != nullptr) {
      delete candidateCombinedSignatureMsg;
      candidateCombinedSignatureMsg = nullptr;
    }

    if (expectedSeqNumber != 0)  // if we have expected values
    {
      expectedSeqNumber = 0;
      expectedView = 0;
      expectedDigest.makeZero();
    }
  }

  enum class SigState { Unknown = 0, Invalid };

  struct RepInfo {
    PART* partialSigMsg;
    SigState state;
  };

  bool processingSignaturesInTheBackground = false;

  uint16_t numberOfUnknownSignatures = 0;
  std::unordered_map<ReplicaId, RepInfo> replicasInfo;  // map from replica Id to RepInfo

  FULL* combinedValidSignatureMsg = nullptr;
  FULL* candidateCombinedSignatureMsg = nullptr;  // holds msg when expectedSeqNumber is not known yet

  SeqNum expectedSeqNumber = 0;
  ViewNum expectedView = 0;
  Digest expectedDigest;
};

}  // namespace impl
}  // namespace bftEngine
