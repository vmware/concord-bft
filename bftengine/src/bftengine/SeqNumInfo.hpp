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

#include <set>

#include "log/logger.hpp"
#include "OpenTracing.hpp"
#include "PrimitiveTypes.hpp"
#include "SysConsts.hpp"
#include "messages/PrePrepareMsg.hpp"
#include "messages/SignedShareMsgs.hpp"
#include "messages/PartialCommitProofMsg.hpp"
#include "messages/FullCommitProofMsg.hpp"
#include "CollectorOfThresholdSignatures.hpp"
#include "SequenceWithActiveWindow.hpp"

namespace util {
class SimpleThreadPool;
}
namespace bftEngine {
namespace impl {

class SeqNumInfo {
 public:
  SeqNumInfo();
  ~SeqNumInfo();

  void resetCommitSignatures(CommitPath cPath);
  void resetPrepareSignatures();
  void resetAndFree();  // TODO(GG): name
  std::pair<PrePrepareMsg*, PrepareFullMsg*> getAndReset();

  bool addMsg(PrePrepareMsgUPtr m, bool directAdd = false, bool isTimeCorrect = true);
  bool addSelfMsg(PrePrepareMsgUPtr m, bool directAdd = false);

  bool addMsg(PreparePartialMsg* m);
  bool addSelfMsg(PreparePartialMsg* m, bool directAdd = false);

  bool addMsg(PrepareFullMsg* m, bool directAdd = false);

  bool addMsg(CommitPartialMsg* m);
  bool addSelfCommitPartialMsgAndDigest(CommitPartialMsg* m, Digest& commitDigest, bool directAdd = false);

  bool addMsg(CommitFullMsg* m, bool directAdd = false);

  void forceComplete();

  PrePrepareMsg* getPrePrepareMsg() const;
  PrePrepareMsg* getSelfPrePrepareMsg() const;

  PreparePartialMsg* getSelfPreparePartialMsg() const;
  PrepareFullMsg* getValidPrepareFullMsg() const;

  CommitPartialMsg* getSelfCommitPartialMsg() const;
  CommitFullMsg* getValidCommitFullMsg() const;

  bool hasPrePrepareMsg() const;
  bool hasMatchingPrePrepare(SeqNum seqNum) const;

  bool isPrepared() const;
  bool isCommitted__gg() const;  // TODO(GG): beware this name may mislead (not sure...). rename ??

  bool isTimeCorrect() const { return isTimeCorrect_; }
  bool preparedOrHasPreparePartialFromReplica(ReplicaId repId) const;
  bool committedOrHasCommitPartialFromReplica(ReplicaId repId) const;

  Time getTimeOfFirstRelevantInfoFromPrimary() const;
  Time getTimeOfLastInfoRequest() const;
  Time lastUpdateTimeOfCommitMsgs() const { return commitUpdateTime; }  // TODO(GG): check usage....

  // Fast path methods
  bool hasFastPathFullCommitProof() const;
  bool hasFastPathPartialCommitProofFromReplica(ReplicaId repId) const;

  PartialCommitProofMsg* getFastPathSelfPartialCommitProofMsg() const;
  FullCommitProofMsg* getFastPathFullCommitProofMsg() const;

  void setFastPathTimeOfSelfPartialProof(const Time& t);
  Time getFastPathTimeOfSelfPartialProof() const;

  bool addFastPathSelfPartialCommitMsgAndDigest(PartialCommitProofMsg* m, Digest& commitDigest);
  bool addFastPathPartialCommitMsg(PartialCommitProofMsg* m);
  bool addFastPathFullCommitMsg(FullCommitProofMsg* m, bool directAdd = false);

  void startSlowPath();
  bool slowPathStarted();

  void setTimeOfLastInfoRequest(Time t);

  void onCompletionOfPrepareSignaturesProcessing(SeqNum seqNumber,
                                                 ViewNum viewNumber,
                                                 const std::set<ReplicaId>& replicasWithBadSigs);
  void onCompletionOfPrepareSignaturesProcessing(SeqNum seqNumber,
                                                 ViewNum viewNumber,
                                                 const char* combinedSig,
                                                 uint16_t combinedSigLen,
                                                 const concordUtils::SpanContext& span_context);
  void onCompletionOfCombinedPrepareSigVerification(SeqNum seqNumber, ViewNum viewNumber, bool isValid);

  void onCompletionOfCommitSignaturesProcessing(SeqNum seqNumber,
                                                ViewNum viewNumber,
                                                CommitPath cPath,
                                                const std::set<uint16_t>& replicasWithBadSigs);

  void onCompletionOfCommitSignaturesProcessing(SeqNum seqNumber,
                                                ViewNum viewNumber,
                                                CommitPath cPath,
                                                const char* combinedSig,
                                                uint16_t combinedSigLen,
                                                const concordUtils::SpanContext& span_context);

  void onCompletionOfCombinedCommitSigVerification(SeqNum seqNumber,
                                                   ViewNum viewNumber,
                                                   CommitPath cPath,
                                                   bool isValid);

  uint64_t getCommitDurationMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(commitUpdateTime - firstSeenFromPrimary).count();
  }

 protected:
  class ExFuncForPrepareCollector {
   public:
    // external messages
    static PrepareFullMsg* createCombinedSignatureMsg(void* context,
                                                      SeqNum seqNumber,
                                                      ViewNum viewNumber,
                                                      const char* const combinedSig,
                                                      uint16_t combinedSigLen,
                                                      const concordUtils::SpanContext& span_context);

    // internal messages
    static InternalMessage createInterCombinedSigFailed(SeqNum seqNumber,
                                                        ViewNum viewNumber,
                                                        const std::set<uint16_t>& replicasWithBadSigs);
    static InternalMessage createInterCombinedSigSucceeded(SeqNum seqNumber,
                                                           ViewNum viewNumber,
                                                           const char* combinedSig,
                                                           uint16_t combinedSigLen,
                                                           const concordUtils::SpanContext& span_context);
    static InternalMessage createInterVerifyCombinedSigResult(SeqNum seqNumber, ViewNum viewNumber, bool isValid);

    // from the Replica object
    static uint16_t numberOfRequiredSignatures(void* context);
    static std::shared_ptr<IThresholdVerifier> thresholdVerifier(SeqNum seqNumber);
    static concord::util::SimpleThreadPool& threadPool(void* context);
    static IncomingMsgsStorage& incomingMsgsStorage(void* context);
  };

  class ExFuncForCommitCollector {
   public:
    // external messages
    static CommitFullMsg* createCombinedSignatureMsg(void* context,
                                                     SeqNum seqNumber,
                                                     ViewNum viewNumber,
                                                     const char* const combinedSig,
                                                     uint16_t combinedSigLen,
                                                     const concordUtils::SpanContext& span_context);

    // internal messages
    static InternalMessage createInterCombinedSigFailed(SeqNum seqNumber,
                                                        ViewNum viewNumber,
                                                        const std::set<uint16_t>& replicasWithBadSigs);
    static InternalMessage createInterCombinedSigSucceeded(SeqNum seqNumber,
                                                           ViewNum viewNumber,
                                                           const char* combinedSig,
                                                           uint16_t combinedSigLen,
                                                           const concordUtils::SpanContext& span_context);
    static InternalMessage createInterVerifyCombinedSigResult(SeqNum seqNumber, ViewNum viewNumber, bool isValid);

    // from the ReplicaImp object
    static uint16_t numberOfRequiredSignatures(void* context);
    static std::shared_ptr<IThresholdVerifier> thresholdVerifier(SeqNum seqNumber);
    static concord::util::SimpleThreadPool& threadPool(void* context);
    static IncomingMsgsStorage& incomingMsgsStorage(void* context);
  };

  class ExFuncForFastPathOptimisticCollector {
   public:
    // external messages
    static FullCommitProofMsg* createCombinedSignatureMsg(void* context,
                                                          SeqNum seqNumber,
                                                          ViewNum viewNumber,
                                                          const char* const combinedSig,
                                                          uint16_t combinedSigLen,
                                                          const concordUtils::SpanContext& span_context);

    // internal messages
    static InternalMessage createInterCombinedSigFailed(SeqNum seqNumber,
                                                        ViewNum viewNumber,
                                                        const std::set<uint16_t>& replicasWithBadSigs);
    static InternalMessage createInterCombinedSigSucceeded(SeqNum seqNumber,
                                                           ViewNum viewNumber,
                                                           const char* combinedSig,
                                                           uint16_t combinedSigLen,
                                                           const concordUtils::SpanContext& span_context);
    static InternalMessage createInterVerifyCombinedSigResult(SeqNum seqNumber, ViewNum viewNumber, bool isValid);

    // from the ReplicaImp object
    static uint16_t numberOfRequiredSignatures(void* context);
    static std::shared_ptr<IThresholdVerifier> thresholdVerifier(SeqNum seqNumber);
    static concord::util::SimpleThreadPool& threadPool(void* context);
    static IncomingMsgsStorage& incomingMsgsStorage(void* context);
  };

  class ExFuncForFastPathThresholdCollector {
   public:
    // external messages
    static FullCommitProofMsg* createCombinedSignatureMsg(void* context,
                                                          SeqNum seqNumber,
                                                          ViewNum viewNumber,
                                                          const char* const combinedSig,
                                                          uint16_t combinedSigLen,
                                                          const concordUtils::SpanContext& span_context);

    // internal messages
    static InternalMessage createInterCombinedSigFailed(SeqNum seqNumber,
                                                        ViewNum viewNumber,
                                                        const std::set<uint16_t>& replicasWithBadSigs);
    static InternalMessage createInterCombinedSigSucceeded(SeqNum seqNumber,
                                                           ViewNum viewNumber,
                                                           const char* combinedSig,
                                                           uint16_t combinedSigLen,
                                                           const concordUtils::SpanContext& span_context);
    static InternalMessage createInterVerifyCombinedSigResult(SeqNum seqNumber, ViewNum viewNumber, bool isValid);

    // from the ReplicaImp object
    static uint16_t numberOfRequiredSignatures(void* context);
    static std::shared_ptr<IThresholdVerifier> thresholdVerifier(SeqNum seqNumber);
    static concord::util::SimpleThreadPool& threadPool(void* context);
    static IncomingMsgsStorage& incomingMsgsStorage(void* context);
  };

  InternalReplicaApi* replica = nullptr;

  PrePrepareMsgUPtr prePrepareMsg;

  // Slow path
  CollectorOfThresholdSignatures<PreparePartialMsg, PrepareFullMsg, ExFuncForPrepareCollector>* prepareSigCollector;
  CollectorOfThresholdSignatures<CommitPartialMsg, CommitFullMsg, ExFuncForCommitCollector>* commitMsgsCollector;

  // Fast path
  template <typename ExFunc>
  using FastPathCollector = CollectorOfThresholdSignatures<PartialCommitProofMsg, FullCommitProofMsg, ExFunc>;
  using FastPathOptimisticCollector = FastPathCollector<ExFuncForFastPathOptimisticCollector>;
  using FastPathThresholdCollector = FastPathCollector<ExFuncForFastPathThresholdCollector>;

  FastPathOptimisticCollector* fastPathOptimisticCollector;
  FastPathThresholdCollector* fastPathThresholdCollector;
  Time fastPathTimeOfSelfPartialProof;

  bool primary;  // true iff PrePrepareMsg was added with addSelfMsg

  bool forcedCompleted;

  bool slowPathHasStarted;

  bool isTimeCorrect_ = true;

  Time firstSeenFromPrimary;
  Time timeOfLastInfoRequest;
  Time commitUpdateTime;

 public:
  // methods for SequenceWithActiveWindow
  static void init(SeqNumInfo& i, void* d);

  static void free(SeqNumInfo& i) { i.resetAndFree(); }

  static void reset(SeqNumInfo& i) { i.resetAndFree(); }
};

}  // namespace impl
}  // namespace bftEngine
