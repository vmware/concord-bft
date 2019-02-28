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

#pragma once

#include <set>
#include <forward_list>

#include "PrimitiveTypes.hpp"
#include "IncomingMsgsStorage.hpp"
#include "SimpleThreadPool.hpp"

class IThresholdVerifier;
class ReplicasInfo;

namespace bftEngine {
namespace impl {
class FullCommitProofMsg;

struct RetSuggestion {
  ReplicaId replicaId;
  uint16_t msgType;
  SeqNum msgSeqNum;
};

class InternalReplicaApi  // TODO(GG): rename + clean + split to several classes
{
 public:
  virtual void onPrepareCombinedSigFailed(
      SeqNum seqNumber,
      ViewNum view,
      const std::set<uint16_t>& replicasWithBadSigs) = 0;
  virtual void onPrepareCombinedSigSucceeded(SeqNum seqNumber,
                                             ViewNum view,
                                             const char* combinedSig,
                                             uint16_t combinedSigLen) = 0;
  virtual void onPrepareVerifyCombinedSigResult(SeqNum seqNumber,
                                                ViewNum view,
                                                bool isValid) = 0;

  virtual void onCommitCombinedSigFailed(
      SeqNum seqNumber,
      ViewNum view,
      const std::set<uint16_t>& replicasWithBadSigs) = 0;
  virtual void onCommitCombinedSigSucceeded(SeqNum seqNumber,
                                            ViewNum view,
                                            const char* combinedSig,
                                            uint16_t combinedSigLen) = 0;
  virtual void onCommitVerifyCombinedSigResult(SeqNum seqNumber,
                                               ViewNum view,
                                               bool isValid) = 0;

  virtual void onInternalMsg(FullCommitProofMsg* m) = 0;
  virtual void onMerkleExecSignature(ViewNum view,
                                     SeqNum seqNum,
                                     uint16_t signatureLength,
                                     const char* signature) = 0;

  virtual void onRetransmissionsProcessingResults(
      SeqNum relatedLastStableSeqNum,
      const ViewNum relatedViewNumber,
      const std::forward_list<RetSuggestion>* const
          suggestedRetransmissions) = 0;  // TODO(GG): use generic iterators

  virtual const ReplicasInfo& getReplicasInfo() = 0;

  virtual IncomingMsgsStorage& getIncomingMsgsStorage() = 0;
  virtual SimpleThreadPool& getInternalThreadPool() = 0;

  virtual IThresholdVerifier* getThresholdVerifierForExecution() = 0;
  virtual IThresholdVerifier* getThresholdVerifierForSlowPathCommit() = 0;
  virtual IThresholdVerifier* getThresholdVerifierForCommit() = 0;
  virtual IThresholdVerifier* getThresholdVerifierForOptimisticCommit() = 0;

  virtual Timer& getViewChangeTimer() = 0;
  virtual Timer& getStateTranTimer() = 0;
  virtual Timer& getRetransmissionsTimer() = 0;
  virtual Timer& getStatusTimer() = 0;
  virtual Timer& getSlowPathTimer() = 0;
  virtual Timer& getInfoRequestTimer() = 0;
  virtual Timer& getDebugStatTimer() = 0;

  virtual void onViewsChangeTimer(Time currTime, Timer& timer) = 0;
  virtual void onStateTranTimer(Time currTime, Timer& timer) = 0;
  virtual void onRetransmissionsTimer(Time cTime, Timer& timer) = 0;
  virtual void onStatusReportTimer(Time cTime, Timer& timer) = 0;
  virtual void onSlowPathTimer(Time cTime, Timer& timer) = 0;
  virtual void onInfoRequestTimer(Time cTime, Timer& timer) = 0;
  virtual void onDebugStatTimer(Time cTime, Timer& timer) = 0;
};

}  // namespace impl
}  // namespace bftEngine
