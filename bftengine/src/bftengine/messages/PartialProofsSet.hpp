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

// TODO(GG): this class/file should be replaced by an instance of CollectorOfThresholdSignatures

#include <set>

#include "PrimitiveTypes.hpp"
#include "Digest.hpp"
#include "TimeUtils.hpp"

class IThresholdVerifier;
class IThresholdAccumulator;

namespace bftEngine::impl {

class InternalReplicaApi;

class PartialCommitProofMsg;
class FullCommitProofMsg;

class PartialProofsSet {
 public:
  explicit PartialProofsSet(InternalReplicaApi* rep);
  ~PartialProofsSet();

  void addSelfMsgAndPPDigest(PartialCommitProofMsg* m, Digest& digest);

  void setTimeOfSelfPartialProof(const Time& t);

  bool addMsg(PartialCommitProofMsg* m);

  bool addMsg(FullCommitProofMsg* m);

  PartialCommitProofMsg* getSelfPartialCommitProof();

  bool hasFullProof();

  FullCommitProofMsg* getFullProof();

  Time getTimeOfSelfPartialProof();

  bool hasPartialProofFromReplica(ReplicaId repId) const;

  void resetAndFree();

 protected:
  void addImp(PartialCommitProofMsg* m, CommitPath cPath);

  IThresholdVerifier* thresholdVerifier(CommitPath cPath);
  IThresholdAccumulator* thresholdAccumulator(CommitPath cPath);

  void tryToCreateFullProof();

 private:
  InternalReplicaApi* const replica_;

  const size_t numOfRequiredPartialProofsForFast_;
  const size_t numOfRequiredPartialProofsForOptimisticFast_;

  SeqNum seqNumber_;
  FullCommitProofMsg* fullCommitProof_;
  PartialCommitProofMsg* selfPartialCommitProof_;
  std::set<ReplicaId> participatingReplicasInFast_;            // not including the current replica
  std::set<ReplicaId> participatingReplicasInOptimisticFast_;  // not including the current replica
  Digest expectedDigest_;
  Time timeOfSelfPartialProof_;
  IThresholdAccumulator* thresholdAccumulatorForFast_;
  IThresholdAccumulator* thresholdAccumulatorForOptimisticFast_;
};

}  // namespace bftEngine::impl
