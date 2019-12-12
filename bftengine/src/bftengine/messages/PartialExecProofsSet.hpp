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

// TODO(GG): this class/file should be replaced by an instance of CollectorOfThresholdSignatures (or a similar module)

#include <set>

#include "PrimitiveTypes.hpp"
#include "Digest.hpp"
#include "TimeUtils.hpp"
#include "ReplicasInfo.hpp"
#include "InternalReplicaApi.hpp"

class IThresholdVerifier;
class IThresholdAccumulator;

namespace bftEngine::impl {

class PartialExecProofMsg;
class FullExecProofMsg;

class PartialExecProofsSet {
 public:
  explicit PartialExecProofsSet(InternalReplicaApi* internalReplicaApi);
  ~PartialExecProofsSet();

  void resetAndFree();

  bool addMsg(PartialExecProofMsg* m);

  void setMerkleSignature(const char* sig, uint16_t sigLength);

  std::set<FullExecProofMsg*>& getExecProofs() { return setOfFullExecProofs_; }

 protected:
  IThresholdVerifier* thresholdVerifier();

  IThresholdAccumulator* thresholdAccumulator();

  void addImp(PartialExecProofMsg* m);

  void tryToCreateFullProof();

  InternalReplicaApi* const replicaApi_;
  const ReplicasInfo& replicasInfo_;
  const size_t numOfRequiredPartialProofs_;
  SeqNum seqNumber_;
  PartialExecProofMsg* myPartialExecProof_;
  std::set<ReplicaId> participatingReplicas_;  // not including the current replica
  Digest expectedDigest_;
  IThresholdAccumulator* accumulator_;
  std::set<FullExecProofMsg*> setOfFullExecProofs_;
};

}  // namespace bftEngine::impl
