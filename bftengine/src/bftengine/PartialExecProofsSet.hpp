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

// TODO(GG): this class/file should be replaced by an instance of
// CollectorOfThresholdSignatures (or a similar module)

#include <set>

#include "PrimitiveTypes.hpp"
#include "Digest.hpp"
#include "TimeUtils.hpp"
#include "ReplicasInfo.hpp"
#include "InternalReplicaApi.hpp"

class IThresholdVerifier;
class IThresholdAccumulator;

namespace bftEngine {
namespace impl {

class PartialExecProofMsg;
class FullExecProofMsg;

class PartialExecProofsSet {
 public:
  PartialExecProofsSet(InternalReplicaApi* internalReplicaApi);
  ~PartialExecProofsSet();

  void resetAndFree();

  void addSelf(PartialExecProofMsg* m,
               Digest& merkleRoot,
               std::set<FullExecProofMsg*> fullExecProofs);

  bool addMsg(PartialExecProofMsg* m);

  void setMerkleSignature(const char* sig, uint16_t sigLength);

  std::set<FullExecProofMsg*>& getExecProofs() { return setOfFullExecProofs; }

 protected:
  IThresholdVerifier* thresholdVerifier();

  IThresholdAccumulator* thresholdAccumulator();

  void addImp(PartialExecProofMsg* m);

  void tryToCreateFullProof();

  InternalReplicaApi* const replicaApi;
  const ReplicasInfo& replicasInfo;
  const size_t numOfRquiredPartialProofs;

  SeqNum seqNumber;
  PartialExecProofMsg* myPartialExecProof;
  std::set<ReplicaId>
      participatingReplicas;  // not including the current replica
  Digest expectedDigest;
  IThresholdAccumulator* accumulator;
  std::set<FullExecProofMsg*> setOfFullExecProofs;
};

}  // namespace impl
}  // namespace bftEngine
