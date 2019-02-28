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

#include "ReplicasInfo.hpp"
#include "assertUtils.hpp"

namespace bftEngine {
namespace impl {

// we assume that the set of replicas is 0,1,2,...,numberOfReplicas (TODO(GG):
// should be changed to support full dynamic reconfiguration)
static std::set<ReplicaId> generateSetOfReplicas_helpFunc(
    const int16_t numberOfReplicas) {
  std::set<ReplicaId> retVal;
  for (int16_t i = 0; i < numberOfReplicas; i++) retVal.insert(i);
  return retVal;
}

static std::set<ReplicaId> generateSetOfPeerReplicas_helpFunc(
    const ReplicaId myId, const int16_t numberOfReplicas) {
  std::set<ReplicaId> retVal;
  for (int16_t i = 0; i < numberOfReplicas; i++)
    if (i != myId) retVal.insert(i);
  return retVal;
}

ReplicasInfo::ReplicasInfo(ReplicaId myId,
                           const SigManager& sigManager,
                           int16_t numberOfReplicas,
                           int16_t fVal,
                           int16_t cVal,
                           bool dynamicCollectorForPartialProofs,
                           bool dynamicCollectorForExecutionProofs)
    : _myId{myId},
      _sigManager{sigManager},
      _numberOfReplicas{numberOfReplicas},
      _fVal{fVal},
      _cVal{cVal},
      _dynamicCollectorForPartialProofs{dynamicCollectorForPartialProofs},
      _dynamicCollectorForExecutionProofs{dynamicCollectorForExecutionProofs},
      _idsOfReplicas{generateSetOfReplicas_helpFunc(numberOfReplicas)},
      _idsOfPeerReplicas{
          generateSetOfPeerReplicas_helpFunc(myId, numberOfReplicas)} {
  Assert(numberOfReplicas == (3 * fVal + 2 * cVal + 1));
}

bool ReplicasInfo::getCollectorsForPartialProofs(
    const ReplicaId refReplica,
    const ViewNum v,
    const SeqNum seq,
    int8_t* outNumOfCollectors,
    ReplicaId* outCollectorsArray) const {
  // TODO(GG): should be based on an external function (should be part of the
  // configuration)

  if (!_dynamicCollectorForPartialProofs) {
    int16_t collector = primaryOfView(v);

    if (outNumOfCollectors) {
      *outNumOfCollectors = 1;
      outCollectorsArray[0] = collector;
    }
    return (collector == refReplica);
  } else {
    const int16_t n = _numberOfReplicas;
    const int16_t c = _cVal;
    const int16_t primary = primaryOfView(v);

    if (c == 0) {
      int16_t collector = (seq % n);

      // ignore primary
      if (collector >= primary) {
        collector = ((collector + 1) % n);
        if (collector == primary) collector = ((collector + 1) % n);
      }

      if (outNumOfCollectors) {
        *outNumOfCollectors = 1;
        outCollectorsArray[0] = collector;
      }

      return (collector == refReplica);
    } else {
      const int16_t half = (n / 2);
      int16_t collector1 = (seq % half);
      int16_t collector2 = collector1 + half;
      //		int16_t collector1 = (seq % n);
      //		int16_t collector2 = collector1 + 1;

      // ignore primary
      if (collector1 >= primary) {
        collector1 = ((collector1 + 1) % n);
        if (collector1 == primary) collector1 = ((collector1 + 1) % n);
      }

      if (collector2 >= primary) {
        collector2 = ((collector2 + 1) % n);
        if (collector2 == primary) collector2 = ((collector2 + 1) % n);
      }

      if (outNumOfCollectors) {
        *outNumOfCollectors = 2;
        outCollectorsArray[0] = collector1;
        outCollectorsArray[1] = collector2;
      }

      return ((collector1 == refReplica) || (collector2 == refReplica));
    }
  }
}

bool ReplicasInfo::getCollectorsForPartialProofs(
    const ViewNum v,
    const SeqNum seq,
    int8_t* outNumOfCollectors,
    ReplicaId* outCollectorsArray) const {
  return getCollectorsForPartialProofs(
      _myId, v, seq, outNumOfCollectors, outCollectorsArray);
}

bool ReplicasInfo::getExecutionCollectors(const ViewNum v,
                                          const SeqNum seq,
                                          int8_t* outNumOfCollectors,
                                          ReplicaId* outCollectorsArray) const {
  // TODO(GG): should be based on an external function (should be part of the
  // configuration)

  if (!_dynamicCollectorForExecutionProofs) {
    int16_t collector = primaryOfView(v);

    if (outNumOfCollectors) {
      *outNumOfCollectors = 1;
      outCollectorsArray[0] = collector;
    }
    return (collector == _myId);
  } else {
    // TODO(GG): update ....
    return getCollectorsForPartialProofs(
        v, seq, outNumOfCollectors, outCollectorsArray);
  }
}

}  // namespace impl
}  // namespace bftEngine
