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

#include "ReplicasInfo.hpp"
#include "ReplicaConfig.hpp"
#include "assertUtils.hpp"

namespace bftEngine {
namespace impl {

// We assume that the range of ids is as follows:
//
// Consensus replicas address range:
//  [0, numReplicas-1] inclusive
//
// RO replicas address range:
//  [numReplicas, numReplicas+numRoReplicas-1] inclusive
//
// Proxy clients address range:
//  [numReplicas+numRoReplicas, numReplicas+numRoReplicas+numOfClientProxies-1] inclusive
//
// External clients address range:
//  [numReplicas+numRoReplicas+numOfClientProxies,
//  numReplicas+numRoReplicas+numOfClientProxies+numOfExternalClients-1] inclusive
//
// Client services address range:
//  [numReplicas+numRoReplicas+numOfClientProxies+numOfExternalClients,
//  numReplicas+numRoReplicas+numOfClientProxies+numOfExternalClients+numOfClientServices-1] inclusive
//
//  Operator-id:
//  numReplicas+numRoReplicas+numOfClientProxies+numOfExternalClients+numOfClientServices
//
// Internal clients address range:
//  [numReplicas+numRoReplicas+numOfClientProxies+numOfExternalClients+numOfClientServices+1,
//  numReplicas+numRoReplicas+numOfClientProxies+numOfExternalClients+numOfClientServices+numOfInternalClients]
//  inclusive
//
// Example:
//  numReplicas=7, numRoReplicas=1, numOfClientProxies=7, numOfExternalClients=100, numOfClientServices=2,
//  operator=1, numOfInternalClients=7,
//  address range in this order: [0,6], [7,7], [8,14], [15,114], [115,116], [117], [118-124] - total 125 participants
//

ReplicasInfo::ReplicasInfo(const ReplicaConfig& config,
                           bool dynamicCollectorForPartialProofs,
                           bool dynamicCollectorForExecutionProofs)
    : _myId{config.replicaId},
      _numberOfReplicas{config.numReplicas},
      _numberOfRoReplicas{config.numRoReplicas},
      _numOfClientProxies{config.numOfClientProxies},
      _numberOfExternalClients{config.numOfExternalClients},
      _numberOfClientServices{config.numOfClientServices},
      _numberOfInternalClients{config.numReplicas},
      _maxValidPrincipalId{static_cast<uint16_t>(config.numReplicas + config.numRoReplicas + config.numOfClientProxies +
                                                 config.numOfExternalClients + _numberOfInternalClients +
                                                 _numberOfClientServices - 1)},
      _fVal{config.fVal},
      _cVal{config.cVal},
      _dynamicCollectorForPartialProofs{dynamicCollectorForPartialProofs},
      _dynamicCollectorForExecutionProofs{dynamicCollectorForExecutionProofs},

      /*
       * Ids order is: [replicas, ro-replicas, clientProxies, client-service clients (aka external clients) including
       * cre, client-service ID, operator, internal-clients]. Notice, that in case we have an operator, it is included
       * in numOfExternalClients, so no need in any special handling.
       */
      _idsOfPeerReplicas{[&config]() {
        std::set<ReplicaId> ret;
        for (auto i = 0; i < config.numReplicas; ++i)
          if (i != config.replicaId) {
            ret.insert(i);
          }
        LOG_INFO(GL, "Principal ids in _idsOfPeerReplicas: 0 to " << config.numReplicas - 1);
        return ret;
      }()},

      _idsOfPeerROReplicas{[&config]() {
        std::set<ReplicaId> ret;
        uint16_t start = config.numReplicas;
        uint16_t end = start + config.numRoReplicas;
        for (uint16_t i{start}; i < end; ++i)
          if (i != config.replicaId) {
            ret.insert(i);
          }
        if (start != end) LOG_INFO(GL, "Principal ids in _idsOfPeerROReplicas: " << start << " to " << end - 1);
        return ret;
      }()},

      _idsOfClientProxies{[&config]() {
        std::set<ReplicaId> ret;
        auto start = config.numReplicas + config.numRoReplicas;
        auto end = start + config.numOfClientProxies;
        for (auto i = start; i < end; ++i) {
          ret.insert(i);
        }
        if (start != end) LOG_INFO(GL, "Principal ids in _idsOfClientProxies: " << start << " to " << end - 1);
        return ret;
      }()},

      _idsOfExternalClients{[&config]() {
        std::set<ReplicaId> ret;
        auto start = config.numReplicas + config.numRoReplicas + config.numOfClientProxies;
        auto end = start + config.numOfExternalClients;
        for (auto i = start; i < (end - ((uint16_t)config.operatorEnabled_)); ++i) {
          ret.insert(i);
        }
        ret.insert(end + config.numOfClientServices - 1);
        if (start != end)
          LOG_INFO(GL,
                   "Principal ids in _idsOfExternalClients: " << start << " to "
                                                              << end - 1 - ((uint16_t)config.operatorEnabled_));
        return ret;
      }()},

      _idsOfClientServices{[&config]() {
        std::set<ReplicaId> ret;
        auto start = config.numReplicas + config.numRoReplicas + config.numOfClientProxies +
                     config.numOfExternalClients - ((uint16_t)config.operatorEnabled_);
        auto end = start + config.numOfClientServices;
        for (auto i = start; i < end; ++i) {
          ret.insert(i);
        }
        if (start != end) LOG_INFO(GL, "Principal ids in _idsOfClientServices: " << start << " to " << end - 1);
        if (config.operatorEnabled_) LOG_INFO(GL, "Operator id: " << end);
        return ret;
      }()},

      _idsOfInternalClients{[&config]() {
        std::set<ReplicaId> ret;
        auto start = config.numReplicas + config.numRoReplicas + config.numOfClientProxies +
                     config.numOfExternalClients + config.numOfClientServices;
        auto end = start + config.numReplicas;
        for (auto i = start; i < end; ++i) {
          ret.insert(i);
        }
        if (start != end) LOG_INFO(GL, "Principal ids in _idsOfInternalClients: " << start << " to " << end - 1);
        return ret;
      }()},
      _idsOfOperators{[&config]() {
        std::set<ReplicaId> ret;
        if (config.operatorEnabled_)
          ret.insert(config.numReplicas + config.numRoReplicas + config.numOfClientProxies +
                     config.numOfExternalClients - 1);
        return ret;
      }()} {
  ConcordAssert(_numberOfReplicas == (3 * _fVal + 2 * _cVal + 1));
}

bool ReplicasInfo::getCollectorsForPartialProofs(const ReplicaId refReplica,
                                                 const ViewNum v,
                                                 const SeqNum seq,
                                                 int8_t* outNumOfCollectors,
                                                 ReplicaId* outCollectorsArray) const {
  // TODO(GG): should be based on an external function (should be part of the configuration)

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

bool ReplicasInfo::getCollectorsForPartialProofs(const ViewNum v,
                                                 const SeqNum seq,
                                                 int8_t* outNumOfCollectors,
                                                 ReplicaId* outCollectorsArray) const {
  return getCollectorsForPartialProofs(_myId, v, seq, outNumOfCollectors, outCollectorsArray);
}

bool ReplicasInfo::getExecutionCollectors(const ViewNum v,
                                          const SeqNum seq,
                                          int8_t* outNumOfCollectors,
                                          ReplicaId* outCollectorsArray) const {
  // TODO(GG): should be based on an external function (should be part of the configuration)

  if (!_dynamicCollectorForExecutionProofs) {
    int16_t collector = primaryOfView(v);

    if (outNumOfCollectors) {
      *outNumOfCollectors = 1;
      outCollectorsArray[0] = collector;
    }
    return (collector == _myId);
  } else {
    // TODO(GG): update ....
    return getCollectorsForPartialProofs(v, seq, outNumOfCollectors, outCollectorsArray);
  }
}

}  // namespace impl
}  // namespace bftEngine
