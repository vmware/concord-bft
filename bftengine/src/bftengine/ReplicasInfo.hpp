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
#include "PrimitiveTypes.hpp"

#include <set>

namespace bftEngine {

class ReplicaConfig;

namespace impl {

class ReplicasInfo {
 public:
  ReplicasInfo(const ReplicaConfig&, bool dynamicCollectorForPartialProofs, bool dynamicCollectorForExecutionProofs);
  ReplicasInfo() {}
  ReplicaId myId() const { return _myId; }

  int16_t numberOfReplicas() const { return _numberOfReplicas; }
  int16_t fVal() const { return _fVal; }
  int16_t cVal() const { return _cVal; }

  bool isIdOfReplica(NodeIdType id) const { return (id < _numberOfReplicas); }
  bool isIdOfPeerReplica(NodeIdType id) const { return (id < _numberOfReplicas) && (id != _myId); }
  bool isIdOfPeerRoReplica(NodeIdType id) const { return _idsOfPeerROReplicas.find(id) != _idsOfPeerROReplicas.end(); }
  bool isIdOfClientProxy(PrincipalId id) const { return _idsOfClientProxies.find(id) != _idsOfClientProxies.end(); }
  bool isIdOfExternalClient(PrincipalId id) const {
    return _idsOfExternalClients.find(id) != _idsOfExternalClients.end();
  }
  bool isIdOfInternalClient(PrincipalId id) const {
    return _idsOfInternalClients.find(id) != _idsOfInternalClients.end();
  }

  bool isValidPrincipalId(PrincipalId id) const { return id <= _maxValidPrincipalId; }
  const std::set<ReplicaId>& idsOfPeerReplicas() const { return _idsOfPeerReplicas; }
  const std::set<ReplicaId>& idsOfPeerROReplicas() const { return _idsOfPeerROReplicas; }
  const std::set<PrincipalId>& idsOfClientProxies() const { return _idsOfClientProxies; }
  const std::set<PrincipalId>& idsOfExternalClients() const { return _idsOfExternalClients; }
  const std::set<PrincipalId>& idsOfInternalClients() const { return _idsOfInternalClients; }

  ReplicaId primaryOfView(ViewNum view) const { return (view % _numberOfReplicas); }

  // TODO(GG): Improve the following methods (Don't use simple arrays. Use iterators or something similar)

  bool getCollectorsForPartialProofs(const ReplicaId refReplica,
                                     const ViewNum v,
                                     const SeqNum n,
                                     int8_t* outNumOfCollectors,
                                     ReplicaId* outCollectorsArray) const;

  bool getCollectorsForPartialProofs(const ViewNum v,
                                     const SeqNum n,
                                     int8_t* outNumOfCollectors,
                                     ReplicaId* outCollectorsArray) const;

  bool isCollectorForPartialProofs(
      const ViewNum v, const SeqNum n) const  // true IFF the current replica is a Collector for sequence number n
  {
    return getCollectorsForPartialProofs(v, n, nullptr, nullptr);
  }

  bool getExecutionCollectors(const ViewNum v,
                              const SeqNum n,
                              int8_t* outNumOfCollectors,
                              ReplicaId* outCollectorsArray) const;

  bool isExecutionCollectorForPartialProofs(const ViewNum v, const SeqNum n) const {
    return getExecutionCollectors(v, n, nullptr, nullptr);
  }

  uint16_t getNumberOfReplicas() { return _numberOfReplicas; }
  uint16_t getNumberOfRoReplicas() { return _numberOfRoReplicas; }
  uint16_t getNumOfClientProxies() { return _numOfClientProxies; }
  uint16_t getNumberOfExternalClients() { return _numberOfExternalClients; }
  uint16_t getNumberOfInternalClients() { return _numberOfInternalClients; }

 protected:
  const ReplicaId _myId = 0;
  const uint16_t _numberOfReplicas = 0;
  const uint16_t _numberOfRoReplicas = 0;
  const uint16_t _numOfClientProxies = 0;
  const uint16_t _numberOfExternalClients = 0;
  const uint16_t _numberOfInternalClients = 0;
  const uint16_t _maxValidPrincipalId = 0;
  const uint16_t _fVal = 0;
  const uint16_t _cVal = 0;

  const bool _dynamicCollectorForPartialProofs = false;
  const bool _dynamicCollectorForExecutionProofs = false;

  const std::set<ReplicaId> _idsOfPeerReplicas;
  const std::set<ReplicaId> _idsOfPeerROReplicas;
  const std::set<PrincipalId> _idsOfClientProxies;
  const std::set<PrincipalId> _idsOfExternalClients;
  const std::set<PrincipalId> _idsOfInternalClients;
};
}  // namespace impl
}  // namespace bftEngine
