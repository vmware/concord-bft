#pragma once

#include "types.h"

#define requiresWF(errMsg) if(!WF()) throw std::runtime_error(#errMsg);

namespace ClusterConfig {

struct Constants {
  nat maxByzantineFaultyReplicas;
  nat numClients;

  bool WF()
  {
    return (maxByzantineFaultyReplicas > 0
            && numClients > 0);
  }

  nat ClusterSize() {
    requiresWF(__PRETTY_FUNCTION__)
    return N() + numClients;
  }

  nat F() {
    requiresWF(__PRETTY_FUNCTION__)
    return maxByzantineFaultyReplicas;
  }

  nat ByzantineSafeQuorum() {
    requiresWF(__PRETTY_FUNCTION__)
    return F() + 1;
  }

  nat N() {
    requiresWF(__PRETTY_FUNCTION__)
    return 3 * F() + 1;
  }

  nat AgreementQuorum()
  {
    requiresWF(__PRETTY_FUNCTION__)
    return 2 * F() + 1;
  }

  bool IsReplica(HostId id) {
    requiresWF(__PRETTY_FUNCTION__)
    return 0 <= id && id < N();
  }

  bool IsClient(HostId id) {
    requiresWF(__PRETTY_FUNCTION__)
    return N() <= id
           && id < NumHosts();
  }

  nat NumHosts() {
    requiresWF(__PRETTY_FUNCTION__)
    return N() + numClients;
  }
};

}