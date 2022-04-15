// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

include "replica.i.dfy"
include "client.i.dfy"
include "cluster_config.s.dfy"
include "messages.dfy"
include "faulty_replica.i.dfy"

// Before we get here, caller must define a type Message that we'll
// use to instantiate network.s.dfy.

module DistributedSystem {
  import opened HostIdentifiers
  import opened Messages
  import ClusterConfig
  import Replica
  import FaultyReplica
  import Client
  import Network

  datatype HostConstants = | Replica(replicaConstants:Replica.Constants)
                           | FaultyReplica(faultyReplicaConstants:FaultyReplica.Constants)
                           | Client(clientConstants:Client.Constants)

  datatype HostVariables = | Replica(replicaVariables:Replica.Variables)
                           | FaultyReplica(faultyReplicaVariables:FaultyReplica.Variables)
                           | Client(clientVariables:Client.Variables)

  datatype Constants = Constants(
    hosts:seq<HostConstants>,
    network:Network.Constants,
    clusterConfig:ClusterConfig.Constants) {
    
    predicate WF() {
      && clusterConfig.WF()
      && clusterConfig.ClusterSize() == NumHosts()
      && |hosts| == NumHosts()
      && (forall id | clusterConfig.IsHonestReplica(id) :: && hosts[id] == HostConstants.Replica(Replica.Constants(id, clusterConfig))
                                                           && hosts[id].replicaConstants.Configure(id, clusterConfig))
      && (forall id | clusterConfig.IsFaultyReplica(id) :: && hosts[id] == HostConstants.FaultyReplica(FaultyReplica.Constants(id, clusterConfig))
                                                           && hosts[id].faultyReplicaConstants.Configure(id, clusterConfig))
      && (forall id | clusterConfig.IsClient(id) :: && hosts[id] == HostConstants.Client(Client.Constants(id, clusterConfig))
                                                    && hosts[id].clientConstants.Configure(id, clusterConfig))
    }
  }

  datatype Variables = Variables(
    hosts:seq<HostVariables>,
    network:Network.Variables<Message>) {
    
    predicate WF(c: Constants) {
      && c.WF()
      && |hosts| == |c.hosts|
      && (forall id | c.clusterConfig.IsHonestReplica(id) :: hosts[id].Replica? && hosts[id].replicaVariables.WF(c.hosts[id].replicaConstants))
      && (forall id | c.clusterConfig.IsClient(id) :: hosts[id].Client? && hosts[id].clientVariables.WF(c.hosts[id].clientConstants))
      && (forall id | c.clusterConfig.IsFaultyReplica(id) :: hosts[id].FaultyReplica?)
    }
  }

  predicate IsCommitted(c:Constants, v:Variables, view:nat, seqID:SequenceID) {
    && v.WF(c)
    && var ReplicasCommitted := set replicaID | && c.clusterConfig.F() <= replicaID < c.clusterConfig.N()
                                                && Replica.QuorumOfPrepares(c.hosts[replicaID].replicaConstants,
                                                                            v.hosts[replicaID].replicaVariables,
                                                                            seqID);
    //TODO: Implement check for hasPrePrepare
    && |ReplicasCommitted| >= c.clusterConfig.AgreementQuorum()
  }

  predicate Init(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall id | && c.clusterConfig.IsHonestReplica(id) 
                    :: Replica.Init(c.hosts[id].replicaConstants, v.hosts[id].replicaVariables))
    && (forall id | && c.clusterConfig.IsClient(id)
                    :: Client.Init(c.hosts[id].clientConstants, v.hosts[id].clientVariables))
    && Network.Init(c.network, v.network)
  }

  // Jay Normal Form - Dafny syntactic sugar, useful for selecting the next step
  datatype Step = Step(id:HostId, msgOps: Network.MessageOps<Message>)

  predicate ReplicaStep(c:Constants, v:Variables, v':Variables, step: Step) {
      && v.WF(c)
      && v'.WF(c)
      && c.clusterConfig.IsHonestReplica(step.id)
      && Replica.Next(c.hosts[step.id].replicaConstants, v.hosts[step.id].replicaVariables, v'.hosts[step.id].replicaVariables, step.msgOps)
  }

  predicate FaultyReplicaStep(c:Constants, v:Variables, v':Variables, step: Step) {
      && v.WF(c)
      && v'.WF(c)
      && c.clusterConfig.IsFaultyReplica(step.id)
      && assert v.hosts[step.id].FaultyReplica?; true
      && FaultyReplica.Next(c.hosts[step.id].faultyReplicaConstants, v.hosts[step.id].faultyReplicaVariables, v'.hosts[step.id].faultyReplicaVariables, step.msgOps)
  }

  predicate ClientStep(c:Constants, v:Variables, v':Variables, step: Step) {
      && v.WF(c)
      && v'.WF(c)
      && c.clusterConfig.IsClient(step.id)
      && Client.Next(c.hosts[step.id].clientConstants, v.hosts[step.id].clientVariables, v'.hosts[step.id].clientVariables, step.msgOps)
  }

  predicate NextStep(c:Constants, v:Variables, v':Variables, step: Step) {
    && v.WF(c)
    && v'.WF(c)
    && (ReplicaStep(c, v, v', step) || ClientStep(c, v, v', step) || FaultyReplicaStep(c, v, v', step))
    && (forall other | ValidHostId(other) && other != step.id :: v'.hosts[other] == v.hosts[other])
    && Network.Next(c.network, v.network, v'.network, step.msgOps, step.id)
  }

  predicate Next(c:Constants, v:Variables, v':Variables) {
    exists step :: NextStep(c, v, v', step)
  }
}
