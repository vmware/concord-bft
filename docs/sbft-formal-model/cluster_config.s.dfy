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

include "network.s.dfy"

module ClusterConfig {
  import opened HostIdentifiers

  datatype Constants = Constants(
    maxByzantineFaultyReplicas:nat,
    numClients:nat,
    workingWindowSize:nat
  ) {

    predicate WF()
    {
      && maxByzantineFaultyReplicas > 0 // Require non-trivial BFT system
      && numClients > 0
    }

    function ClusterSize() : nat
      requires WF()
    {
      N() + numClients
    }

    function F() : nat
      requires WF()
    {
      maxByzantineFaultyReplicas
    }

    function ByzantineSafeQuorum() : nat
      requires WF()
    {
      F() + 1
    }

    function N() : nat  // BFT Replica Size
      requires WF()
    {
      3 * F() + 1
    }

    function AgreementQuorum() : nat
      requires WF()
    {
      2 * F() + 1
    }

    predicate IsHonestReplica(id:HostId)
      requires WF()
    {
      && ValidHostId(id)
      && F() <= id < N()
    }

    predicate IsFaultyReplica(id:HostId)
      requires WF()
    {
      && ValidHostId(id)
      && 0 <= id < F()
    }

    predicate IsReplica(id:HostId)
      requires WF()
    {
      && ValidHostId(id)
      && 0 <= id < N()
    }

    predicate IsClient(id:HostId)
      requires WF()
    {
      && ValidHostId(id)
      && N() <= id < NumHosts()
    }
  }
}
