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
include "cluster_config.s.dfy"
include "messages.dfy"

module Client {
  import opened Library
  import opened HostIdentifiers
  import opened Messages
  import Network
  import ClusterConfig

  // Define your Client protocol state machine here.
  datatype Constants = Constants(myId:HostId, clusterConfig:ClusterConfig.Constants) {
    // host constants coupled to DistributedSystem Constants:
    // DistributedSystem tells us our id so we can recognize inbound messages.
    predicate WF() {
      && clusterConfig.WF()
      && clusterConfig.N() <= myId < NumHosts()
    }

    predicate Configure(id:HostId, clusterConf:ClusterConfig.Constants) {
      && myId == id
      && clusterConfig == clusterConf
    }
  }

  // Placeholder for possible client state
  datatype Variables = Variables(
    lastRequestTimestamp:nat,
    lastReplyTimestamp:nat
  ) {
    
    predicate WF(c:Constants)
    {
      && c.WF()
      && lastRequestTimestamp >= lastReplyTimestamp
    }
  }

  function PendingRequests(c:Constants, v:Variables) : nat
    requires v.WF(c)
  {
    v.lastRequestTimestamp - v.lastReplyTimestamp
  }

  // Predicate that describes what is needed and how we mutate the state v into v' when SendPrePrepare
  // Action is taken. We use the "binding" variable msgOps through which we send/recv messages.
  predicate SendClientOperation(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>)
  {
    && v.WF(c)
    && msgOps.IsSend()
    && PendingRequests(c,v) == 0
    && var msg := msgOps.send.value;
    && msg.payload.ClientRequest?
    && msg.sender == c.myId
    && msg.payload.clientOp.sender == c.myId
    && msg.payload.clientOp.timestamp == v.lastRequestTimestamp + 1
    && v' == v.(lastRequestTimestamp := v.lastRequestTimestamp + 1)
  }
  
  predicate Init(c:Constants, v:Variables) {
    && v.WF(c)
    && v.lastRequestTimestamp == 0
    && v.lastReplyTimestamp == 0
  }

  // Jay Normal Form - syntactic sugar, useful for selecting the next step
  datatype Step =
    | SendClientOperationStep()

  predicate NextStep(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>, step: Step) {
    match step
       case SendClientOperationStep() => SendClientOperation(c, v, v', msgOps)
  }

  predicate Next(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>) {
    exists step :: NextStep(c, v, v', msgOps, step)
  }
}
