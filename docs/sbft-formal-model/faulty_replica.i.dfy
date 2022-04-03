//#title Host protocol
//#desc Define the host state machine here: message type, state machine for executing one
//#desc host's part of the protocol.

// See exercise01.dfy for an English design of the protocol.

include "network.s.dfy"
include "cluster_config.s.dfy"
include "messages.dfy"

module FaultyReplica {
  import opened Library
  import opened HostIdentifiers
  import opened Messages
  import Network
  import ClusterConfig

  datatype Constants = Constants(myId:HostId, clusterConfig:ClusterConfig.Constants) {
    predicate Configure(id:HostId, clusterConf:ClusterConfig.Constants) {
      && myId == id
      && clusterConfig == clusterConf
    }
  }

  datatype Variables = Variables()


  // JayNF
  datatype Step =
    | ArbitraryBehaviorStep()

  predicate ArbitraryBehavior(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>)
  {
    true
  }

  predicate NextStep(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>, step: Step) {
    match step
       case ArbitraryBehaviorStep() => ArbitraryBehavior(c, v, v', msgOps)
  }

  predicate Next(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>) {
    exists step :: NextStep(c, v, v', msgOps, step)
  }
}
