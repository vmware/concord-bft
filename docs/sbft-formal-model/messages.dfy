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
include "library/Library.dfy"
include "cluster_config.s.dfy"

module Messages {
  import Library
  import opened HostIdentifiers
  import Network
  import opened ClusterConfig

  type SequenceID = nat

  datatype ClientOperation = ClientOperation(sender:HostId, timestamp:nat)

  datatype OperationWrapper = Noop | ClientOp(clientOperation: ClientOperation)

  type CommittedClientOperations = map<SequenceID, OperationWrapper>

  function sendersOf(msgs:set<Network.Message<Message>>) : set<HostIdentifiers.HostId> {
    set msg | msg in msgs :: msg.sender
  }

  datatype PreparedCertificate = PreparedCertificate(votes:set<Network.Message<Message>>) {
    function prototype() : Message 
      requires |votes| > 0
    {
      var prot :| prot in votes;
      prot.payload
    }
    predicate WF() {
      (forall v | v in votes :: v.payload.Prepare?)
    }
    predicate validFull(clusterConfig:ClusterConfig.Constants, seqID:SequenceID)
      requires clusterConfig.WF()
    {
      && |votes| >= clusterConfig.AgreementQuorum()
      && WF()
      && prototype().seqID == seqID
      && (forall v | v in votes :: v.payload == prototype()) // messages have to be votes that match eachother by the prototype 
      && UniqueSenders(votes)
      && (forall v | v in votes :: clusterConfig.IsReplica(v.sender))
    }
    predicate valid(clusterConfig:ClusterConfig.Constants, seqID:SequenceID)
      requires clusterConfig.WF()
    {
      || empty()
      || validFull(clusterConfig, seqID)
    }
    predicate empty() {
      && |votes| == 0
    }
    predicate votesRespectView(view:ViewNum) 
      requires WF()
    {
      !empty() ==> prototype().view < view
    }
  }

  datatype ViewChangeMsgsSelectedByPrimary = ViewChangeMsgsSelectedByPrimary(msgs:set<Network.Message<Message>>) {
    predicate valid(view:ViewNum, clusterConfig:ClusterConfig.Constants) 
      requires clusterConfig.WF()
    {
      && |msgs| > 0
      && (forall v | v in msgs :: && v.payload.ViewChangeMsg?
                                  && v.payload.validViewChangeMsg(clusterConfig)
                                  && v.payload.newView == view) // All the ViewChange messages have to be for the same View. 
      && UniqueSenders(msgs)
      && |msgs| == clusterConfig.AgreementQuorum() //TODO: once proof is complete try with >=
    }
  }

  datatype CheckpointsQuorum = CheckpointsQuorum(msgs:set<Network.Message<Message>>) {
    function prototype() : Message 
      requires |msgs| > 0
    {
      var prot :| prot in msgs;
      prot.payload
    }
    predicate valid(lastStableCheckpoint:SequenceID, quorumSize:nat) {
      || (&& lastStableCheckpoint == 0
          && |msgs| == 0)
      || (&& |msgs| > 0
          && UniqueSenders(msgs)
          && (forall m | m in msgs :: && m.payload.CheckpointMsg?
                                      && m.payload == prototype()
                                      && m.payload.seqIDReached == lastStableCheckpoint)
          && |msgs| >= quorumSize)
    }
  }

  predicate {:opaque} UniqueSenders(msgs:set<Network.Message<Message>>) {
    (forall m1, m2 | && m1 in msgs
                     && m2 in msgs
                     && m1 != m2
                       :: m1.sender != m2.sender)
  }

  // Define your Message datatype here. // TODO: rename to payload.
  datatype Message = | PrePrepare(view:ViewNum, seqID:SequenceID, operationWrapper:OperationWrapper)
                     | Prepare(view:ViewNum, seqID:SequenceID, operationWrapper:OperationWrapper)
                     | Commit(view:ViewNum, seqID:SequenceID, operationWrapper:OperationWrapper)
                     | ClientRequest(clientOp:ClientOperation)
                     | ViewChangeMsg(newView:ViewNum,
                                     lastStableCheckpoint:SequenceID,
                                     proofForLastStable:CheckpointsQuorum,
                                     certificates:map<SequenceID, PreparedCertificate>)
                     | NewViewMsg(newView:ViewNum, vcMsgs:ViewChangeMsgsSelectedByPrimary) 
                     | CheckpointMsg(seqIDReached:SequenceID, committedClientOperations:CommittedClientOperations)
                     {
                       predicate valid(clusterConfig:ClusterConfig.Constants)
                         requires clusterConfig.WF()
                       {
                         && (ViewChangeMsg? ==> validViewChangeMsg(clusterConfig))
                         && (NewViewMsg? ==> validNewViewMsg(clusterConfig))
                       }
                       predicate checked(clusterConfig:ClusterConfig.Constants, sigChecks:set<Network.Message<Message>>)
                         requires clusterConfig.WF()
                       {
                         && (ViewChangeMsg? ==> checkedViewChangeMsg(clusterConfig, sigChecks))
                         && (NewViewMsg? ==> checkedNewViewMsg(clusterConfig, sigChecks))
                       }
                       predicate validViewChangeMsg(clusterConfig:ClusterConfig.Constants)
                         requires clusterConfig.WF()
                         requires ViewChangeMsg?
                       {
                         && (forall seqID | seqID in certificates
                                     :: && certificates[seqID].valid(clusterConfig, seqID)
                                        && certificates[seqID].votesRespectView(newView))
                         && proofForLastStable.valid(lastStableCheckpoint, clusterConfig.AgreementQuorum())
                       }
                       predicate validNewViewMsg(clusterConfig:ClusterConfig.Constants)
                         requires clusterConfig.WF()
                         requires NewViewMsg?
                       {
                         && vcMsgs.valid(newView, clusterConfig)
                         && (forall replica | replica in sendersOf(vcMsgs.msgs)
                                              :: clusterConfig.IsReplica(replica))
                       }
                       predicate checkedViewChangeMsg(clusterConfig:ClusterConfig.Constants, sigChecks:set<Network.Message<Message>>)
                         requires ViewChangeMsg?
                         requires clusterConfig.WF()
                       {
                          && valid(clusterConfig)
                          // Check Checkpoint msg-s signatures:
                          && proofForLastStable.msgs <= sigChecks
                          && (forall seqID | seqID in certificates
                                  :: && certificates[seqID].votes <= sigChecks)
                       }
                       predicate checkedNewViewMsg(clusterConfig:ClusterConfig.Constants, sigChecks:set<Network.Message<Message>>)
                         requires NewViewMsg?
                         requires clusterConfig.WF()
                       {
                          && valid(clusterConfig)
                          && vcMsgs.msgs <= sigChecks
                       }
                       predicate IsIntraViewMsg() {
                          || PrePrepare?
                          || Prepare?
                          || Commit? 
                       }
                     }
  predicate ValidNewViewMsg(clusterConfig:ClusterConfig.Constants, msg:Network.Message<Message>)
  {
    && clusterConfig.WF()
    && msg.payload.NewViewMsg?
    && msg.payload.valid(clusterConfig)
    && clusterConfig.PrimaryForView(msg.payload.newView) == msg.sender
  }
  // ToDo: ClientReply
}
