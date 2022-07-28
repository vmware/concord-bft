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

module Messages {
  import Library
  import opened HostIdentifiers
  import Network

  type SequenceID = k:nat | 0 < k witness 1
  type ViewNum = nat

  datatype ClientOperation = ClientOperation(sender:HostId, timestamp:nat)

  datatype OperationWrapper = Noop | ClientOp(clientOperation: ClientOperation)

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
    predicate valid(quorumSize:nat) {
      || empty()
      || (&& |votes| == quorumSize
          && WF()
          && (forall v | v in votes :: v.payload == prototype()) // messages have to be votes that match eachother by the prototype 
          && (forall v1, v2 | && v1 in votes
                              && v2 in votes
                              && v1 != v2
                                :: v1.sender != v2.sender) // unique senders
          )
    }
    predicate empty() {
      && |votes| == 0
    }
  }

  datatype ViewChangeMsgsSelectedByPrimary = ViewChangeMsgsSelectedByPrimary(msgs:set<Network.Message<Message>>) {
    predicate valid(view:ViewNum, quorumSize:nat) {
      && |msgs| > 0
      && (forall v | v in msgs :: && v.payload.ViewChangeMsg?
                                  && v.payload.valid(quorumSize)
                                  && v.payload.newView == view) // All the ViewChange messages have to be for the same View. 
      && (forall v1, v2 | && v1 in msgs
                          && v2 in msgs
                          && v1 != v2
                            :: v1.sender != v2.sender) // unique senders
      && |msgs| == quorumSize //TODO: once proof is complete try with >=
    }
  }

  datatype CheckpointsQuorum = CheckpointsQuorum(msgs:set<Network.Message<Message>>) {
    predicate valid(lastStableCheckpoint:SequenceID, quorumSize:nat) {
      && |msgs| > 0
      && (forall m | m in msgs :: && m.payload.CheckpointMsg?
                                  && m.payload.valid(quorumSize)
                                  && m.payload.seqIDReached == lastStableCheckpoint)
      && (forall m1, m2 | && m1 in msgs
                          && m2 in msgs
                          && m1 != m2
                            :: m1.sender != m2.sender) // unique senders
      && |msgs| >= quorumSize
    }
  }

  // Define your Message datatype here.
  datatype Message = | PrePrepare(view:ViewNum, seqID:SequenceID, operationWrapper:OperationWrapper)
                     | Prepare(view:ViewNum, seqID:SequenceID, operationWrapper:OperationWrapper)
                     | Commit(view:ViewNum, seqID:SequenceID, operationWrapper:OperationWrapper)
                     | ClientRequest(clientOp:ClientOperation)
                     | ViewChangeMsg(newView:ViewNum,
                                     lastStableCheckpoint:SequenceID,
                                     proofForLastStable:CheckpointsQuorum,
                                     certificates:map<SequenceID, PreparedCertificate>)
                     | NewViewMsg(newView:ViewNum, vcMsgs:ViewChangeMsgsSelectedByPrimary) 
                     | CheckpointMsg(seqIDReached:SequenceID)
                     {
                       predicate valid(quorumSize:nat) {
                         && (ViewChangeMsg? ==> proofForLastStable.valid(lastStableCheckpoint, quorumSize))
                         && (NewViewMsg? ==> vcMsgs.valid(newView, quorumSize))
                       }
                     }
  predicate CheckMessageValidity(msg:Message, quorumSize:nat) {
    && (msg.ViewChangeMsg? ==> msg.proofForLastStable.valid(msg.lastStableCheckpoint, quorumSize))
    && (msg.NewViewMsg? ==> msg.vcMsgs.valid(msg.newView, quorumSize))
  }
  // ToDo: ClientReply
}
