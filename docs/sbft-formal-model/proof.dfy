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

include "library/Library.dfy"
include "distributed_system.s.dfy"

module SafetySpec {
  import opened HostIdentifiers
  import DistributedSystem

  // No two hosts think they hold the lock simulatneously.
  predicate Safety(c:DistributedSystem.Constants, v:DistributedSystem.Variables) {
    false // Replace this placeholder with an appropriate safety condition: no two clients hold
  }
}

module Proof {
  import opened HostIdentifiers
  import Replica
  import opened DistributedSystem
  import opened SafetySpec
  import opened Library

  predicate IsHonestReplica(c:Constants, hostId:HostId) 
  {
    && c.WF()
    && c.clusterConfig.IsHonestReplica(hostId)
  }

  // Here's a predicate that will be very useful in constructing invariant conjuncts.
  predicate {:opaque} RecordedPrePreparesRecvdCameFromNetwork(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall replicaIdx, seqID | 
              && IsHonestReplica(c, replicaIdx)
              && var replicaVariables := v.hosts[replicaIdx].replicaVariables;
              && var replicaConstants := c.hosts[replicaIdx].replicaConstants;
              && seqID in replicaVariables.workingWindow.getActiveSequenceIDs(replicaConstants)
              && replicaVariables.workingWindow.prePreparesRcvd[seqID].Some?
                :: v.hosts[replicaIdx].replicaVariables.workingWindow.prePreparesRcvd[seqID].value in v.network.sentMsgs)
  }

  predicate RecordedPreparesRecvdCameFromNetwork(c:Constants, v:Variables, observer:HostId)
  {
    && v.WF(c)
    && IsHonestReplica(c, observer)
    && (forall sender, seqID | 
              && var repicaVariables := v.hosts[observer].replicaVariables;
              && var repicaConstants := c.hosts[observer].replicaConstants;
              && seqID in repicaVariables.workingWindow.getActiveSequenceIDs(repicaConstants)
              && sender in repicaVariables.workingWindow.preparesRcvd[seqID]
                :: (&& var msg := v.hosts[observer].replicaVariables.workingWindow.preparesRcvd[seqID][sender];
                    && msg in v.network.sentMsgs
                    && msg.sender == sender
                    && msg.payload.seqID == seqID)) // The key we stored matches what is in the msg
  }

  predicate {:opaque} RecordedPreparesInAllHostsRecvdCameFromNetwork(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall observer | 
            && IsHonestReplica(c, observer)
                :: RecordedPreparesRecvdCameFromNetwork(c, v, observer))
  }

  predicate {:opaque} EveryCommitMsgIsSupportedByAQuorumOfPrepares(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall commitMsg | && commitMsg in v.network.sentMsgs 
                           && commitMsg.payload.Commit? 
                           && IsHonestReplica(c, commitMsg.sender)
          :: QuorumOfPreparesInNetwork(c, v, commitMsg.payload.view, 
                                       commitMsg.payload.seqID, commitMsg.payload.operationWrapper) )
  }

  // This predicate states that honest replicas accept the first PrePrepare they receive and vote
  // with a Prepare message only for it. The lock on Prepare is meant to highlight the fact that
  // even though a replica can send multiple times a Prepare message for a given Sequence ID for
  // a given View, this message will not change unless a View Change happens.
  predicate {:opaque} HonestReplicasLockOnPrepareForGivenView(c: Constants, v:Variables)
  {
    && (forall msg1, msg2 {:trigger msg1.payload.Prepare?, msg2.payload.Prepare?} | 
        && msg1 in v.network.sentMsgs 
        && msg2 in v.network.sentMsgs 
        && msg1.payload.Prepare?
        && msg2.payload.Prepare?
        && msg1.payload.view == msg2.payload.view
        && msg1.payload.seqID == msg2.payload.seqID
        && msg1.sender == msg2.sender
        && IsHonestReplica(c, msg1.sender)
        :: msg1 == msg2)
  }

  // This predicate states that if a replica sends a Commit message for a given Sequence ID for a given View
  // it will not change its mind, even if it re-sends this message multiple times it will always be the same
  // for a given View.
  predicate {:opaque} HonestReplicasLockOnCommitForGivenView(c:Constants, v:Variables) {
    && (forall msg1, msg2 | 
        && msg1 in v.network.sentMsgs 
        && msg2 in v.network.sentMsgs 
        && msg1.payload.Commit?
        && msg2.payload.Commit?
        && msg1.payload.view == msg2.payload.view
        && msg1.payload.seqID == msg2.payload.seqID
        && msg1.sender == msg2.sender
        && IsHonestReplica(c, msg1.sender)
        :: msg1 == msg2)
  }

  predicate {:opaque} CommitMsgsFromHonestSendersAgree(c:Constants, v:Variables) {
    && (forall msg1, msg2 | 
        && msg1 in v.network.sentMsgs 
        && msg2 in v.network.sentMsgs 
        && msg1.payload.Commit?
        && msg2.payload.Commit?
        && msg1.payload.view == msg2.payload.view
        && msg1.payload.seqID == msg2.payload.seqID
        && IsHonestReplica(c, msg1.sender)
        && IsHonestReplica(c, msg2.sender)
        :: msg1.payload.operationWrapper == msg2.payload.operationWrapper)
  }

  predicate {:opaque} RecordedPreparesHaveValidSenderID(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall replicaIdx, seqID, sender |
          && IsHonestReplica(c, replicaIdx)
          && var prepareMap := v.hosts[replicaIdx].replicaVariables.workingWindow.preparesRcvd;
          && seqID in prepareMap
          && sender in prepareMap[seqID]
          :: && v.hosts[replicaIdx].replicaVariables.workingWindow.preparesRcvd[seqID][sender].sender == sender
        )
  }

  predicate {:opaque} RecordedPreparesClientOpsMatchPrePrepare(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall replicaIdx, seqID, sender |
          && IsHonestReplica(c, replicaIdx)
          && var prepareMap := v.hosts[replicaIdx].replicaVariables.workingWindow.preparesRcvd;
          && seqID in prepareMap
          && sender in prepareMap[seqID]
          :: && var replicaWorkingWindow := v.hosts[replicaIdx].replicaVariables.workingWindow;
             && replicaWorkingWindow.prePreparesRcvd[seqID].Some?
             && replicaWorkingWindow.preparesRcvd[seqID][sender].payload.operationWrapper
                == replicaWorkingWindow.prePreparesRcvd[seqID].value.payload.operationWrapper)
  }

  predicate {:opaque} RecordedCommitsClientOpsMatchPrePrepare(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall replicaIdx, seqID, sender |
          && IsHonestReplica(c, replicaIdx)
          && var commitMap := v.hosts[replicaIdx].replicaVariables.workingWindow.commitsRcvd;
          && seqID in commitMap
          && sender in commitMap[seqID]
          :: && var replicaWorkingWindow := v.hosts[replicaIdx].replicaVariables.workingWindow;
             && replicaWorkingWindow.prePreparesRcvd[seqID].Some?
             && replicaWorkingWindow.commitsRcvd[seqID][sender].payload.operationWrapper 
             == replicaWorkingWindow.prePreparesRcvd[seqID].value.payload.operationWrapper)
  }

  predicate {:opaque} EveryCommitIsSupportedByRecordedPrepares(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall commitMsg | && commitMsg in v.network.sentMsgs
                           && commitMsg.payload.Commit?
                           && IsHonestReplica(c, commitMsg.sender)
          :: && Replica.QuorumOfPrepares(c.hosts[commitMsg.sender].replicaConstants, 
                                         v.hosts[commitMsg.sender].replicaVariables, 
                                         commitMsg.payload.seqID))
  }

  predicate {:opaque} EveryCommitIsSupportedByPreviouslySentPrepares(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall commitMsg | && commitMsg in v.network.sentMsgs
                           && commitMsg.payload.Commit?
                           && IsHonestReplica(c, commitMsg.sender)
          :: QuorumOfPreparesInNetwork(c, v, commitMsg.payload.view, 
                                       commitMsg.payload.seqID, commitMsg.payload.operationWrapper))
  }

  predicate {:opaque} EverySentIntraViewMsgIsInWorkingWindowOrBefore(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall msg | && msg in v.network.sentMsgs
                           && msg.payload.IsIntraViewMsg()
                           && IsHonestReplica(c, msg.sender)
          :: && var replicaVariables := v.hosts[msg.sender].replicaVariables;
             && var replicaConstants := c.hosts[msg.sender].replicaConstants;
             && msg.payload.seqID < replicaVariables.workingWindow.lastStableCheckpoint + replicaConstants.clusterConfig.workingWindowSize)
  }

  predicate {:opaque} EverySentIntraViewMsgIsForAViewLessOrEqualToSenderView(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall msg | && msg in v.network.sentMsgs
                           && msg.payload.IsIntraViewMsg()
                           && IsHonestReplica(c, msg.sender)
          :: && var replicaVariables := v.hosts[msg.sender].replicaVariables;
             && var replicaConstants := c.hosts[msg.sender].replicaConstants;
             && msg.payload.view <= replicaVariables.view)
  }

  predicate {:opaque} EveryCommitClientOpMatchesRecordedPrePrepare(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall commitMsg | && commitMsg in v.network.sentMsgs
                           && commitMsg.payload.Commit?
                           && IsHonestReplica(c, commitMsg.sender)
                           && var replicaVariables := v.hosts[commitMsg.sender].replicaVariables;
                           && var replicaConstants := c.hosts[commitMsg.sender].replicaConstants;
                           && commitMsg.payload.seqID in replicaVariables.workingWindow.getActiveSequenceIDs(replicaConstants)
                           && commitMsg.payload.view == replicaVariables.view
          :: && var recordedPrePrepare := 
                v.hosts[commitMsg.sender].replicaVariables.workingWindow.prePreparesRcvd[commitMsg.payload.seqID];
             && recordedPrePrepare.Some?
             && commitMsg.payload.operationWrapper == recordedPrePrepare.value.payload.operationWrapper)
  }

  predicate {:opaque} EveryPrepareClientOpMatchesRecordedPrePrepare(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall prepareMsg | 
                      && prepareMsg in v.network.sentMsgs
                      && prepareMsg.payload.Prepare?
                      && IsHonestReplica(c, prepareMsg.sender)
                      && var replicaVariables := v.hosts[prepareMsg.sender].replicaVariables;
                      && var replicaConstants := c.hosts[prepareMsg.sender].replicaConstants;
                      && prepareMsg.payload.seqID in replicaVariables.workingWindow.getActiveSequenceIDs(replicaConstants)
                      && prepareMsg.payload.view == replicaVariables.view
          :: && var recordedPrePrepare := 
                v.hosts[prepareMsg.sender].replicaVariables.workingWindow.prePreparesRcvd[prepareMsg.payload.seqID];
             && recordedPrePrepare.Some?
             && prepareMsg.payload.operationWrapper == recordedPrePrepare.value.payload.operationWrapper)
  }

  predicate {:opaque} RecordedPreparesMatchHostView(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall observer, seqID, sender | 
                           && IsHonestReplica(c, observer)
                           && c.clusterConfig.IsReplica(sender)
                           && var replicaVars := v.hosts[observer].replicaVariables;
                           && seqID in replicaVars.workingWindow.preparesRcvd
                           && sender in replicaVars.workingWindow.preparesRcvd[seqID]
                :: && var replicaVars := v.hosts[observer].replicaVariables;
                   && replicaVars.view == replicaVars.workingWindow.preparesRcvd[seqID][sender].payload.view)
  }

  predicate SentPreparesMatchRecordedPrePrepareIfHostInSameView(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall prepare | 
                && prepare in v.network.sentMsgs
                && prepare.payload.Prepare?
                && IsHonestReplica(c, prepare.sender)
                && var replicaVariables := v.hosts[prepare.sender].replicaVariables;
                && var replicaConstants := c.hosts[prepare.sender].replicaConstants;
                && prepare.payload.seqID in replicaVariables.workingWindow.getActiveSequenceIDs(replicaConstants)
                && prepare.payload.view == v.hosts[prepare.sender].replicaVariables.view
                  :: && var replicaWorkingWindow := v.hosts[prepare.sender].replicaVariables.workingWindow;
                     && replicaWorkingWindow.prePreparesRcvd[prepare.payload.seqID].Some?
                     && replicaWorkingWindow.prePreparesRcvd[prepare.payload.seqID].value.payload.operationWrapper
                        == prepare.payload.operationWrapper)
  }

  predicate {:opaque} RecordedCheckpointsRecvdCameFromNetwork(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall replicaIdx, checkpointMsg | 
              && IsHonestReplica(c, replicaIdx)
              && var replicaVariables := v.hosts[replicaIdx].replicaVariables;
              && var replicaConstants := c.hosts[replicaIdx].replicaConstants;
              && checkpointMsg in replicaVariables.checkpointMsgsRecvd.msgs
                :: checkpointMsg in v.network.sentMsgs)
  }

  // predicate PrePreparesCarrySameClientOpsForGivenSeqID(c:Constants, v:Variables)
  // {
  //   && v.WF(c)
  //   && (forall prePrepare1, prePrepare2 | && prePrepare1 in v.network.sentMsgs
  //                                         && prePrepare2 in v.network.sentMsgs
  //                                         && prePrepare1.PrePrepare?
  //                                         && prePrepare2.PrePrepare?
  //                                         && prePrepare1.sender == prePrepare2.sender
  //                                         && prePrepare1.seqID == prePrepare2.seqID
  //                                         && IsHonestReplica(c, prePrepare1.sender)
  //                                       :: prePrepare1 == prePrepare2)
  // }

  predicate {:opaque} AllReplicasLiteInv (c: Constants, v:Variables) {
    && v.WF(c)
    && (forall replicaIdx | 0 <= replicaIdx < |c.hosts| && c.clusterConfig.IsHonestReplica(replicaIdx)
                            :: Replica.LiteInv(c.hosts[replicaIdx].replicaConstants, v.hosts[replicaIdx].replicaVariables))
  }

  predicate Inv(c: Constants, v:Variables) {
    //&& PrePreparesCarrySameClientOpsForGivenSeqID(c, v)
    // Do not remove, lite invariant about internal honest Node invariants:
    && AllReplicasLiteInv(c, v)
    && RecordedPreparesHaveValidSenderID(c, v)
    //&& SentPreparesMatchRecordedPrePrepareIfHostInSameView(c, v)
    && RecordedPrePreparesRecvdCameFromNetwork(c, v)
    && RecordedPreparesInAllHostsRecvdCameFromNetwork(c, v)
    && RecordedPreparesMatchHostView(c, v)
    && EveryCommitMsgIsSupportedByAQuorumOfPrepares(c, v)
    && RecordedPreparesClientOpsMatchPrePrepare(c, v)
    && RecordedCommitsClientOpsMatchPrePrepare(c, v)
    && EveryCommitIsSupportedByPreviouslySentPrepares(c, v)
    && EverySentIntraViewMsgIsInWorkingWindowOrBefore(c, v)
    && EverySentIntraViewMsgIsForAViewLessOrEqualToSenderView(c, v)
    && EveryPrepareClientOpMatchesRecordedPrePrepare(c, v)
    && EveryCommitClientOpMatchesRecordedPrePrepare(c, v)
    && HonestReplicasLockOnPrepareForGivenView(c, v)
    && HonestReplicasLockOnCommitForGivenView(c, v)
    && CommitMsgsFromHonestSendersAgree(c, v)
    && RecordedCheckpointsRecvdCameFromNetwork(c, v)
  }

  function sentPreparesForSeqID(c: Constants, v:Variables, view:nat, seqID:Messages.SequenceID,
                                  operationWrapper:Messages.OperationWrapper) : set<Network.Message<Messages.Message>> 
    requires v.WF(c)
  {
    set msg | && msg in v.network.sentMsgs 
              && msg.payload.Prepare?
              && msg.payload.view == view
              && msg.payload.seqID == seqID
              && msg.payload.operationWrapper == operationWrapper
              && msg.sender in getAllReplicas(c)
  }

  predicate QuorumOfPreparesInNetwork(c:Constants, v:Variables, view:nat, seqID:Messages.SequenceID, 
                                      operationWrapper:Messages.OperationWrapper) {
    && v.WF(c)
    && var prepares := sentPreparesForSeqID(c, v, view, seqID, operationWrapper);
    && |Messages.sendersOf(prepares)| >= c.clusterConfig.AgreementQuorum()
    //&& (forall prepare | prepare in prepares :: prepare.clientOp == clientOperation)
  }

  lemma WlogCommitAgreement(c: Constants, v:Variables, v':Variables, step:Step,
                            msg1:Network.Message<Messages.Message>, msg2:Network.Message<Messages.Message>)
    requires Inv(c, v)
    requires NextStep(c, v, v', step)
    requires msg1 in v'.network.sentMsgs 
    requires msg2 in v'.network.sentMsgs 
    requires msg1.payload.Commit?
    requires msg2.payload.Commit?
    requires msg1.payload.view == msg2.payload.view
    requires msg1.payload.seqID == msg2.payload.seqID
    requires msg1 in v.network.sentMsgs
    requires msg2 !in v.network.sentMsgs
    requires IsHonestReplica(c, msg1.sender)
    requires IsHonestReplica(c, msg2.sender)
    ensures msg1.payload.operationWrapper == msg2.payload.operationWrapper
  {
    var prepares1 := sentPreparesForSeqID(c, v, msg1.payload.view, msg1.payload.seqID, msg1.payload.operationWrapper);
    var senders1 := Messages.sendersOf(prepares1);
    assert |senders1| >= c.clusterConfig.AgreementQuorum();

    var h_c := c.hosts[step.id].replicaConstants;
    var h_v := v.hosts[step.id].replicaVariables;
    var h_v' := v'.hosts[step.id].replicaVariables;
    var h_step :| Replica.NextStep(h_c, h_v, h_v', step.msgOps, h_step);

    var senders2 := h_v.workingWindow.preparesRcvd[h_step.seqID].Keys;
    assert |senders2| >= c.clusterConfig.AgreementQuorum();

    var equivocatingHonestSender := FindQuorumIntersection(c, senders1, senders2);
  }

  function GetKReplicas(c:Constants, k:nat) : (hosts:set<HostIdentifiers.HostId>)
    requires c.WF()
    requires k <= c.clusterConfig.N()
    ensures |hosts| == k
    ensures forall host :: host in hosts <==> 0 <= host < k
    ensures forall host | host in hosts :: c.clusterConfig.IsReplica(host)
  {
    if k == 0 then {}
    else GetKReplicas(c, k-1) + {k - 1}
  }

  function getAllReplicas(c: Constants) : (hostsSet:set<HostIdentifiers.HostId>)
    requires c.WF()
    ensures |hostsSet| == c.clusterConfig.N()
    ensures forall host :: host in hostsSet <==> c.clusterConfig.IsReplica(host)
  {
    GetKReplicas(c, c.clusterConfig.N())
  }

  lemma FindQuorumIntersection(c: Constants, senders1:set<HostIdentifiers.HostId>, senders2:set<HostIdentifiers.HostId>) 
    returns (common:HostIdentifiers.HostId)
    requires c.WF()
    requires |senders1| >= c.clusterConfig.AgreementQuorum()//TODO: rename hosts
    requires |senders2| >= c.clusterConfig.AgreementQuorum()
    requires senders1 <= getAllReplicas(c)
    requires senders2 <= getAllReplicas(c)
    ensures IsHonestReplica(c, common)
    ensures common in senders1
    ensures common in senders2
  {
    var f := c.clusterConfig.F();
    var n := c.clusterConfig.N();

    assert 2 * f + 1 <= |senders1|;
    assert 2 * f + 1 <= |senders2|;

    var commonSenders := senders1 * senders2;
    if(|commonSenders| < f + 1) {
      calc {
        n;
        == (3 * f) + 1;
        < |senders1| + |senders2| - |senders1*senders2|;
        == |senders1 + senders2|; 
        <= {
          Library.SubsetCardinality(senders1 + senders2, getAllReplicas(c));
        }
        n;
      }
      assert false; // Proof by contradiction.
    }
    assert f + 1 <= |commonSenders|;

    var commonHonest := commonSenders * HonestReplicas(c);
    CountHonestReplicas(c);
    if(|commonHonest| == 0) {
      Library.SubsetCardinality(commonSenders + HonestReplicas(c), getAllReplicas(c));
      assert false; //Proof by contradiction
    }

    var result :| result in commonHonest;
    common := result;
  }

  function HonestReplicas(c: Constants) : (honest:set<HostId>) 
    requires c.WF()
  {
    set sender | 0 <= sender < c.clusterConfig.N() && IsHonestReplica(c, sender)
  }

  lemma CountHonestReplicas(c: Constants)
    requires c.WF()
    ensures |HonestReplicas(c)| >= c.clusterConfig.AgreementQuorum()
  {
    var count := 0;
    var f := c.clusterConfig.F();
    var n := c.clusterConfig.N();
    while(count < n)
      invariant count <= n
      invariant |set sender | 0 <= sender < count && IsHonestReplica(c, sender)| == if count < f then 0
                                                                                      else count - f
      {
        var oldSet := set sender | 0 <= sender < count && IsHonestReplica(c, sender);
        var newSet := set sender | 0 <= sender < count + 1 && IsHonestReplica(c, sender);
        if(count >= f) {
          assert newSet == oldSet + {count};
        }
        count := count + 1;
      }
  }

  lemma ProofCommitMsgsFromHonestSendersAgree(c: Constants, v:Variables, v':Variables, step:Step)
    requires Inv(c, v)
    requires NextStep(c, v, v', step)
    ensures CommitMsgsFromHonestSendersAgree(c, v')
  {
    reveal_CommitMsgsFromHonestSendersAgree();
    forall msg1, msg2 | 
      && msg1 in v'.network.sentMsgs 
      && msg2 in v'.network.sentMsgs 
      && msg1.payload.Commit?
      && msg2.payload.Commit?
      && msg1.payload.view == msg2.payload.view
      && msg1.payload.seqID == msg2.payload.seqID
      && IsHonestReplica(c, msg1.sender)
      && IsHonestReplica(c, msg2.sender)
      ensures msg1.payload.operationWrapper == msg2.payload.operationWrapper {
        if(msg1 in v.network.sentMsgs && msg2 in v.network.sentMsgs) {
          assert msg1.payload.operationWrapper == msg2.payload.operationWrapper;
        } else if(msg1 !in v.network.sentMsgs && msg2 !in v.network.sentMsgs) {
          assert msg1.payload.operationWrapper == msg2.payload.operationWrapper;
        } else if(msg1 in v.network.sentMsgs && msg2 !in v.network.sentMsgs) {
          WlogCommitAgreement(c, v, v', step, msg1, msg2);
          assert msg1.payload.operationWrapper == msg2.payload.operationWrapper;
        } else if(msg1 !in v.network.sentMsgs && msg2 in v.network.sentMsgs) {
          WlogCommitAgreement(c, v, v', step, msg2, msg1);
          assert msg1.payload.operationWrapper == msg2.payload.operationWrapper;
        } else {
          assert false;
        }
      }
  }

  predicate HonestReplicaStepTaken(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
  {
    && v.WF(c)
    && v'.WF(c)
    && NextStep(c, v, v', step)
    && IsHonestReplica(c, step.id)
    && var h_c := c.hosts[step.id].replicaConstants;
    && h_v == v.hosts[step.id].replicaVariables
    && var h_v' := v'.hosts[step.id].replicaVariables;
    && Replica.NextStep(h_c, h_v, h_v', step.msgOps, h_step)
  }

  lemma HonestPreservesRecordedPreparesHaveValidSenderID(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures RecordedPreparesHaveValidSenderID(c, v')
  {
    reveal_RecordedPreparesHaveValidSenderID();

    if (h_step.AdvanceWorkingWindowStep?) {
      h_v.workingWindow.reveal_Shift();
    } else if(h_step.PerformStateTransferStep?) {
      h_v.workingWindow.reveal_Shift();
    }
  }

  lemma HonestPreservesRecordedPrePreparesRecvdCameFromNetwork(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures RecordedPrePreparesRecvdCameFromNetwork(c, v')
  {
    reveal_RecordedPrePreparesRecvdCameFromNetwork();

    if (h_step.AdvanceWorkingWindowStep?) {
      h_v.workingWindow.reveal_Shift();
    } else if(h_step.PerformStateTransferStep?) {
      h_v.workingWindow.reveal_Shift();
    }
  }

  lemma HonestPreservesRecordedPreparesInAllHostsRecvdCameFromNetwork(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures RecordedPreparesInAllHostsRecvdCameFromNetwork(c, v')
  {
    reveal_RecordedPreparesInAllHostsRecvdCameFromNetwork();

    if (h_step.AdvanceWorkingWindowStep?) {
      h_v.workingWindow.reveal_Shift();
    } else if(h_step.PerformStateTransferStep?) {
      h_v.workingWindow.reveal_Shift();
    }
  }

  lemma HonestPreservesRecordedPreparesMatchHostView(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures RecordedPreparesMatchHostView(c, v')
  {
    reveal_RecordedPreparesMatchHostView();

    if (h_step.AdvanceWorkingWindowStep?) {
      h_v.workingWindow.reveal_Shift();
    } else if(h_step.PerformStateTransferStep?) {
      h_v.workingWindow.reveal_Shift();
    }
  }

  lemma AlwaysPreservesEveryCommitMsgIsSupportedByAQuorumOfPrepares(c: Constants, v:Variables, v':Variables, step:Step)
    requires Inv(c, v)
    requires NextStep(c, v, v', step)
    ensures EveryCommitMsgIsSupportedByAQuorumOfPrepares(c, v')
  {
    reveal_EveryCommitMsgIsSupportedByAQuorumOfPrepares();
    // A proof of EveryCommitMsgIsSupportedByAQuorumOfPrepares,
    // by selecting an arbitrary commitMsg instance
    forall commitMsg | && commitMsg in v'.network.sentMsgs 
                       && commitMsg.payload.Commit?
                       && IsHonestReplica(c, commitMsg.sender) ensures 
      QuorumOfPreparesInNetwork(c, v', commitMsg.payload.view, commitMsg.payload.seqID, commitMsg.payload.operationWrapper) {
      if(commitMsg in v.network.sentMsgs) { // the commitMsg has been sent in a previous step
        // In this case, the proof is trivial - we just need to "teach" Dafny about subset cardinality
        var senders := Messages.sendersOf(sentPreparesForSeqID(c, v, commitMsg.payload.view, 
                                                      commitMsg.payload.seqID, commitMsg.payload.operationWrapper));
        var senders' := Messages.sendersOf(sentPreparesForSeqID(c, v', commitMsg.payload.view,
                                                      commitMsg.payload.seqID, commitMsg.payload.operationWrapper));
        Library.SubsetCardinality(senders, senders');
      } else { // the commitMsg is being sent in the current step
        reveal_RecordedPreparesInAllHostsRecvdCameFromNetwork();
        reveal_RecordedPreparesMatchHostView();
        reveal_RecordedPreparesClientOpsMatchPrePrepare();
        var prepares := sentPreparesForSeqID(c, v, commitMsg.payload.view,
                                             commitMsg.payload.seqID, commitMsg.payload.operationWrapper);
        var prepares' := sentPreparesForSeqID(c, v', commitMsg.payload.view, 
                                             commitMsg.payload.seqID, commitMsg.payload.operationWrapper);
        assert prepares == prepares'; // Trigger (hint) - sending a commitMsg does not affect the set of prepares
        
        // Prove that the prepares in the working window are a subset of the prepares in the network:
        var prepareSendersFromNetwork := Messages.sendersOf(prepares);
        var h_v := v.hosts[step.id];
        var prepareSendersInWorkingWindow := h_v.replicaVariables.workingWindow.preparesRcvd[commitMsg.payload.seqID].Keys;
        assert (forall sender | sender in prepareSendersInWorkingWindow 
                              :: && var msg := h_v.replicaVariables.workingWindow.preparesRcvd[commitMsg.payload.seqID][sender];
                                 && msg in v.network.sentMsgs);
        assert (forall sender | sender in prepareSendersInWorkingWindow 
                              :: sender in prepareSendersFromNetwork); //Trigger for subset operator
        Library.SubsetCardinality(prepareSendersInWorkingWindow, prepareSendersFromNetwork);
      }
    }
  }

  lemma HonestPreservesRecordedPreparesClientOpsMatchPrePrepare(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures RecordedPreparesClientOpsMatchPrePrepare(c, v')
  {
    reveal_RecordedPreparesClientOpsMatchPrePrepare();

    if (h_step.AdvanceWorkingWindowStep?) {
      h_v.workingWindow.reveal_Shift();
    } else if(h_step.PerformStateTransferStep?) {
      h_v.workingWindow.reveal_Shift();
    }
  }

  lemma HonestPreservesRecordedCommitsClientOpsMatchPrePrepare(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures RecordedCommitsClientOpsMatchPrePrepare(c, v')
  {
    reveal_RecordedCommitsClientOpsMatchPrePrepare();

    if (h_step.AdvanceWorkingWindowStep?) {
      h_v.workingWindow.reveal_Shift();
    } else if(h_step.PerformStateTransferStep?) {
      h_v.workingWindow.reveal_Shift();
    }
  }

  lemma CountPrepareMessages(proofSet:Replica.PrepareProofSet)
    requires forall sender | sender in proofSet.Keys :: proofSet[sender].sender == sender
    ensures |proofSet.Values| == |proofSet.Keys|
  {
    if |proofSet.Keys| > 0 {
      var element :| element in proofSet.Keys;
      var subMap := MapRemoveOne(proofSet, element);
      CountPrepareMessages(subMap);
      assert proofSet.Values == subMap.Values + {proofSet[element]};
      assert proofSet.Keys == subMap.Keys + {element};
    }
  }

  lemma HonestPreservesEveryCommitIsSupportedByPreviouslySentPrepares(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures EveryCommitIsSupportedByPreviouslySentPrepares(c, v')
  {
    reveal_EveryCommitIsSupportedByPreviouslySentPrepares();
    QuorumOfPreparesInNetworkMonotonic(c, v, v', step);
    if (h_step.SendCommitStep?) {
            forall commitMsg | && commitMsg in v'.network.sentMsgs
                           && commitMsg.payload.Commit?
                           && IsHonestReplica(c, commitMsg.sender)
          ensures QuorumOfPreparesInNetwork(c, v', commitMsg.payload.view, 
            commitMsg.payload.seqID, commitMsg.payload.operationWrapper) {
              if commitMsg in v.network.sentMsgs {
                assert QuorumOfPreparesInNetwork(c, v', commitMsg.payload.view, 
                        commitMsg.payload.seqID, commitMsg.payload.operationWrapper);
              } else {
                reveal_RecordedPreparesMatchHostView();
                reveal_EveryPrepareClientOpMatchesRecordedPrePrepare();
                reveal_RecordedPreparesInAllHostsRecvdCameFromNetwork();
                var recordedPrepares := h_v.workingWindow.preparesRcvd[commitMsg.payload.seqID].Values;
                CountPrepareMessages(h_v.workingWindow.preparesRcvd[commitMsg.payload.seqID]);
                var sentPrepares := sentPreparesForSeqID(c, v, commitMsg.payload.view, commitMsg.payload.seqID, commitMsg.payload.operationWrapper);
                assert recordedPrepares <= sentPrepares by {
                  forall msg | msg in recordedPrepares ensures msg in sentPrepares {
                        assert msg in v.network.sentMsgs;
                        assert msg.payload.Prepare?;
                        assert msg.payload.view == commitMsg.payload.view;
                        assert msg.payload.seqID == commitMsg.payload.seqID;
                        assert msg in v.network.sentMsgs;
                        assert msg.payload.operationWrapper == h_v.workingWindow.prePreparesRcvd[commitMsg.payload.seqID].value.payload.operationWrapper;
                        assert msg.payload.operationWrapper == commitMsg.payload.operationWrapper;
                        assert msg.sender in getAllReplicas(c);
                  }
                }
                Library.SubsetCardinality(recordedPrepares, sentPrepares);
                // assert x <= sentPreparesForSeqID(c, v, commitMsg.payload.view, commitMsg.payload.seqID, commitMsg.payload.operationWrapper);
                assert QuorumOfPreparesInNetwork(c, v, commitMsg.payload.view, 
                        commitMsg.payload.seqID, commitMsg.payload.operationWrapper);
                assert QuorumOfPreparesInNetwork(c, v', commitMsg.payload.view, 
                        commitMsg.payload.seqID, commitMsg.payload.operationWrapper);
              }
            }
      
    } else {
      assert EveryCommitIsSupportedByPreviouslySentPrepares(c, v');
    }
  }

  lemma HonestPreservesEveryPrepareClientOpMatchesRecordedPrePrepare(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures EveryPrepareClientOpMatchesRecordedPrePrepare(c, v')

  {
    reveal_EveryPrepareClientOpMatchesRecordedPrePrepare();
    reveal_EverySentIntraViewMsgIsInWorkingWindowOrBefore();
    reveal_EverySentIntraViewMsgIsForAViewLessOrEqualToSenderView();
    h_v.workingWindow.reveal_Shift();
  }

  lemma QuorumOfPreparesInNetworkMonotonic(c: Constants, v:Variables, v':Variables, step:Step)
    requires NextStep(c, v, v', step)
    ensures (forall view, seqID, clientOp | QuorumOfPreparesInNetwork(c, v, view, seqID, clientOp)
                                        :: QuorumOfPreparesInNetwork(c, v', view, seqID, clientOp))
  {
    forall view, seqID, clientOp | QuorumOfPreparesInNetwork(c, v, view, seqID, clientOp)
                                  ensures QuorumOfPreparesInNetwork(c, v', view, seqID, clientOp)
    {
      var senders := Messages.sendersOf(sentPreparesForSeqID(c, v, view, seqID, clientOp));
      var senders' := Messages.sendersOf(sentPreparesForSeqID(c, v', view, seqID, clientOp));
      Library.SubsetCardinality(senders, senders');
    }
  }

  predicate SentCommitStepWasTaken(c: Constants, v:Variables, v':Variables,
                                step:Step, h_step:Replica.Step)
    requires Inv(c, v)
  {
    && NextStep(c, v, v', step)
    && IsHonestReplica(c, step.id)
    && var h_c := c.hosts[step.id].replicaConstants;
    && var h_v := v.hosts[step.id].replicaVariables;
    && var h_v' := v'.hosts[step.id].replicaVariables;
    && Replica.NextStep(h_c, h_v, h_v', step.msgOps, h_step)
    && h_step.SendCommitStep?
    && Replica.SendCommit(h_c, h_v, h_v', step.msgOps, h_step.seqID)
  }

  lemma SendCommitStepPreservesInv(c: Constants, v:Variables, v':Variables, 
                                   step:Step, h_step:Replica.Step)
    requires Inv(c, v)
    requires SentCommitStepWasTaken(c, v, v', step, h_step)
    ensures Inv(c, v')
  {
    CommitMsgStability(c, v, v', step);
  }

  predicate SendClientOperationStepWasTaken(c: Constants, v:Variables, v':Variables,
                                         step:Step, h_step:Client.Step)
    requires Inv(c, v)
  {
    && NextStep(c, v, v', step)
    && c.clusterConfig.IsClient(step.id)
    && var h_c := c.hosts[step.id].clientConstants;
    && var h_v := v.hosts[step.id].clientVariables;
    && var h_v' := v'.hosts[step.id].clientVariables;
    && Client.NextStep(h_c, h_v, h_v', step.msgOps, h_step)
    && h_step.SendClientOperationStep?
    && Client.SendClientOperation(h_c, h_v, h_v', step.msgOps)
  }

  lemma SendClientOperationPreservesInv(c: Constants, v:Variables, v':Variables, 
                                   step:Step, h_step:Client.Step)
    requires Inv(c, v)
    requires SendClientOperationStepWasTaken(c, v, v', step, h_step)
    ensures Inv(c, v')
  {
    CommitMsgStability(c, v, v', step);
  }

  predicate SendPrePrepareStepWasTaken(c: Constants, v:Variables, v':Variables,
                                    step:Step, h_step:Replica.Step)
    requires Inv(c, v)
  {
    && NextStep(c, v, v', step)
    && IsHonestReplica(c, step.id)
    && var h_c := c.hosts[step.id].replicaConstants;
    && var h_v := v.hosts[step.id].replicaVariables;
    && var h_v' := v'.hosts[step.id].replicaVariables;
    && Replica.NextStep(h_c, h_v, h_v', step.msgOps, h_step)
    && h_step.SendPrePrepareStep?
    && Replica.SendPrePrepare(h_c, h_v, h_v', step.msgOps, h_step.seqID)
  }

  lemma SendPrePrepareStepPreservesInv(c: Constants, v:Variables, v':Variables, 
                                       step:Step, h_step:Replica.Step)
    requires Inv(c, v)
    requires SendPrePrepareStepWasTaken(c, v, v', step, h_step)
    ensures Inv(c, v')
  {
    CommitMsgStability(c, v, v', step);
  }

  predicate SendPrepareStepWasTaken(c: Constants, v:Variables, v':Variables,
                                    step:Step, h_step:Replica.Step)
    requires Inv(c, v)
  {
    && NextStep(c, v, v', step)
    && IsHonestReplica(c, step.id)
    && var h_c := c.hosts[step.id].replicaConstants;
    && var h_v := v.hosts[step.id].replicaVariables;
    && var h_v' := v'.hosts[step.id].replicaVariables;
    && Replica.NextStep(h_c, h_v, h_v', step.msgOps, h_step)
    && h_step.SendPrepareStep?
    && Replica.SendPrepare(h_c, h_v, h_v', step.msgOps, h_step.seqID)
  }

  lemma SendPrepareStepPreservesInv(c: Constants, v:Variables, v':Variables, 
                                       step:Step, h_step:Replica.Step)
    requires Inv(c, v)
    requires SendPrepareStepWasTaken(c, v, v', step, h_step)
    ensures Inv(c, v')
  {
    reveal_EveryPrepareClientOpMatchesRecordedPrePrepare();
    CommitMsgStability(c, v, v', step);
  }

  predicate RecvPrePrepareStepWasTaken(c: Constants, v:Variables, v':Variables,
                                    step:Step, h_step:Replica.Step)
    requires Inv(c, v)
  {
    && NextStep(c, v, v', step)
    && IsHonestReplica(c, step.id)
    && var h_c := c.hosts[step.id].replicaConstants;
    && var h_v := v.hosts[step.id].replicaVariables;
    && var h_v' := v'.hosts[step.id].replicaVariables;
    && Replica.NextStep(h_c, h_v, h_v', step.msgOps, h_step)
    && h_step.RecvPrePrepareStep?
    && Replica.RecvPrePrepare(h_c, h_v, h_v', step.msgOps)
  }

  lemma RecvPrePrepareStepPreservesInv(c: Constants, v:Variables, v':Variables, 
                                   step:Step, h_step:Replica.Step)
    requires Inv(c, v)
    requires RecvPrePrepareStepWasTaken(c, v, v', step, h_step)
    ensures Inv(c, v')
  {
    CommitMsgStability(c, v, v', step);
  }

  predicate RecvPrepareStepWasTaken(c: Constants, v:Variables, v':Variables,
                                step:Step, h_step:Replica.Step)
    requires Inv(c, v)
  {
    && NextStep(c, v, v', step)
    && IsHonestReplica(c, step.id)
    && var h_c := c.hosts[step.id].replicaConstants;
    && var h_v := v.hosts[step.id].replicaVariables;
    && var h_v' := v'.hosts[step.id].replicaVariables;
    && Replica.NextStep(h_c, h_v, h_v', step.msgOps, h_step)
    && h_step.RecvPrepareStep?
    && Replica.RecvPrepare(h_c, h_v, h_v', step.msgOps)
  }

  lemma RecvPrepareStepPreservesInv(c: Constants, v:Variables, v':Variables, 
                                   step:Step, h_step:Replica.Step)
    requires Inv(c, v)
    requires RecvPrepareStepWasTaken(c, v, v', step, h_step)
    ensures Inv(c, v')
  {
    CommitMsgStability(c, v, v', step);
  }

  predicate RecvCommitStepWasTaken(c: Constants, v:Variables, v':Variables,
                                step:Step, h_step:Replica.Step)
    requires Inv(c, v)
  {
    && NextStep(c, v, v', step)
    && IsHonestReplica(c, step.id)
    && var h_c := c.hosts[step.id].replicaConstants;
    && var h_v := v.hosts[step.id].replicaVariables;
    && var h_v' := v'.hosts[step.id].replicaVariables;
    && Replica.NextStep(h_c, h_v, h_v', step.msgOps, h_step)
    && h_step.RecvCommitStep?
    && Replica.RecvCommit(h_c, h_v, h_v', step.msgOps)
  }

  lemma RecvCommitStepPreservesInv(c: Constants, v:Variables, v':Variables, 
                                   step:Step, h_step:Replica.Step)
    requires Inv(c, v)
    requires RecvCommitStepWasTaken(c, v, v', step, h_step)
    ensures Inv(c, v')
  {
    CommitMsgStability(c, v, v', step);
  }

  predicate DoCommitStepWasTaken(c: Constants, v:Variables, v':Variables,
                                step:Step, h_step:Replica.Step)
    requires Inv(c, v)
  {
    && NextStep(c, v, v', step)
    && IsHonestReplica(c, step.id)
    && var h_c := c.hosts[step.id].replicaConstants;
    && var h_v := v.hosts[step.id].replicaVariables;
    && var h_v' := v'.hosts[step.id].replicaVariables;
    && Replica.NextStep(h_c, h_v, h_v', step.msgOps, h_step)
    && h_step.DoCommitStep?
    && Replica.DoCommit(h_c, h_v, h_v', step.msgOps, h_step.seqID)
  }

  lemma DoCommitStepPreservesInv(c: Constants, v:Variables, v':Variables, 
                                   step:Step, h_step:Replica.Step)
    requires Inv(c, v)
    requires DoCommitStepWasTaken(c, v, v', step, h_step)
    ensures Inv(c, v')
  {
    CommitMsgStability(c, v, v', step);
  }

  predicate ExecuteStepStepWasTaken(c: Constants, v:Variables, v':Variables,
                                   step:Step, h_step:Replica.Step)
    requires Inv(c, v)
  {
    && NextStep(c, v, v', step)
    && IsHonestReplica(c, step.id)
    && var h_c := c.hosts[step.id].replicaConstants;
    && var h_v := v.hosts[step.id].replicaVariables;
    && var h_v' := v'.hosts[step.id].replicaVariables;
    && Replica.NextStep(h_c, h_v, h_v', step.msgOps, h_step)
    && h_step.ExecuteStep?
    && Replica.Execute(h_c, h_v, h_v', step.msgOps, h_step.seqID)
  }

  lemma ExecuteStepPreservesInv(c: Constants, v:Variables, v':Variables, 
                                  step:Step, h_step:Replica.Step)
    requires Inv(c, v)
    requires ExecuteStepStepWasTaken(c, v, v', step, h_step)
    ensures Inv(c, v')
  {
    CommitMsgStability(c, v, v', step);
  }

  predicate SendCheckpointStepStepWasTaken(c: Constants, v:Variables, v':Variables,
                                   step:Step, h_step:Replica.Step)
    requires Inv(c, v)
  {
    && NextStep(c, v, v', step)
    && IsHonestReplica(c, step.id)
    && var h_c := c.hosts[step.id].replicaConstants;
    && var h_v := v.hosts[step.id].replicaVariables;
    && var h_v' := v'.hosts[step.id].replicaVariables;
    && Replica.NextStep(h_c, h_v, h_v', step.msgOps, h_step)
    && h_step.SendCheckpointStep?
    && Replica.SendCheckpoint(h_c, h_v, h_v', step.msgOps, h_step.seqID)
  }

  lemma SendCheckpointStepPreservesInv(c: Constants, v:Variables, v':Variables, 
                                  step:Step, h_step:Replica.Step)
    requires Inv(c, v)
    requires SendCheckpointStepStepWasTaken(c, v, v', step, h_step)
    ensures Inv(c, v')
  {
    CommitMsgStability(c, v, v', step);
  }

  predicate RecvCheckpointStepStepWasTaken(c: Constants, v:Variables, v':Variables,
                                   step:Step, h_step:Replica.Step)
    requires Inv(c, v)
  {
    && NextStep(c, v, v', step)
    && IsHonestReplica(c, step.id)
    && var h_c := c.hosts[step.id].replicaConstants;
    && var h_v := v.hosts[step.id].replicaVariables;
    && var h_v' := v'.hosts[step.id].replicaVariables;
    && Replica.NextStep(h_c, h_v, h_v', step.msgOps, h_step)
    && h_step.RecvCheckpointStep?
    && Replica.RecvCheckpoint(h_c, h_v, h_v', step.msgOps)
  }

  lemma RecvCheckpointStepPreservesInv(c: Constants, v:Variables, v':Variables, 
                                  step:Step, h_step:Replica.Step)
    requires Inv(c, v)
    requires RecvCheckpointStepStepWasTaken(c, v, v', step, h_step)
    ensures Inv(c, v')
  {
    CommitMsgStability(c, v, v', step);
  }
  predicate AdvanceWorkingWindowStepStepWasTaken(c: Constants, v:Variables, v':Variables,
                                   step:Step, h_step:Replica.Step)
    requires Inv(c, v)
  {
    && NextStep(c, v, v', step)
    && IsHonestReplica(c, step.id)
    && var h_c := c.hosts[step.id].replicaConstants;
    && var h_v := v.hosts[step.id].replicaVariables;
    && var h_v' := v'.hosts[step.id].replicaVariables;
    && Replica.NextStep(h_c, h_v, h_v', step.msgOps, h_step)
    && h_step.AdvanceWorkingWindowStep?
    && Replica.AdvanceWorkingWindow(h_c, h_v, h_v', step.msgOps, h_step.seqID, h_step.checkpointsQuorum)
  }

  lemma AdvanceWorkingWindowStepPreservesInv(c: Constants, v:Variables, v':Variables, 
                                  step:Step, h_step:Replica.Step)
    requires Inv(c, v)
    requires AdvanceWorkingWindowStepStepWasTaken(c, v, v', step, h_step)
    ensures Inv(c, v')
  {
    CommitMsgStability(c, v, v', step);
  }

  predicate PerformStateTransferStepStepWasTaken(c: Constants, v:Variables, v':Variables,
                                   step:Step, h_step:Replica.Step)
    requires Inv(c, v)
  {
    && NextStep(c, v, v', step)
    && IsHonestReplica(c, step.id)
    && var h_c := c.hosts[step.id].replicaConstants;
    && var h_v := v.hosts[step.id].replicaVariables;
    && var h_v' := v'.hosts[step.id].replicaVariables;
    && Replica.NextStep(h_c, h_v, h_v', step.msgOps, h_step)
    && h_step.PerformStateTransferStep?
    && Replica.PerformStateTransfer(h_c, h_v, h_v', step.msgOps, h_step.seqID, h_step.checkpointsQuorum)
  }

  lemma PerformStateTransferStepPreservesInv(c: Constants, v:Variables, v':Variables, 
                                  step:Step, h_step:Replica.Step)
    requires Inv(c, v)
    requires PerformStateTransferStepStepWasTaken(c, v, v', step, h_step)
    ensures Inv(c, v')
  {
    CommitMsgStability(c, v, v', step);
  }

  predicate LeaveViewStepStepWasTaken(c: Constants, v:Variables, v':Variables,
                                   step:Step, h_step:Replica.Step)
    requires Inv(c, v)
  {
    && NextStep(c, v, v', step)
    && IsHonestReplica(c, step.id)
    && var h_c := c.hosts[step.id].replicaConstants;
    && var h_v := v.hosts[step.id].replicaVariables;
    && var h_v' := v'.hosts[step.id].replicaVariables;
    && Replica.NextStep(h_c, h_v, h_v', step.msgOps, h_step)
    && h_step.LeaveViewStep?
    && Replica.LeaveView(h_c, h_v, h_v', step.msgOps, h_step.newView)
  }

  lemma LeaveViewStepPreservesInv(c: Constants, v:Variables, v':Variables, 
                                  step:Step, h_step:Replica.Step)
    requires Inv(c, v)
    requires LeaveViewStepStepWasTaken(c, v, v', step, h_step)
    ensures Inv(c, v')
  {
    CommitMsgStability(c, v, v', step);
  }

  predicate SendViewChangeMsgStepStepWasTaken(c: Constants, v:Variables, v':Variables,
                                   step:Step, h_step:Replica.Step)
    requires Inv(c, v)
  {
    && NextStep(c, v, v', step)
    && IsHonestReplica(c, step.id)
    && var h_c := c.hosts[step.id].replicaConstants;
    && var h_v := v.hosts[step.id].replicaVariables;
    && var h_v' := v'.hosts[step.id].replicaVariables;
    && Replica.NextStep(h_c, h_v, h_v', step.msgOps, h_step)
    && h_step.SendViewChangeMsgStep?
    && Replica.SendViewChangeMsg(h_c, h_v, h_v', step.msgOps)
  }

  lemma SendViewChangeMsgStepPreservesInv(c: Constants, v:Variables, v':Variables, 
                                  step:Step, h_step:Replica.Step)
    requires Inv(c, v)
    requires SendViewChangeMsgStepStepWasTaken(c, v, v', step, h_step)
    ensures Inv(c, v')
  {
    CommitMsgStability(c, v, v', step);
  }

  predicate RecvViewChangeMsgStepStepWasTaken(c: Constants, v:Variables, v':Variables,
                                   step:Step, h_step:Replica.Step)
    requires Inv(c, v)
  {
    && NextStep(c, v, v', step)
    && IsHonestReplica(c, step.id)
    && var h_c := c.hosts[step.id].replicaConstants;
    && var h_v := v.hosts[step.id].replicaVariables;
    && var h_v' := v'.hosts[step.id].replicaVariables;
    && Replica.NextStep(h_c, h_v, h_v', step.msgOps, h_step)
    && h_step.RecvViewChangeMsgStep?
    && Replica.RecvViewChangeMsg(h_c, h_v, h_v', step.msgOps)
  }

  lemma RecvViewChangeMsgStepPreservesInv(c: Constants, v:Variables, v':Variables, 
                                  step:Step, h_step:Replica.Step)
    requires Inv(c, v)
    requires RecvViewChangeMsgStepStepWasTaken(c, v, v', step, h_step)
    ensures Inv(c, v')
  {
    CommitMsgStability(c, v, v', step);
  }

  predicate SelectQuorumOfViewChangeMsgsStepStepWasTaken(c: Constants, v:Variables, v':Variables,
                                   step:Step, h_step:Replica.Step)
    requires Inv(c, v)
  {
    && NextStep(c, v, v', step)
    && IsHonestReplica(c, step.id)
    && var h_c := c.hosts[step.id].replicaConstants;
    && var h_v := v.hosts[step.id].replicaVariables;
    && var h_v' := v'.hosts[step.id].replicaVariables;
    && Replica.NextStep(h_c, h_v, h_v', step.msgOps, h_step)
    && h_step.SelectQuorumOfViewChangeMsgsStep?
    && Replica.SelectQuorumOfViewChangeMsgs(h_c, h_v, h_v', step.msgOps, h_step.viewChangeMsgsSelectedByPrimary)
  }

  lemma SelectQuorumOfViewChangeMsgsStepPreservesInv(c: Constants, v:Variables, v':Variables, 
                                  step:Step, h_step:Replica.Step)
    requires Inv(c, v)
    requires SelectQuorumOfViewChangeMsgsStepStepWasTaken(c, v, v', step, h_step)
    ensures Inv(c, v')
  {
    CommitMsgStability(c, v, v', step);
  }

  predicate SendNewViewMsgStepStepWasTaken(c: Constants, v:Variables, v':Variables,
                                   step:Step, h_step:Replica.Step)
    requires Inv(c, v)
  {
    && NextStep(c, v, v', step)
    && IsHonestReplica(c, step.id)
    && var h_c := c.hosts[step.id].replicaConstants;
    && var h_v := v.hosts[step.id].replicaVariables;
    && var h_v' := v'.hosts[step.id].replicaVariables;
    && Replica.NextStep(h_c, h_v, h_v', step.msgOps, h_step)
    && h_step.SendNewViewMsgStep?
    && Replica.SendNewViewMsg(h_c, h_v, h_v', step.msgOps)
  }

  lemma SendNewViewMsgStepPreservesInv(c: Constants, v:Variables, v':Variables, 
                                  step:Step, h_step:Replica.Step)
    requires Inv(c, v)
    requires SendNewViewMsgStepStepWasTaken(c, v, v', step, h_step)
    ensures Inv(c, v')
  {
    CommitMsgStability(c, v, v', step);
  }

  predicate RecvNewViewMsgStepStepWasTaken(c: Constants, v:Variables, v':Variables,
                                   step:Step, h_step:Replica.Step)
    requires Inv(c, v)
  {
    && NextStep(c, v, v', step)
    && IsHonestReplica(c, step.id)
    && var h_c := c.hosts[step.id].replicaConstants;
    && var h_v := v.hosts[step.id].replicaVariables;
    && var h_v' := v'.hosts[step.id].replicaVariables;
    && Replica.NextStep(h_c, h_v, h_v', step.msgOps, h_step)
    && h_step.RecvNewViewMsgStep?
    && Replica.RecvNewViewMsg(h_c, h_v, h_v', step.msgOps)
  }

  lemma RecvNewViewMsgStepPreservesInv(c: Constants, v:Variables, v':Variables, 
                                  step:Step, h_step:Replica.Step)
    requires Inv(c, v)
    requires RecvNewViewMsgStepStepWasTaken(c, v, v', step, h_step)
    ensures Inv(c, v')
  {
    CommitMsgStability(c, v, v', step);
  }

  lemma InvariantNext(c: Constants, v:Variables, v':Variables)
    requires Inv(c, v)
    requires Next(c, v, v')
    ensures Inv(c, v')
  {
    var step :| NextStep(c, v, v', step);

    if (c.clusterConfig.IsHonestReplica(step.id))
    {
      var h_c := c.hosts[step.id].replicaConstants;
      var h_v := v.hosts[step.id].replicaVariables;
      var h_v' := v'.hosts[step.id].replicaVariables;
      var h_step :| Replica.NextStep(h_c, h_v, h_v', step.msgOps, h_step);
      
      //QuorumOfPreparesInNetworkMonotonic(c, v, v', step, h_step); // not part of the proof yet
      
      match h_step
        case SendPrePrepareStep(seqID) => {
          SendPrePrepareStepPreservesInv(c, v, v', step, h_step);
        }
        case RecvPrePrepareStep() => {
          RecvPrePrepareStepPreservesInv(c, v, v', step, h_step);
        }
        case SendPrepareStep(seqID) => {
          SendPrepareStepPreservesInv(c, v, v', step, h_step);
        }
        case RecvPrepareStep() => { 
          RecvPrepareStepPreservesInv(c, v, v', step, h_step);
        }
        case SendCommitStep(seqID) => {
          SendCommitStepPreservesInv(c, v, v', step, h_step);
        }
        case RecvCommitStep() => {
          RecvCommitStepPreservesInv(c, v, v', step, h_step);
        }
        case DoCommitStep(seqID) => { 
          DoCommitStepPreservesInv(c, v, v', step, h_step);
        }
        case ExecuteStep(seqID) => {
          ExecuteStepPreservesInv(c, v, v', step, h_step);
        }
        case SendCheckpointStep(seqID) => {
          SendCheckpointStepPreservesInv(c, v, v', step, h_step);
        }
        case RecvCheckpointStep() => {
          RecvCheckpointStepPreservesInv(c, v, v', step, h_step);
        }
        case AdvanceWorkingWindowStep(seqID, checkpointsQuorum) => {
          AdvanceWorkingWindowStepPreservesInv(c, v, v', step, h_step);
        }
        case PerformStateTransferStep(seqID, checkpointsQuorum) => {
          PerformStateTransferStepPreservesInv(c, v, v', step, h_step);
        }
        case LeaveViewStep(newView) => {
          LeaveViewStepPreservesInv(c, v, v', step, h_step);
        }
        case SendViewChangeMsgStep() => {
          SendViewChangeMsgStepPreservesInv(c, v, v', step, h_step);
        }
        case RecvViewChangeMsgStep() => {
          RecvViewChangeMsgStepPreservesInv(c, v, v', step, h_step);
        }
        case SelectQuorumOfViewChangeMsgsStep(viewChangeMsgsSelectedByPrimary) => {
          SelectQuorumOfViewChangeMsgsStepPreservesInv(c, v, v', step, h_step);
        }
        case SendNewViewMsgStep() => {
          SendNewViewMsgStepPreservesInv(c, v, v', step, h_step);
        }
        case RecvNewViewMsgStep() => {
          RecvNewViewMsgStepPreservesInv(c, v, v', step, h_step);
        }

    } else if (c.clusterConfig.IsClient(step.id)) {

      var h_c := c.hosts[step.id].clientConstants;
      var h_v := v.hosts[step.id].clientVariables;
      var h_v' := v'.hosts[step.id].clientVariables;
      var h_step :| Client.NextStep(h_c, h_v, h_v', step.msgOps, h_step);

      match h_step
        case SendClientOperationStep() => {
          SendClientOperationPreservesInv(c, v, v', step, h_step);
        }
    } else if(c.clusterConfig.IsFaultyReplica(step.id)) {
      InvNextFaulty(c, v, v', step);
      CommitMsgStability(c, v, v', step);
    } else {
      assert false; // Should not be possible
    }
  }

  lemma InvNextFaulty(c: Constants, v:Variables, v':Variables, step: Step)
    requires v.WF(c)
    requires c.clusterConfig.IsFaultyReplica(step.id)
    requires HonestReplicasLockOnCommitForGivenView(c, v)
    requires NextStep(c, v, v', step)
    ensures HonestReplicasLockOnCommitForGivenView(c, v')
  {
    reveal_HonestReplicasLockOnCommitForGivenView();
    assert forall msg | msg in v'.network.sentMsgs && IsHonestReplica(c, msg.sender) :: msg in v.network.sentMsgs;
  }

  lemma InvariantInductive(c: Constants, v:Variables, v':Variables)
    ensures Init(c, v) ==> Inv(c, v)
    ensures Inv(c, v) && Next(c, v, v') ==> Inv(c, v')
    //ensures Inv(c, v) ==> Safety(c, v)
  {
    if Init(c, v) {
      reveal_RecordedCommitsClientOpsMatchPrePrepare();
      reveal_EveryCommitIsSupportedByPreviouslySentPrepares();
      reveal_EveryPrepareClientOpMatchesRecordedPrePrepare();
      reveal_EveryCommitClientOpMatchesRecordedPrePrepare();
      reveal_HonestReplicasLockOnCommitForGivenView();
      reveal_CommitMsgsFromHonestSendersAgree();
      assert Inv(c, v);
    }
    if Inv(c, v) && Next(c, v, v') {
      InvariantNext(c, v, v');
    }
  }
}
