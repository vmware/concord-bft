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

  type Message = Network.Message<Messages.Message>

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
                :: (&& var msg := v.hosts[replicaIdx].replicaVariables.workingWindow.prePreparesRcvd[seqID].value;
                    && msg in v.network.sentMsgs
                    && msg.payload.seqID == seqID)) // The key we stored matches what is in the msg
                    
  }

  predicate RecordedPreparesRecvdCameFromNetworkForHost(c:Constants, v:Variables, observer:HostId)
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

  predicate {:opaque} RecordedPreparesRecvdCameFromNetwork(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall observer | 
            && IsHonestReplica(c, observer)
                :: RecordedPreparesRecvdCameFromNetworkForHost(c, v, observer))
  }

  predicate {:opaque} EveryCommitMsgIsSupportedByAQuorumOfSentPrepares(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall commitMsg | && commitMsg in v.network.sentMsgs 
                           && commitMsg.payload.Commit? 
                           && IsHonestReplica(c, commitMsg.sender)
          :: QuorumOfPreparesInNetwork(c, v, commitMsg.payload.view, 
                                       commitMsg.payload.seqID, commitMsg.payload.operationWrapper) )
  }

  predicate {:opaque} EveryCommitMsgIsSupportedByAQuorumOfRecordedPrepares(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall commitMsg | && commitMsg in v.network.sentMsgs 
                           && commitMsg.payload.Commit? 
                           && IsHonestReplica(c, commitMsg.sender)
                           && var committer_c := c.hosts[commitMsg.sender].replicaConstants;
                           && var committer_v := v.hosts[commitMsg.sender].replicaVariables;
                           && committer_v.view == commitMsg.payload.view
                           && commitMsg.payload.seqID in committer_v.workingWindow.getActiveSequenceIDs(committer_c)
          :: && var committer_c := c.hosts[commitMsg.sender].replicaConstants;
             && var committer_v := v.hosts[commitMsg.sender].replicaVariables;
             && |Replica.ExtractPreparesFromWorkingWindow(committer_c, committer_v, commitMsg.payload.seqID)| >= c.clusterConfig.AgreementQuorum())
  }

  predicate CertificateComportsWithCommit(c:Constants, certificate:Messages.PreparedCertificate, commitMsg:Message) {
    && certificate.valid(c.clusterConfig, commitMsg.payload.seqID)
    && !certificate.empty()
    && certificate.prototype().view >= commitMsg.payload.view
    && (certificate.prototype().view == commitMsg.payload.view 
        ==> certificate.prototype().operationWrapper == commitMsg.payload.operationWrapper)
  }

  predicate {:opaque} EveryCommitMsgIsRememberedByItsSender(c:Constants, v:Variables) { //TODO: this does not cover Checkpointing
    && v.WF(c)
    && (forall commitMsg | && commitMsg in v.network.sentMsgs 
                           && commitMsg.payload.Commit? 
                           && IsHonestReplica(c, commitMsg.sender)
                           && var h_c := c.hosts[commitMsg.sender].replicaConstants;
                           && var h_v := v.hosts[commitMsg.sender].replicaVariables;
                           && commitMsg.payload.seqID in h_v.workingWindow.getActiveSequenceIDs(h_c) //TODO: remove when Checkpointing gets enabled
          :: && var h_c := c.hosts[commitMsg.sender].replicaConstants;
             && var h_v := v.hosts[commitMsg.sender].replicaVariables;
             && var certificate := Replica.ExtractCertificateForSeqID(h_c, h_v, commitMsg.payload.seqID);
             && CertificateComportsWithCommit(c, certificate, commitMsg)
             )
  }

  predicate {:opaque} EveryCommitMsgIsRememberedByVCMsgs(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall commitMsg, viewChangeMsg | 
                           && commitMsg in v.network.sentMsgs
                           && viewChangeMsg in v.network.sentMsgs
                           && commitMsg.payload.Commit?
                           && viewChangeMsg.payload.ViewChangeMsg?
                           && viewChangeMsg.sender == commitMsg.sender
                           && IsHonestReplica(c, commitMsg.sender)
                           && commitMsg.payload.seqID > viewChangeMsg.payload.lastStableCheckpoint
                           && commitMsg.payload.view < viewChangeMsg.payload.newView
          :: && var certificate := viewChangeMsg.payload.certificates[commitMsg.payload.seqID];
             && CertificateComportsWithCommit(c, certificate, commitMsg)
             )
  }

  predicate {:opaque} ViewChangeMsgsFromHonestInNetworkAreValid(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall viewChangeMsg |
                && viewChangeMsg in v.network.sentMsgs
                && viewChangeMsg.payload.ViewChangeMsg?
                && var replicaIdx := viewChangeMsg.sender;
                && IsHonestReplica(c, replicaIdx)
                   :: && var replicaIdx := viewChangeMsg.sender;
                      && viewChangeMsg.payload.checked(c.clusterConfig, v.network.sentMsgs))
  }

  predicate {:opaque} TemporarilyDisableCheckpointing(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall replicaID | && IsHonestReplica(c, replicaID)
                              :: v.hosts[replicaID].replicaVariables.workingWindow.lastStableCheckpoint == 0)
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
          :: && c.clusterConfig.IsReplica(sender)
             && v.hosts[replicaIdx].replicaVariables.workingWindow.preparesRcvd[seqID][sender].sender == sender
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

  predicate {:opaque} EverySentIntraViewMsgIsInWorkingWindowOrBefore(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall msg | && msg in v.network.sentMsgs
                           && msg.payload.IsIntraViewMsg()
                           && IsHonestReplica(c, msg.sender)
          :: && var replicaVariables := v.hosts[msg.sender].replicaVariables;
             && var replicaConstants := c.hosts[msg.sender].replicaConstants;
             && msg.payload.seqID <= replicaVariables.workingWindow.lastStableCheckpoint + replicaConstants.clusterConfig.workingWindowSize)
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

  predicate {:opaque} EveryPrepareMatchesRecordedPrePrepare(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall prepareMsg | 
                      && prepareMsg in v.network.sentMsgs
                      && prepareMsg.payload.Prepare?
                      && IsHonestReplica(c, prepareMsg.sender)
                      && var replicaVariables := v.hosts[prepareMsg.sender].replicaVariables;
                      && var replicaConstants := c.hosts[prepareMsg.sender].replicaConstants;
                      && prepareMsg.payload.seqID in replicaVariables.workingWindow.getActiveSequenceIDs(replicaConstants)
          :: && var recordedPrePrepare := 
                v.hosts[prepareMsg.sender].replicaVariables.workingWindow.prePreparesRcvd[prepareMsg.payload.seqID];
             && var replicaVariables := v.hosts[prepareMsg.sender].replicaVariables;
             && recordedPrePrepare.Some?
             && prepareMsg.payload.operationWrapper == recordedPrePrepare.value.payload.operationWrapper
             && prepareMsg.payload.view == replicaVariables.view)
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

  predicate {:opaque} RecordedNewViewMsgsAreValid(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall replicaIdx, newViewMsg | 
                        && c.clusterConfig.IsHonestReplica(replicaIdx)
                        && var replicaVariables := v.hosts[replicaIdx].replicaVariables;
                        && newViewMsg in replicaVariables.newViewMsgsRecvd.msgs
                          :: && newViewMsg.payload.valid(c.clusterConfig)
                             && c.clusterConfig.PrimaryForView(newViewMsg.payload.newView) == newViewMsg.sender)
  }

  predicate Inv(c: Constants, v:Variables) {
    //&& PrePreparesCarrySameClientOpsForGivenSeqID(c, v)
    // Do not remove, lite invariant about internal honest Node invariants:
    && v.WF(c)
    && RecordedNewViewMsgsAreValid(c, v)
    && RecordedPreparesHaveValidSenderID(c, v)
    //&& SentPreparesMatchRecordedPrePrepareIfHostInSameView(c, v)
    && RecordedPrePreparesRecvdCameFromNetwork(c, v)
    && RecordedPreparesRecvdCameFromNetwork(c, v)
    && RecordedPrePreparesMatchHostView(c, v)
    && RecordedPreparesMatchHostView(c, v)
    && EveryCommitMsgIsSupportedByAQuorumOfSentPrepares(c, v)
    && EveryCommitMsgIsSupportedByAQuorumOfRecordedPrepares(c, v)
    && RecordedPreparesClientOpsMatchPrePrepare(c, v)
    && RecordedCommitsClientOpsMatchPrePrepare(c, v)
    && EverySentIntraViewMsgIsInWorkingWindowOrBefore(c, v)
    && EverySentIntraViewMsgIsForAViewLessOrEqualToSenderView(c, v)
    && EveryPrepareMatchesRecordedPrePrepare(c, v)
    && EveryCommitClientOpMatchesRecordedPrePrepare(c, v)
    && HonestReplicasLockOnPrepareForGivenView(c, v)
    && HonestReplicasLockOnCommitForGivenView(c, v)
    && CommitMsgsFromHonestSendersAgree(c, v)
    && RecordedCheckpointsRecvdCameFromNetwork(c, v)
    && UnCommitableAgreesWithPrepare(c, v)
    && UnCommitableAgreesWithRecordedPrePrepare(c, v)//Unfinished proof.
    && HonestReplicasLeaveViewsBehind(c, v)
    && RecordedNewViewMsgsContainSentVCMsgs(c, v)
    && RecordedViewChangeMsgsCameFromNetwork(c, v)
    && OneViewChangeMessageFromReplicaPerView(c, v)
    && SentViewChangesMsgsComportWithSentCommits(c, v)//Unfinished proof. This predicate is false.
    && EveryCommitMsgIsRememberedByItsSender(c, v)//Unfinished proof.
    && RecordedViewChangeMsgsAreValid(c, v)
    && ViewChangeMsgsFromHonestInNetworkAreValid(c, v)//Unfinished proof.
    && TemporarilyDisableCheckpointing(c, v)
    // && NewViewMsgsReflectPriorCommitCertificates(c, v)
    // && CommitCertificateEstablishesUncommitableInView(c, v)
    // "AllPreparedCertsInWorkingWindowAreValid"
  }

  function sentPreparesForSeqID(c: Constants, v:Variables, view:nat, seqID:Messages.SequenceID,
                                  operationWrapper:Messages.OperationWrapper) : set<Message> 
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
                            msg1:Message, msg2:Message)
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
    reveal_RecordedPreparesRecvdCameFromNetwork();
    reveal_RecordedPreparesMatchHostView();
    reveal_RecordedPreparesClientOpsMatchPrePrepare();
    reveal_HonestReplicasLockOnPrepareForGivenView();
    reveal_EveryCommitMsgIsSupportedByAQuorumOfSentPrepares();

    var prepares1 := sentPreparesForSeqID(c, v, msg1.payload.view, msg1.payload.seqID, msg1.payload.operationWrapper);
    var senders1 := Messages.sendersOf(prepares1);
    assert |senders1| >= c.clusterConfig.AgreementQuorum();

    var h_c := c.hosts[step.id].replicaConstants;
    var h_v := v.hosts[step.id].replicaVariables;
    var h_v' := v'.hosts[step.id].replicaVariables;
    var h_step :| Replica.NextStep(h_c, h_v, h_v', step.msgOps, h_step);

    h_v.workingWindow.reveal_Shift();

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

  lemma HonestPreservesRecordedNewViewMsgsAreValid(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures RecordedNewViewMsgsAreValid(c, v')
  {
    reveal_RecordedNewViewMsgsAreValid();
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

  lemma HonestPreservesRecordedPreparesRecvdCameFromNetwork(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures RecordedPreparesRecvdCameFromNetwork(c, v')
  {
    reveal_RecordedPreparesRecvdCameFromNetwork();

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

  lemma AlwaysPreservesEveryCommitMsgIsSupportedByAQuorumOfSentPrepares(c: Constants, v:Variables, v':Variables, step:Step)
    requires Inv(c, v)
    requires NextStep(c, v, v', step)
    ensures EveryCommitMsgIsSupportedByAQuorumOfSentPrepares(c, v')
  {
    reveal_EveryCommitMsgIsSupportedByAQuorumOfSentPrepares();
    // A proof of EveryCommitMsgIsSupportedByAQuorumOfSentPrepares,
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
        reveal_RecordedPreparesRecvdCameFromNetwork();
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

  lemma HonestPreservesEveryCommitMsgIsSupportedByAQuorumOfRecordedPrepares(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures EveryCommitMsgIsSupportedByAQuorumOfRecordedPrepares(c, v')
  {
    h_v.workingWindow.reveal_Shift();
    reveal_RecordedPreparesRecvdCameFromNetwork();
    reveal_EveryCommitMsgIsSupportedByAQuorumOfRecordedPrepares();
    reveal_TemporarilyDisableCheckpointing();
    reveal_EverySentIntraViewMsgIsForAViewLessOrEqualToSenderView();
    reveal_EverySentIntraViewMsgIsInWorkingWindowOrBefore();

    if (h_step.SendCommitStep?) {
    forall commitMsg | && commitMsg in v'.network.sentMsgs 
                           && commitMsg.payload.Commit? 
                           && IsHonestReplica(c, commitMsg.sender)
                           && var h_c := c.hosts[commitMsg.sender].replicaConstants;
                           && var h_v' := v'.hosts[commitMsg.sender].replicaVariables;
                           && h_v'.view == commitMsg.payload.view
                           && commitMsg.payload.seqID in h_v'.workingWindow.getActiveSequenceIDs(h_c)
          ensures && var h_c := c.hosts[commitMsg.sender].replicaConstants;
                  && var h_v' := v'.hosts[commitMsg.sender].replicaVariables;
                  && |Replica.ExtractPreparesFromWorkingWindow(h_c, h_v', commitMsg.payload.seqID)| >= c.clusterConfig.AgreementQuorum()
      {
        var h_c := c.hosts[commitMsg.sender].replicaConstants;
        var h_v' := v'.hosts[commitMsg.sender].replicaVariables;
        if(commitMsg !in v.network.sentMsgs) {
          CountPrepareMessages2(c, v, commitMsg.sender, h_c, h_v, h_step.seqID);
        }
      }
      assert EveryCommitMsgIsSupportedByAQuorumOfRecordedPrepares(c, v');
    } else if(h_step.RecvPrepareStep?) {
      var h_c := c.hosts[step.id].replicaConstants;
      var h_v' := v'.hosts[step.id].replicaVariables;
      forall commitMsg | && commitMsg in v'.network.sentMsgs 
                           && commitMsg.payload.Commit? 
                           && IsHonestReplica(c, commitMsg.sender)
                           && var committer_c := c.hosts[commitMsg.sender].replicaConstants;
                           && var committer_v' := v'.hosts[commitMsg.sender].replicaVariables;
                           && committer_v'.view == commitMsg.payload.view
                           && commitMsg.payload.seqID in committer_v'.workingWindow.getActiveSequenceIDs(h_c)
          ensures && var committer_c := c.hosts[commitMsg.sender].replicaConstants;
                  && var committer_v' := v'.hosts[commitMsg.sender].replicaVariables;
                  && |Replica.ExtractPreparesFromWorkingWindow(committer_c, committer_v', commitMsg.payload.seqID)| >= c.clusterConfig.AgreementQuorum()
      {
        var seqID := commitMsg.payload.seqID;
        var committer_c := c.hosts[commitMsg.sender].replicaConstants;
        var committer_v := v.hosts[commitMsg.sender].replicaVariables;
        var committer_v' := v'.hosts[commitMsg.sender].replicaVariables;
        if(commitMsg.sender == step.id) {
          forall p | p in Replica.ExtractPreparesFromWorkingWindow(committer_c, committer_v, seqID)
                  ensures p in Replica.ExtractPreparesFromWorkingWindow(committer_c, committer_v', seqID) {
            assert p.sender in committer_v'.workingWindow.preparesRcvd[seqID]; // Trigger
          }
          Library.SubsetCardinality(Replica.ExtractPreparesFromWorkingWindow(committer_c, committer_v, seqID),
                                    Replica.ExtractPreparesFromWorkingWindow(committer_c, committer_v', seqID));
        }
      }
      assert EveryCommitMsgIsSupportedByAQuorumOfRecordedPrepares(c, v');
    } else {
      assert EveryCommitMsgIsSupportedByAQuorumOfRecordedPrepares(c, v');
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

  lemma CountPrepareMessagesInWorkingWindow(c:Constants, v:Variables, replicaId:HostId, seqID:Messages.SequenceID)
    requires v.WF(c)
    requires RecordedPreparesHaveValidSenderID(c, v)
    requires IsHonestReplica(c, replicaId)
    requires seqID in v.hosts[replicaId].replicaVariables.workingWindow.getActiveSequenceIDs(c.hosts[replicaId].replicaConstants)
    ensures |v.hosts[replicaId].replicaVariables.workingWindow.preparesRcvd[seqID].Values| 
         == |v.hosts[replicaId].replicaVariables.workingWindow.preparesRcvd[seqID].Keys|
  {
    reveal_RecordedPreparesHaveValidSenderID();
    var proofSet := v.hosts[replicaId].replicaVariables.workingWindow.preparesRcvd[seqID];
    forall sender | sender in proofSet.Keys ensures proofSet[sender].sender == sender {
      assert c.clusterConfig.IsReplica(sender);
    }
    CountPrepareMessages(proofSet);
    // assert forall sender | && sender in v.hosts[replicaId].replicaVariables.workingWindow.preparesRcvd
    //               :: && c.clusterConfig.IsReplica(sender)
    //                  && v.hosts[replicaId].replicaVariables.workingWindow.preparesRcvd[seqID][sender].sender == sender;
  }

  lemma CountPrepareMessages2(c:Constants, v:Variables, replicaID:HostId, h_c:Replica.Constants, h_v:Replica.Variables, seqID:Messages.SequenceID)
    requires v.WF(c)
    requires IsHonestReplica(c, replicaID)
    requires h_c == c.hosts[replicaID].replicaConstants
    requires h_v == v.hosts[replicaID].replicaVariables
    requires seqID in h_v.workingWindow.getActiveSequenceIDs(h_c)
    requires RecordedPreparesHaveValidSenderID(c, v)
    ensures |Replica.ExtractPreparesFromWorkingWindow(h_c, h_v, seqID)| == |h_v.workingWindow.preparesRcvd[seqID]|
  {
    var senders := h_v.workingWindow.preparesRcvd[seqID].Keys;
    var f := sender requires sender in senders => h_v.workingWindow.preparesRcvd[seqID][sender];
     forall x, y | && x in senders
                           && y in senders
                           && f(x) == f(y)
                        ensures x == y {
      reveal_RecordedPreparesHaveValidSenderID();
      assert c.clusterConfig.IsReplica(x); // Trigger
      assert c.clusterConfig.IsReplica(y); // Trigger
    }
    var filtered := set x | x in senders :: f(x);
    var filtered2 := set x | x in senders :: f(x);
    assert filtered == filtered2;

    MappedSetCardinality(senders, f, filtered);
    assert |senders| == |filtered|;
    assert h_v.workingWindow.preparesRcvd[seqID].Keys == senders;
    assert |h_v.workingWindow.preparesRcvd[seqID]|  == |h_v.workingWindow.preparesRcvd[seqID].Keys|;
    assert Replica.ExtractPreparesFromWorkingWindow(h_c, h_v, seqID) == filtered;
  }

  lemma HonestPreservesEveryPrepareMatchesRecordedPrePrepare(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures EveryPrepareMatchesRecordedPrePrepare(c, v')

  {
    reveal_EveryPrepareMatchesRecordedPrePrepare();
    reveal_EverySentIntraViewMsgIsInWorkingWindowOrBefore();
    reveal_EverySentIntraViewMsgIsForAViewLessOrEqualToSenderView();
    h_v.workingWindow.reveal_Shift();
  }

  lemma HonestPreservesEverySentIntraViewMsgIsInWorkingWindowOrBefore(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures EverySentIntraViewMsgIsInWorkingWindowOrBefore(c, v')
  {
    reveal_EverySentIntraViewMsgIsInWorkingWindowOrBefore();
  }

  lemma HonestPreservesEverySentIntraViewMsgIsForAViewLessOrEqualToSenderView(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures EverySentIntraViewMsgIsForAViewLessOrEqualToSenderView(c, v')
  {
    reveal_EverySentIntraViewMsgIsForAViewLessOrEqualToSenderView();
  }

  lemma HonestPreservesEveryCommitClientOpMatchesRecordedPrePrepare(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures EveryCommitClientOpMatchesRecordedPrePrepare(c, v')
  {
    reveal_EverySentIntraViewMsgIsInWorkingWindowOrBefore();
    reveal_EverySentIntraViewMsgIsForAViewLessOrEqualToSenderView();
    reveal_EveryCommitClientOpMatchesRecordedPrePrepare();
    h_v.workingWindow.reveal_Shift();
  }

  lemma HonestPreservesHonestReplicasLockOnPrepareForGivenView(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures HonestReplicasLockOnPrepareForGivenView(c, v')
  {
    reveal_HonestReplicasLockOnPrepareForGivenView();
    reveal_EveryPrepareMatchesRecordedPrePrepare();
  }

  lemma HonestPreservesHonestReplicasLockOnCommitForGivenView(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures HonestReplicasLockOnCommitForGivenView(c, v')
  {
    reveal_HonestReplicasLockOnCommitForGivenView();
    reveal_EveryCommitClientOpMatchesRecordedPrePrepare();
  }

  lemma HonestPreservesCommitMsgsFromHonestSendersAgree(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
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

  lemma HonestPreservesRecordedCheckpointsRecvdCameFromNetwork(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures RecordedCheckpointsRecvdCameFromNetwork(c, v')
  {
    reveal_RecordedCheckpointsRecvdCameFromNetwork();
    h_v.workingWindow.reveal_Shift();
  }

  datatype HonestInfo = HonestInfo(h_v:Replica.Variables, h_step:Replica.Step) | NotHonest()

  lemma TriviallyPreserveUnCommitableAgreesWithPrepare(c: Constants, v:Variables, v':Variables, step:Step, info:HonestInfo)
    requires Inv(c, v)
    requires match(info) {
               case HonestInfo(h_v, h_step) => (&& HonestReplicaStepTaken(c, v, v', step, h_v, h_step) 
                                                && !h_step.SendPrepareStep?
                                                && !h_step.LeaveViewStep?)
               case NotHonest() => (|| (NextStep(c, v, v', step) && c.clusterConfig.IsFaultyReplica(step.id))
                                    || (NextStep(c, v, v', step) &&  c.clusterConfig.IsClient(step.id)))
             }
    ensures UnCommitableAgreesWithPrepare(c, v')
  {
    reveal_UnCommitableAgreesWithPrepare();
    forall prepareMsg:Message,
             priorView:nat,
             priorOperationWrapper:Messages.OperationWrapper
                  | && prepareMsg.payload.Prepare? 
                    && priorView < prepareMsg.payload.view
                    && c.clusterConfig.IsHonestReplica(prepareMsg.sender)
                    && priorOperationWrapper != prepareMsg.payload.operationWrapper
                    && prepareMsg in v'.network.sentMsgs
                    ensures UnCommitableInView(c, v', prepareMsg.payload.seqID, priorView, priorOperationWrapper) {
      assert UnCommitableInView(c, v, prepareMsg.payload.seqID, priorView, priorOperationWrapper);
      assert ReplicasThatCanCommitInView(c, v', prepareMsg.payload.seqID, priorView, priorOperationWrapper) 
          == ReplicasThatCanCommitInView(c, v, prepareMsg.payload.seqID, priorView, priorOperationWrapper);
      // assert |ReplicasThatCanCommitInView(c, v', prepareMsg.payload.seqID, priorView, priorOperationWrapper)|
      //     == |ReplicasThatCanCommitInView(c, v, prepareMsg.payload.seqID, priorView, priorOperationWrapper)|;
    }
  }

  lemma TriviallyPreserveUnCommitableAgreesWithRecordedPrePrepare(c: Constants, v:Variables, v':Variables, step:Step, info:HonestInfo)
    requires Inv(c, v)
    requires match(info) {
           case HonestInfo(h_v, h_step) => (&& HonestReplicaStepTaken(c, v, v', step, h_v, h_step) 
                                            && !h_step.RecvPrePrepareStep?
                                            && !h_step.LeaveViewStep?
                                            && !h_step.SendPrePrepareStep?)
           case NotHonest() => (|| (NextStep(c, v, v', step) && c.clusterConfig.IsFaultyReplica(step.id))
                                || (NextStep(c, v, v', step) &&  c.clusterConfig.IsClient(step.id)))
         }
    ensures UnCommitableAgreesWithRecordedPrePrepare(c, v')
  {
    if info.HonestInfo? 
    {
      info.h_v.workingWindow.reveal_Shift();
    }
    reveal_UnCommitableAgreesWithRecordedPrePrepare();
    forall replicaIdx, seqID, priorView:nat, priorOperationWrapper:Messages.OperationWrapper | 
              && IsHonestReplica(c, replicaIdx)
              && var replicaVariables := v'.hosts[replicaIdx].replicaVariables;
              && var replicaConstants := c.hosts[replicaIdx].replicaConstants;
              && seqID in replicaVariables.workingWindow.getActiveSequenceIDs(replicaConstants)
              && replicaVariables.workingWindow.prePreparesRcvd[seqID].Some?
              && var prePrepareMsg := replicaVariables.workingWindow.prePreparesRcvd[seqID].value;
              && priorOperationWrapper != prePrepareMsg.payload.operationWrapper
              && priorView < prePrepareMsg.payload.view
                ensures && var prePrepareMsg := v'.hosts[replicaIdx].replicaVariables.workingWindow.prePreparesRcvd[seqID].value;
                        && UnCommitableInView(c, v', prePrepareMsg.payload.seqID, priorView, priorOperationWrapper) {
      var prePrepareMsg := v'.hosts[replicaIdx].replicaVariables.workingWindow.prePreparesRcvd[seqID].value;
      assert UnCommitableInView(c, v, prePrepareMsg.payload.seqID, priorView, priorOperationWrapper);
      assert ReplicasThatCanCommitInView(c, v', prePrepareMsg.payload.seqID, priorView, priorOperationWrapper) 
          == ReplicasThatCanCommitInView(c, v, prePrepareMsg.payload.seqID, priorView, priorOperationWrapper);
    }
  }

  lemma HonestLeaveViewStepPreservesUnCommitableAgreesWithRecordedPrePrepare(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    requires h_step.LeaveViewStep?
    ensures UnCommitableAgreesWithRecordedPrePrepare(c, v')
  {
    reveal_UnCommitableAgreesWithRecordedPrePrepare();
    forall replicaIdx, seqID, priorView:nat, priorOperationWrapper:Messages.OperationWrapper | 
      && IsHonestReplica(c, replicaIdx)
      && var replicaVariables := v'.hosts[replicaIdx].replicaVariables;
      && var replicaConstants := c.hosts[replicaIdx].replicaConstants;
      && seqID in replicaVariables.workingWindow.getActiveSequenceIDs(replicaConstants)
      && replicaVariables.workingWindow.prePreparesRcvd[seqID].Some?
      && var prePrepareMsg := replicaVariables.workingWindow.prePreparesRcvd[seqID].value;
      && priorOperationWrapper != prePrepareMsg.payload.operationWrapper
      && priorView < prePrepareMsg.payload.view
        ensures && var prePrepareMsg := v'.hosts[replicaIdx].replicaVariables.workingWindow.prePreparesRcvd[seqID].value;
          && UnCommitableInView(c, v', prePrepareMsg.payload.seqID, priorView, priorOperationWrapper) {
      var prePrepareMsg := v'.hosts[replicaIdx].replicaVariables.workingWindow.prePreparesRcvd[seqID].value;
      assert UnCommitableInView(c, v, prePrepareMsg.payload.seqID, priorView, priorOperationWrapper);
      Library.SubsetCardinality(ReplicasThatCanCommitInView(c, v', prePrepareMsg.payload.seqID, priorView, priorOperationWrapper), 
                                ReplicasThatCanCommitInView(c, v, prePrepareMsg.payload.seqID, priorView, priorOperationWrapper));

    }
  }

  lemma HonestLeaveViewStepPreservesUnCommitableAgreesWithPrepare(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    requires h_step.LeaveViewStep?
    ensures UnCommitableAgreesWithPrepare(c, v')
  {
    reveal_UnCommitableAgreesWithPrepare();
    forall prepareMsg:Message,
             priorView:nat,
             priorOperationWrapper:Messages.OperationWrapper
                  | && prepareMsg.payload.Prepare? 
                    && priorView < prepareMsg.payload.view
                    && c.clusterConfig.IsHonestReplica(prepareMsg.sender)
                    && priorOperationWrapper != prepareMsg.payload.operationWrapper
                    && prepareMsg in v'.network.sentMsgs
                    ensures UnCommitableInView(c, v', prepareMsg.payload.seqID, priorView, priorOperationWrapper) {
      assert UnCommitableInView(c, v, prepareMsg.payload.seqID, priorView, priorOperationWrapper);
      // assert ReplicasThatCanCommitInView(c, v', prepareMsg.payload.seqID, priorView, priorOperationWrapper) 
      //     <= ReplicasThatCanCommitInView(c, v, prepareMsg.payload.seqID, priorView, priorOperationWrapper);
      // assert |ReplicasThatCanCommitInView(c, v', prepareMsg.payload.seqID, priorView, priorOperationWrapper)|
      //     <= |ReplicasThatCanCommitInView(c, v, prepareMsg.payload.seqID, priorView, priorOperationWrapper)|;
      Library.SubsetCardinality(ReplicasThatCanCommitInView(c, v', prepareMsg.payload.seqID, priorView, priorOperationWrapper), 
                                ReplicasThatCanCommitInView(c, v, prepareMsg.payload.seqID, priorView, priorOperationWrapper));
    }
  }

  lemma HonestSendPrepareStepPreservesUnCommitableAgreesWithPrepare(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    requires h_step.SendPrepareStep?
    ensures UnCommitableAgreesWithPrepare(c, v')
  {
    ///
    // reveal_EveryCommitClientOpMatchesRecordedPrePrepare();
    // reveal_RecordedNewViewMsgsAreValid();
    // reveal_RecordedPreparesHaveValidSenderID();
    reveal_RecordedPrePreparesRecvdCameFromNetwork();
    // reveal_RecordedPreparesRecvdCameFromNetwork();
    // reveal_RecordedPreparesMatchHostView();
    // reveal_RecordedPreparesClientOpsMatchPrePrepare();
    // reveal_RecordedCommitsClientOpsMatchPrePrepare();
    // reveal_EverySentIntraViewMsgIsInWorkingWindowOrBefore();
    // reveal_EverySentIntraViewMsgIsForAViewLessOrEqualToSenderView();
    // reveal_EveryPrepareMatchesRecordedPrePrepare();
    // reveal_EveryCommitClientOpMatchesRecordedPrePrepare();
    // reveal_HonestReplicasLockOnPrepareForGivenView();
    // reveal_HonestReplicasLockOnCommitForGivenView();
    // reveal_CommitMsgsFromHonestSendersAgree();
    // reveal_RecordedCheckpointsRecvdCameFromNetwork();
    // reveal_HonestReplicasLockOnCommitForGivenView();
    // reveal_EveryCommitMsgIsSupportedByAQuorumOfSentPrepares();
    // h_v.workingWindow.reveal_Shift();
    reveal_UnCommitableAgreesWithRecordedPrePrepare();
    reveal_RecordedPrePreparesMatchHostView();
    ///

    reveal_UnCommitableAgreesWithPrepare();
    //TODO: minimise proof
    forall prepareMsg:Message,
             priorView:nat,
             priorOperationWrapper:Messages.OperationWrapper
                  | && prepareMsg.payload.Prepare? 
                    && priorView < prepareMsg.payload.view
                    && c.clusterConfig.IsHonestReplica(prepareMsg.sender)
                    && priorOperationWrapper != prepareMsg.payload.operationWrapper
                    && prepareMsg in v'.network.sentMsgs
                    ensures UnCommitableInView(c, v', prepareMsg.payload.seqID, priorView, priorOperationWrapper) {
      if prepareMsg in v.network.sentMsgs {
        assert UnCommitableInView(c, v, prepareMsg.payload.seqID, priorView, priorOperationWrapper);
        assert ReplicasThatCanCommitInView(c, v', prepareMsg.payload.seqID, priorView, priorOperationWrapper) 
            == ReplicasThatCanCommitInView(c, v, prepareMsg.payload.seqID, priorView, priorOperationWrapper);
      } else {
        assert h_v.workingWindow.prePreparesRcvd[prepareMsg.payload.seqID].Some?;
        var prePrepareMsg := h_v.workingWindow.prePreparesRcvd[prepareMsg.payload.seqID].value;
        assert prePrepareMsg.payload.seqID == prepareMsg.payload.seqID;
        assert UnCommitableInView(c, v, prePrepareMsg.payload.seqID, priorView, priorOperationWrapper);
        assert UnCommitableInView(c, v, prepareMsg.payload.seqID, priorView, priorOperationWrapper);
        assert ReplicasThatCanCommitInView(c, v', prepareMsg.payload.seqID, priorView, priorOperationWrapper) 
            == ReplicasThatCanCommitInView(c, v, prepareMsg.payload.seqID, priorView, priorOperationWrapper);
      }
      //END TODO

      // assert |ReplicasThatCanCommitInView(c, v', prepareMsg.payload.seqID, priorView, priorOperationWrapper)|
      //     == |ReplicasThatCanCommitInView(c, v, prepareMsg.payload.seqID, priorView, priorOperationWrapper)|;
      // assert |ReplicasThatCanCommitInView(c, v, prepareMsg.payload.seqID, priorView, priorOperationWrapper)| < c.clusterConfig.AgreementQuorum();
    }
  }

  /// End Of Case Splitting

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

  predicate AgreementQuorumOfMessages(c:Constants, msgs:set<Message>) {
    && c.WF()
    && |msgs| >= c.clusterConfig.AgreementQuorum()
    && Messages.UniqueSenders(msgs)
  }

  predicate isCommitQuorum(c:Constants,
                           v:Variables,
                           view:nat,
                           seqID:Messages.SequenceID,
                           operationWrapper:Messages.OperationWrapper,
                           sentCommits:set<Message>) 
    requires v.WF(c)
  {
    && sentCommits <= v.network.sentMsgs
    && AgreementQuorumOfMessages(c, sentCommits)
    && (forall msg | msg in sentCommits
                    :: && msg.payload.Commit?
                       && msg.payload.view == view
                       && msg.payload.seqID == seqID
                       && msg.payload.operationWrapper == operationWrapper
                       && msg.sender in getAllReplicas(c))
  }

  lemma UniqueSendersCardinality(msgs:set<Message>)
    requires Messages.UniqueSenders(msgs)
    ensures |Messages.sendersOf(msgs)| == |msgs|
  {
    Messages.reveal_UniqueSenders();
    if |msgs| > 0 {
      var m :| m in msgs;
      var subMsgs := msgs - {m};
      UniqueSendersCardinality(subMsgs);
      assert Messages.sendersOf(msgs) == Messages.sendersOf(subMsgs) + {m.sender};
    }
  }

  predicate ReplicaSentCommit(c:Constants,
                              v:Variables,
                              seqID:Messages.SequenceID,
                              view:nat,
                              operationWrapper:Messages.OperationWrapper,
                              replica:HostIdentifiers.HostId)
    requires v.WF(c)
  {
    && c.clusterConfig.IsHonestReplica(replica)
    && Network.Message(replica,
                       Messages.Commit(view,
                                       seqID,
                                       operationWrapper)) in v.network.sentMsgs
  }

  predicate ReplicasInViewOrLower(c:Constants,
                                  v:Variables,
                                  seqID:Messages.SequenceID,
                                  view:nat,
                                  operationWrapper:Messages.OperationWrapper,
                                  replica:HostIdentifiers.HostId)
    requires v.WF(c)
  {
    && c.clusterConfig.IsHonestReplica(replica)
    && v.hosts[replica].replicaVariables.view <= view
  }

  function ReplicasThatCanCommitInView(c:Constants,
                                       v:Variables,
                                       seqID:Messages.SequenceID,
                                       view:nat,
                                       operationWrapper:Messages.OperationWrapper)
                                       : set<HostIdentifiers.HostId>
    requires v.WF(c)
  {
    set replica | replica in getAllReplicas(c) &&
                   (|| ReplicaSentCommit(c, v, seqID, view, operationWrapper, replica)
                    || ReplicasInViewOrLower(c, v, seqID, view, operationWrapper, replica)
                    || c.clusterConfig.IsFaultyReplica(replica))
  }

  predicate UnCommitableInView(c:Constants,
                               v:Variables,
                               seqID:Messages.SequenceID,
                               view:nat,
                               operationWrapper:Messages.OperationWrapper) {
    && v.WF(c)
    && |ReplicasThatCanCommitInView(c, v, seqID, view, operationWrapper)| < c.clusterConfig.AgreementQuorum()
  }

  // UnCommitable agrees with Prepares.
  predicate {:opaque} UnCommitableAgreesWithPrepare(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall prepareMsg:Message,
               priorView:nat,
               priorOperationWrapper:Messages.OperationWrapper
                    | && prepareMsg.payload.Prepare? 
                      && priorView < prepareMsg.payload.view
                      && c.clusterConfig.IsHonestReplica(prepareMsg.sender)
                      && priorOperationWrapper != prepareMsg.payload.operationWrapper
                      && prepareMsg in v.network.sentMsgs
                         :: UnCommitableInView(c, v, prepareMsg.payload.seqID, priorView, priorOperationWrapper))
  }

  predicate {:opaque} UnCommitableAgreesWithRecordedPrePrepare(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall replicaIdx, seqID, priorView:nat, priorOperationWrapper:Messages.OperationWrapper | 
              && IsHonestReplica(c, replicaIdx)
              && var replicaVariables := v.hosts[replicaIdx].replicaVariables;
              && var replicaConstants := c.hosts[replicaIdx].replicaConstants;
              && seqID in replicaVariables.workingWindow.getActiveSequenceIDs(replicaConstants)
              && replicaVariables.workingWindow.prePreparesRcvd[seqID].Some?
              && var prePrepareMsg := replicaVariables.workingWindow.prePreparesRcvd[seqID].value;
              && priorOperationWrapper != prePrepareMsg.payload.operationWrapper
              && priorView < prePrepareMsg.payload.view
                :: && var prePrepareMsg := v.hosts[replicaIdx].replicaVariables.workingWindow.prePreparesRcvd[seqID].value;
                   && UnCommitableInView(c, v, prePrepareMsg.payload.seqID, priorView, priorOperationWrapper))
  }

  predicate {:opaque} HonestReplicasLeaveViewsBehind(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall viewChangeMsg | && viewChangeMsg in v.network.sentMsgs
                               && viewChangeMsg.payload.ViewChangeMsg?
                               && IsHonestReplica(c, viewChangeMsg.sender)
                                  :: && var replicaVariables := v.hosts[viewChangeMsg.sender].replicaVariables;
                                     && replicaVariables.view >= viewChangeMsg.payload.newView)
  }

  predicate MessagesAreFromReplicas(c:Constants, msgs:set<Message>) {
    && c.WF()
    && (forall msg | msg in msgs :: c.clusterConfig.IsReplica(msg.sender))
  }

  predicate {:opaque} RecordedNewViewMsgsContainSentVCMsgs(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall replicaIdx, newViewMsg | && IsHonestReplica(c, replicaIdx)
                                        && var replicaVariables := v.hosts[replicaIdx].replicaVariables;
                                        && newViewMsg in replicaVariables.newViewMsgsRecvd.msgs
                                           :: && newViewMsg.payload.vcMsgs.msgs <= v.network.sentMsgs
                                              && MessagesAreFromReplicas(c, newViewMsg.payload.vcMsgs.msgs))
  }

  predicate {:opaque} RecordedPrePreparesMatchHostView(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall observer, seqID, sender | 
                           && IsHonestReplica(c, observer)
                           && c.clusterConfig.IsReplica(sender)
                           && var replicaVars := v.hosts[observer].replicaVariables;
                           && seqID in replicaVars.workingWindow.preparesRcvd
                           && replicaVars.workingWindow.prePreparesRcvd[seqID].Some?
                :: && var replicaVars := v.hosts[observer].replicaVariables;
                   && replicaVars.view == replicaVars.workingWindow.prePreparesRcvd[seqID].value.payload.view)
  }

  predicate {:opaque} RecordedViewChangeMsgsCameFromNetwork(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall replicaIdx, viewChangeMsg | 
                              && IsHonestReplica(c, replicaIdx)
                              && var replicaVariables := v.hosts[replicaIdx].replicaVariables;
                              && viewChangeMsg in replicaVariables.viewChangeMsgsRecvd.msgs
                                 :: viewChangeMsg in v.network.sentMsgs)
  }

  predicate {:opaque} OneViewChangeMessageFromReplicaPerView(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall msg1, msg2 | 
              && msg1 in v.network.sentMsgs
              && msg2 in v.network.sentMsgs
              && msg1.sender == msg2.sender
              && IsHonestReplica(c, msg1.sender)
              && msg1.payload.ViewChangeMsg?
              && msg2.payload.ViewChangeMsg?
              && msg1.payload.newView == msg2.payload.newView
                 :: msg1 == msg2)
  }

  predicate {:opaque} RecordedViewChangeMsgsAreValid(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall replicaIdx, viewChangeMsg | 
                              && IsHonestReplica(c, replicaIdx)
                              && var replicaVariables := v.hosts[replicaIdx].replicaVariables;
                              && viewChangeMsg in replicaVariables.viewChangeMsgsRecvd.msgs
                                 :: && var replicaConstants := c.hosts[replicaIdx].replicaConstants;
                                    && Replica.ValidViewChangeMsg(replicaConstants,
                                                                  viewChangeMsg,
                                                                  v.network.sentMsgs))
  }

  // This predicate is false and will be refactored to SentViewChangesMsgsComportWithUncommitableInView
  predicate {:opaque} SentViewChangesMsgsComportWithSentCommits(c:Constants, v:Variables) {
    && true
    // && v.WF(c)
    // && (forall viewChangeMsg, commitMsg |
    //       && viewChangeMsg in v.network.sentMsgs
    //       && commitMsg in v.network.sentMsgs
    //       && viewChangeMsg.payload.ViewChangeMsg?
    //       && commitMsg.payload.Commit?
    //       && commitMsg.payload.view <= viewChangeMsg.payload.newView
    //       && commitMsg.sender == viewChangeMsg.sender
    //       //TODO: add Shift consequences (this works only if no one advances the WW)
    //       && IsHonestReplica(c, viewChangeMsg.sender)
    //         :: && commitMsg.payload.seqID in viewChangeMsg.payload.certificates
    //            && var certificate := viewChangeMsg.payload.certificates[commitMsg.payload.seqID];
    //            && certificate.valid(c.clusterConfig, commitMsg.payload.seqID)
    //            && !certificate.empty()
    //            && certificate.prototype().view >= commitMsg.payload.view
    //           //  && (commitMsg.payload.view == viewChangeMsg.payload.newView) ==> 
    //           //             certificate.prototype().operationWrapper == commitMsg.payload.operationWrapper
    //            )
  }
  //TODO: refactor
  predicate {:opaque} SentViewChangesMsgsComportWithUncommitableInView(c:Constants, v:Variables) {
    && true
    // && v.WF(c)
    // && (forall viewChangeMsg, 
    //            seqID:Messages.SequenceID,
    //            view:nat,
    //            operationWrapper:Messages.OperationWrapper |
    //       && viewChangeMsg in v.network.sentMsgs
    //       && viewChangeMsg.payload.ViewChangeMsg?
    //       && IsHonestReplica(c, viewChangeMsg.sender)
    //       && view < viewChangeMsg.payload.newView
    //       && var certificate := viewChangeMsg.payload.certificates[seqID];
    //       && !certificate.empty()
    //       && operationWrapper != certificate.prototype().operationWrapper
    //         :: && UnCommitableInView(c, v, seqID, view, operationWrapper))
  }

//TODO: write a predicate Prepare matches Commit
  lemma GetPrepareFromHonestSenderForCommit(c:Constants, v:Variables, commitMsg:Message)
    returns (prepare:Message)
    requires Inv(c, v)
    requires commitMsg.payload.Commit?
    requires commitMsg in v.network.sentMsgs
    requires c.clusterConfig.IsHonestReplica(commitMsg.sender)
    ensures prepare.payload.Prepare?
    ensures c.clusterConfig.IsHonestReplica(prepare.sender)
    ensures prepare in v.network.sentMsgs
    ensures prepare.payload.view == commitMsg.payload.view
    ensures prepare.payload.seqID == commitMsg.payload.seqID
    ensures prepare.payload.operationWrapper == commitMsg.payload.operationWrapper
  {
    reveal_EveryCommitMsgIsSupportedByAQuorumOfSentPrepares();
    var prepares := sentPreparesForSeqID(c, v, commitMsg.payload.view, commitMsg.payload.seqID, commitMsg.payload.operationWrapper);
    prepare := GetMsgFromHonestSender(c, prepares);
  }

  function FaultyReplicas(c: Constants) : (faulty:set<HostId>) 
    requires c.WF()
  {
    set sender | 0 <= sender < c.clusterConfig.N() && !IsHonestReplica(c, sender)
  }

  lemma CountFaultyReplicas(c: Constants)
    requires c.WF()
    ensures |FaultyReplicas(c)| <= c.clusterConfig.F()
  {
    var count := 0;
    var f := c.clusterConfig.F();
    var n := c.clusterConfig.N();
    while(count < n)
      invariant count <= n
      invariant |set sender | 0 <= sender < count && !IsHonestReplica(c, sender)| == if count <= f then count
                                                                                      else f
      {
        var oldSet := set sender | 0 <= sender < count && !IsHonestReplica(c, sender);
        var newSet := set sender | 0 <= sender < count + 1 && !IsHonestReplica(c, sender);
        if(count + 1 <= f) {
          assert newSet == oldSet + {count};
        } else {
          assert newSet == oldSet;
        }
        count := count + 1;
      }
  }

  lemma FindHonestNode(c: Constants, replicas:set<HostIdentifiers.HostId>) 
    returns (honest:HostIdentifiers.HostId)
    requires c.WF()
    requires |replicas| >= c.clusterConfig.ByzantineSafeQuorum()
    requires replicas <= getAllReplicas(c)
    ensures IsHonestReplica(c, honest)
    ensures honest in replicas
  {
    var honestReplicas := replicas * HonestReplicas(c);
    CountFaultyReplicas(c);
    if(|honestReplicas| == 0) {
      Library.SubsetCardinality(replicas, FaultyReplicas(c));
      assert false; //Proof by contradiction
    }

    var result :| result in honestReplicas;
    honest := result;
  }

  lemma GetMsgFromHonestSender(c:Constants, quorum:set<Message>)
    returns (message:Message)
    requires c.WF()
    requires |Messages.sendersOf(quorum)| >= c.clusterConfig.AgreementQuorum()
    requires Messages.sendersOf(quorum) <= getAllReplicas(c)
    ensures c.clusterConfig.IsHonestReplica(message.sender)
    ensures message in quorum
  {
    var senders := Messages.sendersOf(quorum);
    var honestSender := FindHonestNode(c, senders);
    var honest :| honest in quorum && honest.sender == honestSender;
    message := honest;
  }

  lemma FindHonestCommitInQuorum(c:Constants,
                      v:Variables,
                      seqID:Messages.SequenceID,
                      view:nat,
                      commits:set<Message>,
                      operationWrapper:Messages.OperationWrapper)
        returns (honestCommit:Message)
    requires Inv(c, v)
    requires isCommitQuorum(c, v, view, seqID, operationWrapper, commits)
    ensures honestCommit in commits
    ensures IsHonestReplica(c, honestCommit.sender)
  {
    var senders := Messages.sendersOf(commits);
    UniqueSendersCardinality(commits);
    var honestSender := FindHonestNode(c, senders);
    var honest :| honest in commits && honest.sender == honestSender;
    honestCommit := honest;
  }

  lemma CommitQuorumAgree(c:Constants, v:Variables)
    requires Inv(c, v)
    ensures CommitQuorumsAgreeOnOperation(c, v)
  {
    forall seqID:Messages.SequenceID,
           view1:nat,
           view2:nat,
           commits1:set<Message>,
           commits2:set<Message>,
           operationWrapper1:Messages.OperationWrapper,
           operationWrapper2:Messages.OperationWrapper
                | && isCommitQuorum(c, v, view1, seqID, operationWrapper1, commits1)
                  && isCommitQuorum(c, v, view2, seqID, operationWrapper2, commits2)
    ensures (operationWrapper1 == operationWrapper2) {
      if view1 == view2 {
        // var honestCommit1 := FindHonestCommitInQuorum();
        var commit1 := FindHonestCommitInQuorum(c, v, seqID, view1, commits1, operationWrapper1);
        var commit2 := FindHonestCommitInQuorum(c, v, seqID, view2, commits2, operationWrapper2);
        assert commit1 in v.network.sentMsgs; // Trigger for CommitMsgsFromHonestSendersAgree
        assert commit2 in v.network.sentMsgs; // Trigger for CommitMsgsFromHonestSendersAgree
        reveal_CommitMsgsFromHonestSendersAgree();
        assert commit1.payload.operationWrapper == commit2.payload.operationWrapper;
        assert operationWrapper1 == operationWrapper2;
      } else if view1 < view2 {
        CommitQuorumAgreeDifferentViews(c, v, seqID, view1, view2, commits1, commits2, operationWrapper1, operationWrapper2);
      } else {
        CommitQuorumAgreeDifferentViews(c, v, seqID, view2, view1, commits2, commits1, operationWrapper2, operationWrapper1);
      }
    }
  }

  lemma CommitQuorumAgreeDifferentViews(c:Constants, v:Variables,seqID:Messages.SequenceID,
           view1:nat,
           view2:nat,
           commits1:set<Message>,
           commits2:set<Message>,
           operationWrapper1:Messages.OperationWrapper,
           operationWrapper2:Messages.OperationWrapper)
    requires Inv(c, v)
    requires isCommitQuorum(c, v, view1, seqID, operationWrapper1, commits1)
    requires isCommitQuorum(c, v, view2, seqID, operationWrapper2, commits2)
    requires view1 < view2
    ensures (operationWrapper1 == operationWrapper2)
  {
    reveal_UnCommitableAgreesWithPrepare();
    if operationWrapper1 != operationWrapper2 {
      UniqueSendersCardinality(commits2);
      var commit := GetMsgFromHonestSender(c, commits2);
      var prepare := GetPrepareFromHonestSenderForCommit(c, v, commit); // This term instantiates UnCommitableAgreesWithPrepare
      UniqueSendersCardinality(commits1);
      Library.SubsetCardinality(Messages.sendersOf(commits1), ReplicasThatCanCommitInView(c, v, seqID, view1, operationWrapper1));
      assert !UnCommitableInView(c, v, seqID, view1, operationWrapper1); // We have proven both UnCommitableInView !UnCommitableInView
      assert false; //proof by contradiction
    }
  }

  // Quorum of commits >= 2F+1 agree on all views
  predicate CommitQuorumsAgreeOnOperation(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall seqID:Messages.SequenceID,
               view1:nat,
               view2:nat,
               commits1:set<Message>,
               commits2:set<Message>,
               operationWrapper1:Messages.OperationWrapper,
               operationWrapper2:Messages.OperationWrapper
                    | && isCommitQuorum(c, v, view1, seqID, operationWrapper1, commits1)
                      && isCommitQuorum(c, v, view2, seqID, operationWrapper2, commits2)
                         :: operationWrapper1 == operationWrapper2)
  }

  predicate HonestReplicasAgreeOnOperationsOrdering(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall replica1:HostId, 
               replica2:HostId,
               seqID:Messages.SequenceID | && IsHonestReplica(c, replica1)
                                           && IsHonestReplica(c, replica2)
                              :: && var r1_c := c.hosts[replica1].replicaConstants;
                                 && var r2_c := c.hosts[replica2].replicaConstants;
                                 && var r1_v := v.hosts[replica1].replicaVariables;
                                 && var r2_v := v.hosts[replica2].replicaVariables;
                                 && (&& Replica.IsCommitted(r1_c, r1_v, seqID)
                                     && Replica.IsCommitted(r2_c, r2_v, seqID)) ==> Replica.GetCommittedOperation(r1_c, r1_v, seqID) == Replica.GetCommittedOperation(r2_c, r2_v, seqID))

  }

  lemma HonestPreservesRecordedPrePreparesMatchHostView(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures RecordedPrePreparesMatchHostView(c, v')
  {
    h_v.workingWindow.reveal_Shift();
    reveal_RecordedPrePreparesMatchHostView();
  }

  lemma HonestPreservesUnCommitableAgreesWithPrepare(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures UnCommitableAgreesWithPrepare(c, v')
  {
    if(h_step.LeaveViewStep?) {
      HonestLeaveViewStepPreservesUnCommitableAgreesWithPrepare(c, v, v', step, h_v, h_step);
    } else if(h_step.SendPrepareStep?) {
      HonestSendPrepareStepPreservesUnCommitableAgreesWithPrepare(c, v, v', step, h_v, h_step);
    } else {
      TriviallyPreserveUnCommitableAgreesWithPrepare(c, v, v', step, HonestInfo(h_v, h_step));
    }
  }

  lemma HonestPreservesHonestReplicasLeaveViewsBehind(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures HonestReplicasLeaveViewsBehind(c, v')
  {
    reveal_HonestReplicasLeaveViewsBehind();
  }

  lemma HonestPreservesRecordedNewViewMsgsContainSentVCMsgs(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures RecordedNewViewMsgsContainSentVCMsgs(c, v')
  {
    reveal_RecordedNewViewMsgsContainSentVCMsgs();
    reveal_RecordedViewChangeMsgsCameFromNetwork();

    forall replicaIdx, newViewMsg | && IsHonestReplica(c, replicaIdx)
                                        && var replicaVariables' := v'.hosts[replicaIdx].replicaVariables;
                                        && newViewMsg in replicaVariables'.newViewMsgsRecvd.msgs
                                           ensures 
                                              && newViewMsg.payload.vcMsgs.msgs <= v'.network.sentMsgs
                                              && MessagesAreFromReplicas(c, newViewMsg.payload.vcMsgs.msgs)
    {
      var replicaVariables := v.hosts[replicaIdx].replicaVariables;
      if (newViewMsg in replicaVariables.newViewMsgsRecvd.msgs) {
        assert newViewMsg.payload.vcMsgs.msgs <= v'.network.sentMsgs;
        assert MessagesAreFromReplicas(c, newViewMsg.payload.vcMsgs.msgs);
      } else {
        if (h_step.RecvNewViewMsgStep?) {
          assert newViewMsg.payload.vcMsgs.msgs <= v'.network.sentMsgs;
          assert MessagesAreFromReplicas(c, newViewMsg.payload.vcMsgs.msgs);
        } else if (h_step.SelectQuorumOfViewChangeMsgsStep?) {
          assert newViewMsg.payload.vcMsgs.msgs <= v'.network.sentMsgs;
          assert MessagesAreFromReplicas(c, newViewMsg.payload.vcMsgs.msgs);
        } else {
          assert false;
        }
      }
    }
  }

  lemma HonestPreservesRecordedViewChangeMsgsCameFromNetwork(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures RecordedViewChangeMsgsCameFromNetwork(c, v')
  {
    reveal_RecordedViewChangeMsgsCameFromNetwork();
  }

  lemma HonestPreservesOneViewChangeMessageFromReplicaPerView(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures OneViewChangeMessageFromReplicaPerView(c, v')
  {
    reveal_HonestReplicasLeaveViewsBehind();
    reveal_ViewChangeMsgsFromHonestInNetworkAreValid();
    reveal_OneViewChangeMessageFromReplicaPerView();
    reveal_RecordedViewChangeMsgsCameFromNetwork();
  }

  lemma HonestRecvPrePrepareStepPreservesUnCommitableAgreesWithRecordedPrePrepare(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    requires h_step.RecvPrePrepareStep?
    ensures UnCommitableAgreesWithRecordedPrePrepare(c, v')
  {
    reveal_UnCommitableAgreesWithRecordedPrePrepare();
    reveal_RecordedNewViewMsgsAreValid();
    forall replicaIdx, seqID, priorView:nat, priorOperationWrapper:Messages.OperationWrapper | 
      && IsHonestReplica(c, replicaIdx)
      && var replicaVariables' := v'.hosts[replicaIdx].replicaVariables;
      && var replicaConstants' := c.hosts[replicaIdx].replicaConstants;
      && seqID in replicaVariables'.workingWindow.getActiveSequenceIDs(replicaConstants')
      && replicaVariables'.workingWindow.prePreparesRcvd[seqID].Some?
      && var prePrepareMsg := replicaVariables'.workingWindow.prePreparesRcvd[seqID].value;
      && priorOperationWrapper != prePrepareMsg.payload.operationWrapper
      && priorView < prePrepareMsg.payload.view
        ensures && var prePrepareMsg := v'.hosts[replicaIdx].replicaVariables.workingWindow.prePreparesRcvd[seqID].value;
                && UnCommitableInView(c, v', prePrepareMsg.payload.seqID, priorView, priorOperationWrapper) {
      var replicaVariables := v.hosts[replicaIdx].replicaVariables;
      var replicaVariables' := v'.hosts[replicaIdx].replicaVariables;
      if replicaVariables.workingWindow.prePreparesRcvd[seqID].None? {    // Interesting case is when we record a new PrePrepare.
        var newViewMsgs := set msg | && msg in h_v.newViewMsgsRecvd.msgs  // The checks that the Replica does before recording
                                     && msg.payload.newView == h_v.view;  // is sufficient to proove UnCommitableAgreesWithRecordedPrePrepare
        if |newViewMsgs| == 0 {
          assert h_v.view == 0;
          assert false;//This cannot happen because there is a priorView < prePrepareMsg's view.
        }
        var newViewMsg :| newViewMsg in newViewMsgs;
        var viewChangers := Messages.sendersOf(newViewMsg.payload.vcMsgs.msgs);
        var troubleMakers := ReplicasThatCanCommitInView(c, v', seqID, priorView, priorOperationWrapper);
        if |troubleMakers| >= c.clusterConfig.AgreementQuorum() {
          // Contradiction hypothesis
          UniqueSendersCardinality(newViewMsg.payload.vcMsgs.msgs);
          assert viewChangers <= getAllReplicas(c) by {
            reveal_RecordedNewViewMsgsContainSentVCMsgs();
          }
          var doubleAgent := FindQuorumIntersection(c, viewChangers, troubleMakers);
          assert !c.clusterConfig.IsFaultyReplica(doubleAgent);
          assert doubleAgent in viewChangers;
          var vcMsg:Message :| && vcMsg in newViewMsg.payload.vcMsgs.msgs 
                               && vcMsg.sender == doubleAgent;
          assert vcMsg.payload.ViewChangeMsg?;
          assert vcMsg in v.network.sentMsgs by {
            reveal_RecordedNewViewMsgsContainSentVCMsgs();
          }

          assert !ReplicasInViewOrLower(c, v, seqID, priorView, priorOperationWrapper, doubleAgent) by {
             reveal_HonestReplicasLeaveViewsBehind();
             assert v.hosts[doubleAgent].replicaVariables.view >= vcMsg.payload.newView;
          }

          if (seqID !in vcMsg.payload.certificates) {
            // We are looking for an Inv that says every ViewChange msg after a Commit mentions the Committed SeqID.
          } else {
            var certView := vcMsg.payload.certificates[seqID].prototype().view;
            if certView <= priorView {
              assert !ReplicaSentCommit(c, v, seqID, priorView, priorOperationWrapper, doubleAgent) by {
                // viewchangemessages from honest sender comport with uncommitable in view
                //reveal_SentViewChangesMsgsComportWithUncommitableInView();
                //assume false;
                //reveal_CommitMsgsFromHonestSendersAgree();
                reveal_EveryCommitMsgIsRememberedByVCMsgs();
                assume EveryCommitMsgIsRememberedByVCMsgs(c, v);
              }
              assert doubleAgent !in troubleMakers;
              assert false;
            } else { // certView > priorView // Use the mutual induction hypothesis to get UncommitableInView directly.
              /*
                Grab the cert
                One of the prepares in the cert has to be from an honest node
                This prepare has to be in the network
                Apply UnCommitableAgreesWithPrepare
                Hint: We don't need a double agent in this case. We only need an honest certificate sender.
              */
              var cert := vcMsg.payload.certificates[seqID];
              var prepareFromHonest := GetMsgFromHonestSender(c, cert.votes);
              assert UnCommitableInView(c, v, seqID, priorView, priorOperationWrapper) by {
                reveal_UnCommitableAgreesWithPrepare();
              }
              assert false;
            }
          }
        }
        var prePrepareMsg := v'.hosts[replicaIdx].replicaVariables.workingWindow.prePreparesRcvd[seqID].value;
        assert prePrepareMsg.payload.seqID == seqID;
        assert |ReplicasThatCanCommitInView(c, v', seqID, priorView, priorOperationWrapper)| < c.clusterConfig.AgreementQuorum();
        assert UnCommitableInView(c, v', prePrepareMsg.payload.seqID, priorView, priorOperationWrapper);
      } else {
        var prePrepareMsg := replicaVariables'.workingWindow.prePreparesRcvd[seqID].value;
        assert UnCommitableInView(c, v, prePrepareMsg.payload.seqID, priorView, priorOperationWrapper);
        Library.SubsetCardinality(ReplicasThatCanCommitInView(c, v', prePrepareMsg.payload.seqID, priorView, priorOperationWrapper), 
                                ReplicasThatCanCommitInView(c, v, prePrepareMsg.payload.seqID, priorView, priorOperationWrapper));
      }
    }
  }

  lemma HonestPreservesUnCommitableAgreesWithRecordedPrePrepare(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures UnCommitableAgreesWithRecordedPrePrepare(c, v')
  {
    if(h_step.LeaveViewStep?) {
      HonestLeaveViewStepPreservesUnCommitableAgreesWithRecordedPrePrepare(c, v, v', step, h_v, h_step);
    } else if(h_step.RecvPrePrepareStep?) {
      HonestRecvPrePrepareStepPreservesUnCommitableAgreesWithRecordedPrePrepare(c, v, v', step, h_v, h_step);
    } else if(h_step.SendPrePrepareStep?) {
      assume false;
    } else {
      TriviallyPreserveUnCommitableAgreesWithRecordedPrePrepare(c, v, v', step, HonestInfo(h_v, h_step));
    }
  }

 lemma HonestPreservesSentViewChangesMsgsComportWithSentCommits(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures SentViewChangesMsgsComportWithSentCommits(c, v')
  {
    reveal_SentViewChangesMsgsComportWithSentCommits();

    reveal_EveryCommitClientOpMatchesRecordedPrePrepare();
    reveal_RecordedPreparesHaveValidSenderID();
    reveal_RecordedPrePreparesRecvdCameFromNetwork();
    reveal_RecordedPreparesMatchHostView();
    reveal_RecordedPreparesClientOpsMatchPrePrepare();
    reveal_RecordedCommitsClientOpsMatchPrePrepare();
    reveal_EverySentIntraViewMsgIsInWorkingWindowOrBefore();
    reveal_EverySentIntraViewMsgIsForAViewLessOrEqualToSenderView();
    reveal_EveryPrepareMatchesRecordedPrePrepare();
    reveal_EveryCommitClientOpMatchesRecordedPrePrepare();
    reveal_HonestReplicasLockOnPrepareForGivenView();
    reveal_HonestReplicasLockOnCommitForGivenView();
    reveal_CommitMsgsFromHonestSendersAgree();
    reveal_RecordedCheckpointsRecvdCameFromNetwork();
    reveal_HonestReplicasLockOnCommitForGivenView();
    reveal_EveryCommitMsgIsSupportedByAQuorumOfSentPrepares();
    h_v.workingWindow.reveal_Shift();
  }

 lemma HonestPreservesViewChangeMsgsFromHonestInNetworkAreValid(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures ViewChangeMsgsFromHonestInNetworkAreValid(c, v')
  {
    reveal_ViewChangeMsgsFromHonestInNetworkAreValid();
    forall viewChangeMsg |
                && viewChangeMsg in v'.network.sentMsgs
                && viewChangeMsg.payload.ViewChangeMsg?
                && var replicaIdx := viewChangeMsg.sender;
                && IsHonestReplica(c, replicaIdx)
                   ensures && var replicaIdx := viewChangeMsg.sender;
                           && viewChangeMsg.payload.checked(c.clusterConfig, v'.network.sentMsgs) {
      var h_c := c.hosts[step.id].replicaConstants;
      var replicaIdx := viewChangeMsg.sender;
      if viewChangeMsg !in v.network.sentMsgs {
        assert h_step.LeaveViewStep? by {
          reveal_RecordedViewChangeMsgsCameFromNetwork();
        }
        reveal_EveryPrepareMatchesRecordedPrePrepare();
        reveal_TemporarilyDisableCheckpointing();
        var certificates := viewChangeMsg.payload.certificates;
        forall seqID | seqID in certificates
                           ensures && certificates[seqID].valid(c.clusterConfig, seqID)
                                   && certificates[seqID].votesRespectView(viewChangeMsg.payload.newView)
                                   && certificates[seqID].votes <= v.network.sentMsgs {
          var certificate := certificates[seqID];
          var workingWindowPreparesRecvd := Replica.ExtractPreparesFromWorkingWindow(h_c, h_v, seqID);
          var viewChangeMsgPreparesRecvd := Replica.ExtractPreparesFromLatestViewChangeMsg(h_c, h_v, seqID);
          if |workingWindowPreparesRecvd| >= h_c.clusterConfig.AgreementQuorum()
          {
            reveal_RecordedPreparesRecvdCameFromNetwork();
            assert certificates == Replica.ExtractCertificates(h_c, h_v);
            assert certificate == Replica.ExtractCertificateForSeqID(h_c, h_v, seqID);
            if !certificate.empty() {
              var key :| && key in h_v.workingWindow.preparesRcvd[seqID]
                         && h_v.workingWindow.preparesRcvd[seqID][key].payload == certificate.prototype();
              
              assert certificate.validFull(c.clusterConfig, seqID);
            }
            assert certificate.valid(c.clusterConfig, seqID);
            assert certificate.votesRespectView(viewChangeMsg.payload.newView);
          }
          else if |viewChangeMsgPreparesRecvd| >= c.clusterConfig.AgreementQuorum() {
            assume false;
            assert certificate.valid(c.clusterConfig, seqID);
            assert certificate.votesRespectView(viewChangeMsg.payload.newView);
            assert certificate.votes <= v.network.sentMsgs;
          }
          else {
            assert certificate.valid(c.clusterConfig, seqID);
            assert certificate.votesRespectView(viewChangeMsg.payload.newView);
            assert certificate.votes <= v.network.sentMsgs;
          }
        }
        assert Replica.ExtractValidStableCheckpointProof(h_c, h_v).msgs == {} by {
          // We need 2 Invariants:
          // 1. All Checkpoint messages we record are in the netork.
          // 2. Checkpint messages from honest senders precede its last stable variable.
          // Since Checkpointing is disabled the honest senders have last stable == 0, 
          // therefore we cannot get a quorum of honest senders.
          assume false;
        }
        assert h_v.workingWindow.lastStableCheckpoint == 0;
        assert viewChangeMsg.payload.checked(c.clusterConfig, v'.network.sentMsgs);
      } else {
        assert viewChangeMsg.payload.checked(c.clusterConfig, v'.network.sentMsgs);
      }
    }
  }

  lemma HonestPreservesEveryCommitMsgIsRememberedByItsSenderForCommitStep(
        c:Constants, 
        v:Variables, 
        v':Variables, 
        step:Step,
        stepper_v:Replica.Variables,
        h_step:Replica.Step,
        commitMsg:Message,
        committer_c:Replica.Constants,
        committer_v':Replica.Variables) returns (certificate:Messages.PreparedCertificate)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, stepper_v, h_step)
    requires commitMsg in v'.network.sentMsgs 
    requires commitMsg.payload.Commit? 
    requires IsHonestReplica(c, commitMsg.sender)
    requires committer_c == c.hosts[commitMsg.sender].replicaConstants
    requires committer_v' == v'.hosts[commitMsg.sender].replicaVariables
    requires commitMsg.payload.seqID in committer_v'.workingWindow.getActiveSequenceIDs(committer_c)
    requires h_step.SendCommitStep?
    ensures certificate == Replica.ExtractCertificateForSeqID(committer_c, committer_v', commitMsg.payload.seqID)
    ensures certificate.valid(c.clusterConfig, commitMsg.payload.seqID)
    ensures !certificate.empty()
    ensures certificate.prototype().view >= commitMsg.payload.view
    ensures (certificate.prototype().view == commitMsg.payload.view 
                 ==> certificate.prototype().operationWrapper == commitMsg.payload.operationWrapper)
  {
    reveal_EveryCommitMsgIsRememberedByItsSender();
    reveal_TemporarilyDisableCheckpointing();
    reveal_RecordedViewChangeMsgsCameFromNetwork();
    reveal_OneViewChangeMessageFromReplicaPerView();

    certificate := Replica.ExtractCertificateForSeqID(committer_c, committer_v', commitMsg.payload.seqID);

    // assert forall vcMsg
    //           :: Replica.IsRecordedViewChangeMsgForView(committer_c, committer_v', committer_v'.view, vcMsg) ==
    //              Replica.IsRecordedViewChangeMsgForView(committer_c, stepper_v, stepper_v.view, vcMsg);
    if(commitMsg !in v.network.sentMsgs) {
      assert commitMsg.sender == step.id; // Committer and stepper are same in this branch
      Messages.reveal_UniqueSenders();
      var seqID := commitMsg.payload.seqID;
      var workingWindowPreparesRecvd := set key | key in stepper_v.workingWindow.preparesRcvd[seqID] // !!! TODO: We probably don't want the stepper here
                                        :: stepper_v.workingWindow.preparesRcvd[seqID][key];
      reveal_RecordedPreparesHaveValidSenderID();
      reveal_RecordedPreparesRecvdCameFromNetwork();
      reveal_RecordedPreparesClientOpsMatchPrePrepare();
      reveal_RecordedPreparesMatchHostView();
      CountPrepareMessages(stepper_v.workingWindow.preparesRcvd[seqID]);
      assert workingWindowPreparesRecvd == stepper_v.workingWindow.preparesRcvd[seqID].Values; // Extentionality
    }
  }

  // lemma FullQuorumOfPreparesEnsuresValidFullCertificate(
  //       c:Constants, 
  //       v:Variables,
  //       h_c:Replica.Constants,
  //       h_v:Replica.Variables,
  //       seqID:Messages.SequenceID)
  //   requires v.WF(c)
  //   requires |Replica.ExtractCertificateForSeqID(h_c, h_v, seqID).votes| >= h_c.clusterConfig.AgreementQuorum()
  //   ensures Replica.ExtractCertificateForSeqID(h_c, h_v, seqID).validFull(h_c.clusterConfig, seqID)
  // {

  // }

  lemma HonestPreservesEveryCommitMsgIsRememberedByItsSenderForRecvPrepareStep(
        c:Constants, 
        v:Variables, 
        v':Variables, 
        step:Step,
        stepper_v:Replica.Variables,
        h_step:Replica.Step,
        commitMsg:Message,
        committer_c:Replica.Constants,
        committer_v':Replica.Variables) returns (certificate:Messages.PreparedCertificate)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, stepper_v, h_step)
    requires commitMsg in v'.network.sentMsgs 
    requires commitMsg.payload.Commit? 
    requires IsHonestReplica(c, commitMsg.sender)
    requires committer_c == c.hosts[commitMsg.sender].replicaConstants
    requires committer_v' == v'.hosts[commitMsg.sender].replicaVariables
    requires commitMsg.payload.seqID in committer_v'.workingWindow.getActiveSequenceIDs(committer_c)
    requires h_step.RecvPrepareStep?
    ensures certificate == Replica.ExtractCertificateForSeqID(committer_c, committer_v', commitMsg.payload.seqID)
    ensures certificate.valid(c.clusterConfig, commitMsg.payload.seqID)
    ensures !certificate.empty()
    ensures certificate.prototype().view >= commitMsg.payload.view
    ensures (certificate.prototype().view == commitMsg.payload.view 
                 ==> certificate.prototype().operationWrapper == commitMsg.payload.operationWrapper)
  {
    reveal_EveryCommitMsgIsRememberedByItsSender();
    reveal_TemporarilyDisableCheckpointing();
    reveal_RecordedViewChangeMsgsCameFromNetwork();
    reveal_OneViewChangeMessageFromReplicaPerView();
    reveal_RecordedPreparesMatchHostView();
    reveal_RecordedPreparesRecvdCameFromNetwork();
    reveal_RecordedPreparesClientOpsMatchPrePrepare();
    Messages.reveal_UniqueSenders();

    var seqID := commitMsg.payload.seqID;

    assert EveryCommitMsgIsRememberedByItsSender(c, v);

    var committer_v := v.hosts[commitMsg.sender].replicaVariables;

    var oldCertificate := Replica.ExtractCertificateForSeqID(committer_c, committer_v, seqID);
    certificate := Replica.ExtractCertificateForSeqID(committer_c, committer_v', seqID);

    if(commitMsg !in v.network.sentMsgs) {
      assert false;
    }
    assert Replica.PrepareProofSetWF(committer_c, committer_v'.workingWindow.preparesRcvd[seqID]);

    if(step.id == commitMsg.sender) {
      if(commitMsg.payload.view < committer_v.view) {
        if(oldCertificate.votes != Replica.ExtractPreparesFromLatestViewChangeMsg(committer_c, committer_v, seqID)) {
          forall prepare | prepare in Replica.ExtractPreparesFromWorkingWindow(committer_c, committer_v, seqID)
                 ensures prepare in Replica.ExtractPreparesFromWorkingWindow(committer_c, committer_v', seqID) {
            assert committer_v.workingWindow.preparesRcvd[seqID][prepare.sender] == 
                   committer_v'.workingWindow.preparesRcvd[seqID][prepare.sender]; //Trigger
          }
          SubsetCardinality(Replica.ExtractPreparesFromWorkingWindow(committer_c, committer_v, seqID),
                            Replica.ExtractPreparesFromWorkingWindow(committer_c, committer_v', seqID));
        }
        assert certificate.valid(c.clusterConfig, seqID);
        assert (certificate.prototype().view == commitMsg.payload.view 
           ==> certificate.prototype().operationWrapper == commitMsg.payload.operationWrapper);
      } else {
        assert commitMsg.payload.view == committer_v.view by {
          reveal_EverySentIntraViewMsgIsForAViewLessOrEqualToSenderView();
        }
        var oldPrepares := Replica.ExtractPreparesFromWorkingWindow(committer_c, committer_v, seqID);
        var newPrepares := Replica.ExtractPreparesFromWorkingWindow(committer_c, committer_v', seqID);
        if(|newPrepares| >= c.clusterConfig.AgreementQuorum()) {
          assert certificate.validFull(c.clusterConfig, seqID);
          if |oldPrepares| >= c.clusterConfig.AgreementQuorum() {
            assert certificate.prototype().operationWrapper == commitMsg.payload.operationWrapper;
          } else {
            reveal_EveryCommitMsgIsSupportedByAQuorumOfRecordedPrepares();
            assert false; // Couldn't have sent a Commit
          }
        } else {
          if (|oldPrepares| >= c.clusterConfig.AgreementQuorum()) {
            forall p | p in oldPrepares ensures p in newPrepares {
              assert committer_v'.workingWindow.preparesRcvd[seqID][p.sender] == p; // Trigger
            }
            SubsetCardinality(oldPrepares,newPrepares);
          }
          if |Replica.ExtractPreparesFromLatestViewChangeMsg(committer_c, committer_v, seqID)| >= c.clusterConfig.AgreementQuorum() {
            var viewChangeMsg := Replica.GetViewChangeMsgForView(committer_c, committer_v.viewChangeMsgsRecvd, committer_v.view);
            var viewChangeMsgVotes := viewChangeMsg.payload.certificates[seqID];
            assert viewChangeMsg.payload.newView == committer_v.view;
            assert viewChangeMsg.payload.validViewChangeMsg(c.clusterConfig) by {
              reveal_ViewChangeMsgsFromHonestInNetworkAreValid();
            }
            assert viewChangeMsgVotes.prototype().view < committer_v.view;

            assert false;
          }
          assert oldCertificate.validFull(c.clusterConfig, seqID);
          assert oldCertificate.votes == Replica.ExtractPreparesFromWorkingWindow(committer_c, committer_v, seqID);
          assert certificate.validFull(c.clusterConfig, seqID);
          assert certificate.prototype().operationWrapper == commitMsg.payload.operationWrapper;
        }
        assert certificate.valid(c.clusterConfig, seqID);
        assert certificate.prototype().view == commitMsg.payload.view; 
        assert certificate.prototype().operationWrapper == commitMsg.payload.operationWrapper;
      }

    } else {
      assert oldCertificate.validFull(c.clusterConfig, seqID);
      assert oldCertificate.votes <= certificate.votes;
      assert certificate.validFull(c.clusterConfig, seqID);
      assert certificate.valid(c.clusterConfig, seqID); 
    }
  }

  lemma HonestPreservesEveryCommitMsgIsRememberedByItsSender(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures EveryCommitMsgIsRememberedByItsSender(c, v')
  {
    reveal_EveryCommitMsgIsRememberedByItsSender();

    forall commitMsg | && commitMsg in v'.network.sentMsgs 
                           && commitMsg.payload.Commit? 
                           && IsHonestReplica(c, commitMsg.sender)
                           && var h_c := c.hosts[commitMsg.sender].replicaConstants;
                           && var h_v' := v'.hosts[commitMsg.sender].replicaVariables;
                           && commitMsg.payload.seqID in h_v'.workingWindow.getActiveSequenceIDs(h_c)
     ensures && var h_c := c.hosts[commitMsg.sender].replicaConstants;
             && var h_v' := v'.hosts[commitMsg.sender].replicaVariables;
             && var certificate := Replica.ExtractCertificateForSeqID(h_c, h_v', commitMsg.payload.seqID);
             && certificate.valid(c.clusterConfig, commitMsg.payload.seqID)
             && !certificate.empty()
             && certificate.prototype().view >= commitMsg.payload.view
             && (certificate.prototype().view == commitMsg.payload.view 
                 ==> certificate.prototype().operationWrapper == commitMsg.payload.operationWrapper)
    {

      /*
    | SendPrePrepareStep(seqID:SequenceID)
    | RecvPrePrepareStep()
    | SendPrepareStep(seqID:SequenceID)
    | RecvPrepareStep()
    | SendCommitStep(seqID:SequenceID)
    | RecvCommitStep()
    | DoCommitStep(seqID:SequenceID)
    | ExecuteStep(seqID:SequenceID)
    | SendCheckpointStep(seqID:SequenceID)
    | RecvCheckpointStep()
    | AdvanceWorkingWindowStep(seqID:SequenceID, checkpointsQuorum:CheckpointsQuorum)
    | PerformStateTransferStep(seqID:SequenceID, checkpointsQuorum:CheckpointsQuorum)
    //| SendReplyToClient(seqID:SequenceID)
    // TODO: uncomment those steps when we start working on the proof
    | LeaveViewStep(newView:ViewNum)
    | SendViewChangeMsgStep()
    | RecvViewChangeMsgStep()
    | SelectQuorumOfViewChangeMsgsStep(viewChangeMsgsSelectedByPrimary:ViewChangeMsgsSelectedByPrimary)
    | SendNewViewMsgStep()
    | RecvNewViewMsgStep()
      */
      var h_c := c.hosts[step.id].replicaConstants;
      var h_v' := v'.hosts[step.id].replicaVariables;
      reveal_TemporarilyDisableCheckpointing();
      reveal_RecordedViewChangeMsgsCameFromNetwork();
      reveal_OneViewChangeMessageFromReplicaPerView();
      if(h_step.LeaveViewStep?) {
        assume false;
      } else if(h_step.SendCommitStep?) {
        var res := HonestPreservesEveryCommitMsgIsRememberedByItsSenderForCommitStep(
                      c,
                      v,
                      v',
                      step,
                      h_v,
                      h_step,
                      commitMsg,
                      c.hosts[commitMsg.sender].replicaConstants,
                      v'.hosts[commitMsg.sender].replicaVariables);
      } else if(h_step.RecvPrepareStep?) {
        var res := HonestPreservesEveryCommitMsgIsRememberedByItsSenderForRecvPrepareStep(
                      c,
                      v,
                      v',
                      step,
                      h_v,
                      h_step,
                      commitMsg,
                      c.hosts[commitMsg.sender].replicaConstants,
                      v'.hosts[commitMsg.sender].replicaVariables);
      } else if(h_step.PerformStateTransferStep?) {
        assert forall vcMsg
                  :: Replica.IsRecordedViewChangeMsgForView(h_c, h_v'.viewChangeMsgsRecvd, h_v'.view, vcMsg) ==
                     Replica.IsRecordedViewChangeMsgForView(h_c, h_v.viewChangeMsgsRecvd, h_v.view, vcMsg);
        assume false;
      } else if(h_step.AdvanceWorkingWindowStep?) {
        assert forall vcMsg
                  :: Replica.IsRecordedViewChangeMsgForView(h_c, h_v'.viewChangeMsgsRecvd, h_v'.view, vcMsg) ==
                     Replica.IsRecordedViewChangeMsgForView(h_c, h_v.viewChangeMsgsRecvd, h_v.view, vcMsg);
        assume false;
      } else if(h_step.RecvViewChangeMsgStep?) {
        assume false;
      } else {
        assert forall vcMsg
                  :: Replica.IsRecordedViewChangeMsgForView(h_c, h_v'.viewChangeMsgsRecvd, h_v'.view, vcMsg) ==
                     Replica.IsRecordedViewChangeMsgForView(h_c, h_v.viewChangeMsgsRecvd, h_v.view, vcMsg);
      }
    }
  }

  lemma HonestPreservesRecordedViewChangeMsgsAreValid(c: Constants, v:Variables, v':Variables, step:Step, h_v:Replica.Variables, h_step:Replica.Step)
    requires Inv(c, v)
    requires HonestReplicaStepTaken(c, v, v', step, h_v, h_step)
    ensures RecordedViewChangeMsgsAreValid(c, v')
  {
    reveal_RecordedViewChangeMsgsAreValid();
    reveal_TemporarilyDisableCheckpointing(); // Needed for LeaveViewStep
    if (h_step.LeaveViewStep?) {
      reveal_RecordedPreparesRecvdCameFromNetwork();
      reveal_RecordedPreparesMatchHostView();
      reveal_RecordedPreparesClientOpsMatchPrePrepare();
      reveal_TemporarilyDisableCheckpointing();

      forall replicaIdx, viewChangeMsg | 
                              && IsHonestReplica(c, replicaIdx)
                              && var replicaVariables' := v'.hosts[replicaIdx].replicaVariables;
                              && viewChangeMsg in replicaVariables'.viewChangeMsgsRecvd.msgs
                                 ensures (&& var replicaConstants := c.hosts[replicaIdx].replicaConstants;
                                          && Replica.ValidViewChangeMsg(replicaConstants,
                                                                        viewChangeMsg,
                                                                        v'.network.sentMsgs))
      {
        if(viewChangeMsg !in v.hosts[replicaIdx].replicaVariables.viewChangeMsgsRecvd.msgs) {
          Messages.reveal_UniqueSenders();
          assert viewChangeMsg.payload.lastStableCheckpoint == 0;
          assume |viewChangeMsg.payload.proofForLastStable.msgs| == 0; // TODO: remove once we introduce Checkpointing reasoning
        }
      }
    }
  }

  lemma InvariantNext(c: Constants, v:Variables, v':Variables)
    requires Inv(c, v)
    requires Next(c, v, v')
    ensures Inv(c, v')
  {
    var step :| NextStep(c, v, v', step);
    if IsHonestReplica(c, step.id) {
      var h_c := c.hosts[step.id].replicaConstants;
      var h_v := v.hosts[step.id].replicaVariables;
      var h_v' := v'.hosts[step.id].replicaVariables;
      var h_step :| Replica.NextStep(h_c, h_v, h_v', step.msgOps, h_step);
      assert HonestReplicaStepTaken(c, v, v', step, h_v, h_step);
      HonestPreservesRecordedNewViewMsgsAreValid(c, v, v', step, h_v, h_step);
      HonestPreservesRecordedPreparesHaveValidSenderID(c, v, v', step, h_v, h_step);
      HonestPreservesRecordedPrePreparesRecvdCameFromNetwork(c, v, v', step, h_v, h_step);
      HonestPreservesRecordedPreparesRecvdCameFromNetwork(c, v, v', step, h_v, h_step);
      HonestPreservesRecordedPreparesMatchHostView(c, v, v', step, h_v, h_step);
      AlwaysPreservesEveryCommitMsgIsSupportedByAQuorumOfSentPrepares(c, v, v', step);
      HonestPreservesEveryCommitMsgIsSupportedByAQuorumOfRecordedPrepares(c, v, v', step, h_v, h_step);
      HonestPreservesRecordedPreparesClientOpsMatchPrePrepare(c, v, v', step, h_v, h_step);
      HonestPreservesRecordedCommitsClientOpsMatchPrePrepare(c, v, v', step, h_v, h_step);
      HonestPreservesEverySentIntraViewMsgIsInWorkingWindowOrBefore(c, v, v', step, h_v, h_step);
      HonestPreservesEverySentIntraViewMsgIsForAViewLessOrEqualToSenderView(c, v, v', step, h_v, h_step);
      HonestPreservesEveryPrepareMatchesRecordedPrePrepare(c, v, v', step, h_v, h_step);
      HonestPreservesEveryCommitClientOpMatchesRecordedPrePrepare(c, v, v', step, h_v, h_step);
      HonestPreservesHonestReplicasLockOnPrepareForGivenView(c, v, v', step, h_v, h_step);
      HonestPreservesHonestReplicasLockOnCommitForGivenView(c, v, v', step, h_v, h_step);
      HonestPreservesCommitMsgsFromHonestSendersAgree(c, v, v', step, h_v, h_step);
      HonestPreservesRecordedCheckpointsRecvdCameFromNetwork(c, v, v', step, h_v, h_step);
      HonestPreservesRecordedPrePreparesMatchHostView(c, v, v', step, h_v, h_step);
      HonestPreservesUnCommitableAgreesWithRecordedPrePrepare(c, v, v', step, h_v, h_step);
      HonestPreservesUnCommitableAgreesWithPrepare(c, v, v', step, h_v, h_step);
      HonestPreservesHonestReplicasLeaveViewsBehind(c, v, v', step, h_v, h_step);
      HonestPreservesRecordedNewViewMsgsContainSentVCMsgs(c, v, v', step, h_v, h_step);
      HonestPreservesRecordedViewChangeMsgsCameFromNetwork(c, v, v', step, h_v, h_step);
      HonestPreservesSentViewChangesMsgsComportWithSentCommits(c, v, v', step, h_v, h_step);
      HonestPreservesEveryCommitMsgIsRememberedByItsSender(c, v, v', step, h_v, h_step);
      HonestPreservesViewChangeMsgsFromHonestInNetworkAreValid(c, v, v', step, h_v, h_step);
      HonestPreservesRecordedViewChangeMsgsAreValid(c, v, v', step, h_v, h_step);
      HonestPreservesOneViewChangeMessageFromReplicaPerView(c, v, v', step, h_v, h_step);
      assume TemporarilyDisableCheckpointing(c, v');
    } else {
      InvNextFaultyOrClient(c, v, v', step);
    }
  }

  lemma InvNextFaultyOrClient(c: Constants, v:Variables, v':Variables, step: Step)
    requires v.WF(c)
    requires Inv(c, v)
    requires NextStep(c, v, v', step)
    requires (c.clusterConfig.IsFaultyReplica(step.id) || c.clusterConfig.IsClient(step.id))
    ensures Inv(c, v')
  {
    assert RecordedNewViewMsgsAreValid(c, v') by {
      reveal_RecordedNewViewMsgsAreValid();
    }
    assert RecordedPreparesHaveValidSenderID(c, v') by {
      reveal_RecordedPreparesHaveValidSenderID();
    }
    assert RecordedPrePreparesRecvdCameFromNetwork(c, v') by {
      reveal_RecordedPrePreparesRecvdCameFromNetwork();
    }
    assert RecordedPreparesRecvdCameFromNetwork(c, v') by {
      reveal_RecordedPreparesRecvdCameFromNetwork();
    }
    assert RecordedPreparesMatchHostView(c, v') by {
      reveal_RecordedPreparesMatchHostView();
    }
    assert RecordedPreparesClientOpsMatchPrePrepare(c, v') by {
      reveal_RecordedPreparesClientOpsMatchPrePrepare();
    }
    assert RecordedCommitsClientOpsMatchPrePrepare(c, v') by {
      reveal_RecordedCommitsClientOpsMatchPrePrepare();
    }
    assert EverySentIntraViewMsgIsInWorkingWindowOrBefore(c, v') by {
      reveal_EverySentIntraViewMsgIsInWorkingWindowOrBefore();
    }
    assert EverySentIntraViewMsgIsForAViewLessOrEqualToSenderView(c, v') by {
      reveal_EverySentIntraViewMsgIsForAViewLessOrEqualToSenderView();
    }
    assert EveryPrepareMatchesRecordedPrePrepare(c, v') by {
      reveal_EveryPrepareMatchesRecordedPrePrepare();
    }
    assert EveryCommitClientOpMatchesRecordedPrePrepare(c, v') by {
      reveal_EveryCommitClientOpMatchesRecordedPrePrepare();
    }
    assert HonestReplicasLockOnPrepareForGivenView(c, v') by {
      reveal_HonestReplicasLockOnPrepareForGivenView();
    }
    assert HonestReplicasLockOnCommitForGivenView(c, v') by {
      reveal_HonestReplicasLockOnCommitForGivenView();
    }
    assert CommitMsgsFromHonestSendersAgree(c, v') by {
      reveal_CommitMsgsFromHonestSendersAgree();
    }
    assert RecordedCheckpointsRecvdCameFromNetwork(c, v') by {
      reveal_RecordedCheckpointsRecvdCameFromNetwork();
    }
    assert HonestReplicasLockOnCommitForGivenView(c, v') by {
      reveal_HonestReplicasLockOnCommitForGivenView();
    }
    assert EveryCommitMsgIsSupportedByAQuorumOfSentPrepares(c, v') by {
      AlwaysPreservesEveryCommitMsgIsSupportedByAQuorumOfSentPrepares(c, v, v', step);
    }
    assert EveryCommitMsgIsSupportedByAQuorumOfRecordedPrepares(c, v') by {
     reveal_EveryCommitMsgIsSupportedByAQuorumOfRecordedPrepares();
    }
    assert RecordedPrePreparesMatchHostView(c, v') by {
      reveal_RecordedPrePreparesMatchHostView();
    }
    assert UnCommitableAgreesWithRecordedPrePrepare(c, v') by {
      TriviallyPreserveUnCommitableAgreesWithRecordedPrePrepare(c, v, v', step, NotHonest());
    }
    assert UnCommitableAgreesWithPrepare(c, v') by {
      TriviallyPreserveUnCommitableAgreesWithPrepare(c, v, v', step, NotHonest());
    }
    assert HonestReplicasLeaveViewsBehind(c, v') by {
      reveal_HonestReplicasLeaveViewsBehind();
    }
    assert RecordedNewViewMsgsContainSentVCMsgs(c, v') by {
      reveal_RecordedNewViewMsgsContainSentVCMsgs();
    }
    assert RecordedViewChangeMsgsCameFromNetwork(c, v') by {
      reveal_RecordedViewChangeMsgsCameFromNetwork();
    }
    assert SentViewChangesMsgsComportWithSentCommits(c, v') by {
      reveal_SentViewChangesMsgsComportWithSentCommits();
    }
    assert EveryCommitMsgIsRememberedByItsSender(c, v') by {
      reveal_EveryCommitMsgIsRememberedByItsSender();
    }
    assert RecordedViewChangeMsgsAreValid(c, v') by {
      reveal_RecordedViewChangeMsgsAreValid();
    }
    assert ViewChangeMsgsFromHonestInNetworkAreValid(c, v') by {
      reveal_ViewChangeMsgsFromHonestInNetworkAreValid();
    }
    assert OneViewChangeMessageFromReplicaPerView(c, v') by {
      reveal_OneViewChangeMessageFromReplicaPerView();
    }
    assume TemporarilyDisableCheckpointing(c, v');
  }

  lemma InitEstablishesInv(c: Constants, v:Variables)
    requires Init(c, v)
    ensures Inv(c, v)
  {
    assert Inv(c, v) by {
      reveal_RecordedNewViewMsgsAreValid();
      reveal_RecordedCommitsClientOpsMatchPrePrepare();
      reveal_EveryPrepareMatchesRecordedPrePrepare();
      reveal_EveryCommitClientOpMatchesRecordedPrePrepare();
      reveal_HonestReplicasLockOnCommitForGivenView();
      reveal_CommitMsgsFromHonestSendersAgree();
      reveal_RecordedPreparesHaveValidSenderID();
      reveal_RecordedPrePreparesRecvdCameFromNetwork();
      reveal_RecordedPreparesRecvdCameFromNetwork();
      reveal_RecordedPreparesMatchHostView();
      reveal_RecordedPreparesClientOpsMatchPrePrepare();
      reveal_EverySentIntraViewMsgIsInWorkingWindowOrBefore();
      reveal_EverySentIntraViewMsgIsForAViewLessOrEqualToSenderView();
      reveal_HonestReplicasLockOnPrepareForGivenView();
      reveal_RecordedCheckpointsRecvdCameFromNetwork();
      reveal_EveryCommitMsgIsSupportedByAQuorumOfSentPrepares();
      reveal_EveryCommitMsgIsSupportedByAQuorumOfRecordedPrepares();
      reveal_RecordedPrePreparesMatchHostView();
      reveal_UnCommitableAgreesWithPrepare();
      reveal_UnCommitableAgreesWithRecordedPrePrepare();
      reveal_HonestReplicasLeaveViewsBehind();
      reveal_RecordedNewViewMsgsContainSentVCMsgs();
      reveal_RecordedViewChangeMsgsCameFromNetwork();
      reveal_SentViewChangesMsgsComportWithSentCommits();
      reveal_EveryCommitMsgIsRememberedByItsSender();
      reveal_RecordedViewChangeMsgsAreValid();
      reveal_ViewChangeMsgsFromHonestInNetworkAreValid();
      reveal_OneViewChangeMessageFromReplicaPerView();
      reveal_TemporarilyDisableCheckpointing();
    }
  }

  lemma InvariantInductive(c: Constants, v:Variables, v':Variables)
    ensures Init(c, v) ==> Inv(c, v)
    ensures Inv(c, v) && Next(c, v, v') ==> Inv(c, v')
    //ensures Inv(c, v) ==> Safety(c, v)
  {
    if Init(c, v) {
      InitEstablishesInv(c, v);
    }
    if Inv(c, v) && Next(c, v, v') {
      InvariantNext(c, v, v');
    }
  }
} //Module
