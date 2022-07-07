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

module Replica {
  import opened Library
  import opened HostIdentifiers
  import opened Messages
  import Network
  import ClusterConfig
  
  type PrepareProofSet = map<HostId, Network.Message<Message>> 
  predicate PrepareProofSetWF(c:Constants, ps:PrepareProofSet)
    requires c.WF()
  {
      && forall x | x in ps :: && ps[x].payload.Prepare? 
                               && c.clusterConfig.IsReplica(ps[x].sender)
  }
  function EmptyPrepareProofSet() : PrepareProofSet {
    map[]
  }

  type CommitProofSet = map<HostId, Network.Message<Message>>
  predicate CommitProofSetWF(c:Constants, cs:CommitProofSet)
    requires c.WF()
  {
      && forall x | x in cs :: && cs[x].payload.Commit?
                               && c.clusterConfig.IsReplica(cs[x].sender)
  }
  function EmptyCommitProofSet() : CommitProofSet {
    map[]
  }

  // The Working Window data structure. Here Replicas keep the PrePrepare from the Primary
  // and the votes from all peers. Once a Client Operation is committed by a given Replica
  // to a specific Sequence ID (the Replica has collected the necessary quorum of votes from
  // peers) the Client Operation is inserted in the committedClientOperations as appropriate.
  datatype WorkingWindow = WorkingWindow(
    committedClientOperations:map<SequenceID, Option<OperationWrapper>>,
    prePreparesRcvd:map<SequenceID, Option<Network.Message<Message>>>,
    preparesRcvd:map<SequenceID, PrepareProofSet>,
    commitsRcvd:map<SequenceID, CommitProofSet>,
    lastStableCheckpoint:SequenceID
  ) {
    function getActiveSequenceIDs(c:Constants) : set<SequenceID> 
      requires c.WF()
    {
      set seqID | lastStableCheckpoint <= seqID < lastStableCheckpoint + c.clusterConfig.workingWindowSize //TODO: refactor to finite sets/maps
    }
    predicate WF(c:Constants)
      requires c.WF()
    {
      && committedClientOperations.Keys == getActiveSequenceIDs(c)
      && preparesRcvd.Keys == getActiveSequenceIDs(c)
      && commitsRcvd.Keys == getActiveSequenceIDs(c)
      && prePreparesRcvd.Keys == getActiveSequenceIDs(c)
      && (forall x | x in prePreparesRcvd && prePreparesRcvd[x].Some? :: prePreparesRcvd[x].value.payload.PrePrepare?)
      && (forall seqID | seqID in preparesRcvd :: PrepareProofSetWF(c, preparesRcvd[seqID]))
      && (forall seqID | seqID in commitsRcvd :: CommitProofSetWF(c, commitsRcvd[seqID]))
    }
    function Shift<T>(c:Constants, m:map<SequenceID,T>, empty:T) : map<SequenceID,T> 
      requires c.WF()
    {
      map seqID | seqID in getActiveSequenceIDs(c) :: if seqID in m then m[seqID] else empty
    }
  }


  // Define your Host protocol state machine here.
  datatype Constants = Constants(myId:HostId, clusterConfig:ClusterConfig.Constants) {
    // host constants coupled to DistributedSystem Constants:
    // DistributedSystem tells us our id so we can recognize inbound messages.
    // clusterSize is in clusterConfig.
    predicate WF() {
      && clusterConfig.WF()
      && clusterConfig.IsReplica(myId)
    }

    predicate Configure(id:HostId, clusterConf:ClusterConfig.Constants) {
      && myId == id
      && clusterConfig == clusterConf
    }
  }

  datatype ViewChangeMsgs = ViewChangeMsgs(msgs:set<Network.Message<Message>>) {
    predicate WF(c:Constants) {
      && c.WF()
      && (forall msg | msg in msgs :: && msg.payload.ViewChangeMsg?
                                      && c.clusterConfig.IsReplica(msg.sender))
    }
  }

  datatype NewViewMsgs = NewViewMsgs(msgs:set<Network.Message<Message>>) {
    predicate WF(c:Constants) {
      && c.WF()
      && (forall msg | msg in msgs :: && msg.payload.NewViewMsg?
                                      && c.clusterConfig.IsReplica(msg.sender))
    }
  }

  datatype CheckpointMsgs = CheckpointMsgs(msgs:set<Network.Message<Message>>) {
    predicate WF(c:Constants) {
      && c.WF()
      && (forall msg | msg in msgs :: && msg.payload.CheckpointMsg?
                                      && c.clusterConfig.IsReplica(msg.sender))
    }
  }

  datatype Variables = Variables(
    view:ViewNum,
    workingWindow:WorkingWindow,
    viewChangeMsgsRecvd:ViewChangeMsgs,
    newViewMsgsRecvd:NewViewMsgs,
    countExecutedSeqIDs:SequenceID,
    checkpointMsgsRecvd:CheckpointMsgs
  ) {
    predicate WF(c:Constants)
    {
      && c.WF()
      && workingWindow.WF(c)
      && viewChangeMsgsRecvd.WF(c)
      && newViewMsgsRecvd.WF(c)
      && checkpointMsgsRecvd.WF(c)
    }
  }

  function PrimaryForView(c:Constants, view:ViewNum) : nat 
    requires c.WF()
  {
    view % c.clusterConfig.N()
  }

  function CurrentPrimary(c:Constants, v:Variables) : nat 
    requires v.WF(c)
  {
    PrimaryForView(c, v.view)
  }

  predicate HaveSufficientVCMsgsToMoveTo(c:Constants, v:Variables, newView:ViewNum)
    requires v.WF(c)
  {
    && var relevantVCMsgs := set vcMsg | && vcMsg in v.viewChangeMsgsRecvd.msgs
                                         && vcMsg.payload.newView >= newView;
    && var senders := Messages.sendersOf(relevantVCMsgs);
    && |senders| >= c.clusterConfig.ByzantineSafeQuorum() //F+1
  }

  predicate HasCollectedProofMyViewIsAgreed(c:Constants, v:Variables) {
    && v.WF(c)
    && var vcMsgsForMyView := set msg | && msg in v.viewChangeMsgsRecvd.msgs 
                                        && msg.payload.newView == v.view;
    && var senders := Messages.sendersOf(vcMsgsForMyView);
    && ( || v.view == 0 // View 0 is active initially therefore it is initially agreed.
         || |senders| >= c.clusterConfig.AgreementQuorum())
  }

  // Constructively demonstrate that we can compute the certificate with the highest View.
  function HighestViewPrepareCertificate(prepareCertificates:set<PreparedCertificate>) : (highestViewCert:PreparedCertificate)
    requires (forall cert | cert in prepareCertificates :: cert.WF() && !cert.empty())
    requires |prepareCertificates| > 0
    ensures highestViewCert in prepareCertificates
    ensures (forall other | other in prepareCertificates ::
                      highestViewCert.prototype().view >= other.prototype().view)
  {
    var any :| any in prepareCertificates;
    if |prepareCertificates| == 1
    then Library.SingletonSetAxiom(any, prepareCertificates);
         any
    else var rest := prepareCertificates - {any};
         var highestOfRest := HighestViewPrepareCertificate(rest);
         if any.prototype().view > highestOfRest.prototype().view 
         then any
         else highestOfRest
  }

  function CalculateRestrictionForSeqID(c:Constants, v:Variables, seqID:SequenceID, newViewMsg:Network.Message<Message>) 
    : Option<OperationWrapper>
      requires v.WF(c)
      requires newViewMsg.payload.NewViewMsg?
      requires newViewMsg.payload.vcMsgs.valid(v.view, c.clusterConfig.AgreementQuorum())
      // readability:
      requires newViewMsg.payload.newView == v.view
      requires CurrentPrimary(c, v) == newViewMsg.sender
    {
    // 1. Take the NewViewMsg for the current View.
    // 2. Go through all the ViewChangeMsg-s in the NewView and take the valid full 
    //    PreparedCertificates from them for the seqID.
    // 3. From all the collected PreparedCertificates take the one with the highest View.
    // 4. If it is empty  we need to fill with NoOp.
    // 5. If it contains valid full quorum we take the Client Operation and insist it will be committed in the new View.

    var relevantPrepareCertificates := set viewChangeMsg, cert |
                                   && viewChangeMsg in newViewMsg.payload.vcMsgs.msgs
                                   && cert == viewChangeMsg.payload.certificates[seqID]
                                   && cert.WF()
                                   && !cert.empty()
                                     :: cert;
    if |relevantPrepareCertificates| == 0
    then
      Some(Noop)
    else
      var highestViewCert := HighestViewPrepareCertificate(relevantPrepareCertificates);
      Some(highestViewCert.prototype().operationWrapper)
  }

  predicate ViewIsActive(c:Constants, v:Variables) {
    && v.WF(c)
    && var relevantNewViewMsgs := set msg | msg in v.newViewMsgsRecvd.msgs && msg.payload.newView == v.view;
    && ( || v.view == 0 // View 0 is active initially. There are no View Change messages for it.
         || |relevantNewViewMsgs| == 1) // The NewViewMsg that the Primary sends contains in itself the selected Quorum of
                                        // ViewChangeMsg-s based on which we are going to rebuild the previous View's working window.
  }

  // Predicate that describes what is needed and how we mutate the state v into v' when SendPrePrepare
  // Action is taken. We use the "binding" variable msgOps through which we send/recv messages.
  predicate SendPrePrepare(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>)
  {
    && v.WF(c)
    && msgOps.IsSend()
    && CurrentPrimary(c, v) == c.myId
    && var msg := msgOps.send.value;
    && msg.payload.PrePrepare? // We have a liveness bug here, we need some state that says for the client which operation ID-s we have executed
    && v == v'
  }

  // Node local invariants that we need to satisfy dafny requires. This gets proven as part of the Distributed system invariants.
  // That is why it can appear as enabling condition, but does not need to be translated to runtime checks to C++.
  // For this to be safe it has to appear in the main invarinat in the proof.
  predicate LiteInv(c:Constants, v:Variables) {
    && v.WF(c)
    && (forall newViewMsg | newViewMsg in v.newViewMsgsRecvd.msgs ::
               && newViewMsg.payload.vcMsgs.valid(v.view, c.clusterConfig.AgreementQuorum())
               && PrimaryForView(c, newViewMsg.payload.newView) == newViewMsg.sender)
  }

  // For clarity here we have extracted all preconditions that must hold for a Replica to accept a PrePrepare
  predicate IsValidPrePrepareToAccept(c:Constants, v:Variables, msg:Network.Message<Message>)
  {
    && v.WF(c)
    && LiteInv(c, v)
    && msg.payload.PrePrepare?
    && msg.payload.seqID in v.workingWindow.getActiveSequenceIDs(c)
    && c.clusterConfig.IsReplica(msg.sender)
    && ViewIsActive(c, v)
    && msg.payload.view == v.view
    && msg.sender == CurrentPrimary(c, v)
    && v.workingWindow.prePreparesRcvd[msg.payload.seqID].None?
    && var newViewMsgs := set msg | && msg in v.newViewMsgsRecvd.msgs 
                                    && msg.payload.newView == v.view;
    && (if |newViewMsgs| == 0 
        then true
        else && |newViewMsgs| == 1
             && var newViewMsg :| newViewMsg in newViewMsgs;
             && Some(msg.payload.operationWrapper) == CalculateRestrictionForSeqID(c, 
                                                                                   v,
                                                                                   msg.payload.seqID, 
                                                                                   newViewMsg))

  }

  // Predicate that describes what is needed and how we mutate the state v into v' when RecvPrePrepare
  // Action is taken. We use the "binding" variable msgOps through which we send/recv messages. In this 
  // predicate we need to reflect in our next state that we have received the PrePrepare message.
  predicate RecvPrePrepare(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>)
  {
    && v.WF(c)
    && msgOps.IsRecv()
    && var msg := msgOps.recv.value;
    && IsValidPrePrepareToAccept(c, v, msg)
    && v' == v.(workingWindow := 
                v.workingWindow.(prePreparesRcvd := 
                                 v.workingWindow.prePreparesRcvd[msg.payload.seqID := Some(msg)]))
  }

  // For clarity here we have extracted all preconditions that must hold for a Replica to accept a Prepare
  predicate IsValidPrepareToAccept(c:Constants, v:Variables, msg:Network.Message<Message>)
  {
    && v.WF(c)
    && msg.payload.Prepare?
    && msg.payload.seqID in v.workingWindow.getActiveSequenceIDs(c)
    && c.clusterConfig.IsReplica(msg.sender)
    && ViewIsActive(c, v)
    && msg.payload.view == v.view
    && v.workingWindow.prePreparesRcvd[msg.payload.seqID].Some?
    && v.workingWindow.prePreparesRcvd[msg.payload.seqID].value.payload.operationWrapper == msg.payload.operationWrapper
    && msg.sender !in v.workingWindow.preparesRcvd[msg.payload.seqID] // We stick to the first vote from a peer.
  }

  // Predicate that describes what is needed and how we mutate the state v into v' when RecvPrepare
  // Action is taken. We use the "binding" variable msgOps through which we send/recv messages. In this 
  // predicate we need to reflect in our next state that we have received the Prepare message.
  predicate RecvPrepare(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>)
  {
    && v.WF(c)
    && msgOps.IsRecv()
    && var msg := msgOps.recv.value;
    && IsValidPrepareToAccept(c, v, msg)
    && v' == v.(workingWindow := 
                v.workingWindow.(preparesRcvd := 
                                 v.workingWindow.preparesRcvd[msg.payload.seqID := 
                                 v.workingWindow.preparesRcvd[msg.payload.seqID][msg.sender := msg]]))
  }

  // 
  predicate IsValidCommitToAccept(c:Constants, v:Variables, msg:Network.Message<Message>)
  {
    && v.WF(c)
    && msg.payload.Commit?
    && msg.payload.seqID in v.workingWindow.getActiveSequenceIDs(c)
    && c.clusterConfig.IsReplica(msg.sender)
    && ViewIsActive(c, v)
    && msg.payload.view == v.view
    && v.workingWindow.prePreparesRcvd[msg.payload.seqID].Some?
    && v.workingWindow.prePreparesRcvd[msg.payload.seqID].value.payload.operationWrapper == msg.payload.operationWrapper
    && msg.sender !in v.workingWindow.commitsRcvd[msg.payload.seqID] // We stick to the first vote from a peer.
  }

  predicate RecvCommit(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>)
  {
    && v.WF(c)
    && msgOps.IsRecv()
    && var msg := msgOps.recv.value;
    && IsValidCommitToAccept(c, v, msg)
    && v' == v.(workingWindow := 
               v.workingWindow.(commitsRcvd :=
                                 v.workingWindow.commitsRcvd[msg.payload.seqID := 
                                 v.workingWindow.commitsRcvd[msg.payload.seqID][msg.sender := msg]]))
  }

  predicate QuorumOfCommits(c:Constants, v:Variables, seqID:SequenceID) 
    requires v.WF(c)
  {
    && seqID in v.workingWindow.getActiveSequenceIDs(c)
    && |v.workingWindow.commitsRcvd[seqID]| >= c.clusterConfig.AgreementQuorum()
  }

  predicate DoCommit(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>, seqID:SequenceID)
  {
    && v.WF(c)
    && msgOps.NoSendRecv()
    && seqID in v.workingWindow.getActiveSequenceIDs(c)
    && QuorumOfPrepares(c, v, seqID)
    && QuorumOfCommits(c, v, seqID)
    && v.workingWindow.prePreparesRcvd[seqID].Some?
    && var msg := v.workingWindow.prePreparesRcvd[seqID].value;
    // TODO: We should be able to commit empty (Noop) operations as well
    && v' == v.(workingWindow := 
               v.workingWindow.(committedClientOperations :=
                                 v.workingWindow.committedClientOperations[msg.payload.seqID := 
                                                                          Some(msg.payload.operationWrapper)]))
  }

  predicate ContiguousCommits(c:Constants, v:Variables, targetSeqID:SequenceID)
    requires v.WF(c)
    requires targetSeqID in v.workingWindow.getActiveSequenceIDs(c)
  {
    && (forall seqID | && seqID <= targetSeqID
                       && seqID > v.workingWindow.lastStableCheckpoint
                     :: v.workingWindow.committedClientOperations[seqID].Some?)
  }

  predicate Execute(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>, seqID:SequenceID)
  {
    && v.WF(c)
    && msgOps.NoSendRecv()
    && seqID in v.workingWindow.getActiveSequenceIDs(c)
    && v.countExecutedSeqIDs < seqID
    && ContiguousCommits(c, v, seqID)
    && v' == v.(countExecutedSeqIDs := seqID)
  }

  predicate QuorumOfPrepares(c:Constants, v:Variables, seqID:SequenceID)
    requires v.WF(c)
  {
    && seqID in v.workingWindow.getActiveSequenceIDs(c)
    && |v.workingWindow.preparesRcvd[seqID]| >= c.clusterConfig.AgreementQuorum()
  }

  // Predicate that describes what is needed and how we mutate the state v into v' when SendPrepare
  // Action is taken. We use the "binding" variable msgOps through which we send/recv messages. In this 
  // predicate we do not mutate the next state, relying on the fact that messages will be broadcast
  // and we will be able to receive our own message and store it as described in the RecvPrepare predicate.
  predicate SendPrepare(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>, seqID:SequenceID)
  {
    && v.WF(c)
    && msgOps.IsSend()
    && seqID in v.workingWindow.getActiveSequenceIDs(c)
    && ViewIsActive(c, v)
    && v.workingWindow.prePreparesRcvd[seqID].Some?
    && msgOps.send == Some(Network.Message(c.myId,
                                       Prepare(v.view, 
                                               seqID,
                                               v.workingWindow.prePreparesRcvd[seqID].value.payload.operationWrapper)))
    && assert msgOps.send.value.payload.Prepare?; true
    && v' == v
  }

  // Predicate that describes what is needed and how we mutate the state v into v' when SendCommit
  // Action is taken. We use the "binding" variable msgOps through which we send/recv messages. In this 
  // predicate we do not mutate the next state, relying on the fact that messages will be broadcast
  // and we will be able to receive our own message and store it as described in the RecvCommit predicate.
  predicate SendCommit(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>, seqID:SequenceID)
  {
    && v.WF(c)
    && msgOps.IsSend()
    && seqID in v.workingWindow.getActiveSequenceIDs(c)
    && ViewIsActive(c, v)
    && QuorumOfPrepares(c, v, seqID)
    && v.workingWindow.prePreparesRcvd[seqID].Some?
    && msgOps.send == Some(Network.Message(c.myId,
                                     Commit(v.view,
                                            seqID,
                                            v.workingWindow.prePreparesRcvd[seqID].value.payload.operationWrapper)))
    && assert msgOps.send.value.payload.Commit?; true

    && v' == v
  }

  predicate LeaveView(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>, newView:ViewNum) {
    // TODO: Clear all Working Window after we leave a View.
    && v.WF(c)
    && msgOps.NoSendRecv()
    // We can only leave a view we have collected at least 2F+1 View 
    // Change messages for in viewChangeMsgsRecvd or View is 0.
    && (|| (HasCollectedProofMyViewIsAgreed(c, v) && newView == v.view + 1)
        || HaveSufficientVCMsgsToMoveTo(c, v, newView))
    && var vcMsg := Network.Message(c.myId, ViewChangeMsg(newView, ExtractCertificatesFromWorkingWindow(c, v)));
    && (forall seqID :: seqID in vcMsg.payload.certificates ==> 
               (vcMsg.payload.certificates[seqID].valid(c.clusterConfig.AgreementQuorum())))
    && v' == v.(view := newView)
              .(viewChangeMsgsRecvd := v.viewChangeMsgsRecvd.(msgs := v.viewChangeMsgsRecvd.msgs + {vcMsg}))
  }

  function ExtractCertificatesFromWorkingWindow(c:Constants, v:Variables) : imap<SequenceID, PreparedCertificate> //TODO refactor after Checkpoint is added.
    requires v.WF(c)
  {
    imap seqID | seqID in v.workingWindow.preparesRcvd :: ExtractCertificateForSeqID(c, v, seqID)
  }

  function ExtractCertificateForSeqID(c:Constants, v:Variables, seqID:SequenceID) : PreparedCertificate
    requires v.WF(c)
    requires seqID in v.workingWindow.getActiveSequenceIDs(c)
  {
    var preparesRecvd := set msg | msg in v.workingWindow.preparesRcvd[seqID].Values && msg.payload.Prepare?;
    if |preparesRecvd| < c.clusterConfig.AgreementQuorum() 
    then PreparedCertificate({})
    else PreparedCertificate(preparesRecvd)
  }

  predicate SendViewChangeMsg(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>)
  {
    && v.WF(c)
    && msgOps.IsSend()
    && var msg := msgOps.send.value;
    && msg.payload.ViewChangeMsg?
    && msg.payload.newView <= v.view
    && msg.sender == c.myId
    && msg in v.viewChangeMsgsRecvd.msgs
    && v' == v
  }

  predicate SelectQuorumOfViewChangeMsgs(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>, viewChangeMsgsSelectedByPrimary:ViewChangeMsgsSelectedByPrimary) {
    && v.WF(c)
    && msgOps.NoSendRecv()
    && CurrentPrimary(c, v) == c.myId
    && (forall msg | && msg in v.newViewMsgsRecvd.msgs 
                     && msg.sender == c.myId
                       :: msg.payload.newView != v.view) // We can only select 1 set of VC msgs
    && (forall vcMsg | vcMsg in viewChangeMsgsSelectedByPrimary.msgs 
                       :: && vcMsg in v.viewChangeMsgsRecvd.msgs)
    && viewChangeMsgsSelectedByPrimary.valid(v.view, c.clusterConfig.AgreementQuorum())
    && var newViewMsg := Network.Message(c.myId, 
                                         NewViewMsg(v.view, viewChangeMsgsSelectedByPrimary));
    && v' == v.(newViewMsgsRecvd := v.newViewMsgsRecvd.(msgs := v.newViewMsgsRecvd.msgs + {newViewMsg}))
  }

  predicate SendNewViewMsg(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>)
  {
    && v.WF(c)
    && msgOps.IsSend()
    && var msg := msgOps.send.value;
    && msg.payload.NewViewMsg?
    && msg.payload.newView <= v.view
    && msg.sender == c.myId
    && msg in v.newViewMsgsRecvd.msgs
    && v' == v
  }

  predicate RecvViewChangeMsg(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>)
  {
    && v.WF(c)
    && msgOps.IsRecv()
    && var msg := msgOps.recv.value;
    && msg.payload.ViewChangeMsg?
    && (forall seqID | seqID in msg.payload.certificates
            :: && msg.payload.certificates[seqID].votes <= msgOps.signedMsgsToCheck
               && msg.payload.certificates[seqID].valid(c.clusterConfig.AgreementQuorum()))
    && v' == v.(viewChangeMsgsRecvd := v.viewChangeMsgsRecvd.(msgs := v.viewChangeMsgsRecvd.msgs + {msg}))
  }

  predicate RecvNewViewMsg(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>)
  {
    && v.WF(c)
    && msgOps.IsRecv()
    && var msg := msgOps.recv.value;
    && msg.payload.NewViewMsg?
    && CurrentPrimary(c, v) == msg.sender
    && msg.payload.newView == v.view
    && msg.payload.vcMsgs.msgs <= msgOps.signedMsgsToCheck
    // Check that all the PreparedCertificates are valid
    && msg.payload.vcMsgs.valid(v.view, c.clusterConfig.AgreementQuorum())
    // We only allow the primary to select 1 set of View Change messages per view.
    && (forall storedMsg | storedMsg in v.newViewMsgsRecvd.msgs :: msg.payload.newView != storedMsg.payload.newView)
    && v' == v.(newViewMsgsRecvd := v.newViewMsgsRecvd.(msgs := v.newViewMsgsRecvd.msgs + {msg}))
  }

  predicate SendCheckpoint(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>, seqID:SequenceID) {
    && v.WF(c)
    && msgOps.IsSend()
    && var msg := msgOps.send.value;
    && msg.payload.CheckpointMsg?
    && msg.payload.seqIDReached <= v.countExecutedSeqIDs
    && v == v'
  }

  predicate RecvCheckpoint(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>) {
    && v.WF(c)
    && msgOps.IsRecv()
    && var msg := msgOps.recv.value;
    && msg.payload.CheckpointMsg?
    && v' == v.(checkpointMsgsRecvd := v.checkpointMsgsRecvd.(msgs := v.checkpointMsgsRecvd.msgs + {msg}))
  }

  predicate HasStableCheckpointForSeqID(c:Constants, v:Variables, seqID:SequenceID) {
    && v.WF(c)
    && var relevantCheckpointMsgs := set msg | && msg in v.checkpointMsgsRecvd.msgs 
                                               && msg.payload.CheckpointMsg?
                                               && msg.payload.seqIDReached == seqID;
    && |relevantCheckpointMsgs| >= c.clusterConfig.AgreementQuorum()
  }

  predicate AdvanceWorkingWindow(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>, seqID:SequenceID) {
    && v.WF(c)
    && msgOps.NoSendRecv()
    && seqID > v.workingWindow.lastStableCheckpoint
    && HasStableCheckpointForSeqID(c, v, seqID)
    && v' == v.(workingWindow := v.workingWindow.(
      lastStableCheckpoint := seqID, 
      prePreparesRcvd := v.workingWindow.Shift(c, v.workingWindow.prePreparesRcvd, None),
      preparesRcvd := v.workingWindow.Shift(c, v.workingWindow.preparesRcvd, EmptyPrepareProofSet()),
      commitsRcvd := v.workingWindow.Shift(c, v.workingWindow.commitsRcvd, EmptyCommitProofSet())))
  }

  predicate Init(c:Constants, v:Variables) {
    && v.WF(c)
    && v.view == 0
    && (forall seqID | seqID in v.workingWindow.committedClientOperations
                :: v.workingWindow.committedClientOperations[seqID].None?)
    && (forall seqID | seqID in v.workingWindow.prePreparesRcvd
                :: v.workingWindow.prePreparesRcvd[seqID].None?)
    && (forall seqID | seqID in v.workingWindow.preparesRcvd :: v.workingWindow.preparesRcvd[seqID] == EmptyPrepareProofSet())
    && (forall seqID | seqID in v.workingWindow.commitsRcvd :: v.workingWindow.commitsRcvd[seqID] == EmptyCommitProofSet())
    && v.viewChangeMsgsRecvd.msgs == {}
    && v.newViewMsgsRecvd.msgs == {}
  }

  // Jay Normal Form - Dafny syntactic sugar, useful for selecting the next step
  datatype Step =
    //| RecvClientOperation()
    | SendPrePrepareStep()
    | RecvPrePrepareStep()
    | SendPrepareStep(seqID:SequenceID)
    | RecvPrepareStep()
    | SendCommitStep(seqID:SequenceID)
    | RecvCommitStep()
    | DoCommitStep(seqID:SequenceID)
    | ExecuteStep(seqID:SequenceID)
    | SendCheckpointStep(seqID:SequenceID)
    | RecvCheckpointStep()
    | AdvanceWorkingWindowStep(seqID:SequenceID)
    //| SendReplyToClient(seqID:SequenceID)
    // TODO: uncomment those steps when we start working on the proof
    // | LeaveViewStep(newView:ViewNum)
    // | SendViewChangeMsgStep()
    // | RecvViewChangeMsgStep()
    // | SelectQuorumOfViewChangeMsgsStep(viewChangeMsgsSelectedByPrimary:ViewChangeMsgsSelectedByPrimary)
    // | SendNewViewMsgStep()
    // | RecvNewViewMsgStep()

  predicate NextStep(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>, step: Step) {
    match step
       case SendPrePrepareStep() => SendPrePrepare(c, v, v', msgOps)
       case RecvPrePrepareStep() => RecvPrePrepare(c, v, v', msgOps)
       case SendPrepareStep(seqID) => SendPrepare(c, v, v', msgOps, seqID)
       case RecvPrepareStep() => RecvPrepare(c, v, v', msgOps)
       case SendCommitStep(seqID) => SendCommit(c, v, v', msgOps, seqID)
       case RecvCommitStep() => RecvCommit(c, v, v', msgOps)
       case DoCommitStep(seqID) => DoCommit(c, v, v', msgOps, seqID)
       case ExecuteStep(seqID) => Execute(c, v, v', msgOps, seqID)
       case SendCheckpointStep(seqID) => SendCheckpoint(c, v, v', msgOps, seqID)
       case RecvCheckpointStep() => RecvCheckpoint(c, v, v', msgOps)
       case AdvanceWorkingWindowStep(seqID) => AdvanceWorkingWindow(c, v, v', msgOps, seqID)
       // TODO: uncomment those steps when we start working on the proof
       // case LeaveViewStep(newView) => LeaveView(c, v, v', msgOps, newView)
       // case SendViewChangeMsgStep() => SendViewChangeMsg(c, v, v', msgOps)
       // case RecvViewChangeMsgStep() => RecvViewChangeMsg(c, v, v', msgOps)
       // case SelectQuorumOfViewChangeMsgsStep(viewChangeMsgsSelectedByPrimary) => SelectQuorumOfViewChangeMsgs(c, v, v', msgOps, viewChangeMsgsSelectedByPrimary)
       // case SendNewViewMsgStep() => SendNewViewMsg(c, v, v', msgOps)
       // case RecvNewViewMsgStep() => RecvNewViewMsg(c, v, v', msgOps)
  }

  predicate Next(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>) {
    exists step :: NextStep(c, v, v', msgOps, step)
  }
}
