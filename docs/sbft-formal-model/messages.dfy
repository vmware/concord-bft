include "network.s.dfy"
include "library/Library.dfy"

module Messages {
  import Library
  import opened HostIdentifiers
  import Network

  type SequenceID = nat
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
                                  && v.payload.WF()
                                  && v.payload.newView == view) // All the ViewChange messages have to be for the same View. 
      && (forall v1, v2 | && v1 in msgs
                          && v2 in msgs
                          && v1 != v2
                            :: v1.sender != v2.sender) // unique senders
      && |msgs| == quorumSize
    }
  }

  // Define your Message datatype here.
  datatype Message = | PrePrepare(view:ViewNum, seqID:SequenceID, operationWrapper:OperationWrapper)
                     | Prepare(view:ViewNum, seqID:SequenceID, operationWrapper:OperationWrapper)
                     | Commit(view:ViewNum, seqID:SequenceID, operationWrapper:OperationWrapper)
                     | ClientRequest(clientOp:ClientOperation)
                     | ViewChangeMsg(newView:ViewNum, certificates:imap<SequenceID, PreparedCertificate>) // omitting last stable because we don't have checkpointing yet.
                     | NewViewMsg(newView:ViewNum, vcMsgs:ViewChangeMsgsSelectedByPrimary) {
                       predicate WF() {
                         && (ViewChangeMsg? ==> Library.FullImap(certificates)) //TODO: rename TotalImap
                       }
                     }
  // ToDo: ClientReply
}
