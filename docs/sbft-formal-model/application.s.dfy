//#title Host protocol
//#desc Define the host state machine here: message type, state machine for executing one
//#desc host's part of the protocol.

// See exercise01.dfy for an English design of the protocol.

include "network.s.dfy"

module Application {
  import opened Library
  type State(!new, ==)
  type ClientRequest(!new, ==)
  type ClientReply(!new, ==)
  
  function InitialState() : State
  function Transition(state:State, clientRequest:ClientRequest) : (result:(State, ClientReply))
  // This is an assumption given to us by the user.

  datatype Variables = Variables(
    state:State,
    unprocessedRequests:set<ClientRequest>,
    undeliveredReplies:set<ClientReply>
  )

  predicate Init(v:Variables)
  {
    && v.s == InitialState()
  }

  predicate AcceptRequest(v:Variables, v':Variables, clientRequest:ClientRequest)
  {
    && v' == v.(unprocessedRequests := v.unprocessedRequests + {clientRequest})
  }

  predicate ProcessRequest(v:Variables, v':Variables, clientRequest:ClientRequest)
  {
    && clientRequest in v.unprocessedRequests
    && var newState, clientReply := Transition(v.state, clientRequest);
    && v' == v.(state := newState)
              .(undeliveredReplies := v.undeliveredReplies + {clientReply})
              .(unprocessedRequests := v.unprocessedRequests - {clientRequest})
  }

  predicate DeliverReply(v:Variables, v':Variables, clientReply:ClientReply)
  {
    && clientReply in v.undeliveredReplies
    && v' == v.(undeliveredReplies := v.undeliveredReplies - {clientReply})
  }

  // JayNF
  datatype Step =
// Recvs:
    | AcceptRequestStep()

  predicate NextStep(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>, step: Step) {
    match step
       case RecvPrePrepareStep => RecvPrePrepare(c, v, v', msgOps)
  }

  predicate Next(c:Constants, v:Variables, v':Variables, msgOps:Network.MessageOps<Message>) {
    exists step :: NextStep(c, v, v', msgOps, step)
  }
}
