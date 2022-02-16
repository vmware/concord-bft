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

  // Jay Normal Form - Dafny syntactic sugar, useful for selecting the next step
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
