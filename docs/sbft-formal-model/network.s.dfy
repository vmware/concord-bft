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

module HostIdentifiers {
  // A trusted axiom telling us how many hosts are participating.
  function NumHosts() : int
    ensures NumHosts() > 0

  type HostId = int // Pretty type synonym (a la C typedef) //TODO move accordingly

  predicate ValidHostId(hostid: HostId) {
    0 <= hostid < NumHosts()
  }

  // The set of all host identities.
  function AllHosts() : set<HostId> {
    set hostid:HostId |
      && 0<=hostid<NumHosts() // This line is entirely redundant, but it satisfies Dafny's finite-set heuristic
      && ValidHostId(hostid)
  }
}

// This version of Network uses a template parameter to avoid having to declare
// the Message type before the Network module. (Contrast with ch05/ex02.)
module Network {
  import opened Library
  import opened HostIdentifiers

  datatype Message<Payload(==)> = Message(sender:HostId, payload:Payload)

  // A MessageOps is a "binding variable" used to connect a Host's Next step
  // (what message got received, what got sent?) with the Network (only allow
  // receipt of messages sent prior; record newly-sent messages).
  // Note that both fields are Option. A step predicate can say recv.None?
  // to indicate that it doesn't need to receive a message to occur.
  // It can say send.None? to indicate that it doesn't want to transmit a message.
  datatype MessageOps<Payload> = MessageOps(recv:Option<Message<Payload>>, 
                                            send:Option<Message<Payload>>,
                                            signedMsgsToCheck:set<Message<Payload>>) {
    predicate IsSend()
    {
      && recv.None?
      && send.Some?
    }

    predicate IsRecv()
    {
      && recv.Some?
      && send.None?
    }

    predicate NoSendRecv()
    {
      && recv.None?
      && send.None?
    }
  }

  datatype Constants = Constants  // no constants for network

  // Network state is the set of messages ever sent. Once sent, we'll
  // allow it to be delivered over and over.
  // (We don't have packet headers, so duplication, besides being realistic,
  // also doubles as how multiple parties can hear the message.)
  datatype Variables<Payload> = Variables(sentMsgs:set<Message<Payload>> // We only record messages that are not forged.
                                         )

  predicate Init(c: Constants, v: Variables)
  {
    && v.sentMsgs == {}
  }

  predicate Next(c: Constants, v: Variables, v': Variables, msgOps: MessageOps, sender:HostId)
  {
    // Only allow receipt of a message if we've seen it has been sent.
    && (msgOps.recv.Some? ==> msgOps.recv.value in v.sentMsgs)
    // Record the sent message, if there was one.
    && (v'.sentMsgs == v.sentMsgs 
                       + if msgOps.send.Some? && msgOps.send.value.sender == sender
                       then {msgOps.send.value} 
                       else {})
    && msgOps.signedMsgsToCheck <= v.sentMsgs
  }
}
