// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#pragma once
#include <stdexcept>

// A generic channel interface based around static polymorphism (templates).
//
// We expect to have multiple channel implementations for different purposes, but the sending and
// receiving code should be agnostic to this fact. We want to be able to swap implementations based
// on performance and/or capability.
//
// A channel is made up of a sender and receiver pair. This split prevents misuse of concrete
// channels. For example, if we only returned an "IChannel" type, and the channel was an MPSC channel, we could
// accidentally have two receivers for it, since we would be forced to allow cloning of the type so that we could have
// multiple senders. With seperate sender and receiver types we make it so that we can make the sender copyable, but the
// receiver unique.
//
// Since a channel is based on static polymorphism, and concepts aren't supported in C++ 17, we
// define the interfaces below with an example "NullChannel". All channels should support this
// interface. The rationale for static polymorphism is three fold:
//    * Performance - avoiding virtual call overhead
//    * Avoiding the need to expose the sender and receiver types as pointers or references
//    * Dynamic polymorphism may not be particularly helpful, because we already must parameterize on a message type. It
//      would require all channels to send the same type if stored in the same map, etc...
//
// These channels are also optimized for move operations and as such do not require Msg to be copyable, although in
// almost all instances they will be. This means that if we implement this interface with something like
// boost::lockfree::spsc_queue (which requires copying), and our messages are large, we will likely lose some speed.
namespace concord::channel {

// Thrown when receiver.recv() is called but there are no corresponding senders for the channel anymore.
class NoSendersError : public std::runtime_error {
 public:
  NoSendersError() : std::runtime_error("All senders have been destructed.") {}
};

// Thrown when sender.send() is called but there are no corresponding receivers for the channel anymore.
class NoReceiversError : public std::runtime_error {
 public:
  NoReceiversError() : std::runtime_error("All receivers have been destructed.") {}
};

// The interface for the sender side of a channel
template <typename Msg>
class NullSender {
  // Send a message over a channel.
  //
  // Sending a message on a channel must be non-blocking to prevent distributed deadlocks as a
  // result of cyclic topologies. To enable this, if the channel is full we return the message to
  // the sender. The sender can then determine whether to drop it or buffer it and apply
  // backpressure.
  //
  // Returns std::nullopt when the message is succesfully sent.
  // Returns Msg when the channel is full.
  //
  // Throws NoReceiversError if all receivers have been destructed.
  //
  // It's important to note that there is a race condition, whereby a receiver can be destructed
  // immediately after a successful send. There is no absolute guarantee of delivery. The
  // purpose of returning an error is to prevent endlessly retrying and never noticing that the
  // channel is effectively destroyed.
  std::optional<Msg> send(Msg&& msg) { return std::nullopt; }

  // Return how many elements are in the channel's internal buffer. For some channels, only an
  // estimate will be avaialable, and for some others (mainly lock-free/wait-free) channels, this
  // value will not be available at all. In those cases std::nullopt should be returned.
  std::optional<size_t> size() const { return std::nullopt; }

  // Bounded channels should return how many messages they can store in their internal buffer.
  // Unbounded channels should return std::nullopt;
  std::optional<size_t> capacity() const { return std::nullopt; }
};

// The interface for the receiver side of a channel
template <typename Msg>
class NullReceiver {
  // Receive a message over a channel.
  // Block waiting indefinitely.
  //
  // This example implies that Msg must be default constructable. In a real channel this is not
  // necessary.
  //
  // Throws NoSendersError if all senders have been destructed and there are no more messages left
  // in the communication buffer.
  Msg recv() { return Msg(); }

  // Receive a message over a channel.
  // Wait only until the timeout.
  //
  // Return std::nullopt if a timeout occurred.
  std::optional<Msg> recv(std::chrono::milliseconds timeout) { return std::nullopt; };

  // Return how many elements are in the channel's internal buffer. For some channels, only an
  // estimate will be avaialable, and for some others (mainly lock-free/wait-free) channels, this
  // value will not be available at all. In those cases std::nullopt should be returned.
  std::optional<size_t> size() const { return std::nullopt; }

  // Bounded channels should return how many messages they can store in their internal buffer.
  // Unbounded channels should return std::nullopt;
  std::optional<size_t> capacity() const { return std::nullopt; }
};

}  // namespace concord::channel
