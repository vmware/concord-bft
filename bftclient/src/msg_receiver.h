// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use
// this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license
// terms. Your use of these subcomponents is subject to the terms and conditions of the
// subcomponent's license, as noted in the LICENSE file.

#pragma once

#include <atomic>
#include <condition_variable>
#include <queue>

#include "communication/ICommunication.hpp"
#include "Logger.hpp"
#include "bftclient/config.h"

namespace bft::client {

// Metadata stripped from a ClientReplyMsgHeader
//
// This makes the metadata independent of the structure of the messages, allowing us to change them
// later without changing the matcher.
struct ReplyMetadata {
  std::optional<ReplicaId> primary;
  uint64_t seq_num;

  bool operator==(const ReplyMetadata& other) const { return primary == other.primary && seq_num == other.seq_num; }
  bool operator!=(const ReplyMetadata& other) const { return !(*this == other); }
  bool operator<(const ReplyMetadata& other) const {
    if (primary < other.primary) {
      return true;
    }
    return primary == other.primary && seq_num < other.seq_num;
  }
};

// A Reply that has been received, but not yet matched.
struct UnmatchedReply {
  ReplyMetadata metadata;
  Msg data;
  ReplicaSpecificInfo rsi;
};

// A thread-safe queue that allows the ASIO thread to push newly received messages and the client
// send thread to wait for those messages to be received.
class UnmatchedReplyQueue {
 public:
  // Push a new msg on the queue
  //
  // This function is thread safe.
  void push(UnmatchedReply&& reply);

  // Wait for any messages to be pushed.
  //
  // This function is thread safe.
  std::vector<UnmatchedReply> wait(std::chrono::milliseconds timeout);

  // Clear the queue.
  //
  // This is only used when the receiver is deactivated.
  // This function is threadsafe, but expected to be called by the client send thread when there is
  // no waiter.
  void clear();

 private:
  std::vector<UnmatchedReply> msgs_;
  std::mutex lock_;
  std::condition_variable cond_var_;
};

// A message receiver receives packed ClientReplyMsg structs off the wire. This is a
// ClientReplyMsgHeader followed by opaque reply data.
//
// We want to destructure the reply into something sane for the rest of the client to handle.
class MsgReceiver : public bft::communication::IReceiver {
 public:
  // IReceiver methods
  // These are called from the ASIO thread when a new message is received.
  void onNewMessage(bft::communication::NodeNum sourceNode, const char* const message, size_t messageLength) override;

  void onConnectionStatusChanged(bft::communication::NodeNum node,
                                 bft::communication::ConnectionStatus newStatus) override {}

  // Wait for messages to be received.
  //
  // Return all received messages.
  // Return an empty queue if timeout occurs.
  //
  // This should be called from the thread that calls `Client::send`.
  std::vector<UnmatchedReply> wait(std::chrono::milliseconds timeout);

  // We want to drop all replies when there isn't an outstanding request. We also want to know the
  // max size of a reply for an outstanding request so we can drop replies that are too large. This
  // method informs us that a request is in progress. A max_reply_size of 0 means no request is in
  // progress and we should drop everything. Activate should never be called with max_reply_size of 0.
  void activate(uint32_t max_reply_size);
  void deactivate();

 private:
  std::atomic<uint32_t> max_reply_size_ = 0;
  UnmatchedReplyQueue queue_;
  logging::Logger logger_ = logging::getLogger("bftclient.msgreceiver");
};

}  // namespace bft::client
