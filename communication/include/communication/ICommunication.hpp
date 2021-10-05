// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#pragma once

#include <cstdint>
#include <set>
#include <vector>

namespace bft::communication {

typedef uint64_t NodeNum;

enum class ConnectionStatus { Unknown = 0, Connected, Disconnected };

class IReceiver {
 public:
  // Invoked when a new message is received
  // Notice that the memory pointed by message may be freed immediately
  // after the execution of this method.
  virtual void onNewMessage(NodeNum sourceNode, const char* const message, size_t messageLength) = 0;

  // Invoked when the known status of a connection is changed.
  // For each NodeNum, this method will never be concurrently
  // executed by two different threads.
  virtual void onConnectionStatusChanged(NodeNum node, ConnectionStatus newStatus) = 0;
};

class ICommunication {
 public:
  // returns the maximum supported  message size supported by this object
  virtual int getMaxMessageSize() = 0;

  // Starts the object (including its internal threads).
  // On success, returns 0.
  virtual int start() = 0;

  // Stops the object (including its internal threads).
  // On success, returns 0.
  virtual int stop() = 0;

  virtual bool isRunning() const = 0;

  virtual ConnectionStatus getCurrentConnectionStatus(NodeNum node) = 0;

  // Sends a message on the underlying communication layer to a given
  // destination node. Asynchronous (non-blocking) method.
  // The function takes ownership of the buffer provided.
  // Returns 0 on success.
  virtual int send(NodeNum destNode, std::vector<uint8_t>&& msg) = 0;

  // Sends a message to all nodes in dests set.
  // The function takes ownership of the buffer provided.
  // The return value is a set<NodeNum>.
  //    On success the set is empty.
  //    On failure it contains the NodeNum-s of the destinations for which the message sending has failed.
  virtual std::set<NodeNum> send(std::set<NodeNum> dests, std::vector<uint8_t>&& msg) = 0;

  virtual void setReceiver(NodeNum receiverNum, IReceiver* receiver) = 0;

  virtual void dispose(NodeNum i) = 0;
  virtual ~ICommunication() = default;
};
}  // namespace bft::communication
