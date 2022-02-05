// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the 'License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#include <thread>

#include "bftengine/ClientMsgs.hpp"
#include "bftclient/bft_client.h"

using namespace bft::client;
using namespace bft::communication;

// A message sent from a client
struct MsgFromClient {
  ReplicaId destination;
  Msg data;
};

// A message queue used to send messages to the behavior thread faking the replicas
class BehaviorQueue {
 public:
  // send a message from the client to the behavior thread
  void push(MsgFromClient&& msg) {
    {
      std::lock_guard<std::mutex> guard(lock_);
      msgs_.push_back(std::move(msg));
    }
    cond_var_.notify_one();
  }

  // Wait for client messages on the behavior thread
  std::vector<MsgFromClient> wait(std::chrono::milliseconds timeout) {
    std::vector<MsgFromClient> new_msgs;
    std::unique_lock<std::mutex> lock(lock_);
    cond_var_.wait_for(lock, timeout, [this] { return !msgs_.empty(); });
    if (!msgs_.empty()) {
      msgs_.swap(new_msgs);
    }
    return new_msgs;
  }

 private:
  std::vector<MsgFromClient> msgs_;
  std::mutex lock_;
  std::condition_variable cond_var_;
};

using Behavior = std::function<void(MsgFromClient, IReceiver*)>;

// Take a callable behavior and run it when a message is received that is intended for a replica.
class BehaviorThreadRunner {
 public:
  BehaviorThreadRunner(Behavior&& b) : behavior_(std::move(b)) {}

  // Replies to the client from the behavior thread are sent using this receiver
  void setClientReceiver(IReceiver* receiver) { receiver_ = receiver; }

  // send messages to the behavior thread
  void send(MsgFromClient&& msg) { msg_queue_.push(std::move(msg)); }

  void operator()() {
    while (!stop_) {
      auto received = msg_queue_.wait(1s);
      for (const auto& msg : received) {
        behavior_(msg, receiver_);
      }
    }
  }

  void stop() { stop_ = true; }

 private:
  IReceiver* receiver_;
  std::atomic<bool> stop_ = false;

  Behavior behavior_;

  // This is used to receive messages sent from the client thread with sendAsyncMessage.
  BehaviorQueue msg_queue_;
};

// This communication fake takes a Behavior callback parameterized by each test. This callback
// receives messages sent into the network via sendAsyncMessage. Depending on the test specific
// behavior it replies with the appropriate reply messages to the receiver.
//
// sendAsyncMessage is called by the client thread (the test itself). Any receiver callbacks must be run in another
// thread or a deadlock will result over the locking of the underlying queue in the receiver. The client thread waits
// on the queue, while the communication thread puts received messages (from fake BFT servers) on the queue by calling
// receiver_->onNewMessage();
class FakeCommunication : public bft::communication::ICommunication {
 public:
  FakeCommunication(Behavior&& behavior) : runner_(std::move(behavior)) {}

  int getMaxMessageSize() override { return 1024; }
  int start() override {
    runner_.setClientReceiver(receiver_);
    fakeCommThread_ = std::thread(std::ref(runner_));
    return 0;
  }
  int stop() override {
    runner_.stop();
    fakeCommThread_.join();
    return 0;
  }
  bool isRunning() const override { return true; }
  ConnectionStatus getCurrentConnectionStatus(NodeNum node) override { return ConnectionStatus::Connected; }

  int send(NodeNum destNode, std::vector<uint8_t>&& msg, NodeNum endpointNum) override {
    runner_.send(MsgFromClient{ReplicaId{(uint16_t)destNode}, std::move(msg)});
    return 0;
  }

  std::set<NodeNum> send(std::set<NodeNum> dests, std::vector<uint8_t>&& msg, NodeNum endpointNum) override {
    for (auto& d : dests) {
      // This is a class used for unit testing, so a copy is passed for simplicity
      runner_.send(MsgFromClient{ReplicaId{(uint16_t)d}, msg});  // a copy is made here!
    }
    return std::set<NodeNum>();
  }

  void setReceiver(NodeNum id, IReceiver* receiver) override { receiver_ = receiver; }

  void restartCommunication(NodeNum i) override {}

 private:
  IReceiver* receiver_;
  BehaviorThreadRunner runner_;
  std::thread fakeCommThread_;
};

inline std::vector<uint8_t> createReply(const MsgFromClient& msg, std::vector<uint8_t> rep_data = {}) {
  const auto* req_header = reinterpret_cast<const bftEngine::ClientRequestMsgHeader*>(msg.data.data());
  std::string reply_data = "reply";
  if (!rep_data.empty()) {
    reply_data = std::string(rep_data.begin(), rep_data.end());
  }
  auto reply_header_size = sizeof(bftEngine::ClientReplyMsgHeader);
  std::vector<uint8_t> reply(reply_header_size + reply_data.size());
  auto* reply_header = reinterpret_cast<bftEngine::ClientReplyMsgHeader*>(reply.data());
  reply_header->currentPrimaryId = 0;
  reply_header->msgType = REPLY_MSG_TYPE;
  reply_header->replicaSpecificInfoLength = 0;
  reply_header->replyLength = reply_data.size();
  reply_header->result = req_header->result;
  reply_header->reqSeqNum = req_header->reqSeqNum;
  reply_header->spanContextSize = 0;
  // Copy the reply data;
  memcpy(reply.data() + reply_header_size, reply_data.data(), reply_data.size());
  return reply;
}

inline void immediateBehaviour(const MsgFromClient& msg, IReceiver* client_receiver) {
  std::vector<uint8_t> reply = createReply(msg);
  client_receiver->onNewMessage(msg.destination.val, reinterpret_cast<const char*>(reply.data()), reply.size());
}

inline void delayedBehaviour(const MsgFromClient& msg, IReceiver* client_receiver) {
  std::vector<uint8_t> reply = createReply(msg);
  std::this_thread::sleep_for(5ms);
  client_receiver->onNewMessage(msg.destination.val, reinterpret_cast<const char*>(reply.data()), reply.size());
}
