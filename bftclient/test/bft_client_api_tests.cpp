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

#include <cstring>
#include <set>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

#include "bftengine/ClientMsgs.hpp"
#include "bftclient/bft_client.h"
#include "bftclient/fake_comm.h"
#include "msg_receiver.h"

using namespace bft::client;
using namespace bft::communication;

ClientConfig test_config{
    ClientId{5}, {ReplicaId{0}, ReplicaId{1}, ReplicaId{2}, ReplicaId{3}}, 1, 0, RetryTimeoutConfig{}};

// Just print all received messages from a client
#include <iostream>
void PrintBehavior(const MsgFromClient& msg, IReceiver* client_receiver) {
  std::cout << "Received message for " << msg.destination.val << std::endl;
}

TEST(client_api_tests, print_received_messages_and_timeout) {
  std::unique_ptr<FakeCommunication> comm(new FakeCommunication(PrintBehavior));
  Client client(std::move(comm), test_config);
  ReadConfig read_config{RequestConfig{false, 1}, All{}};
  read_config.request.timeout = 500ms;
  ASSERT_THROW(client.send(read_config, Msg({1, 2, 3, 4, 5})), TimeoutException);
  client.stop();
}

Msg replyFromRequest(const MsgFromClient& request) {
  const auto* req_header = reinterpret_cast<const bftEngine::ClientRequestMsgHeader*>(request.data.data());
  std::string reply_data = "world";
  auto reply_header_size = sizeof(bftEngine::ClientReplyMsgHeader);
  Msg reply(reply_header_size + reply_data.size());

  auto* reply_header = reinterpret_cast<bftEngine::ClientReplyMsgHeader*>(reply.data());
  reply_header->currentPrimaryId = 0;
  reply_header->msgType = REPLY_MSG_TYPE;
  reply_header->replicaSpecificInfoLength = 0;
  reply_header->replyLength = reply_data.size();
  reply_header->reqSeqNum = req_header->reqSeqNum;
  reply_header->spanContextSize = 0;

  // Copy the reply data;
  std::memcpy(reply.data() + reply_header_size, reply_data.data(), reply_data.size());

  return reply;
}

Msg replyFromRequestWithRSI(const MsgFromClient& request, const Msg& rsi) {
  const auto* req_header = reinterpret_cast<const bftEngine::ClientRequestMsgHeader*>(request.data.data());
  std::string reply_data = "world";
  auto reply_header_size = sizeof(bftEngine::ClientReplyMsgHeader);
  Msg reply(reply_header_size + reply_data.size() + rsi.size());

  auto* reply_header = reinterpret_cast<bftEngine::ClientReplyMsgHeader*>(reply.data());
  reply_header->currentPrimaryId = 0;
  reply_header->msgType = REPLY_MSG_TYPE;
  reply_header->replicaSpecificInfoLength = rsi.size();
  reply_header->replyLength = reply_data.size() + rsi.size();
  reply_header->reqSeqNum = req_header->reqSeqNum;
  reply_header->spanContextSize = 0;

  // Copy the reply data;
  std::memcpy(reply.data() + reply_header_size, reply_data.data(), reply_data.size());

  // Copy the RSI data
  std::memcpy(reply.data() + reply_header_size + reply_data.size(), rsi.data(), rsi.size());

  return reply;
}

// Wait for a single retry then return all replies
class RetryBehavior {
 public:
  void operator()(const MsgFromClient& msg, IReceiver* client_receiver) {
    not_heard_from_yet.erase(msg.destination);
    if (not_heard_from_yet.empty()) {
      auto reply = replyFromRequest(msg);
      client_receiver->onNewMessage((NodeNum)msg.destination.val, (const char*)reply.data(), reply.size());
    }
  }

 private:
  std::set<ReplicaId> not_heard_from_yet = test_config.all_replicas;
};

TEST(client_api_tests, receive_reply_after_retry_timeout) {
  std::unique_ptr<FakeCommunication> comm(new FakeCommunication(RetryBehavior{}));
  Client client(std::move(comm), test_config);
  ReadConfig read_config{RequestConfig{false, 1}, All{}};
  read_config.request.timeout = 1s;
  auto reply = client.send(read_config, Msg({'h', 'e', 'l', 'l', 'o'}));
  Msg expected{'w', 'o', 'r', 'l', 'd'};
  ASSERT_EQ(expected, reply.matched_data);
  for (auto& rsi : reply.rsi) {
    ASSERT_TRUE(rsi.second.empty());
  }
  client.stop();
}

static constexpr NodeNum bad_replica_id = 0xbad1d;

TEST(client_api_tests, test_ignore_reply_from_wrong_replica) {
  std::atomic<bool> sent_reply_from_wrong_replica = false;
  std::atomic<bool> sent_reply_from_correct_replica = false;
  auto WrongReplicaBehavior = [&](const MsgFromClient& msg, IReceiver* client_receiver) {
    auto reply = replyFromRequest(msg);
    if (!sent_reply_from_wrong_replica) {
      client_receiver->onNewMessage(bad_replica_id, (const char*)reply.data(), reply.size());
      sent_reply_from_wrong_replica = true;
    } else {
      // We have to update the bool first, since this runs in a separate thread from the test below. Otherwise we could
      // check the value of the bool after the match, but before the bool was set. Setting it early does no harm, as the
      // intention is just to show that the callback fired, which it clearly will. The code will block until the
      // callback fires anyway. Comment out the callback and rerun if you don't believe me :)
      sent_reply_from_correct_replica = true;
      client_receiver->onNewMessage((NodeNum)msg.destination.val, (const char*)reply.data(), reply.size());
    }
  };

  std::unique_ptr<FakeCommunication> comm(new FakeCommunication(WrongReplicaBehavior));
  Client client(std::move(comm), test_config);
  ReadConfig read_config{RequestConfig{false, 1}, All{{ReplicaId{1}}}};
  read_config.request.timeout = 1s;
  auto reply = client.send(read_config, Msg({'h', 'e', 'l', 'l', 'o'}));
  Msg expected{'w', 'o', 'r', 'l', 'd'};
  ASSERT_EQ(expected, reply.matched_data);
  ASSERT_EQ(reply.rsi.size(), 1);
  ASSERT_EQ(reply.rsi.count(ReplicaId{1}), 1);
  ASSERT_TRUE(sent_reply_from_wrong_replica);
  ASSERT_TRUE(sent_reply_from_correct_replica);
  client.stop();
}

TEST(client_api_tests, primary_gets_learned_on_successful_write_and_cleared_on_timeout) {
  // Writes should initially go to all replicas
  std::atomic<bool> quorum_of_replies_sent = false;

  auto WriteBehavior = [&](const MsgFromClient& msg, IReceiver* client_receiver) {
    static std::set<ReplicaId> not_heard_from_yet = test_config.all_replicas;
    auto reply = replyFromRequest(msg);
    // Check for linearizable quorum
    if (not_heard_from_yet.size() != 1) {
      not_heard_from_yet.erase(msg.destination);
      if (not_heard_from_yet.size() == 1) {
        quorum_of_replies_sent = true;
      }
      client_receiver->onNewMessage((NodeNum)msg.destination.val, (const char*)reply.data(), reply.size());
    }
  };

  std::unique_ptr<FakeCommunication> comm(new FakeCommunication(WriteBehavior));
  Client client(std::move(comm), test_config);
  WriteConfig config{RequestConfig{false, 1}, LinearizableQuorum{}};
  config.request.timeout = 500ms;
  auto reply = client.send(config, Msg({'h', 'e', 'l', 'l', 'o'}));
  Msg expected{'w', 'o', 'r', 'l', 'd'};
  ASSERT_EQ(expected, reply.matched_data);
  ASSERT_EQ(reply.rsi.size(), 3);
  ASSERT_TRUE(quorum_of_replies_sent);
  ASSERT_EQ(client.primary(), ReplicaId{0});
  ASSERT_THROW(client.send(config, Msg({1, 2, 3, 4, 5})), TimeoutException);
  ASSERT_FALSE(client.primary().has_value());
  client.stop();
}

TEST(client_api_tests, write_f_plus_one) {
  auto WriteBehavior = [&](const MsgFromClient& msg, IReceiver* client_receiver) {
    auto reply = replyFromRequest(msg);
    client_receiver->onNewMessage((NodeNum)msg.destination.val, (const char*)reply.data(), reply.size());
  };

  std::unique_ptr<FakeCommunication> comm(new FakeCommunication(WriteBehavior));
  Client client(std::move(comm), test_config);
  // Ensure we only wait for F+1 replies (ByzantineSafeQuorum)
  WriteConfig config{RequestConfig{false, 1}, ByzantineSafeQuorum{}};
  config.request.timeout = 500ms;
  auto reply = client.send(config, Msg({'h', 'e', 'l', 'l', 'o'}));
  Msg expected{'w', 'o', 'r', 'l', 'd'};
  ASSERT_EQ(expected, reply.matched_data);
  ASSERT_EQ(reply.rsi.size(), 2);
  ASSERT_EQ(client.primary(), ReplicaId{0});
  client.stop();
}

TEST(client_api_tests, write_f_plus_one_get_differnt_rsi) {
  std::map<ReplicaId, Msg> rsi = {{ReplicaId{0}, {0}}, {ReplicaId{1}, {1}}};
  auto WriteBehavior = [&](const MsgFromClient& msg, IReceiver* client_receiver) {
    if (msg.destination.val == 0 || msg.destination.val == 1) {
      auto reply = replyFromRequestWithRSI(msg, rsi[msg.destination]);
      client_receiver->onNewMessage((NodeNum)msg.destination.val, (const char*)reply.data(), reply.size());
    }
  };

  std::unique_ptr<FakeCommunication> comm(new FakeCommunication(WriteBehavior));
  Client client(std::move(comm), test_config);
  // Ensure we only wait for F+1 replies (ByzantineSafeQuorum)
  WriteConfig config{RequestConfig{false, 1}, ByzantineSafeQuorum{}};
  config.request.timeout = 500ms;
  auto reply = client.send(config, Msg({'h', 'e', 'l', 'l', 'o'}));
  Msg expected{'w', 'o', 'r', 'l', 'd'};
  ASSERT_EQ(expected, reply.matched_data);
  ASSERT_EQ(reply.rsi.size(), 2);
  ASSERT_EQ(reply.rsi, rsi);
  ASSERT_EQ(client.primary(), ReplicaId{0});
  client.stop();
}

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
