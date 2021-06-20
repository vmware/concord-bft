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

#include "gtest/gtest.h"

#include "assertUtils.hpp"
#include "bftengine/ClientMsgs.hpp"

#include "msg_receiver.h"
#include "bftclient/bft_client.h"

using namespace bft::client;

TEST(msg_receiver_tests, unmatched_replies_returned_no_rsi) {
  MsgReceiver receiver;
  receiver.activate(64 * 1024);
  auto data_len = 20u;
  std::vector<char> reply(sizeof(bftEngine::ClientReplyMsgHeader) + data_len);

  // Fill in the header part of the reply
  auto* header = reinterpret_cast<bftEngine::ClientReplyMsgHeader*>(reply.data());
  header->msgType = REPLY_MSG_TYPE;
  header->currentPrimaryId = 1;
  header->reqSeqNum = 100;
  header->replyLength = data_len;
  header->replicaSpecificInfoLength = 0;

  // Handle the message
  auto source = 1;
  receiver.onNewMessage(source, reply.data(), reply.size());

  // Wait to see that the message gets properly unpacked and delivered.
  auto replies = receiver.wait(1ms);

  ASSERT_EQ(1, replies.size());
  ASSERT_EQ(header->currentPrimaryId, replies[0].metadata.primary->val);
  ASSERT_EQ(header->reqSeqNum, replies[0].metadata.seq_num);
  ASSERT_EQ(Msg(data_len), replies[0].data);
  ASSERT_EQ(1, replies[0].rsi.from.val);
  ASSERT_EQ(0, replies[0].rsi.data.size());
}

TEST(msg_receiver_tests, unmatched_replies_with_rsi) {
  MsgReceiver receiver;
  receiver.activate(64 * 1024);
  auto data_len = 20u;
  std::vector<char> reply(sizeof(bftEngine::ClientReplyMsgHeader) + data_len);

  // Fill in the header part of the reply
  auto* header = reinterpret_cast<bftEngine::ClientReplyMsgHeader*>(reply.data());
  header->msgType = REPLY_MSG_TYPE;
  header->currentPrimaryId = 1;
  header->reqSeqNum = 100;
  header->replyLength = data_len;
  header->replicaSpecificInfoLength = 5;

  // Handle the message
  auto source = 1;
  receiver.onNewMessage(source, reply.data(), reply.size());

  // Wait to see that the message gets properly unpacked and delivered.
  auto replies = receiver.wait(1ms);

  ASSERT_EQ(1, replies.size());
  ASSERT_EQ(header->currentPrimaryId, replies[0].metadata.primary->val);
  ASSERT_EQ(header->reqSeqNum, replies[0].metadata.seq_num);
  ASSERT_EQ(Msg(data_len - header->replicaSpecificInfoLength), replies[0].data);
  ASSERT_EQ(1, replies[0].rsi.from.val);
  ASSERT_EQ(Msg(header->replicaSpecificInfoLength), replies[0].rsi.data);
}

TEST(msg_receiver_tests, no_replies_small_msg) {
  MsgReceiver receiver;
  receiver.activate(64 * 1024);
  std::vector<char> reply(sizeof(bftEngine::ClientReplyMsgHeader) - 1);

  // Handle the message
  auto source = 1;
  receiver.onNewMessage(source, reply.data(), reply.size());

  // Wait to see that the message gets properly unpacked and delivered.
  auto replies = receiver.wait(1ms);

  ASSERT_EQ(0, replies.size());
}

TEST(msg_receiver_tests, no_replies_bad_msg_type) {
  MsgReceiver receiver;
  receiver.activate(64 * 1024);
  std::vector<char> reply(sizeof(bftEngine::ClientReplyMsgHeader) + 20);

  // Fill in the header part of the reply
  auto* header = reinterpret_cast<bftEngine::ClientReplyMsgHeader*>(reply.data());
  header->msgType = REQUEST_MSG_TYPE;

  // Handle the message
  auto source = 1;
  receiver.onNewMessage(source, reply.data(), reply.size());

  // Wait to see that the message gets properly unpacked and delivered.
  auto replies = receiver.wait(1ms);
  ASSERT_EQ(0, replies.size());
}

std::set<ReplicaId> destinations(uint16_t n) {
  std::set<ReplicaId> replicas;
  for (uint16_t i = 0; i < n; i++) {
    replicas.insert(ReplicaId{i});
  }
  return replicas;
}

std::set<ReplicaId> ro_destinations(uint16_t n, uint16_t start_index) {
  std::set<ReplicaId> replicas;
  for (uint16_t i = start_index; i < start_index + n; i++) {
    replicas.insert(ReplicaId{i});
  }
  return replicas;
}

std::vector<ReplicaSpecificInfo> create_rsi(uint16_t n) {
  std::vector<ReplicaSpecificInfo> rsi;
  for (uint16_t i = 0; i < n; i++) {
    rsi.push_back(ReplicaSpecificInfo{ReplicaId{i}, {(uint8_t)i}});
  }
  return rsi;
}

std::vector<UnmatchedReply> unmatched_replies(uint16_t n,
                                              const ReplyMetadata& metadata,
                                              const Msg& msg,
                                              const std::vector<ReplicaSpecificInfo>& rsi) {
  ConcordAssert(n <= rsi.size());
  std::vector<UnmatchedReply> replies;
  for (uint16_t i = 0; i < n; i++) {
    replies.emplace_back(UnmatchedReply{metadata, msg, rsi[i]});
  }
  return replies;
}

TEST(matcher_tests, wait_for_1_out_of_1) {
  ReplicaId source{1};
  uint64_t seq_num = 5;
  MatchConfig config{MofN{1, {source}}, seq_num};
  Matcher matcher(config);

  Msg msg = {'h', 'e', 'l', 'l', 'o'};
  auto rsi = ReplicaSpecificInfo{source, {'r', 's', 'i'}};

  ReplicaId primary{1};
  UnmatchedReply reply{ReplyMetadata{primary, seq_num}, msg, rsi};
  auto match = matcher.onReply(std::move(reply));
  ASSERT_TRUE(match.has_value());
  ASSERT_EQ(match.value().reply.matched_data, msg);
  ASSERT_EQ(match.value().reply.rsi[source], rsi.data);
  ASSERT_EQ(match.value().primary.value(), primary);
}

TEST(matcher_tests, wait_for_3_out_of_4) {
  uint64_t seq_num = 5;
  auto sources = destinations(4);
  MatchConfig config{MofN{3, sources}, seq_num};
  Matcher matcher(config);
  ReplicaId primary{1};
  Msg msg = {'h', 'e', 'l', 'l', 'o'};
  auto unmatched = unmatched_replies(4, ReplyMetadata{primary, seq_num}, msg, create_rsi(4));

  // The first two replies don't have quorum. So we get back a nullopt;
  ASSERT_EQ(std::nullopt, matcher.onReply(std::move(unmatched[0])));
  ASSERT_EQ(std::nullopt, matcher.onReply(std::move(unmatched[1])));

  // The third matching reply should trigger success
  auto match = matcher.onReply(std::move(unmatched[2]));
  ASSERT_TRUE(match.has_value());
  ASSERT_EQ(match.value().reply.matched_data, msg);
  ASSERT_EQ(match.value().primary.value(), primary);
  ASSERT_EQ(3, match.value().reply.rsi.size());
  for (auto i = 0u; i < match.value().reply.rsi.size(); i++) {
    const auto& rsi_data = match.value().reply.rsi[ReplicaId{(uint16_t)i}];
    Msg expected{(uint8_t)i};
    ASSERT_EQ(expected, rsi_data);
  }
}

TEST(matcher_tests, wait_for_3_out_of_4_with_mismatches_and_dupes) {
  uint64_t seq_num = 5;
  auto sources = destinations(4);
  MatchConfig config{MofN{3, sources}, seq_num};
  Matcher matcher(config);
  ReplicaId primary{1};
  Msg msg = {'h', 'e', 'l', 'l', 'o'};
  auto unmatched = unmatched_replies(4, ReplyMetadata{primary, seq_num}, msg, create_rsi(4));

  auto dup = unmatched[1];

  auto diff_rsi = dup;
  diff_rsi.rsi.data = {'x'};

  auto bad_seq_num = dup;
  bad_seq_num.metadata.seq_num = 4;

  auto non_matching_data = unmatched[3];
  non_matching_data.data.push_back('x');

  // The first two replies don't have quorum. So we get back a nullopt;
  ASSERT_EQ(std::nullopt, matcher.onReply(std::move(unmatched[0])));
  ASSERT_EQ(std::nullopt, matcher.onReply(std::move(unmatched[1])));

  // Inserting a duplicate of the last message doesn't trigger quorum.
  ASSERT_EQ(std::nullopt, matcher.onReply(std::move(dup)));

  // Inserting the same message but with different rsi doesn't trigger quorum (it overwrites).
  ASSERT_EQ(std::nullopt, matcher.onReply(std::move(diff_rsi)));

  // Inserting the same message but with diff sequence number doesn't trigger quorum
  ASSERT_EQ(std::nullopt, matcher.onReply(std::move(bad_seq_num)));

  // Inserting a message from a new replica, but where the data doesn't match doesn't trigger quorum
  ASSERT_EQ(std::nullopt, matcher.onReply(std::move(non_matching_data)));

  // Finally, inserting a 3rd match triggers success
  auto match = matcher.onReply(std::move(unmatched[2]));
  ASSERT_TRUE(match.has_value());
  ASSERT_EQ(match.value().reply.matched_data, msg);
  ASSERT_EQ(match.value().primary.value(), primary);
  ASSERT_EQ(3, match.value().reply.rsi.size());
  for (auto i = 0u; i < match.value().reply.rsi.size(); i++) {
    const auto& rsi_data = match.value().reply.rsi[ReplicaId{(uint16_t)i}];
    Msg expected{(uint8_t)i};
    if (i == 1) {
      ASSERT_EQ(Msg{'x'}, rsi_data);
    } else {
      ASSERT_EQ(expected, rsi_data);
    }
  }
}

TEST(matcher_tests, wait_for_replies_with_ignoring_primary) {
  auto sources = destinations(3);
  uint64_t seq_num = 5;
  MatchConfig config{MofN{4, destinations(4)}, seq_num, false};
  Matcher matcher(config);

  Msg msg = {'h', 'e', 'l', 'l', 'o'};
  for (const auto& r : sources) {
    auto rsi = ReplicaSpecificInfo{r, {'r', 's', 'i'}};
    UnmatchedReply reply{ReplyMetadata{r, seq_num}, msg, rsi};
    matcher.onReply(std::move(reply));
  }
  auto last_rep = ReplicaId{3};
  auto rsi = ReplicaSpecificInfo{last_rep, {'r', 's', 'i'}};
  UnmatchedReply reply{ReplyMetadata{last_rep, seq_num}, msg, rsi};
  auto match = matcher.onReply(std::move(reply));
  ASSERT_TRUE(match.has_value());
  ASSERT_EQ(match.value().reply.matched_data, msg);
  ASSERT_EQ(match.value().reply.rsi[last_rep], rsi.data);
  ASSERT_FALSE(match.value().primary.has_value());
}

TEST(quorum_tests, valid_quorums_without_destinations) {
  auto all_replicas = destinations(4);
  // Even that we have ro replicas, empty destinations should include only committers. To issue a request to ro replica
  // the user should explicity specify them in the quorum
  auto ro_replicas = ro_destinations(2, 4);
  uint16_t f_val = 1;
  uint16_t c_val = 0;

  QuorumConverter qc(all_replicas, ro_replicas, f_val, c_val);

  // Quorums without destinations should always work, unless they are MofN.
  {
    auto quorum = LinearizableQuorum{};
    auto output = qc.toMofN(quorum);
    ASSERT_EQ(3, output.wait_for);
    ASSERT_EQ(all_replicas, output.destinations);
  }
  {
    auto quorum = ByzantineSafeQuorum{};
    auto output = qc.toMofN(quorum);
    ASSERT_EQ(2, output.wait_for);
    ASSERT_EQ(all_replicas, output.destinations);
  }
  {
    auto quorum = All{};
    auto output = qc.toMofN(quorum);
    ASSERT_EQ(4, output.wait_for);
    ASSERT_EQ(all_replicas, output.destinations);
  }

  // MofN Quorums must have destinations
  {
    auto quorum = MofN{};
    ASSERT_THROW(qc.toMofN(quorum), InvalidDestinationException);
  }
}

TEST(quorum_tests, valid_quorums_with_destinations) {
  auto all_replicas = destinations(4);
  auto ro_replicas = ro_destinations(2, 4);
  uint16_t f_val = 1;
  uint16_t c_val = 0;

  QuorumConverter qc(all_replicas, ro_replicas, f_val, c_val);

  {
    auto quorum = LinearizableQuorum{destinations(3)};
    auto output = qc.toMofN(quorum);
    ASSERT_EQ(3, output.wait_for);
    ASSERT_EQ(destinations(3), output.destinations);
  }
  {
    auto quorum = ByzantineSafeQuorum{destinations(2)};
    auto output = qc.toMofN(quorum);
    ASSERT_EQ(2, output.wait_for);
    ASSERT_EQ(destinations(2), output.destinations);
  }

  {
    // Test a quorum with ro replicas
    auto quorum = All{all_replicas};
    quorum.destinations.insert(ro_replicas.begin(), ro_replicas.end());
    auto output = qc.toMofN(quorum);
    ASSERT_EQ(6, output.wait_for);
    ASSERT_EQ(quorum.destinations, output.destinations);
  }

  {
    auto quorum = All{destinations(1)};
    auto output = qc.toMofN(quorum);
    ASSERT_EQ(1, output.wait_for);
    ASSERT_EQ(destinations(1), output.destinations);
  }

  {
    auto quorum = MofN{2, destinations(3)};
    auto output = qc.toMofN(quorum);
    ASSERT_EQ(2, output.wait_for);
    ASSERT_EQ(destinations(3), output.destinations);
  }
}

TEST(quorum_tests, invalid_destinations) {
  auto all_replicas = destinations(4);
  auto ro_replicas = ro_destinations(2, 4);
  uint16_t f_val = 1;
  uint16_t c_val = 0;

  QuorumConverter qc(all_replicas, ro_replicas, f_val, c_val);

  auto invalid_replicas = destinations(2);
  invalid_replicas.insert(ReplicaId{7});
  {
    auto quorum = LinearizableQuorum{invalid_replicas};
    ASSERT_THROW(qc.toMofN(quorum), InvalidDestinationException);
  }
  {
    auto quorum = ByzantineSafeQuorum{invalid_replicas};
    ASSERT_THROW(qc.toMofN(quorum), InvalidDestinationException);
  }
  {
    auto quorum = All{invalid_replicas};
    ASSERT_THROW(qc.toMofN(quorum), InvalidDestinationException);
  }
  {
    auto quorum = MofN{1, invalid_replicas};
    ASSERT_THROW(qc.toMofN(quorum), InvalidDestinationException);
  }
}

TEST(quorum_tests, bad_quorum_configs) {
  auto all_replicas = destinations(4);
  uint16_t f_val = 1;
  uint16_t c_val = 0;

  QuorumConverter qc(all_replicas, {}, f_val, c_val);

  {
    // Destinations is less than 2F + C + 1
    auto quorum = LinearizableQuorum{destinations(2)};
    ASSERT_THROW(qc.toMofN(quorum), BadQuorumConfigException);
  }
  {
    // Destinations is less than F + 1
    auto quorum = ByzantineSafeQuorum{destinations(1)};
    ASSERT_THROW(qc.toMofN(quorum), BadQuorumConfigException);
  }
  {
    // wait_for > destinations.size()
    auto wait_for = 3u;
    auto quorum = MofN{wait_for, destinations(1)};
    ASSERT_THROW(qc.toMofN(quorum), BadQuorumConfigException);
  }
}

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
