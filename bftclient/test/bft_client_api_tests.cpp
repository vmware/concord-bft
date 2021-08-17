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
#include <cstdlib>
#include <set>
#include <thread>
#include <vector>
#include <iostream>
#include <ctime>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#include <cryptopp/dll.h>
#include <cryptopp/rsa.h>
#include <cryptopp/osrng.h>
#include <cryptopp/base64.h>
#include <cryptopp/files.h>
#include "gtest/gtest.h"
#pragma GCC diagnostic pop

#include "bftengine/ClientMsgs.hpp"
#include "bftclient/bft_client.h"
#include "bftclient/fake_comm.h"
#include "msg_receiver.h"

using namespace std;
using namespace bft::client;
using namespace bft::communication;
using namespace CryptoPP;
using namespace bftEngine::impl;
using namespace bftEngine;
using namespace placeholders;
using namespace concord::secretsmanager;
using ReplicaId_t = bft::client::ReplicaId;

constexpr char KEYS_BASE_PARENT_PATH[] = "/tmp/";
constexpr char KEYS_BASE_PATH[] = "/tmp/transaction_signing_keys";
constexpr char PRIV_KEY_NAME[] = "privkey.pem";
constexpr char PUB_KEY_NAME[] = "pubkey.pem";
constexpr char ENC_PRIV_KEY_NAME[] = "privkey.enc";
constexpr char ENC_SYMM_KEY[] = "15ec11a047f630ca00f65c25f0b3bfd89a7054a5b9e2e3cdb6a772a58251b4c2";
constexpr char ENC_IV[] = "38106509f6528ff859c366747aa04f21";
constexpr char KEYS_GEN_SCRIPT_PATH[] =
    "/concord-bft//scripts/linux/create_concord_clients_transaction_signing_keys.sh";

class ClientApiTestFixture : public ::testing::Test {
 public:
  ClientConfig test_config_ = {ClientId{5},
                               {ReplicaId_t{0}, ReplicaId_t{1}, ReplicaId_t{2}, ReplicaId_t{3}},
                               {},
                               1,
                               0,
                               RetryTimeoutConfig{},
                               nullopt};

  // Just print all received messages from a client
  void PrintBehavior(const MsgFromClient& msg, IReceiver* client_receiver) {
    cout << "Received message for " << msg.destination.val << endl;
  }
};

// parametrzied fixture
class ClientApiTestParametrizedFixture : public ClientApiTestFixture,
                                         public testing::WithParamInterface<tuple<bool, bool, string>> {
 public:
  static void SetUpTestSuite() { srand(time(NULL)); }
  void SetUp() {
    string cmd;
    cmd += "rm -rf " + string(KEYS_BASE_PATH);
    ASSERT_EQ(0, system(cmd.c_str()));
  }

  // validate signature, then print all received messages from a client
  void PrintAndVerifySignature(const MsgFromClient& msg,
                               IReceiver* client_receiver,
                               bool sig_verification_success_expected,
                               bool corrupt_request) {
    auto sig_len = transaction_verifier_->signatureLength();
    auto request = msg.data.data();
    auto request_size = msg.data.size();
    auto signature_data = request + request_size - sig_len;
    const ClientRequestMsgHeader* request_header = reinterpret_cast<const ClientRequestMsgHeader*>(request);
    auto request_data = request + sizeof(ClientRequestMsgHeader) + request_header->spanContextSize;
    if (corrupt_request) {
      if (rand() % 9 < 5)
        request_data--;  // corrupt the request data
      else
        signature_data--;  // corrupt the signature
    }
    std::string data;
    data.resize(request_header->requestLength);
    std::string sig;
    sig.resize(request_header->reqSignatureLength);
    std::memcpy(data.data(), reinterpret_cast<const char*>(request_data), request_header->requestLength);
    std::memcpy(sig.data(), reinterpret_cast<const char*>(signature_data), sig_len);
    bool sig_verification_result = transaction_verifier_->verify(data, sig);
    ASSERT_EQ(sig_verification_result, sig_verification_success_expected);
    PrintBehavior(msg, client_receiver);
    if (sig_verification_result)
      cout << "Verified Signature on message for " << msg.destination.val << endl;
    else
      cout << "Signature verification failed as expected on message for " << msg.destination.val << endl;
  }

  void PrintAndVerifySignatureBehavior(const MsgFromClient& msg, IReceiver* client_receiver) {
    PrintAndVerifySignature(msg, client_receiver, true, false);  // expect verification success
  }

  void PrintAndFailVerifySignatureBehavior(const MsgFromClient& msg, IReceiver* client_receiver) {
    PrintAndVerifySignature(msg, client_receiver, false, corrupt_request_);  // expect verification failure
  }

  void GenerateKeyPair(
      string& out) {  // ASSERT_ macros cannot be used in non-void functions - transfer output as a paramter
    ostringstream cmd;

    cmd << KEYS_GEN_SCRIPT_PATH << " -n 1 -r " << PRIV_KEY_NAME << " -u " << PUB_KEY_NAME << " -o "
        << KEYS_BASE_PARENT_PATH;
    ASSERT_EQ(0, system(cmd.str().c_str()));
    out = string(KEYS_BASE_PATH) + "/1/";  // return keypair path for a single participant
  }

  SecretData GetSecretData() {
    SecretData sd;
    sd.algo = "AES/CBC/PKCS5Padding";
    sd.key = ENC_SYMM_KEY;
    sd.iv = ENC_IV;
    sd.key_length = 256;
    return sd;
  }

  void EncryptPrivKey(SecretData& out, const string& priv_key_file_path) {
    ostringstream encrypted_file_path, cmd;

    encrypted_file_path << KEYS_BASE_PATH << "/1/" << ENC_PRIV_KEY_NAME;
    cmd << "openssl enc -base64 -debug -aes-256-cbc -e -in " << priv_key_file_path << " -K " << ENC_SYMM_KEY << " -iv "
        << ENC_IV << " -p -out " << encrypted_file_path.str();
    ASSERT_EQ(0, system(cmd.str().c_str()));

    out = GetSecretData();  // return secret data
  }

  unique_ptr<concord::util::crypto::IVerifier> transaction_verifier_;
  bool corrupt_request_ = false;
};

// Parameterized test - see INSTANTIATE_TEST_CASE_P for input vector
TEST_P(ClientApiTestParametrizedFixture, print_received_messages_and_timeout) {
  bool sign_transaction = get<0>(GetParam());
  bool is_priv_key_encrypted = get<1>(GetParam());
  const string& scenario = get<2>(GetParam());

  if (sign_transaction) {
    // generate keypair, set test_config_ with path for the actual client code to load the RSASigner
    ostringstream file_path;
    string keypair_path;

    GenerateKeyPair(keypair_path);
    file_path << keypair_path << PRIV_KEY_NAME;
    test_config_.transaction_signing_private_key_file_path = file_path.str();
    if (is_priv_key_encrypted) {
      // encrypt the private key
      ostringstream encrypted_file_path;
      SecretData sd;

      encrypted_file_path << keypair_path << ENC_PRIV_KEY_NAME;
      test_config_.transaction_signing_private_key_file_path = encrypted_file_path.str();
      EncryptPrivKey(sd, file_path.str());
      test_config_.secrets_manager_config = sd;
    }

    // initialize the test's RSAVerifier
    string public_key_full_path({keypair_path + PUB_KEY_NAME});
    std::ifstream file(public_key_full_path);
    std::stringstream stream;
    stream << file.rdbuf();
    auto pub_key_str = stream.str();
    transaction_verifier_.reset(
        new concord::util::crypto::RSAVerifier(pub_key_str, concord::util::crypto::KeyFormat::PemFormat));
  }
  unique_ptr<FakeCommunication> comm;
  if (sign_transaction) {
    if (scenario == "happy_flow")
      comm = make_unique<FakeCommunication>(
          bind(&ClientApiTestParametrizedFixture::PrintAndVerifySignatureBehavior, this, _1, _2));
    else if (scenario == "corrupt_in_dest") {
      comm = make_unique<FakeCommunication>(
          bind(&ClientApiTestParametrizedFixture::PrintAndFailVerifySignatureBehavior, this, _1, _2));
      corrupt_request_ = true;
    }
  } else
    comm = make_unique<FakeCommunication>(bind(&ClientApiTestParametrizedFixture::PrintBehavior, this, _1, _2));

  ASSERT_TRUE(comm);
  Client client(move(comm), test_config_);
  ReadConfig read_config{RequestConfig{false, 1}, All{}};
  read_config.request.timeout = 500ms;
  ASSERT_THROW(client.send(read_config, Msg({1, 2, 3, 4, 5})), TimeoutException);
  client.stop();
}

// 1st element - sign transaction
// 2nd element - is private key encrypted
// 3rd element - negative scenario string
// The comma at the end is due to a bug in gtest 3.09 - https://github.com/google/googletest/issues/2271 - see last
// comment
typedef tuple<bool, bool, string> ClientApiTestParametrizedFixtureInput;
INSTANTIATE_TEST_CASE_P(ClientApiTest,
                        ClientApiTestParametrizedFixture,
                        ::testing::Values(ClientApiTestParametrizedFixtureInput(false, false, "happy_flow"),
                                          ClientApiTestParametrizedFixtureInput(true, false, "happy_flow"),
                                          ClientApiTestParametrizedFixtureInput(true, true, "happy_flow"),
                                          ClientApiTestParametrizedFixtureInput(true, false, "corrupt_in_dest")), );

Msg replyFromRequest(const MsgFromClient& request) {
  const auto* req_header = reinterpret_cast<const ClientRequestMsgHeader*>(request.data.data());
  string reply_data = "world";
  auto reply_header_size = sizeof(ClientReplyMsgHeader);
  Msg reply(reply_header_size + reply_data.size());

  auto* reply_header = reinterpret_cast<ClientReplyMsgHeader*>(reply.data());
  reply_header->currentPrimaryId = 0;
  reply_header->msgType = REPLY_MSG_TYPE;
  reply_header->replicaSpecificInfoLength = 0;
  reply_header->replyLength = reply_data.size();
  reply_header->reqSeqNum = req_header->reqSeqNum;
  reply_header->spanContextSize = 0;

  // Copy the reply data;
  memcpy(reply.data() + reply_header_size, reply_data.data(), reply_data.size());

  return reply;
}

Msg replyFromRequestWithRSI(const MsgFromClient& request, const Msg& rsi) {
  const auto* req_header = reinterpret_cast<const ClientRequestMsgHeader*>(request.data.data());
  string reply_data = "world";
  auto reply_header_size = sizeof(ClientReplyMsgHeader);
  Msg reply(reply_header_size + reply_data.size() + rsi.size());

  auto* reply_header = reinterpret_cast<ClientReplyMsgHeader*>(reply.data());
  reply_header->currentPrimaryId = 0;
  reply_header->msgType = REPLY_MSG_TYPE;
  reply_header->replicaSpecificInfoLength = rsi.size();
  reply_header->replyLength = reply_data.size() + rsi.size();
  reply_header->reqSeqNum = req_header->reqSeqNum;
  reply_header->spanContextSize = 0;

  // Copy the reply data;
  memcpy(reply.data() + reply_header_size, reply_data.data(), reply_data.size());

  // Copy the RSI data
  memcpy(reply.data() + reply_header_size + reply_data.size(), rsi.data(), rsi.size());

  return reply;
}

// Wait for a single retry then return all replies
class RetryBehavior {
 public:
  RetryBehavior(set<ReplicaId_t>& all_replicas) : not_heard_from_yet(all_replicas){};
  void operator()(const MsgFromClient& msg, IReceiver* client_receiver) {
    not_heard_from_yet.erase(msg.destination);
    if (not_heard_from_yet.empty()) {
      auto reply = replyFromRequest(msg);
      client_receiver->onNewMessage((NodeNum)msg.destination.val, (const char*)reply.data(), reply.size());
    }
  }

 private:
  set<ReplicaId_t> not_heard_from_yet;
};

TEST_F(ClientApiTestFixture, receive_reply_after_retry_timeout) {
  unique_ptr<FakeCommunication> comm(new FakeCommunication(RetryBehavior{test_config_.all_replicas}));
  Client client(move(comm), test_config_);
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

TEST_F(ClientApiTestFixture, test_ignore_reply_from_wrong_replica) {
  atomic<bool> sent_reply_from_wrong_replica = false;
  atomic<bool> sent_reply_from_correct_replica = false;
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

  unique_ptr<FakeCommunication> comm(new FakeCommunication(WrongReplicaBehavior));
  Client client(move(comm), test_config_);
  ReadConfig read_config{RequestConfig{false, 1}, All{{ReplicaId_t{1}}}};
  read_config.request.timeout = 1s;
  auto reply = client.send(read_config, Msg({'h', 'e', 'l', 'l', 'o'}));
  Msg expected{'w', 'o', 'r', 'l', 'd'};
  ASSERT_EQ(expected, reply.matched_data);
  ASSERT_EQ(reply.rsi.size(), 1);
  ASSERT_EQ(reply.rsi.count(ReplicaId_t{1}), 1);
  ASSERT_TRUE(sent_reply_from_wrong_replica);
  ASSERT_TRUE(sent_reply_from_correct_replica);
  client.stop();
}

TEST_F(ClientApiTestFixture, primary_gets_learned_on_successful_write_and_cleared_on_timeout) {
  // Writes should initially go to all replicas
  atomic<bool> quorum_of_replies_sent = false;

  auto WriteBehavior = [&](const MsgFromClient& msg, IReceiver* client_receiver) {
    static set<ReplicaId_t> not_heard_from_yet = test_config_.all_replicas;
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

  unique_ptr<FakeCommunication> comm(new FakeCommunication(WriteBehavior));
  Client client(move(comm), test_config_);
  WriteConfig config{RequestConfig{false, 1}, LinearizableQuorum{}};
  config.request.timeout = 500ms;
  auto reply = client.send(config, Msg({'h', 'e', 'l', 'l', 'o'}));
  Msg expected{'w', 'o', 'r', 'l', 'd'};
  ASSERT_EQ(expected, reply.matched_data);
  ASSERT_EQ(reply.rsi.size(), 3);
  ASSERT_TRUE(quorum_of_replies_sent);
  ASSERT_EQ(client.primary(), ReplicaId_t{0});
  ASSERT_THROW(client.send(config, Msg({1, 2, 3, 4, 5})), TimeoutException);
  ASSERT_FALSE(client.primary().has_value());
  client.stop();
}

TEST_F(ClientApiTestFixture, write_f_plus_one) {
  auto WriteBehavior = [&](const MsgFromClient& msg, IReceiver* client_receiver) {
    auto reply = replyFromRequest(msg);
    client_receiver->onNewMessage((NodeNum)msg.destination.val, (const char*)reply.data(), reply.size());
  };

  unique_ptr<FakeCommunication> comm(new FakeCommunication(WriteBehavior));
  Client client(move(comm), test_config_);
  // Ensure we only wait for F+1 replies (ByzantineSafeQuorum)
  WriteConfig config{RequestConfig{false, 1}, ByzantineSafeQuorum{}};
  config.request.timeout = 500ms;
  auto reply = client.send(config, Msg({'h', 'e', 'l', 'l', 'o'}));
  Msg expected{'w', 'o', 'r', 'l', 'd'};
  ASSERT_EQ(expected, reply.matched_data);
  ASSERT_EQ(reply.rsi.size(), 2);
  ASSERT_EQ(client.primary(), ReplicaId_t{0});
  client.stop();
}

TEST_F(ClientApiTestFixture, batch_of_writes) {
  auto BatchBehavior = [&](const MsgFromClient& msg, IReceiver* client_receiver) {
    const auto* req_header = reinterpret_cast<const ClientBatchRequestMsgHeader*>(msg.data.data());
    auto* position = msg.data.data();
    position += sizeof(ClientBatchRequestMsgHeader) + req_header->cidSize;
    string reply_data = "world";
    auto reply_header_size = sizeof(ClientReplyMsgHeader);
    Msg reply(reply_header_size + reply_data.size());

    auto* reply_header = reinterpret_cast<ClientReplyMsgHeader*>(reply.data());
    reply_header->currentPrimaryId = 0;
    reply_header->msgType = REPLY_MSG_TYPE;
    reply_header->replicaSpecificInfoLength = 0;
    reply_header->replyLength = reply_data.size();
    for (uint32_t i = 0; i < req_header->numOfMessagesInBatch; i++) {
      const auto* req_header1 = reinterpret_cast<const ClientRequestMsgHeader*>(position);
      reply_header->reqSeqNum = req_header1->reqSeqNum;
      reply_header->spanContextSize = 0;
      // Copy the reply data;
      memcpy(reply.data() + reply_header_size, reply_data.data(), reply_data.size());
      client_receiver->onNewMessage((NodeNum)msg.destination.val, (const char*)reply.data(), reply.size());
      position += sizeof(ClientRequestMsgHeader) + req_header1->cidLength + req_header1->requestLength +
                  req_header1->spanContextSize;
    }
  };

  unique_ptr<FakeCommunication> comm(new FakeCommunication(BatchBehavior));
  Client client(move(comm), test_config_);
  // Ensure we only wait for F+1 replies (ByzantineSafeQuorum)
  WriteConfig config{RequestConfig{false, 1}, ByzantineSafeQuorum{}};
  WriteConfig config2{RequestConfig{false, 2}, ByzantineSafeQuorum{}};
  config.request.timeout = 500ms;
  config2.request.timeout = 500ms;
  WriteRequest request1{config, Msg({'c', 'o', 'n', 'c', 'o', 'r', 'd'})};
  WriteRequest request2{config2, Msg({'h', 'e', 'l', 'l', 'o'})};
  std::deque<WriteRequest> request_queue;
  request_queue.push_back(request1);
  request_queue.push_back(request2);
  auto replies = client.sendBatch(request_queue, request1.config.request.correlation_id);
  Msg expected{'w', 'o', 'r', 'l', 'd'};
  ASSERT_EQ(replies.size(), request_queue.size());
  ASSERT_EQ(expected, replies.begin()->second.matched_data);
  ASSERT_EQ(replies.begin()->second.rsi.size(), 2);
  ASSERT_EQ(client.primary(), ReplicaId_t{0});
  client.stop();
}

TEST_F(ClientApiTestFixture, client_handle_several_batches) {
  auto BatchBehavior = [&](const MsgFromClient& msg, IReceiver* client_receiver) {
    const auto* req_header = reinterpret_cast<const ClientBatchRequestMsgHeader*>(msg.data.data());
    auto* position = msg.data.data();
    position += sizeof(ClientBatchRequestMsgHeader) + req_header->cidSize;
    string reply_data = "world";
    auto reply_header_size = sizeof(ClientReplyMsgHeader);
    Msg reply(reply_header_size + reply_data.size());

    auto* reply_header = reinterpret_cast<ClientReplyMsgHeader*>(reply.data());
    reply_header->currentPrimaryId = 0;
    reply_header->msgType = REPLY_MSG_TYPE;
    reply_header->replicaSpecificInfoLength = 0;
    reply_header->replyLength = reply_data.size();
    for (uint32_t i = 0; i < req_header->numOfMessagesInBatch; i++) {
      const auto* req_header1 = reinterpret_cast<const ClientRequestMsgHeader*>(position);
      reply_header->reqSeqNum = req_header1->reqSeqNum;
      reply_header->spanContextSize = 0;
      // Copy the reply data;
      memcpy(reply.data() + reply_header_size, reply_data.data(), reply_data.size());
      client_receiver->onNewMessage((NodeNum)msg.destination.val, (const char*)reply.data(), reply.size());
      position += sizeof(ClientRequestMsgHeader) + req_header1->cidLength + req_header1->requestLength +
                  req_header1->spanContextSize;
    }
  };

  unique_ptr<FakeCommunication> comm(new FakeCommunication(BatchBehavior));
  Client client(move(comm), test_config_);
  for (uint64_t i = 1; i < 10; i += 2) {
    // Ensure we only wait for F+1 replies (ByzantineSafeQuorum)
    WriteConfig config{RequestConfig{false, i}, ByzantineSafeQuorum{}};
    WriteConfig config2{RequestConfig{false, i + 1}, ByzantineSafeQuorum{}};
    config.request.timeout = 500ms;
    config2.request.timeout = 500ms;
    WriteRequest request1{config, Msg({'c', 'o', 'n', 'c', 'o', 'r', 'd'})};
    WriteRequest request2{config2, Msg({'h', 'e', 'l', 'l', 'o'})};
    std::deque<WriteRequest> request_queue;
    request_queue.push_back(request1);
    request_queue.push_back(request2);
    auto replies = client.sendBatch(request_queue, request1.config.request.correlation_id);
    Msg expected{'w', 'o', 'r', 'l', 'd'};
    ASSERT_EQ(replies.size(), request_queue.size());
    ASSERT_EQ(expected, replies.begin()->second.matched_data);
    ASSERT_EQ(replies.begin()->second.rsi.size(), 2);
    ASSERT_EQ(client.primary(), ReplicaId_t{0});
  }
  client.stop();
}

TEST_F(ClientApiTestFixture, batch_of_writes_no_reply) {
  auto NoReplyBehavior = [&](const MsgFromClient& msg, IReceiver* client_receiver) { return; };

  unique_ptr<FakeCommunication> comm(new FakeCommunication(NoReplyBehavior));
  Client client(move(comm), test_config_);
  // Ensure we only wait for F+1 replies (ByzantineSafeQuorum)
  WriteConfig config{RequestConfig{false, 1}, ByzantineSafeQuorum{}};
  WriteConfig config2{RequestConfig{false, 2}, ByzantineSafeQuorum{}};
  config.request.timeout = 100ms;
  config2.request.timeout = 100ms;
  WriteRequest request1{config, Msg({'c', 'o', 'n', 'c', 'o', 'r', 'd'})};
  WriteRequest request2{config2, Msg({'h', 'e', 'l', 'l', 'o'})};
  std::deque<WriteRequest> request_queue;
  request_queue.push_back(request1);
  request_queue.push_back(request2);
  try {
    auto replies = client.sendBatch(request_queue, request1.config.request.correlation_id);
  } catch (BatchTimeoutException& e) {
    std::cout << e.what();
    client.stop();
  }
}

TEST_F(ClientApiTestFixture, write_f_plus_one_get_differnt_rsi) {
  map<ReplicaId_t, Msg> rsi = {{ReplicaId_t{0}, {0}}, {ReplicaId_t{1}, {1}}};
  auto WriteBehavior = [&](const MsgFromClient& msg, IReceiver* client_receiver) {
    if (msg.destination.val == 0 || msg.destination.val == 1) {
      auto reply = replyFromRequestWithRSI(msg, rsi[msg.destination]);
      client_receiver->onNewMessage((NodeNum)msg.destination.val, (const char*)reply.data(), reply.size());
    }
  };

  unique_ptr<FakeCommunication> comm(new FakeCommunication(WriteBehavior));
  Client client(move(comm), test_config_);
  // Ensure we only wait for F+1 replies (ByzantineSafeQuorum)
  WriteConfig config{RequestConfig{false, 1}, ByzantineSafeQuorum{}};
  config.request.timeout = 500ms;
  auto reply = client.send(config, Msg({'h', 'e', 'l', 'l', 'o'}));
  Msg expected{'w', 'o', 'r', 'l', 'd'};
  ASSERT_EQ(expected, reply.matched_data);
  ASSERT_EQ(reply.rsi.size(), 2);
  ASSERT_EQ(reply.rsi, rsi);
  ASSERT_EQ(client.primary(), ReplicaId_t{0});
  client.stop();
}

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
