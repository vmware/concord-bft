// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

// This test suite is intended to test that the ThinReplicaClient implementation
// makes Thin Replica RPC calls to the Thin Replica Servers as intended by the
// Thin Replica mechanism design. While the servers should tolerate
// Byzantine-faulty Thin Replica Clients, the production ThinReplicaClient
// implementation is not intended to be Byzantine-faulty, so we expect it will
// only use the RPC calls as intended.

#include "client/thin-replica-client/thin_replica_client.hpp"
#include "client/thin-replica-client/trs_connection.hpp"

#include <log4cplus/configurator.h>
#include "gtest/gtest.h"
#include "thin_replica_client_mocks.hpp"

using com::vmware::concord::thin_replica::Data;
using com::vmware::concord::thin_replica::Events;
using com::vmware::concord::thin_replica::KVPair;
using std::make_shared;
using std::make_unique;
using std::shared_ptr;
using std::string;
using std::vector;
using client::thin_replica_client::BasicUpdateQueue;
using client::thin_replica_client::ThinReplicaClient;
using client::thin_replica_client::ThinReplicaClientConfig;

const string kTestingClientID = "mock_client_id";
const string kTestingJaegerAddress = "127.0.0.1:6831";

namespace {

TEST(trc_rpc_use_test, test_trc_constructor_and_destructor) {
  Data update;
  Events events;
  events.set_block_id(0);
  *update.mutable_events() = events;
  KVPair* events_data = events.add_data();
  events_data->set_key("key");
  events_data->set_value("value");

  shared_ptr<MockDataStreamPreparer> stream_preparer(new RepeatedMockDataStreamPreparer(update, 1));
  auto hasher = make_shared<MockOrderedDataStreamHasher>(stream_preparer);

  uint16_t max_faulty = 1;
  size_t num_replicas = 3 * max_faulty + 1;

  auto update_queue = make_shared<BasicUpdateQueue>();
  auto record = make_shared<ThinReplicaCommunicationRecord>();

  auto server_recorders = CreateMockServerRecorders(num_replicas, stream_preparer, hasher, record);
  auto mock_servers = CreateTrsConnections(server_recorders);
  auto trc_config =
      make_unique<ThinReplicaClientConfig>(kTestingClientID, update_queue, max_faulty, std::move(mock_servers));
  std::shared_ptr<concordMetrics::Aggregator> aggregator;
  auto trc = make_unique<ThinReplicaClient>(std::move(trc_config), aggregator);

  EXPECT_EQ(record->GetTotalCallCount(), 0) << "ThinReplicaClient's constructor appears to have generated RPC "
                                               "call(s) (none were expected).";
  trc_config.reset();
  trc.reset();
  EXPECT_EQ(record->GetTotalCallCount(), 0) << "ThinReplicaClient's destructor appears to have generated RPC call(s) "
                                               "(none were expected).";
}

TEST(trc_rpc_use_test, test_trc_subscribe) {
  Data update;
  Events events;
  events.set_block_id(0);
  *update.mutable_events() = events;
  KVPair* events_data = events.add_data();
  events_data->set_key("key");
  events_data->set_value("value");

  shared_ptr<MockDataStreamPreparer> stream_preparer(new RepeatedMockDataStreamPreparer(update, 1));
  auto hasher = make_shared<MockOrderedDataStreamHasher>(stream_preparer);

  uint16_t max_faulty = 3;
  size_t num_replicas = 3 * max_faulty + 1;

  auto update_queue = make_shared<BasicUpdateQueue>();
  auto record = make_shared<ThinReplicaCommunicationRecord>();

  auto server_recorders = CreateMockServerRecorders(num_replicas, stream_preparer, hasher, record);
  auto mock_servers = CreateTrsConnections(server_recorders);
  auto trc_config =
      make_unique<ThinReplicaClientConfig>(kTestingClientID, update_queue, max_faulty, std::move(mock_servers));
  std::shared_ptr<concordMetrics::Aggregator> aggregator;
  auto trc = make_unique<ThinReplicaClient>(std::move(trc_config), aggregator);

  trc->Subscribe();
  vector<bool> servers_used(num_replicas, false);
  ASSERT_EQ(record->GetReadStateCalls().size(), 1)
      << "ThinReplicaClient::Subscribe's 1-parameter overload generated an "
         "unexpected number of ReadState calls.";
  servers_used[record->GetReadStateCalls().front().first] = true;
  EXPECT_EQ(record->GetReadStateHashCalls().size(), max_faulty)
      << "ThinReplicaClient::Subscribe's 1-parameter overload generated an "
         "unexpected number of ReadStateHash calls.";
  for (const auto& call : record->GetReadStateHashCalls()) {
    EXPECT_FALSE(servers_used[call.first]) << "ThinReplicaClient::Subscribe's 1-parameter overload re-used a "
                                              "server when looking for state consensus.";
    servers_used[call.first] = true;
    EXPECT_EQ(call.second.events().block_id(), 0)
        << "ThinReplicaClient::Subscribe's 1-parameter overload made a "
           "ReadStateHash call with a Block ID inconsistent with the state the "
           "mock servers have been configured to return for ReadState.";
  }

  // Block until an update is received through the subscription stream; we pop 2
  // updates to do this in order to account for the single update the mock
  // server we are using will provide via initial state.
  update_queue->Pop();
  update_queue->Pop();
  servers_used = vector<bool>(num_replicas, false);
  ASSERT_EQ(record->GetSubscribeToUpdatesCalls().size(), 1)
      << "ThinReplicaClient::Subscribe's 1-parameter overload generated an "
         "unexpected number of Subscribe calls.";
  servers_used[record->GetSubscribeToUpdatesCalls().front().first] = true;
  EXPECT_EQ(record->GetSubscribeToUpdatesCalls().front().second.events().block_id(), 1)
      << "ThinReplicaClient::Subscribe's 1-parameter overload made a "
         "SubscribeToUpdates call with a Block ID inconsistent with the "
         "initial state it was provided.";
  EXPECT_EQ(record->GetSubscribeToUpdateHashesCalls().size(), max_faulty)
      << "ThinReplicaClient::Subscribe's 1-parameter overloader generated an "
         "unexpected number of SubscribeToUpdateHashes calls.";
  for (const auto& call : record->GetSubscribeToUpdateHashesCalls()) {
    EXPECT_FALSE(servers_used[call.first]) << "ThinReplicaClient::Subscribe's 1-parameter overload re-used a "
                                              "server when opening subscription streams.";
    servers_used[call.first] = true;
    EXPECT_EQ(call.second.events().block_id(), 1)
        << "ThinReplicaClient::Subscribe's 1-parameter overload made a "
           "SubscribeToUpdateHashes call with a Block ID inconsistent with the "
           "initial state it was provided.";
  }

  EXPECT_LE(record->GetTotalCallCount(), 2 * (1 + max_faulty))
      << "ThinReplicaClient::Subscribe's 1-parameter overload generated "
         "unexpected RPC calls.";

  record->ClearRecords();
  mock_servers = CreateTrsConnections(server_recorders);

  // Note we explicitly reset the trc pointer before the line constructing a new
  // ThinReplicaClient and assigning it to the pointer in order to force the
  // destruction of the existing ThinReplicaClient before construction of a new
  // one.
  trc_config.reset();
  trc.reset();
  trc_config =
      make_unique<ThinReplicaClientConfig>(kTestingClientID, update_queue, max_faulty, std::move(mock_servers));
  trc = make_unique<ThinReplicaClient>(std::move(trc_config), aggregator);
  update_queue->Clear();
  trc->Subscribe(1);

  // Block until an update is received through the subscription stream.
  update_queue->Pop();
  servers_used = vector<bool>(num_replicas, false);
  ASSERT_EQ(record->GetSubscribeToUpdatesCalls().size(), 1)
      << "ThinReplicaClient::Subscribe's 2-parameter overload generated an "
         "unexpected number of Subscribe calls.";
  servers_used[record->GetSubscribeToUpdatesCalls().front().first] = true;
  EXPECT_EQ(record->GetSubscribeToUpdatesCalls().front().second.events().block_id(), 2)
      << "ThinReplicaClient::Subscribe's 2-parameter overload made a "
         "SubscribeToUpdates call with a Block ID inconsistent with the one "
         "given as a parameter to Subscribe.";
  EXPECT_EQ(record->GetSubscribeToUpdateHashesCalls().size(), max_faulty)
      << "ThinReplicaClient::Subscribe's 2-parameter overloade generated an "
         "unexpected number of SubscribeToUpdateHashes calls.";
  for (const auto& call : record->GetSubscribeToUpdateHashesCalls()) {
    EXPECT_FALSE(servers_used[call.first]) << "ThinReplicaClient::Subscribe's 2-parameter overload re-used a "
                                              "server when opening subscription streams.";
    servers_used[call.first] = true;
    EXPECT_EQ(call.second.events().block_id(), 2)
        << "ThinReplicaClient::Subscribe's 2-parameter overload made a "
           "SubscribeToUpdateHashes call with a Block ID inconsistent with the "
           "one given as a parameter to Subscribe.";
  }
  EXPECT_LE(record->GetTotalCallCount(), (1 + max_faulty))
      << "ThinReplicaClient::Subscribe's 2-parameter overload generated "
         "unexpected RPC calls.";
}

// The following additional behaviors should be tested in this unit test suite
// once the AckUpate and Unsubscribe calls are implemented on the Thin Replica
// Server side and the lines to actually make those calls have been added to the
// ThinReplicaClient implementation:
// - A call to ThinReplicaClient::AcknowledgeBlockID should generate exactly one
//   RPC call to AckUpdate.
// - If a ThinReplicaClient has an active subscription, a call to Unsubscribe
//   should generate exactly one RPC call to Unsubscribe.
// - RPC calls to AckUpdate generated by calls to AcknowlegeBlockId should be
//   made with block_id values matching the ones passed to AcknowledgeBlockID.

}  // anonymous namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  log4cplus::initialize();
  log4cplus::BasicConfigurator config;
  config.configure();
  return RUN_ALL_TESTS();
}
