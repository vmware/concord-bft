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

#include "client/thin-replica-client/thin_replica_client.hpp"
#include "client/thin-replica-client/trc_hash.hpp"
#include "client/thin-replica-client/trs_connection.hpp"

#include <log4cplus/configurator.h>
#include "gtest/gtest.h"
#include "assertUtils.hpp"
#include "thin_replica_client_mocks.hpp"

using com::vmware::concord::thin_replica::Data;
using com::vmware::concord::thin_replica::Hash;
using com::vmware::concord::thin_replica::KVPair;
using com::vmware::concord::thin_replica::ReadStateHashRequest;
using com::vmware::concord::thin_replica::ReadStateRequest;
using com::vmware::concord::thin_replica::SubscriptionRequest;
using grpc::ClientContext;
using grpc::ClientReaderInterface;
using grpc::Status;
using std::make_shared;
using std::make_unique;
using std::shared_ptr;
using std::string;
using std::to_string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::steady_clock;
using std::chrono::time_point;
using std::this_thread::sleep_for;
using client::thin_replica_client::BasicUpdateQueue;
using client::thin_replica_client::kThinReplicaHashLength;
using client::thin_replica_client::ThinReplicaClient;
using client::thin_replica_client::ThinReplicaClientConfig;
using client::thin_replica_client::Update;
using client::thin_replica_client::UpdateQueue;

const string kTestingClientID = "mock_client_id";
const string kTestingJaegerAddress = "127.0.0.1:6831";
const uint16_t kMaxFaultyIn4NodeCluster = 1;
static_assert((kMaxFaultyIn4NodeCluster * 3 + 1) == 4);

// Helper function(s) and struct(s) to test case(s) in this suite.

bool UpdateMatchesExpected(const Update& update_received, const Data& update_expected) {
  if ((update_received.block_id != update_expected.block_id()) ||
      (update_received.kv_pairs.size() != (size_t)update_expected.data_size())) {
    return false;
  }
  for (size_t i = 0; i < (size_t)update_expected.data_size(); ++i) {
    if ((update_received.kv_pairs[i].first != update_expected.data(i).key()) ||
        (update_received.kv_pairs[i].second != update_expected.data(i).value())) {
      return false;
    }
  }
  return true;
}

void VerifyInitialState(shared_ptr<UpdateQueue>& received_updates,
                        const vector<Data>& expected_updates,
                        size_t num_updates,
                        const string& faulty_description) {
  for (size_t i = 0; i < num_updates; ++i) {
    unique_ptr<Update> received_update = received_updates->Pop();
    ASSERT_TRUE((bool)received_update) << "ThinReplicaClient failed to fetch an expected update from the "
                                          "initial state in the presence of "
                                       << faulty_description << ".";
    EXPECT_TRUE(UpdateMatchesExpected(*received_update, expected_updates[i]))
        << "ThinReplicaClient reported an update not matching an expected "
           "update from the initial state in the presence of "
        << faulty_description << ".";
  }
}

void VerifyUpdates(shared_ptr<UpdateQueue>& received_updates,
                   const vector<Data>& expected_updates,
                   const string& faulty_description) {
  for (size_t i = 0; i < expected_updates.size(); ++i) {
    unique_ptr<Update> received_update = received_updates->Pop();
    ASSERT_TRUE((bool)received_update) << "ThinReplicaClient failed to stream an expected update from a "
                                          "subscription in the presence of "
                                       << faulty_description << ".";
    EXPECT_TRUE(UpdateMatchesExpected(*received_update, expected_updates[i]))
        << "ThinReplicaClient reported an update not matching an expected "
           "update in the subscription stream in the presence of "
        << faulty_description << ".";
  }
}

vector<Data> GenerateSampleUpdateData(size_t data_size) {
  vector<Data> data;
  for (size_t i = 0; i < data_size; ++i) {
    Data update;
    update.set_block_id(i);
    KVPair* kvp = update.add_data();
    kvp->set_key("key" + to_string(i));
    kvp->set_value("value" + to_string(i));
    if (i % 2 == 0) {
      kvp = update.add_data();
      kvp->set_key("recurring_key");
      kvp->set_value(to_string(i / 2));
    }
    data.push_back(update);
  }
  return data;
}

// Struct containing objects commonly used in the Byzantine test cases in this
// suite, with constructor(s) for common pattern(s) of initializing these
// objects. This struct exists primarilly for the purpose of factoring out
// common declarations and initializations that follow routine patterns in the
// test cases in this suite in order to make those cases more concise and less
// repetitive.
struct ByzantineTestCaseState {
 public:
  shared_ptr<MockDataStreamPreparer> correct_data_preparer_;
  shared_ptr<MockOrderedDataStreamHasher> correct_hasher_;
  shared_ptr<ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior> byzantine_behavior_;
  ByzantineMockThinReplicaServerPreparer server_preparer_;
  vector<unique_ptr<ByzantineMockThinReplicaServerPreparer::ByzantineMockServer>> mock_servers_;

  // Objects we anticipate the test case(s) will actually use once the
  // ByzantineTestCaseState is fully set up.
  shared_ptr<UpdateQueue> update_queue_;
  unique_ptr<ThinReplicaClientConfig> trc_config_;
  shared_ptr<concordMetrics::Aggregator> aggregator_;
  unique_ptr<ThinReplicaClient> trc_;

  // Initialize this ByzantineTestCaseState such that trc_ points to a
  // ThinReplicaClient object constructed to connect to a mock cluster of 4 mock
  // servers whose non-faulty behavior is described by data_preparer and whose
  // faulty behavior is described by byzantine_behavior, constructing objects
  // needed to do this as needed and storing those objects (possibly either
  // directly or by smart pointer at the discretion of ByzantineTestCaseState's
  // implementation) in the constructed ByzantineTestCaseState. Note this
  // construction of needed objects includes construction of a new UpdateQueue
  // that trc_ will use and storage of a pointer to this update queue in
  // update_queue_.
  ByzantineTestCaseState(shared_ptr<MockDataStreamPreparer> data_preparer,
                         shared_ptr<ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior> byzantine_behavior,
                         uint16_t max_faulty = kMaxFaultyIn4NodeCluster,
                         size_t num_servers = 4)
      : correct_data_preparer_(data_preparer),
        correct_hasher_(new MockOrderedDataStreamHasher(correct_data_preparer_)),
        byzantine_behavior_(byzantine_behavior),
        server_preparer_(correct_data_preparer_, correct_hasher_, byzantine_behavior_),
        mock_servers_(CreateByzantineMockServers(num_servers, server_preparer_)),
        update_queue_(new BasicUpdateQueue()),
        trc_config_(new ThinReplicaClientConfig(
            kTestingClientID,
            update_queue_,
            max_faulty,
            CreateTrsConnections<ByzantineMockThinReplicaServerPreparer::ByzantineMockServer>(mock_servers_))),
        aggregator_(std::make_shared<concordMetrics::Aggregator>()),
        trc_(new ThinReplicaClient(std::move(trc_config_), aggregator_)) {}
};

namespace {

TEST(trc_byzantine_test, test_read_state_empty_state) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;

  ByzantineTestCaseState test_state(
      make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
      make_shared<InitialStateFabricator>(make_shared<VectorMockDataStreamPreparer>(update_data, 0)));

  test_state.trc_->Subscribe();
  VerifyInitialState(
      test_state.update_queue_, update_data, num_initial_updates, "a faulty server providing empty initial state");
}

TEST(trc_byzantine_test, test_read_state_suffix_missing) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;
  size_t num_initial_updates_in_incomplete_state = 2;

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<InitialStateFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        update_data, num_initial_updates_in_incomplete_state)));

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that omits a suffix of the initial state");
}

TEST(trc_byzantine_test, test_read_state_infix_missing) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;
  vector<Data> update_data_missing_state_infix = update_data;
  update_data_missing_state_infix.erase(update_data_missing_state_infix.begin() + 1);
  size_t num_initial_updates_in_incomplete_state = 2;

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<InitialStateFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        update_data_missing_state_infix, num_initial_updates_in_incomplete_state)));

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that omits an infix of the initial state");
}

TEST(trc_byzantine_test, test_read_state_fabricated_state) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;
  size_t fabricated_update_index = 1;
  for (size_t i = fabricated_update_index; i < update_data.size(); ++i) {
    update_data[i].set_block_id(update_data[i].block_id() + 1);
  }
  vector<Data> update_data_with_fabrication = update_data;
  Data fabricated_update;
  fabricated_update.set_block_id(fabricated_update_index);
  KVPair* fabricated_update_entry = fabricated_update.add_data();
  fabricated_update_entry->set_key("fabricated_key");
  fabricated_update_entry->set_value("fabricated_value");
  update_data_with_fabrication.insert((update_data_with_fabrication.begin() + fabricated_update_index),
                                      fabricated_update);

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<InitialStateFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        update_data_with_fabrication, num_initial_updates + 1)));

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that injects a fabricated update to the state");
}

TEST(trc_byzantine_test, test_read_state_erased_block_id) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;
  size_t corrupted_update_index = 1;
  vector<Data> corrupted_update_data = update_data;
  corrupted_update_data[corrupted_update_index].clear_block_id();

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<InitialStateFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        corrupted_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that clears the block ID in one of the "
                     "updates in the initial state");
}

TEST(trc_byzantine_test, test_read_state_wrong_block_id) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;
  size_t corrupted_update_index = 1;
  for (size_t i = corrupted_update_index; i < update_data.size(); ++i) {
    update_data[i].set_block_id(update_data[i].block_id() + 1);
  }
  vector<Data> corrupted_update_data = update_data;
  corrupted_update_data[corrupted_update_index].set_block_id(corrupted_update_data[corrupted_update_index].block_id() -
                                                             1);

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<InitialStateFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        corrupted_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that gives the wrong block ID for an "
                     "update in the initial state");
}

TEST(trc_byzantine_test, test_read_state_decreasing_block_id) {
  vector<Data> update_data = GenerateSampleUpdateData(6);
  size_t num_initial_updates = 4;
  size_t corrupted_update_index = 3;
  size_t corrupted_block_id = 1;
  for (size_t i = corrupted_block_id; i < update_data.size(); ++i) {
    update_data[i].set_block_id(update_data[i].block_id() + 1);
  }
  vector<Data> corrupted_update_data = update_data;
  corrupted_update_data[corrupted_update_index].set_block_id(corrupted_block_id);

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<InitialStateFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        corrupted_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that gives a wrong block ID that is decreasing relative "
                     "to its predecessor for an update in the initial state");
}

TEST(trc_byzantine_test, test_read_state_updates_incorrectly_ordered) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;
  vector<Data> incorrectly_ordered_update_data = update_data;
  Data swap_temp = incorrectly_ordered_update_data[1];
  incorrectly_ordered_update_data[1] = incorrectly_ordered_update_data[2];
  incorrectly_ordered_update_data[2] = swap_temp;

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<InitialStateFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        incorrectly_ordered_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that incorrectly orders initial state updates");
}

TEST(trc_byzantine_test, test_read_state_update_data_erased) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;
  size_t corrupted_update_index = 1;
  vector<Data> corrupted_update_data = update_data;
  corrupted_update_data[corrupted_update_index].clear_data();

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<InitialStateFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        corrupted_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that gives one of the initial state "
                     "updates with its data cleared");
}

TEST(trc_byzantine_test, test_read_state_update_data_subset_erased) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;
  size_t corrupted_update_index = 2;
  ConcordAssert(update_data[corrupted_update_index].data_size() > 1);
  vector<Data> corrupted_update_data = update_data;
  corrupted_update_data[corrupted_update_index].clear_data();
  KVPair* non_erased_data = corrupted_update_data[corrupted_update_index].add_data();
  *non_erased_data = update_data[corrupted_update_index].data(0);

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<InitialStateFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        corrupted_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that gives one of the initial updates "
                     "with a subset of its data missing");
}

TEST(trc_byzantine_test, test_read_state_update_data_added) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;
  size_t update_with_fabrication_index = 1;
  vector<Data> update_data_with_fabrication = update_data;
  KVPair* fabricated_entry = update_data_with_fabrication[update_with_fabrication_index].add_data();
  fabricated_entry->set_key("fabricated_key");
  fabricated_entry->set_value("fabricated_value");

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<InitialStateFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        update_data_with_fabrication, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that appends a fabricated entry to the "
                     "data for one of the initial updates");
}

TEST(trc_byzantine_test, test_read_state_update_data_key_erased) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;
  size_t corrupted_update_index = 1;
  vector<Data> corrupted_update_data = update_data;
  (corrupted_update_data[corrupted_update_index].mutable_data(0))->clear_key();

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<InitialStateFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        corrupted_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that gives one of the updates in the "
                     "initial state with a key cleared");
}

TEST(trc_byzantine_test, test_read_state_update_data_value_erased) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;
  size_t corrupted_update_index = 1;
  vector<Data> corrupted_update_data = update_data;
  (corrupted_update_data[corrupted_update_index].mutable_data(0))->clear_value();

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<InitialStateFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        corrupted_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that gives one of the updates in the "
                     "initial state with a value cleared");
}

TEST(trc_byzantine_test, test_read_state_update_data_incorrect_key) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;
  size_t corrupted_update_index = 1;
  vector<Data> corrupted_update_data = update_data;
  (corrupted_update_data[corrupted_update_index].mutable_data(0))->set_key("incorrect_key");

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<InitialStateFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        corrupted_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that gives one of the updates in the "
                     "initial state with an incorrect key");
}

TEST(trc_byzantine_test, test_read_state_update_data_incorrect_value) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;
  size_t corrupted_update_index = 1;
  vector<Data> corrupted_update_data = update_data;
  (corrupted_update_data[corrupted_update_index].mutable_data(0))->set_value("incorrect_value");

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<InitialStateFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        corrupted_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that gives one of the updates in the "
                     "initial state with an incorrect value");
}

TEST(trc_byzantine_test, test_read_state_update_data_value_swap) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;
  size_t corrupted_update_index = 2;
  ConcordAssert(update_data[corrupted_update_index].data_size() > 1);
  vector<Data> corrupted_update_data = update_data;
  Data& corrupted_update = corrupted_update_data[corrupted_update_index];
  string swap_temp = corrupted_update.data(0).value();
  (corrupted_update.mutable_data(0))->set_value(corrupted_update.data(1).value());
  (corrupted_update.mutable_data(1))->set_value(swap_temp);

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<InitialStateFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        corrupted_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that swaps the values for two keys in a "
                     "single update in the initial state");
}

TEST(trc_byzantine_test, test_read_state_update_data_kvp_moved) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;
  vector<Data> corrupted_update_data = update_data;
  ConcordAssert(update_data[2].data_size() == 2);
  *(corrupted_update_data[1].add_data()) = corrupted_update_data[2].data(1);

  // Protobuf message objects do not appear to have a function for removing a
  // specific instance of a repeated field, so, after copying the key/value pair
  // we are moving to an earlier update, we clear the data from the update it
  // was taken from and then re-add the key/value pair not moved in order to
  // remove the moved key/value pair from its source update.
  KVPair kvp_retained = corrupted_update_data[2].data(0);
  corrupted_update_data[2].clear_data();
  *(corrupted_update_data[2].add_data()) = kvp_retained;

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<InitialStateFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        corrupted_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that reports the initial state with a "
                     "key/value pair moved from a later to an earlier update");
}

TEST(trc_byzantine_test, test_read_state_update_data_value_swapped_between_updates) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;
  vector<Data> corrupted_update_data = update_data;
  ConcordAssert(update_data[0].data_size() == 2);
  ConcordAssert(update_data[2].data_size() == 2);
  ConcordAssert(update_data[0].data(1).key() == update_data[2].data(1).key());
  ConcordAssert(update_data[0].data(1).value() != update_data[2].data(1).value());
  string swap_temp = update_data[0].data(1).value();
  update_data[0].mutable_data(1)->set_value(update_data[2].data(1).value());
  update_data[2].mutable_data(1)->set_value(swap_temp);

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<InitialStateFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        corrupted_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that reports the initial state with two different "
                     "values for the same key in two different updates swapped such that the "
                     "most recent value for that key is the wrong one");
}

TEST(trc_byzantine_test, test_read_state_hash_block_id_erased) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;

  class ReadStateHashBlockIdOmitter : public ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior {
   public:
    Status ReadStateHash(size_t server_index,
                         ClientContext* context,
                         const ReadStateHashRequest& request,
                         Hash* response,
                         Status correct_status) override {
      if (MakeByzantineFaulty(server_index, 1)) {
        response->clear_block_id();
      }
      return correct_status;
    }
  };

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<ReadStateHashBlockIdOmitter>());

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that omits Block IDs from its response "
                     "to ReadStateHash");
}

TEST(trc_byzantine_test, test_read_state_hash_hash_erased) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;

  class ReadStateHashHashOmitter : public ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior {
   public:
    Status ReadStateHash(size_t server_index,
                         ClientContext* context,
                         const ReadStateHashRequest& request,
                         Hash* response,
                         Status correct_status) override {
      if (MakeByzantineFaulty(server_index, 1)) {
        response->clear_hash();
      }
      return correct_status;
    }
  };

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<ReadStateHashHashOmitter>());

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that omits hashes from its responses to ReadStateHash");
}

TEST(trc_byzantine_test, test_read_state_hash_wrong_block_id) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;

  class ReadStateHashBlockIdCorrupter : public ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior {
   public:
    Status ReadStateHash(size_t server_index,
                         ClientContext* context,
                         const ReadStateHashRequest& request,
                         Hash* response,
                         Status correct_status) override {
      if (MakeByzantineFaulty(server_index, 1)) {
        response->set_block_id(response->block_id() + 1);
      }
      return correct_status;
    }
  };

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<ReadStateHashBlockIdCorrupter>());

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that gives incorrect Block IDs in its "
                     "responses to ReadStateHash");
}

TEST(trc_byzantine_test, test_read_state_hash_wrong_hash) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;

  class ReadStateHashHashCorrupter : public ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior {
   public:
    Status ReadStateHash(size_t server_index,
                         ClientContext* context,
                         const ReadStateHashRequest& request,
                         Hash* response,
                         Status correct_status) override {
      if (MakeByzantineFaulty(server_index, 1)) {
        string hash_string = response->hash();
        ConcordAssert(hash_string.length() > 0);
        ++(hash_string[0]);
        response->set_hash(hash_string);
      }
      return correct_status;
    }
  };

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<ReadStateHashHashCorrupter>());

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that gives incorrect hashes in response "
                     "to ReadStateHash");
}

TEST(trc_byzantine_test, test_read_state_hash_hash_too_short) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;

  class ReadStateHashHashTruncater : public ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior {
   public:
    Status ReadStateHash(size_t server_index,
                         ClientContext* context,
                         const ReadStateHashRequest& request,
                         Hash* response,
                         Status correct_status) override {
      if (MakeByzantineFaulty(server_index, 1)) {
        string hash_string = response->hash();
        ConcordAssert(hash_string.length() > 1);
        hash_string = hash_string.substr(1);
        ConcordAssert(hash_string.length() < kThinReplicaHashLength);
        response->set_hash(hash_string);
      }
      return correct_status;
    }
  };

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<ReadStateHashHashTruncater>());

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that gives incorrect hashes shorter than "
                     "the expected length in response to ReadStateHash");
}

TEST(trc_byzantine_test, test_read_state_hash_hash_too_long) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;

  class ReadStateHashHashExtender : public ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior {
   public:
    Status ReadStateHash(size_t server_index,
                         ClientContext* context,
                         const ReadStateHashRequest& request,
                         Hash* response,
                         Status correct_status) override {
      if (MakeByzantineFaulty(server_index, 1)) {
        string hash_string = response->hash();
        ConcordAssert(hash_string.length() > 1);
        while (hash_string.length() < kThinReplicaHashLength) {
          hash_string.append(hash_string);
        }
        response->set_hash(hash_string);
      }
      return correct_status;
    }
  };

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<ReadStateHashHashExtender>());

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that gives incorrect hashes longer than "
                     "the expected length in response to ReadStateHash");
}

TEST(trc_byzantine_test, test_read_state_times_out) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;

  class ReadStateDelayer : public ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior {
   public:
    ClientReaderInterface<Data>* ReadStateRaw(size_t server_index,
                                              ClientContext* context,
                                              const ReadStateRequest& request,
                                              ClientReaderInterface<Data>* correct_data) override {
      if (MakeByzantineFaulty(server_index, 1)) {
        sleep_for(kTestingTimeout * 2);
      }
      return correct_data;
    }
  };

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<ReadStateDelayer>());

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that responds to ReadState more slowly "
                     "than the timeout");
}

TEST(trc_byzantine_test, test_first_state_stream_read_times_out) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<StateStreamDelayer>(0));

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that opens a state stream that responds "
                     "to reads more slowly than the timeout");
}

TEST(trc_byzantine_test, test_in_progress_state_stream_read_times_out) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<StateStreamDelayer>(2));

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that opens a state stream that starts "
                     "responding to reads more slowly than the timeout after "
                     "responding to the first two reads in a timely manner");
}

TEST(trc_byzantine_test, test_read_state_hash_times_out) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;

  class ReadStateHashDelayer : public ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior {
   public:
    Status ReadStateHash(size_t server_index,
                         ClientContext* context,
                         const ReadStateHashRequest& request,
                         Hash* response,
                         Status correct_status) override {
      if (MakeByzantineFaulty(server_index, 1)) {
        sleep_for(kTestingTimeout * 2);
      }
      return correct_status;
    }
  };

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<ReadStateHashDelayer>());

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     "a faulty server that responds to ReadStateHash more "
                     "slowly than the timeout");
}

TEST(trc_byzantine_test, test_subscribe_to_updates_times_out) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;

  class SubscribeToUpdatesDelayer : public ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior {
   public:
    ClientReaderInterface<Data>* SubscribeToUpdatesRaw(size_t server_index,
                                                       ClientContext* context,
                                                       const SubscriptionRequest& request,
                                                       ClientReaderInterface<Data>* correct_data) override {
      if (MakeByzantineFaulty(server_index, 1)) {
        sleep_for(kTestingTimeout * 2);
      }
      return correct_data;
    }
  };

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<SubscribeToUpdatesDelayer>());

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that responds to SubscribeToUpdates more "
                "slowly than the timeout");
}

TEST(trc_byzantine_test, test_subscribe_to_updates_stream_ends_immediately) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  vector<Data> missing_update_data;
  size_t num_initial_updates = 2;

  ByzantineTestCaseState test_state(
      make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
      make_shared<UpdateDataFabricator>(make_shared<VectorMockDataStreamPreparer>(missing_update_data, 0)));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that opens streams in response to "
                "SubscribeToUpdates calls but then reports those streams have "
                "ended before sending any data over them");
}

TEST(trc_byzantine_test, test_subscribe_to_updates_stream_immediately_unresponsive) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<DataStreamDelayer>(0));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that opens streams in response to SubscribeToUpdate "
                "calls that respond to reads slower than the timeout");
}

TEST(trc_byzantine_test, test_subscribe_to_updates_stream_becomes_unresponsive) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<DataStreamDelayer>(2));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that opens streams in response to SubscribeToUpdate "
                "which are initially responsive but begin responding to read calls "
                "slower than the timeout after streaming a few updates.");
}

TEST(trc_byzantine_test, test_subscribe_to_updates_stream_ends_unexpectedly) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  size_t num_updates_to_truncate = 3;
  ConcordAssert((num_initial_updates + num_updates_to_truncate) < update_data.size());
  vector<Data> truncated_update_data(update_data.begin(), update_data.end() - num_updates_to_truncate);

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<UpdateDataFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        truncated_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that opens streams in response to "
                "SubscribeToUpdates calls but, after returning some updates "
                "over them normally, unexpectedly indicates they have ended");
}

TEST(trc_byzantine_test, test_subscribe_to_updates_prefix_of_updates_omitted) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  vector<Data> incomplete_update_data = update_data;
  size_t num_updates_to_omit = 2;
  ConcordAssert((num_initial_updates + num_updates_to_omit) < update_data.size());
  for (size_t i = 0; i < num_updates_to_omit; ++i) {
    incomplete_update_data.erase(incomplete_update_data.begin() + num_initial_updates);
  }

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<UpdateDataFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        incomplete_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that omits a prefix of the relevant updates "
                "when it streams data in response to SubscribeToUpdates");
}

TEST(trc_byzantine_test, test_subscribe_to_updates_infix_of_updates_omitted) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  vector<Data> incomplete_update_data = update_data;
  size_t num_updates_to_omit = 2;
  size_t omitted_update_offset = num_initial_updates + 2;
  ConcordAssert((omitted_update_offset + num_updates_to_omit) < update_data.size());
  for (size_t i = 0; i < num_updates_to_omit; ++i) {
    incomplete_update_data.erase(incomplete_update_data.begin() + omitted_update_offset);
  }

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<UpdateDataFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        incomplete_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that omits an infix of the relevant updates "
                "when it streams data in response to SubscribeToUpdates");
}

TEST(trc_byzantine_test, test_subscribe_to_updates_updates_reordered) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  vector<Data> reordered_update_data = update_data;
  size_t reordered_update_offset = num_initial_updates + 2;
  ConcordAssert((reordered_update_offset + 1) < update_data.size());
  Data swap_temp = reordered_update_data[reordered_update_offset];
  reordered_update_data[reordered_update_offset] = reordered_update_data[reordered_update_offset + 1];
  reordered_update_data[reordered_update_offset + 1] = swap_temp;

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<UpdateDataFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        reordered_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that reorders some of the relevant updates "
                "when it streams data in response to SubscribeToUpdates");
}

TEST(trc_byzantine_test, test_subscribe_to_updates_fabricated_update) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  vector<Data> update_data_with_fabrication = update_data;
  size_t fabricated_update_offset = num_initial_updates + 2;
  ConcordAssert(fabricated_update_offset < update_data.size());

  // Note erasing an update from the correct data is simpler to implement and
  // will achieve the same desired result as inserting a fabricated update to
  // the incorrect data (that is, causing the incorrect data to have an update
  // the correct data doesn't).
  update_data.erase(update_data.begin() + fabricated_update_offset);

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<UpdateDataFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        update_data_with_fabrication, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that includes a fabricated update when it "
                "streams data in response to SubscribeToUpdates");
}

TEST(trc_byzantine_test, test_subscribe_to_updates_update_block_id_omitted) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  vector<Data> corrupted_update_data = update_data;
  size_t corrupted_update_offset = num_initial_updates + 2;
  ConcordAssert(corrupted_update_offset < update_data.size());
  corrupted_update_data[corrupted_update_offset].clear_block_id();

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<UpdateDataFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        corrupted_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that omits the Block ID from an update while "
                "streaming data in response to SubscribeToUpdates");
}

TEST(trc_byzantine_test, test_subscribe_to_updates_update_block_id_incorrect) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  vector<Data> corrupted_update_data = update_data;
  size_t corrupted_update_offset = num_initial_updates + 2;
  ConcordAssert((corrupted_update_offset + 1) < update_data.size());
  corrupted_update_data[corrupted_update_offset].set_block_id(update_data[corrupted_update_offset + 1].block_id());

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<UpdateDataFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        corrupted_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that gives an incorrect Block ID for an update while "
                "streaming data in response to SubscribeToUpdates");
}

TEST(trc_byzantine_test, test_subscribe_to_updates_update_block_id_decreasing) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  vector<Data> corrupted_update_data = update_data;
  size_t corrupted_update_offset = num_initial_updates + 2;
  ConcordAssert(corrupted_update_offset < update_data.size());
  corrupted_update_data[corrupted_update_offset].set_block_id(update_data[corrupted_update_offset - 1].block_id() - 1);

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<UpdateDataFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        corrupted_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that gives a decreasing Block ID for an update while "
                "streaming data in response to SubscribeToUpdates");
}

TEST(trc_byzantine_test, test_subscribe_to_updates_update_kvps_omitted) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  vector<Data> corrupted_update_data = update_data;
  size_t corrupted_update_offset = num_initial_updates + 2;
  ConcordAssert(corrupted_update_offset < update_data.size());
  ConcordAssert(corrupted_update_data[corrupted_update_offset].data_size() > 0);
  corrupted_update_data[corrupted_update_offset].clear_data();

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<UpdateDataFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        corrupted_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that omits all key value pairs for an update "
                "while streaming data in response to SubscribeToUpdates");
}

TEST(trc_byzantine_test, test_subscribe_to_updates_update_kvps_partially_omitted) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  vector<Data> corrupted_update_data = update_data;
  size_t corrupted_update_offset = num_initial_updates + 2;
  ConcordAssert(corrupted_update_offset < update_data.size());
  ConcordAssert(corrupted_update_data[corrupted_update_offset].data_size() > 1);
  corrupted_update_data[corrupted_update_offset].clear_data();
  KVPair* non_erased_data = corrupted_update_data[corrupted_update_offset].add_data();
  *non_erased_data = update_data[corrupted_update_offset].data(0);

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<UpdateDataFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        corrupted_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that omits a subset of the key value pairs for an "
                "update while streaming data in response to SubscribeToUpdates");
}

TEST(trc_byzantine_test, test_subscribe_to_updates_update_includes_fabricated_kvp) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  vector<Data> update_data_with_fabrication = update_data;
  size_t fabricated_update_offset = num_initial_updates + 2;
  ConcordAssert(fabricated_update_offset < update_data.size());
  KVPair* fabricated_data = update_data_with_fabrication[fabricated_update_offset].add_data();
  fabricated_data->set_key("fabricated_key");
  fabricated_data->set_value("fabricated_value");

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<UpdateDataFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        update_data_with_fabrication, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that includes a fabricated key-value pair in an update "
                "while streaming data in response to SubscribeToUpdates");
}

TEST(trc_byzantine_test, test_subscribe_to_updates_update_key_omitted) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  vector<Data> corrupted_update_data = update_data;
  size_t corrupted_update_offset = num_initial_updates + 2;
  ConcordAssert(corrupted_update_offset < update_data.size());
  ConcordAssert(corrupted_update_data[corrupted_update_offset].data_size() > 0);
  corrupted_update_data[corrupted_update_offset].mutable_data(0)->clear_key();

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<UpdateDataFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        corrupted_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that omits the key for one of the key value pairs for "
                "an update while streaming data in response to SubscribeToUpdates");
}

TEST(trc_byzantine_test, test_subscribe_to_updates_update_value_omitted) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  vector<Data> corrupted_update_data = update_data;
  size_t corrupted_update_offset = num_initial_updates + 2;
  ConcordAssert(corrupted_update_offset < update_data.size());
  ConcordAssert(corrupted_update_data[corrupted_update_offset].data_size() > 0);
  corrupted_update_data[corrupted_update_offset].mutable_data(0)->clear_value();

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<UpdateDataFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        corrupted_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that omits the value for one of the key value pairs for "
                "an update while streaming data in response to SubscribeToUpdates");
}

TEST(trc_byzantine_test, test_subscribe_to_updates_update_incorrect_key) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  vector<Data> corrupted_update_data = update_data;
  size_t corrupted_update_offset = num_initial_updates + 2;
  ConcordAssert(corrupted_update_offset < update_data.size());
  ConcordAssert(corrupted_update_data[corrupted_update_offset].data_size() > 0);
  corrupted_update_data[corrupted_update_offset].mutable_data(0)->set_key("incorrect_key");

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<UpdateDataFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        corrupted_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that gives an incorrect key for one of the "
                "key value pairs for an update while streaming data in "
                "response to SubscribeToUpdates");
}

TEST(trc_byzantine_test, test_subscribe_to_updates_update_incorrect_value) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  vector<Data> corrupted_update_data = update_data;
  size_t corrupted_update_offset = num_initial_updates + 2;
  ConcordAssert(corrupted_update_offset < update_data.size());
  ConcordAssert(corrupted_update_data[corrupted_update_offset].data_size() > 0);
  corrupted_update_data[corrupted_update_offset].mutable_data(0)->set_value("incorrect_value");

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<UpdateDataFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        corrupted_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that gives an incorrect value for one of the "
                "key value pairs for an update while streaming data in "
                "response to SubscribeToUpdates");
}

TEST(trc_byzantine_test, test_subscribe_to_updates_keys_swapped_within_updat) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  vector<Data> corrupted_update_data = update_data;
  size_t corrupted_update_offset = num_initial_updates + 2;
  ConcordAssert(corrupted_update_offset < update_data.size());
  ConcordAssert(corrupted_update_data[corrupted_update_offset].data_size() > 1);
  string swap_temp = corrupted_update_data[corrupted_update_offset].data(0).key();
  corrupted_update_data[corrupted_update_offset].mutable_data(0)->set_key(
      corrupted_update_data[corrupted_update_offset].data(1).key());
  corrupted_update_data[corrupted_update_offset].mutable_data(1)->set_key(swap_temp);

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<UpdateDataFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        corrupted_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that swaps the keys between two key-value pairs within "
                "an update while streaming data in response to SubscribeToUpdates");
}

TEST(trc_byzantine_test, test_subscribe_to_update_hashes_times_out) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;

  class SubscribeToUpdateHashesDelayer : public ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior {
   public:
    ClientReaderInterface<Hash>* SubscribeToUpdateHashesRaw(size_t server_index,
                                                            ClientContext* context,
                                                            const SubscriptionRequest& request,
                                                            ClientReaderInterface<Hash>* correct_hashes) override {
      if (MakeByzantineFaulty(server_index, 1)) {
        sleep_for(kTestingTimeout * 2);
      }
      return correct_hashes;
    }
  };

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<SubscribeToUpdateHashesDelayer>());

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that responds to SubscribeToUpdateHashes more "
                "slowly than the timeout");
}

TEST(trc_byzantine_test, test_subscribe_to_update_hashes_stream_ends_immediately) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;

  // Note MockOrderedDataStreamHasher (used internally by UpdateHashFabricator)
  // does not support construction with no initial state updates, so we
  // construct our UpdateHashFabricator that gives immediately-ending streams in
  // response to SubscribeToUpdateHashes with data truncated to include just the
  // initial state rather than a completely empty data vector.
  vector<Data> incomplete_update_data(update_data.begin(), update_data.begin() + num_initial_updates);

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<UpdateHashFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        incomplete_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that opens streams in response to "
                "SubscribeToUpdateHashes calls but then reports those streams "
                "have ended before sending any hashes over them");
}

TEST(trc_byzantine_test, test_subscribe_to_update_hashes_stream_immediately_unresponsive) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<HashStreamDelayer>(0));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that opens streams in response to "
                "SubscribeToUpdateHashes calls that respond to reads slower "
                "than the timeout");
}

TEST(trc_byzantine_test, test_subscribe_to_update_hashes_stream_becomes_unresponsive) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<HashStreamDelayer>(2));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that opens streams in response to "
                "SubscribeToUpdateHashes which are initially responsive but "
                "begin responding to read calls slower than the timeout after "
                "streaming a few updates.");
}

TEST(trc_byzantine_test, test_subscribe_to_update_hashes_stream_ends_unexpectedly) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  size_t num_updates_to_truncate = 3;
  ConcordAssert((num_initial_updates + num_updates_to_truncate) < update_data.size());
  vector<Data> truncated_update_data(update_data.begin(), update_data.end() - num_updates_to_truncate);

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<UpdateHashFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        truncated_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that opens streams in response to "
                "SubscribeToUpdateHashes calls but, after returning some hashes over "
                "them normally, unexpectedly indicates they have ended");
}

TEST(trc_byzantine_test, test_subscribe_to_update_hashes_prefix_of_hashes_omitted) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  vector<Data> incomplete_update_data = update_data;
  size_t num_updates_to_omit = 2;
  ConcordAssert((num_initial_updates + num_updates_to_omit) < update_data.size());
  for (size_t i = 0; i < num_updates_to_omit; ++i) {
    incomplete_update_data.erase(incomplete_update_data.begin() + num_initial_updates);
  }

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<UpdateHashFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        incomplete_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that omits a prefix of the relevant update hashes when "
                "it streams update hashes in response to SubscribeToUpdateHashes");
}

TEST(trc_byzantine_test, test_subscribe_to_update_hashes_infix_of_hashes_omitted) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  vector<Data> incomplete_update_data = update_data;
  size_t num_updates_to_omit = 2;
  size_t omitted_update_offset = num_initial_updates + 2;
  ConcordAssert((omitted_update_offset + num_updates_to_omit) < update_data.size());
  for (size_t i = 0; i < num_updates_to_omit; ++i) {
    incomplete_update_data.erase(incomplete_update_data.begin() + omitted_update_offset);
  }

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<UpdateHashFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        incomplete_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that omits an infix of the relevant update hashes when "
                "it streams update hashes in response to SubscribeToUpdateHashes");
}

TEST(trc_byzantine_test, test_subscribe_to_update_hashes_hashes_reordered) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  vector<Data> reordered_update_data = update_data;
  size_t reordered_update_offset = num_initial_updates + 2;
  ConcordAssert((reordered_update_offset + 1) < update_data.size());
  Data swap_temp = reordered_update_data[reordered_update_offset];
  reordered_update_data[reordered_update_offset] = reordered_update_data[reordered_update_offset + 1];
  reordered_update_data[reordered_update_offset + 1] = swap_temp;

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<UpdateHashFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        reordered_update_data, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that reorders some of the relevant update hashes when "
                "it streams update hashes in response to SubscribeToUpdateHashes");
}

TEST(trc_byzantine_test, test_subscribe_to_update_hashes_fabricated_hash) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  vector<Data> update_data_with_fabrication = update_data;
  size_t fabricated_update_offset = num_initial_updates + 2;
  ConcordAssert(fabricated_update_offset < update_data.size());

  // Note erasing an update from the correct data is simpler to implement and
  // will achieve the same desired result as inserting a fabricated update to
  // the incorrect data (that is, causing the incorrect hashes to include a hash
  // update that the correct data and hashes don't).
  update_data.erase(update_data.begin() + fabricated_update_offset);

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<UpdateHashFabricator>(make_shared<VectorMockDataStreamPreparer>(
                                        update_data_with_fabrication, num_initial_updates)));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that includes a fabricated hash when it "
                "streams update hashes in response to SubscribeToUpdateHashes");
}

TEST(trc_byzantine_test, test_subscribe_to_update_hashes_block_id_omitted) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  size_t corrupted_hash_offset = 2;
  ConcordAssert(num_initial_updates + corrupted_hash_offset < update_data.size());

  class OmitBlockId : public UpdateHashCorrupter::CorruptHash {
    void operator()(Hash* hash) override { hash->clear_block_id(); }
  };

  ByzantineTestCaseState test_state(
      make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
      make_shared<UpdateHashCorrupter>(make_shared<OmitBlockId>(), corrupted_hash_offset));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that omits the Block ID from an update hash while "
                "streaming update hashes in response to SubscribeToUpdateHashes");
}

TEST(trc_byzantine_test, test_subscribe_to_update_hashes_block_id_incorrect) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  size_t corrupted_hash_offset = 2;
  ConcordAssert(num_initial_updates + corrupted_hash_offset < update_data.size());

  class CorruptBlockId : public UpdateHashCorrupter::CorruptHash {
    void operator()(Hash* hash) override { hash->set_block_id(hash->block_id() + 1); }
  };

  ByzantineTestCaseState test_state(
      make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
      make_shared<UpdateHashCorrupter>(make_shared<CorruptBlockId>(), corrupted_hash_offset));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that gives an incorrect Block ID for an update hash "
                "while streaming update hashes in response to SubscribeToUpdateHashes");
}

TEST(trc_byzantine_test, test_subscribe_to_update_hashes_block_id_decreasing) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  size_t corrupted_hash_offset = 2;
  ConcordAssert(num_initial_updates + corrupted_hash_offset < update_data.size());

  ConcordAssert(update_data[num_initial_updates + corrupted_hash_offset - 1].block_id() > 1);
  class MakeBlockIdDecreasing : public UpdateHashCorrupter::CorruptHash {
    void operator()(Hash* hash) override { hash->set_block_id(1); }
  };

  ByzantineTestCaseState test_state(
      make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
      make_shared<UpdateHashCorrupter>(make_shared<MakeBlockIdDecreasing>(), corrupted_hash_offset));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that gives an incorrect and decreasing Block "
                "ID for an update hash while streaming update hashes in "
                "response to SubscribeToUpdateHashes");
}

TEST(trc_byzantine_test, test_subscribe_to_update_hashes_hash_omitted) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  size_t corrupted_hash_offset = 2;
  ConcordAssert(num_initial_updates + corrupted_hash_offset < update_data.size());

  class OmitHash : public UpdateHashCorrupter::CorruptHash {
    void operator()(Hash* hash) override { hash->clear_hash(); }
  };

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<UpdateHashCorrupter>(make_shared<OmitHash>(), corrupted_hash_offset));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that omits the hash from an update hash while streaming "
                "update hashes in response to SubscribeToUpdateHashes");
}

TEST(trc_byzantine_test, test_subscribe_to_update_hashes_hash_incorrect) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  size_t corrupted_hash_offset = 2;
  ConcordAssert(num_initial_updates + corrupted_hash_offset < update_data.size());

  class MakeHashIncorrect : public UpdateHashCorrupter::CorruptHash {
    void operator()(Hash* hash) override {
      string hash_value = hash->hash();
      ConcordAssert(hash_value.length() > 0);
      ++hash_value[0];
      hash->set_hash(hash_value);
    }
  };

  ByzantineTestCaseState test_state(
      make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
      make_shared<UpdateHashCorrupter>(make_shared<MakeHashIncorrect>(), corrupted_hash_offset));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that gives an incorrect hash for an update hash while "
                "streaming update hashes in response to SubscribeToUpdateHashes");
}

TEST(trc_byzantine_test, test_subscribe_to_update_hashes_hash_too_short) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  size_t corrupted_hash_offset = 2;
  ConcordAssert(num_initial_updates + corrupted_hash_offset < update_data.size());

  class TruncateHash : public UpdateHashCorrupter::CorruptHash {
    void operator()(Hash* hash) override {
      string hash_value = hash->hash();
      ConcordAssert(hash_value.length() > 0);
      while (hash_value.length() >= kThinReplicaHashLength) {
        hash_value = hash_value.substr(1);
      }
      hash->set_hash(hash_value);
    }
  };

  ByzantineTestCaseState test_state(
      make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
      make_shared<UpdateHashCorrupter>(make_shared<TruncateHash>(), corrupted_hash_offset));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that gives an incorrect hash that is too "
                "short for an update hash while streaming update hashes in "
                "response to SubscribeToUpdateHashes");
}

TEST(trc_byzantine_test, test_subscribe_to_update_hashes_hash_too_long) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  size_t corrupted_hash_offset = 2;
  ConcordAssert(num_initial_updates + corrupted_hash_offset < update_data.size());

  class ExtendHash : public UpdateHashCorrupter::CorruptHash {
    void operator()(Hash* hash) override {
      string hash_value = hash->hash();
      ConcordAssert(hash_value.length() > 0);
      while (hash_value.length() <= kThinReplicaHashLength) {
        hash_value += hash_value[0];
      }
      hash->set_hash(hash_value);
    }
  };

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<UpdateHashCorrupter>(make_shared<ExtendHash>(), corrupted_hash_offset));

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a faulty server that gives an incorrect hash that is too long "
                "for an update hash while streaming update hashes in response "
                "to SubscribeToUpdateHashes");
}

TEST(trc_byzantine_test, test_multiple_servers_timeout) {
  vector<Data> update_data = GenerateSampleUpdateData(12);
  size_t num_initial_updates = 4;

  uint16_t max_faulty = 3;
  size_t num_servers = max_faulty * 3 + 1;

  class FServerUnresponsiveness : public ClusterResponsivenessLimiter::ResponsivenessManager {
    void UpdateResponsiveness(ClusterResponsivenessLimiter& responsiveness_limiter,
                              size_t server_called,
                              uint64_t block_id) override {
      if (responsiveness_limiter.GetNumUnresponsiveServers() < responsiveness_limiter.GetMaxFaulty()) {
        responsiveness_limiter.SetUnresponsive(server_called);
      }
    }
  };

  ByzantineTestCaseState test_state(
      make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
      make_shared<ClusterResponsivenessLimiter>(max_faulty, num_servers, make_unique<FServerUnresponsiveness>()),
      max_faulty,
      num_servers);

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                to_string(max_faulty) + " faulty servers in a " + to_string(num_servers) +
                    "-node cluster that respond to all calls more slowly than "
                    "the timeout");
}

TEST(trc_byzantine_test, test_multiple_servers_begin_timing_out) {
  vector<Data> update_data = GenerateSampleUpdateData(12);
  size_t num_initial_updates = 4;
  uint64_t block_id_to_limit_responsiveness_at = update_data[6].block_id();

  uint16_t max_faulty = 3;
  size_t num_servers = max_faulty * 3 + 1;

  class FServerUnresponsivenessAtBlockID : public ClusterResponsivenessLimiter::ResponsivenessManager {
   private:
    uint64_t block_id_;

   public:
    FServerUnresponsivenessAtBlockID(uint64_t block_id) : block_id_(block_id) {}
    void UpdateResponsiveness(ClusterResponsivenessLimiter& responsiveness_limiter,
                              size_t server_called,
                              uint64_t block_id) override {
      if ((block_id >= block_id_) &&
          (responsiveness_limiter.GetNumUnresponsiveServers() < responsiveness_limiter.GetMaxFaulty())) {
        responsiveness_limiter.SetUnresponsive(server_called);
      }
    }
  };

  ByzantineTestCaseState test_state(
      make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
      make_shared<ClusterResponsivenessLimiter>(
          max_faulty, num_servers, make_unique<FServerUnresponsivenessAtBlockID>(block_id_to_limit_responsiveness_at)),
      max_faulty,
      num_servers);

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                to_string(max_faulty) + " faulty servers in a " + to_string(num_servers) +
                    "-node cluster that begin responding to all calls more slowly than "
                    "the timeout after a few updates have been streamed");
}

TEST(trc_byzantine_test, test_minimal_subset_of_servers_responsive) {
  vector<Data> update_data = GenerateSampleUpdateData(12);
  size_t num_initial_updates = 4;

  uint16_t max_faulty = 3;
  size_t num_servers = max_faulty * 3 + 1;

  class MinimalResponsivenessForSubscription : public ClusterResponsivenessLimiter::ResponsivenessManager {
    void UpdateResponsiveness(ClusterResponsivenessLimiter& responsiveness_limiter,
                              size_t server_called,
                              uint64_t block_id) override {
      size_t target_num_unresponsive_servers =
          responsiveness_limiter.GetClusterSize() - (responsiveness_limiter.GetMaxFaulty() + 1);
      if (responsiveness_limiter.GetNumUnresponsiveServers() < target_num_unresponsive_servers) {
        responsiveness_limiter.SetUnresponsive(server_called);
      }
    }
  };

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<ClusterResponsivenessLimiter>(
                                        max_faulty, num_servers, make_unique<MinimalResponsivenessForSubscription>()),
                                    max_faulty,
                                    num_servers);

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "all but " + to_string(max_faulty + 1) + " servers in a " + to_string(num_servers) +
                    "-node cluster responding to all calls more slowly than "
                    "the timeout");
}

TEST(trc_byzantine_test, test_minimal_subset_of_servers_left_responsive_after_others_timeout) {
  vector<Data> update_data = GenerateSampleUpdateData(12);
  size_t num_initial_updates = 4;
  uint64_t block_id_to_limit_responsiveness_at = update_data[6].block_id();

  uint16_t max_faulty = 3;
  size_t num_servers = max_faulty * 3 + 1;

  class MinimalResponsivenessForSubscriptionAtBlockID : public ClusterResponsivenessLimiter::ResponsivenessManager {
   private:
    uint64_t block_id_;

   public:
    MinimalResponsivenessForSubscriptionAtBlockID(uint64_t block_id) : block_id_(block_id) {}
    void UpdateResponsiveness(ClusterResponsivenessLimiter& responsiveness_limiter,
                              size_t server_called,
                              uint64_t block_id) override {
      size_t target_num_unresponsive_servers =
          responsiveness_limiter.GetClusterSize() - (responsiveness_limiter.GetMaxFaulty() + 1);
      if ((block_id >= block_id_) &&
          (responsiveness_limiter.GetNumUnresponsiveServers() < target_num_unresponsive_servers)) {
        responsiveness_limiter.SetUnresponsive(server_called);
      }
    }
  };

  ByzantineTestCaseState test_state(
      make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
      make_shared<ClusterResponsivenessLimiter>(
          max_faulty,
          num_servers,
          make_unique<MinimalResponsivenessForSubscriptionAtBlockID>(block_id_to_limit_responsiveness_at)),
      max_faulty,
      num_servers);

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "all but " + to_string(max_faulty + 1) + " servers in a " + to_string(num_servers) +
                    "-node cluster responding to all calls more slowly than "
                    "the timeout after a few updates have been streamed");
}

TEST(trc_byzantine_test, test_cluster_temporarily_becomes_unresponsive) {
  vector<Data> update_data = GenerateSampleUpdateData(12);
  size_t num_initial_updates = 4;
  uint64_t block_id_to_limit_responsiveness_at = update_data[6].block_id();

  uint16_t max_faulty = 3;
  size_t num_servers = max_faulty * 3 + 1;

  class TemporaryClusterUnresponsiveness : public ClusterResponsivenessLimiter::ResponsivenessManager {
   private:
    uint64_t block_id_;
    bool unresponsiveness_started_;
    time_point<steady_clock> unresponsiveness_start_time_;

   public:
    TemporaryClusterUnresponsiveness(uint64_t block_id)
        : block_id_(block_id), unresponsiveness_started_(false), unresponsiveness_start_time_() {}
    void UpdateResponsiveness(ClusterResponsivenessLimiter& responsiveness_limiter,
                              size_t server_called,
                              uint64_t block_id) override {
      if (block_id >= block_id_) {
        if (!unresponsiveness_started_) {
          unresponsiveness_start_time_ = steady_clock::now();
          unresponsiveness_started_ = true;
        }
        milliseconds target_unresponsiveness_window = kTestingTimeout * responsiveness_limiter.GetClusterSize() * 5;
        time_point<steady_clock> current_time = steady_clock::now();
        milliseconds time_unresponsive = duration_cast<milliseconds>(current_time - unresponsiveness_start_time_);
        if (time_unresponsive <= target_unresponsiveness_window) {
          responsiveness_limiter.SetUnresponsive(server_called);
        } else {
          responsiveness_limiter.SetResponsive(server_called);
        }
      }
    }
  };

  ByzantineTestCaseState test_state(
      make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
      make_shared<ClusterResponsivenessLimiter>(
          max_faulty, num_servers, make_unique<TemporaryClusterUnresponsiveness>(block_id_to_limit_responsiveness_at)),
      max_faulty,
      num_servers);

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "all servers in a " + to_string(num_servers) +
                    "-node cluster responding to all calls more slowly than "
                    "the timeout after a few updates have been streamed, then "
                    "returning to normal responsiveness after some delay");
}

TEST(trc_byzantine_test, test_cluster_reponsiveness_unstable) {
  vector<Data> update_data = GenerateSampleUpdateData(24);
  size_t num_initial_updates = 1;

  uint16_t max_faulty = 3;
  size_t num_servers = max_faulty * 3 + 1;

  class UnstableResponsiveness : public ClusterResponsivenessLimiter::ResponsivenessManager {
   private:
    unordered_map<size_t, uint64_t> most_recent_blocks_responded_to_;

   public:
    void UpdateResponsiveness(ClusterResponsivenessLimiter& responsiveness_limiter,
                              size_t server_called,
                              uint64_t block_id) override {
      if ((most_recent_blocks_responded_to_.count(server_called) > 0) &&
          (most_recent_blocks_responded_to_[server_called] == (block_id - 1))) {
        responsiveness_limiter.SetUnresponsive(server_called);
      } else {
        responsiveness_limiter.SetResponsive(server_called);
        most_recent_blocks_responded_to_[server_called] = block_id;
      }
    }
  };

  ByzantineTestCaseState test_state(
      make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
      make_shared<ClusterResponsivenessLimiter>(max_faulty, num_servers, make_unique<UnstableResponsiveness>()),
      max_faulty,
      num_servers);

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                "a cluster of " + to_string(num_servers) +
                    " nodes of unstable responsiveness all of which will individually "
                    "respond to any stream reads more slowly than the timeout if those "
                    "reads would return a block immediately consecutive to the previous "
                    "block read from that specific server");
}

TEST(trc_byzantine_test, test_f_servers_give_false_state) {
  vector<Data> update_data = GenerateSampleUpdateData(6);
  size_t num_initial_updates = 3;
  vector<Data> update_data_with_fabrication = update_data;
  size_t fabricated_update_offset = 1;

  // Note erasing an update from the correct data is simpler to implement and
  // will achieve the same desired result as inserting a fabricated update to
  // the incorrect data (that is, causing the incorrect data to have an update
  // the correct data doesn't).
  update_data.erase(update_data.begin() + fabricated_update_offset);

  uint16_t max_faulty = 3;
  size_t num_servers = max_faulty * 3 + 1;

  ByzantineTestCaseState test_state(
      make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
      make_shared<InitialStateFabricator>(
          make_shared<VectorMockDataStreamPreparer>(update_data_with_fabrication, num_initial_updates + 1), max_faulty),
      max_faulty,
      num_servers);

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     to_string(max_faulty) + " faulty servers in a " + to_string(num_servers) +
                         "-node cluster that inject a fabricated update to the state");
}

TEST(trc_byzantine_test, test_f_servers_give_false_state_hashes) {
  vector<Data> update_data = GenerateSampleUpdateData(5);
  size_t num_initial_updates = 3;

  uint16_t max_faulty = 3;
  size_t num_servers = max_faulty * 3 + 1;

  class ReadStateHashHashCorrupter : public ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior {
   private:
    uint16_t max_faulty_;

   public:
    ReadStateHashHashCorrupter(uint16_t max_faulty) : max_faulty_(max_faulty) {}
    Status ReadStateHash(size_t server_index,
                         ClientContext* context,
                         const ReadStateHashRequest& request,
                         Hash* response,
                         Status correct_status) override {
      if (MakeByzantineFaulty(server_index, max_faulty_)) {
        string hash_string = response->hash();
        ConcordAssert(hash_string.length() > 0);
        ++(hash_string[0]);
        response->set_hash(hash_string);
      }
      return correct_status;
    }
  };

  ByzantineTestCaseState test_state(make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
                                    make_shared<ReadStateHashHashCorrupter>(max_faulty),
                                    max_faulty,
                                    num_servers);

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     to_string(max_faulty) + " serves in a " + to_string(num_servers) +
                         "-node cluster that give incorrect hashes in response "
                         "to ReadStateHash");
}

TEST(trc_byzantine_test, test_f_servers_give_false_updates) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  vector<Data> update_data_with_fabrication = update_data;
  size_t fabricated_update_offset = num_initial_updates + 2;
  ConcordAssert(fabricated_update_offset < update_data.size());

  // Note erasing an update from the correct data is simpler to implement and
  // will achieve the same desired result as inserting a fabricated update to
  // the incorrect data (that is, causing the incorrect data to have an update
  // the correct data doesn't).
  update_data.erase(update_data.begin() + fabricated_update_offset);

  uint16_t max_faulty = 3;
  size_t num_servers = max_faulty * 3 + 1;

  ByzantineTestCaseState test_state(
      make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
      make_shared<UpdateDataFabricator>(
          make_shared<VectorMockDataStreamPreparer>(update_data_with_fabrication, num_initial_updates), max_faulty),
      max_faulty,
      num_servers);

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                to_string(max_faulty) + " servers in a " + to_string(num_servers) +
                    "-node cluster that include a fabricated update when they "
                    "stream data in response to SubscribeToUpdates");
}

TEST(trc_byzantine_test, test_f_servers_give_false_update_hashes) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  vector<Data> update_data_with_fabrication = update_data;
  size_t fabricated_update_offset = num_initial_updates + 2;
  ConcordAssert(fabricated_update_offset < update_data.size());

  // Note erasing an update from the correct data is simpler to implement and
  // will achieve the same desired result as inserting a fabricated update to
  // the incorrect data (that is, causing the incorrect hashes to include a hash
  // update that the correct data and hashes don't).
  update_data.erase(update_data.begin() + fabricated_update_offset);

  uint16_t max_faulty = 3;
  size_t num_servers = max_faulty * 3 + 1;

  ByzantineTestCaseState test_state(
      make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
      make_shared<UpdateHashFabricator>(
          make_shared<VectorMockDataStreamPreparer>(update_data_with_fabrication, num_initial_updates), max_faulty),
      max_faulty,
      num_servers);

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                to_string(max_faulty) + " servers in a " + to_string(num_servers) +
                    "-node cluster that includes a fabricated hash when they stream "
                    "update hashes in response to SubscribeToUpdateHashes");
}

TEST(trc_byzantine_test, test_f_servers_collude_on_false_initial_state) {
  vector<Data> update_data = GenerateSampleUpdateData(6);
  size_t num_initial_updates = 3;
  vector<Data> update_data_with_fabrication = update_data;
  size_t fabricated_update_offset = 1;

  // Note erasing an update from the correct data is simpler to implement and
  // will achieve the same desired result as inserting a fabricated update to
  // the incorrect data (that is, causing the incorrect data to have an update
  // the correct data doesn't).
  update_data.erase(update_data.begin() + fabricated_update_offset);

  uint16_t max_faulty = 3;
  size_t num_servers = max_faulty * 3 + 1;

  ByzantineTestCaseState test_state(
      make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
      make_shared<ComprehensiveDataFabricator>(
          make_shared<VectorMockDataStreamPreparer>(update_data_with_fabrication, num_initial_updates + 1), max_faulty),
      max_faulty,
      num_servers);

  test_state.trc_->Subscribe();
  VerifyInitialState(test_state.update_queue_,
                     update_data,
                     num_initial_updates,
                     to_string(max_faulty) + " faulty servers in a " + to_string(num_servers) +
                         "-node cluster that collude (in their responses to "
                         "both ReadState and ReadStateHash) to present an "
                         "initial state with a fabricated update");
}

TEST(trc_byzantine_test, test_f_servers_collude_on_streaming_fabricated_update) {
  vector<Data> update_data = GenerateSampleUpdateData(8);
  size_t num_initial_updates = 2;
  vector<Data> update_data_with_fabrication = update_data;
  size_t fabricated_update_offset = num_initial_updates + 2;
  ConcordAssert(fabricated_update_offset < update_data.size());

  // Note erasing an update from the correct data is simpler to implement and
  // will achieve the same desired result as inserting a fabricated update to
  // the incorrect data (that is, causing the incorrect data to have an update
  // the correct data doesn't).
  update_data.erase(update_data.begin() + fabricated_update_offset);

  uint16_t max_faulty = 3;
  size_t num_servers = max_faulty * 3 + 1;

  ByzantineTestCaseState test_state(
      make_shared<VectorMockDataStreamPreparer>(update_data, num_initial_updates),
      make_shared<ComprehensiveDataFabricator>(
          make_shared<VectorMockDataStreamPreparer>(update_data_with_fabrication, num_initial_updates), max_faulty),
      max_faulty,
      num_servers);

  test_state.trc_->Subscribe();
  VerifyUpdates(test_state.update_queue_,
                update_data,
                to_string(max_faulty) + " servers in a " + to_string(num_servers) +
                    "-node cluster that collude (in their responses to both "
                    "SubscribeToUpdates and SubscribeToUpdateHashes) to "
                    "present a fabricated update");
}

}  // anonymous namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  log4cplus::initialize();
  log4cplus::BasicConfigurator config;
  config.configure();
  return RUN_ALL_TESTS();
}
