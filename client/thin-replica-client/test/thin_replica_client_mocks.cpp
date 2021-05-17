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
#include "client/thin-replica-client/trs_connection.hpp"

#include "gmock/gmock.h"
#include "thin_replica_client_mocks.hpp"
#include "client/thin-replica-client/trc_hash.hpp"

using com::vmware::concord::thin_replica::BlockId;
using com::vmware::concord::thin_replica::Data;
using com::vmware::concord::thin_replica::Hash;
using com::vmware::concord::thin_replica::KVPair;
using com::vmware::concord::thin_replica::MockThinReplicaStub;
using com::vmware::concord::thin_replica::ReadStateHashRequest;
using com::vmware::concord::thin_replica::ReadStateRequest;
using com::vmware::concord::thin_replica::SubscriptionRequest;
using google::protobuf::Empty;
using grpc::ClientContext;
using grpc::ClientReaderInterface;
using grpc::Status;
using grpc::StatusCode;
using std::condition_variable;
using std::invalid_argument;
using std::list;
using std::lock_guard;
using std::make_unique;
using std::mutex;
using std::pair;
using std::shared_ptr;
using std::string;
using std::unique_lock;
using std::unique_ptr;
using std::vector;
using std::this_thread::sleep_for;
using testing::Invoke;
using testing::InvokeWithoutArgs;
using testing::Return;
using thin_replica_client::hashState;
using thin_replica_client::hashUpdate;
using thin_replica_client::TrsConnection;

MockTrsConnection::MockTrsConnection() : TrsConnection("mock_address", "mock_client_id", 1, 1) {
  this->data_timeout_ = kTestingTimeout;
  this->hash_timeout_ = kTestingTimeout;
  this->stub_.reset(new MockThinReplicaStub());
}
MockThinReplicaStub* MockTrsConnection::GetStub() { return dynamic_cast<MockThinReplicaStub*>(this->stub_.get()); }
bool MockTrsConnection::isConnected() { return true; }

Data FilterUpdate(const Data& raw_update, const string& filter) {
  Data filtered_update;
  filtered_update.set_block_id(raw_update.block_id());
  filtered_update.set_correlation_id(raw_update.correlation_id());
  for (const KVPair& raw_kvp : raw_update.data()) {
    const string key = raw_kvp.key();
    if ((key.length() >= filter.length()) && (key.compare(0, filter.size(), filter) == 0)) {
      KVPair* filtered_kvp = filtered_update.add_data();
      *filtered_kvp = raw_kvp;
    }
  }
  return filtered_update;
}

VectorMockDataStreamPreparer::DataQueue::DataQueue(const list<Data>& data) : queue_(data) {}
VectorMockDataStreamPreparer::DataQueue::~DataQueue() {}
Status VectorMockDataStreamPreparer::DataQueue::Finish() {
  // Note this Finish implementation does not account for the case where the
  // Thin Replica Client ends the stream early with TryCancel; neglecting
  // this case should still be technically correct behavior that the Thin
  // Replica Client needs to handle as TryCancel is not guaranteed to
  // succeed.
  while (!queue_.empty()) {
    unique_lock<mutex> empty_condition_lock(empty_condition_mutex_);
    empty_condition_.wait(empty_condition_lock);
  }
  return Status::OK;
}

bool VectorMockDataStreamPreparer::DataQueue::Read(Data* msg) {
  assert(msg);

  if (queue_.empty()) {
    return false;
  } else {
    {
      lock_guard<mutex> queue_lock(queue_mutex_);
      *msg = queue_.front();
      queue_.pop_front();
    }
    if (queue_.empty()) {
      empty_condition_.notify_all();
    }
    return true;
  }
}

ClientReaderInterface<Data>* VectorMockDataStreamPreparer::PrepareInitialStateDataStream(const string& filter) const {
  auto data_stream = new MockThinReplicaStream<Data>();
  list<Data> data_queue;
  assert(num_updates_in_initial_state_ <= data_.size());
  for (size_t i = 0; i < num_updates_in_initial_state_; ++i) {
    data_queue.push_back(FilterUpdate(data_[i], filter));
  }
  auto data_stream_state = new DataQueue(data_queue);
  data_stream->state.reset(data_stream_state);

  ON_CALL(*data_stream, Finish).WillByDefault(Invoke(data_stream_state, &DataQueue::Finish));
  ON_CALL(*data_stream, Read).WillByDefault(Invoke(data_stream_state, &DataQueue::Read));
  return data_stream;
}

ClientReaderInterface<Data>* VectorMockDataStreamPreparer::PrepareSubscriptionDataStream(uint64_t block_id,
                                                                                         const string& filter) const {
  auto data_stream = new MockThinReplicaStream<Data>();
  list<Data> data_queue;
  size_t subscription_start = 0;
  while ((subscription_start < data_.size()) && (data_[subscription_start].block_id() < block_id)) {
    ++subscription_start;
  }
  for (size_t i = subscription_start; i < data_.size(); ++i) {
    data_queue.push_back(FilterUpdate(data_[i], filter));
  }
  auto data_stream_state = new DataQueue(data_queue);
  data_stream->state.reset(data_stream_state);

  ON_CALL(*data_stream, Finish).WillByDefault(Invoke(data_stream_state, &DataQueue::Finish));
  ON_CALL(*data_stream, Read).WillByDefault(Invoke(data_stream_state, &DataQueue::Read));
  return data_stream;
}

VectorMockDataStreamPreparer::VectorMockDataStreamPreparer(const vector<Data>& data, size_t initial_state_size)
    : data_(data), num_updates_in_initial_state_(initial_state_size) {
  if (initial_state_size > data.size()) {
    throw invalid_argument(
        "Attempting to construct VectorMockDataStreamPreparer with initial "
        "state longer than provided data.");
  }
}

VectorMockDataStreamPreparer::~VectorMockDataStreamPreparer() {}
ClientReaderInterface<Data>* VectorMockDataStreamPreparer::ReadStateRaw(ClientContext* context,
                                                                        const ReadStateRequest& request) const {
  return PrepareInitialStateDataStream(request.key_prefix());
}

ClientReaderInterface<Data>* VectorMockDataStreamPreparer::SubscribeToUpdatesRaw(
    ClientContext* context, const SubscriptionRequest& request) const {
  return PrepareSubscriptionDataStream(request.block_id(), request.key_prefix());
}

RepeatedMockDataStreamPreparer::DataRepeater::DataRepeater(const Data& data, uint64_t starting_block_id)
    : data_(data), current_block_id_(starting_block_id), finite_length_(false) {}

RepeatedMockDataStreamPreparer::DataRepeater::DataRepeater(const Data& data,
                                                           uint64_t starting_block_id,
                                                           size_t num_updates)
    : data_(data), current_block_id_(starting_block_id), finite_length_(true), num_updates_(num_updates) {}

RepeatedMockDataStreamPreparer::DataRepeater::~DataRepeater() {}
Status RepeatedMockDataStreamPreparer::DataRepeater::Finish() {
  // Note this Finish implementation does not account for the case where the
  // Thin Replica Client ends the stream early with TryCancel; neglecting
  // this case should still be technically correct behavior that the Thin
  // Replica Client needs to handle as TryCancel is not guaranteed to
  // succeed.
  // Note this loop will never finish if finite_length_ is false.
  while (!(finite_length_ && (num_updates_ < 1))) {
    unique_lock<mutex> finished_condition_lock(finished_condition_mutex_);
    finished_condition_.wait(finished_condition_lock);
  }
  return Status::OK;
}

bool RepeatedMockDataStreamPreparer::DataRepeater::Read(Data* msg) {
  assert(msg);

  lock_guard<mutex> block_id_lock(block_id_mutex_);
  if (finite_length_ && (num_updates_ < 1)) {
    return false;
  } else {
    data_.set_block_id(current_block_id_++);
    if (finite_length_ && (--num_updates_ < 1)) {
      finished_condition_.notify_all();
    }
    *msg = data_;
    return true;
  }
}

ClientReaderInterface<Data>* RepeatedMockDataStreamPreparer::PrepareInitialStateDataStream(const string& filter) const {
  auto data_stream = new MockThinReplicaStream<Data>();
  auto data_stream_state =
      new DataRepeater(FilterUpdate(data_, filter), data_.block_id(), num_updates_in_initial_state_);
  data_stream->state.reset(data_stream_state);

  ON_CALL(*data_stream, Finish).WillByDefault(Invoke(data_stream_state, &DataRepeater::Finish));
  ON_CALL(*data_stream, Read).WillByDefault(Invoke(data_stream_state, &DataRepeater::Read));
  return data_stream;
}

ClientReaderInterface<Data>* RepeatedMockDataStreamPreparer::PrepareSubscriptionDataStream(uint64_t block_id,
                                                                                           const string& filter) const {
  auto data_stream = new MockThinReplicaStream<Data>();
  auto data_stream_state = new DataRepeater(FilterUpdate(data_, filter), block_id);
  data_stream->state.reset(data_stream_state);

  ON_CALL(*data_stream, Finish).WillByDefault(Invoke(data_stream_state, &DataRepeater::Finish));
  ON_CALL(*data_stream, Read).WillByDefault(Invoke(data_stream_state, &DataRepeater::Read));
  return data_stream;
}

RepeatedMockDataStreamPreparer::RepeatedMockDataStreamPreparer(const Data& data, size_t initial_state_length)
    : data_(data), num_updates_in_initial_state_(initial_state_length) {}

RepeatedMockDataStreamPreparer::~RepeatedMockDataStreamPreparer() {}
ClientReaderInterface<Data>* RepeatedMockDataStreamPreparer::ReadStateRaw(ClientContext* context,
                                                                          const ReadStateRequest& request) const {
  return PrepareInitialStateDataStream(request.key_prefix());
}

ClientReaderInterface<Data>* RepeatedMockDataStreamPreparer::SubscribeToUpdatesRaw(
    ClientContext* context, const SubscriptionRequest& request) const {
  return PrepareSubscriptionDataStream(request.block_id(), request.key_prefix());
}

DelayedMockDataStreamPreparer::DataDelayer::DataDelayer(ClientReaderInterface<Data>* data,
                                                        const shared_ptr<condition_variable>& delay_condition,
                                                        const shared_ptr<mutex>& delay_mutex,
                                                        const shared_ptr<bool>& spurious_wakeup)
    : undelayed_data_(data),
      waiting_condition_(delay_condition),
      spurious_wakeup_(spurious_wakeup),
      condition_mutex_(delay_mutex) {}

DelayedMockDataStreamPreparer::DataDelayer::~DataDelayer() {}

Status DelayedMockDataStreamPreparer::DataDelayer::Finish() {
  Status status = undelayed_data_->Finish();
  while (*spurious_wakeup_) {
    unique_lock<mutex> condition_lock(*condition_mutex_);
    waiting_condition_->wait(condition_lock);
  }
  return status;
}

bool DelayedMockDataStreamPreparer::DataDelayer::Read(Data* msg) {
  assert(msg);

  while (*spurious_wakeup_) {
    unique_lock<mutex> condition_lock(*condition_mutex_);
    waiting_condition_->wait(condition_lock);
  }
  return undelayed_data_->Read(msg);
}

DelayedMockDataStreamPreparer::DelayedMockDataStreamPreparer(shared_ptr<MockDataStreamPreparer>& data,
                                                             shared_ptr<condition_variable> condition,
                                                             shared_ptr<bool> spurious_wakeup_indicator,
                                                             shared_ptr<mutex> condition_mutex)
    : undelayed_data_preparer_(data),
      delay_condition_(condition),
      spurious_wakeup_(spurious_wakeup_indicator),
      condition_mutex_(condition_mutex) {}

DelayedMockDataStreamPreparer::~DelayedMockDataStreamPreparer() {}

ClientReaderInterface<Data>* DelayedMockDataStreamPreparer::ReadStateRaw(ClientContext* context,
                                                                         const ReadStateRequest& request) const {
  return undelayed_data_preparer_->ReadStateRaw(context, request);
}

ClientReaderInterface<Data>* DelayedMockDataStreamPreparer::SubscribeToUpdatesRaw(
    ClientContext* context, const SubscriptionRequest& request) const {
  auto data_stream = new MockThinReplicaStream<Data>();
  auto data_stream_state = new DataDelayer(undelayed_data_preparer_->SubscribeToUpdatesRaw(context, request),
                                           delay_condition_,
                                           condition_mutex_,
                                           spurious_wakeup_);
  data_stream->state.reset(data_stream_state);

  ON_CALL(*data_stream, Finish).WillByDefault(Invoke(data_stream_state, &DataDelayer::Finish));
  ON_CALL(*data_stream, Read).WillByDefault(Invoke(data_stream_state, &DataDelayer::Read));
  return data_stream;
}

MockOrderedDataStreamHasher::StreamHasher::StreamHasher(ClientReaderInterface<Data>* data) : data_stream_(data) {}

MockOrderedDataStreamHasher::StreamHasher::~StreamHasher() {}

Status MockOrderedDataStreamHasher::StreamHasher::Finish() { return data_stream_->Finish(); }

bool MockOrderedDataStreamHasher::StreamHasher::Read(Hash* msg) {
  assert(msg);

  Data data;
  bool read_status = data_stream_->Read(&data);
  if (read_status) {
    msg->set_block_id(data.block_id());
    msg->set_hash(hashUpdate(data));
  }
  return read_status;
}

MockOrderedDataStreamHasher::MockOrderedDataStreamHasher(shared_ptr<MockDataStreamPreparer>& data)
    : data_preparer_(data) {
  // MockOrderedDataStreamHasher determines the starting block ID from the
  // stream prepared for ReadState by the MockDataStreamPreparer.
  auto context = make_unique<ClientContext>();
  ReadStateRequest request;
  request.set_key_prefix("");
  unique_ptr<ClientReaderInterface<Data>> data_stream(data->ReadStateRaw(context.get(), request));
  Data base_block;
  if (!data_stream->Read(&base_block)) {
    throw invalid_argument(
        "Attempting to construct a MockOrderedDataStreamHasher with a "
        "MockDataStreamPreparer that does not successfully provide any "
        "initial state.");
  }
  base_block_id_ = base_block.block_id();
}

Status MockOrderedDataStreamHasher::ReadStateHash(ClientContext* context,
                                                  const ReadStateHashRequest& request,
                                                  Hash* response) const {
  assert(response);

  list<string> update_hashes;
  auto data_context = make_unique<ClientContext>();
  SubscriptionRequest data_request;
  data_request.set_key_prefix(request.key_prefix());
  data_request.set_block_id(base_block_id_);
  unique_ptr<ClientReaderInterface<Data>> data_stream(
      data_preparer_->SubscribeToUpdatesRaw(data_context.get(), data_request));

  Data data;
  uint64_t latest_block_id_in_hash = 0;
  while (data_stream->Read(&data) && data.block_id() <= request.block_id()) {
    update_hashes.push_back(hashUpdate(data));
    latest_block_id_in_hash = data.block_id();
  }
  if (request.block_id() > latest_block_id_in_hash) {
    return Status(StatusCode::UNKNOWN,
                  "Attempting to read state hash from a block number that "
                  "does not exist.");
  }
  response->set_block_id(request.block_id());
  response->set_hash(hashState(update_hashes));
  return Status::OK;
}

ClientReaderInterface<Hash>* MockOrderedDataStreamHasher::SubscribeToUpdateHashesRaw(
    ClientContext* context, const SubscriptionRequest& request) const {
  auto hash_stream = new MockThinReplicaStream<Hash>();
  auto hash_stream_state = new StreamHasher(data_preparer_->SubscribeToUpdatesRaw(context, request));
  hash_stream->state.reset(hash_stream_state);

  ON_CALL(*hash_stream, Finish).WillByDefault(Invoke(hash_stream_state, &StreamHasher::Finish));
  ON_CALL(*hash_stream, Read).WillByDefault(Invoke(hash_stream_state, &StreamHasher::Read));
  return hash_stream;
}

void ThinReplicaCommunicationRecord::ClearRecords() {
  lock_guard<mutex> record_lock(record_mutex_);
  read_state_calls_.clear();
  read_state_hash_calls_.clear();
  subscribe_to_updates_calls_.clear();
  ack_update_calls_.clear();
  subscribe_to_update_hashes_calls_.clear();
  unsubscribe_calls_.clear();
}

void ThinReplicaCommunicationRecord::RecordReadState(size_t server_index, const ReadStateRequest& request) {
  lock_guard<mutex> record_lock(record_mutex_);
  read_state_calls_.emplace_back(server_index, request);
}

void ThinReplicaCommunicationRecord::RecordReadStateHash(size_t server_index, const ReadStateHashRequest& request) {
  lock_guard<mutex> record_lock(record_mutex_);
  read_state_hash_calls_.emplace_back(server_index, request);
}

void ThinReplicaCommunicationRecord::RecordSubscribeToUpdates(size_t server_index, const SubscriptionRequest& request) {
  lock_guard<mutex> record_lock(record_mutex_);
  subscribe_to_updates_calls_.emplace_back(server_index, request);
}

void ThinReplicaCommunicationRecord::RecordAckUpdate(size_t server_index, const BlockId& block_id) {
  lock_guard<mutex> record_lock(record_mutex_);
  ack_update_calls_.emplace_back(server_index, block_id);
}

void ThinReplicaCommunicationRecord::RecordSubscribeToUpdateHashes(size_t server_index,
                                                                   const SubscriptionRequest& request) {
  lock_guard<mutex> record_lock(record_mutex_);
  subscribe_to_update_hashes_calls_.emplace_back(server_index, request);
}

void ThinReplicaCommunicationRecord::RecordUnsubscribe(size_t server_index) {
  lock_guard<mutex> record_lock(record_mutex_);
  unsubscribe_calls_.emplace_back(server_index);
}

const list<pair<size_t, ReadStateRequest>>& ThinReplicaCommunicationRecord::GetReadStateCalls() const {
  return read_state_calls_;
}

const list<pair<size_t, ReadStateHashRequest>>& ThinReplicaCommunicationRecord::GetReadStateHashCalls() const {
  return read_state_hash_calls_;
}

const list<pair<size_t, SubscriptionRequest>>& ThinReplicaCommunicationRecord::GetSubscribeToUpdatesCalls() const {
  return subscribe_to_updates_calls_;
}

const list<pair<size_t, BlockId>>& ThinReplicaCommunicationRecord::GetAckUpdateCalls() const {
  return ack_update_calls_;
}

const list<pair<size_t, SubscriptionRequest>>& ThinReplicaCommunicationRecord::GetSubscribeToUpdateHashesCalls() const {
  return subscribe_to_update_hashes_calls_;
}

const list<size_t>& ThinReplicaCommunicationRecord::GetUnsubscribeCalls() const { return unsubscribe_calls_; }

size_t ThinReplicaCommunicationRecord::GetTotalCallCount() const {
  return read_state_calls_.size() + read_state_hash_calls_.size() + subscribe_to_updates_calls_.size() +
         ack_update_calls_.size() + subscribe_to_update_hashes_calls_.size() + unsubscribe_calls_.size();
}

MockThinReplicaServerRecorder::MockThinReplicaServerRecorder(shared_ptr<MockDataStreamPreparer> data,
                                                             shared_ptr<MockOrderedDataStreamHasher> hasher,
                                                             shared_ptr<ThinReplicaCommunicationRecord> record,
                                                             size_t server_index)
    : data_preparer_(data), hasher_(hasher), record_(record), server_index_(server_index) {}

ClientReaderInterface<Data>* MockThinReplicaServerRecorder::ReadStateRaw(ClientContext* context,
                                                                         const ReadStateRequest& request) {
  assert(data_preparer_);
  assert(record_);

  record_->RecordReadState(server_index_, request);
  return data_preparer_->ReadStateRaw(context, request);
}

Status MockThinReplicaServerRecorder::ReadStateHash(ClientContext* context,
                                                    const ReadStateHashRequest& request,
                                                    Hash* response) {
  assert(hasher_);
  assert(record_);

  record_->RecordReadStateHash(server_index_, request);
  return hasher_->ReadStateHash(context, request, response);
}

ClientReaderInterface<Data>* MockThinReplicaServerRecorder::SubscribeToUpdatesRaw(ClientContext* context,
                                                                                  const SubscriptionRequest& request) {
  assert(data_preparer_);
  assert(record_);

  record_->RecordSubscribeToUpdates(server_index_, request);
  return data_preparer_->SubscribeToUpdatesRaw(context, request);
}

Status MockThinReplicaServerRecorder::AckUpdate(ClientContext* context, const BlockId& block_id, Empty* response) {
  assert(record_);

  record_->RecordAckUpdate(server_index_, block_id);
  return Status::OK;
}

ClientReaderInterface<Hash>* MockThinReplicaServerRecorder::SubscribeToUpdateHashesRaw(
    ClientContext* context, const SubscriptionRequest& request) {
  assert(hasher_);
  assert(record_);

  record_->RecordSubscribeToUpdateHashes(server_index_, request);
  return hasher_->SubscribeToUpdateHashesRaw(context, request);
}

Status MockThinReplicaServerRecorder::Unsubscribe(ClientContext* context, const Empty& request, Empty* response) {
  assert(record_);

  record_->RecordUnsubscribe(server_index_);
  return Status::OK;
}

ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior::ByzantineServerBehavior()
    : byzantine_faulty_servers_(), faulty_server_record_mutex_() {}

bool ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior::IsByzantineFaulty(size_t index) {
  lock_guard<mutex> faulty_server_record_lock_(faulty_server_record_mutex_);
  return byzantine_faulty_servers_.count(index) > 0;
}

size_t ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior::GetNumFaultyServers() {
  lock_guard<mutex> faulty_server_record_lock_(faulty_server_record_mutex_);
  return byzantine_faulty_servers_.size();
}

bool ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior::MakeByzantineFaulty(size_t index,
                                                                                          size_t max_faulty) {
  lock_guard<mutex> faulty_server_record_lock_(faulty_server_record_mutex_);
  if (byzantine_faulty_servers_.size() < max_faulty) {
    byzantine_faulty_servers_.insert(index);
  }
  return byzantine_faulty_servers_.count(index) > 0;
}

ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior::~ByzantineServerBehavior() {}

ClientReaderInterface<Data>* ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior::ReadStateRaw(
    size_t server_index,
    ClientContext* context,
    const ReadStateRequest& request,
    ClientReaderInterface<Data>* correct_data) {
  return correct_data;
}

Status ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior::ReadStateHash(
    size_t server_index,
    ClientContext* context,
    const ReadStateHashRequest& request,
    Hash* response,
    Status correct_status) {
  return correct_status;
}

ClientReaderInterface<Data>* ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior::SubscribeToUpdatesRaw(
    size_t server_index,
    ClientContext* context,
    const SubscriptionRequest& request,
    ClientReaderInterface<Data>* correct_data) {
  return correct_data;
}

Status ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior::AckUpdate(
    size_t server_index, ClientContext* context, const BlockId& block_id, Empty* response, Status correct_status) {
  return correct_status;
}

ClientReaderInterface<Hash>*
ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior::SubscribeToUpdateHashesRaw(
    size_t server_index,
    ClientContext* context,
    const SubscriptionRequest& request,
    ClientReaderInterface<Hash>* correct_hashes) {
  return correct_hashes;
}

Status ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior::Unsubscribe(
    size_t server_index, ClientContext* context, const Empty& request, Empty* response, Status correct_status) {
  return correct_status;
}

ByzantineMockThinReplicaServerPreparer::ByzantineMockServer::ByzantineMockServer(
    shared_ptr<MockDataStreamPreparer> non_faulty_data,
    shared_ptr<MockOrderedDataStreamHasher> non_faulty_hasher,
    shared_ptr<ByzantineServerBehavior> byzantine_behavior,
    size_t index)
    : non_faulty_data_(non_faulty_data),
      non_faulty_hasher_(non_faulty_hasher),
      byzantine_behavior_(byzantine_behavior),
      index_(index) {}

ClientReaderInterface<Data>* ByzantineMockThinReplicaServerPreparer::ByzantineMockServer::ReadStateRaw(
    ClientContext* context, const ReadStateRequest& request) {
  assert(non_faulty_data_);
  assert(byzantine_behavior_);

  return byzantine_behavior_->ReadStateRaw(index_, context, request, non_faulty_data_->ReadStateRaw(context, request));
}

Status ByzantineMockThinReplicaServerPreparer::ByzantineMockServer::ReadStateHash(ClientContext* context,
                                                                                  const ReadStateHashRequest& request,
                                                                                  Hash* response) {
  assert(non_faulty_hasher_);
  assert(byzantine_behavior_);

  return byzantine_behavior_->ReadStateHash(
      index_, context, request, response, non_faulty_hasher_->ReadStateHash(context, request, response));
}

ClientReaderInterface<Data>* ByzantineMockThinReplicaServerPreparer::ByzantineMockServer::SubscribeToUpdatesRaw(
    ClientContext* context, const SubscriptionRequest& request) {
  assert(non_faulty_data_);
  assert(byzantine_behavior_);

  return byzantine_behavior_->SubscribeToUpdatesRaw(
      index_, context, request, non_faulty_data_->SubscribeToUpdatesRaw(context, request));
}

Status ByzantineMockThinReplicaServerPreparer::ByzantineMockServer::AckUpdate(ClientContext* context,
                                                                              const BlockId& block_id,
                                                                              Empty* response) {
  assert(byzantine_behavior_);

  return byzantine_behavior_->AckUpdate(index_, context, block_id, response, Status::OK);
}

ClientReaderInterface<Hash>* ByzantineMockThinReplicaServerPreparer::ByzantineMockServer::SubscribeToUpdateHashesRaw(
    ClientContext* context, const SubscriptionRequest& request) {
  assert(non_faulty_hasher_);
  assert(byzantine_behavior_);

  return byzantine_behavior_->SubscribeToUpdateHashesRaw(
      index_, context, request, non_faulty_hasher_->SubscribeToUpdateHashesRaw(context, request));
}

Status ByzantineMockThinReplicaServerPreparer::ByzantineMockServer::Unsubscribe(ClientContext* context,
                                                                                const Empty& request,
                                                                                Empty* response) {
  assert(byzantine_behavior_);

  return byzantine_behavior_->Unsubscribe(index_, context, request, response, Status::OK);
}

ByzantineMockThinReplicaServerPreparer::ByzantineMockThinReplicaServerPreparer(
    shared_ptr<MockDataStreamPreparer> non_faulty_data,
    shared_ptr<MockOrderedDataStreamHasher> non_faulty_hasher,
    shared_ptr<ByzantineServerBehavior> byzantine_behavior)
    : non_faulty_data_(non_faulty_data),
      non_faulty_hasher_(non_faulty_hasher),
      byzantine_behavior_(byzantine_behavior) {}

ByzantineMockThinReplicaServerPreparer::ByzantineMockServer*
ByzantineMockThinReplicaServerPreparer::CreateByzantineMockServer(size_t index) {
  return new ByzantineMockServer(non_faulty_data_, non_faulty_hasher_, byzantine_behavior_, index);
}

vector<unique_ptr<MockThinReplicaServerRecorder>> CreateMockServerRecorders(
    size_t num_servers,
    shared_ptr<MockDataStreamPreparer> data,
    shared_ptr<MockOrderedDataStreamHasher> hasher,
    shared_ptr<ThinReplicaCommunicationRecord> record) {
  vector<unique_ptr<MockThinReplicaServerRecorder>> recorders;
  for (size_t i = 0; i < num_servers; ++i) {
    recorders.push_back(make_unique<MockThinReplicaServerRecorder>(data, hasher, record, i));
  }
  return recorders;
}

vector<unique_ptr<ByzantineMockThinReplicaServerPreparer::ByzantineMockServer>> CreateByzantineMockServers(
    size_t num_servers, ByzantineMockThinReplicaServerPreparer& server_preparer) {
  vector<unique_ptr<ByzantineMockThinReplicaServerPreparer::ByzantineMockServer>> mock_servers;
  for (size_t i = 0; i < num_servers; ++i) {
    mock_servers.push_back(unique_ptr<ByzantineMockThinReplicaServerPreparer::ByzantineMockServer>(
        server_preparer.CreateByzantineMockServer(i)));
  }
  return mock_servers;
}

void SetMockServerBehavior(MockTrsConnection* server,
                           const shared_ptr<MockDataStreamPreparer>& data_preparer,
                           const MockOrderedDataStreamHasher& hasher) {
  ON_CALL(*(server->GetStub()), ReadStateRaw)
      .WillByDefault(Invoke(data_preparer.get(), &MockDataStreamPreparer::ReadStateRaw));
  ON_CALL(*(server->GetStub()), ReadStateHash)
      .WillByDefault(Invoke(&hasher, &MockOrderedDataStreamHasher::ReadStateHash));
  ON_CALL(*(server->GetStub()), SubscribeToUpdatesRaw)
      .WillByDefault(Invoke(data_preparer.get(), &MockDataStreamPreparer::SubscribeToUpdatesRaw));
  ON_CALL(*(server->GetStub()), SubscribeToUpdateHashesRaw)
      .WillByDefault(Invoke(&hasher, &MockOrderedDataStreamHasher::SubscribeToUpdateHashesRaw));
}

void SetMockServerUnresponsive(MockTrsConnection* server) {
  ON_CALL(*(server->GetStub()), ReadStateRaw).WillByDefault(InvokeWithoutArgs(&CreateUnresponsiveMockStream<Data>));
  ON_CALL(*(server->GetStub()), ReadStateHash)
      .WillByDefault(Return(Status(StatusCode::UNAVAILABLE, "This server is non-responsive")));
  ON_CALL(*(server->GetStub()), SubscribeToUpdatesRaw)
      .WillByDefault(InvokeWithoutArgs(&CreateUnresponsiveMockStream<Data>));
  ON_CALL(*(server->GetStub()), AckUpdate)
      .WillByDefault(Return(Status(StatusCode::UNAVAILABLE, "This server is non-responsive")));
  ON_CALL(*(server->GetStub()), SubscribeToUpdateHashesRaw)
      .WillByDefault(InvokeWithoutArgs(&CreateUnresponsiveMockStream<Hash>));
  ON_CALL(*(server->GetStub()), Unsubscribe)
      .WillByDefault(Return(Status(StatusCode::UNAVAILABLE, "This server is non-responsive")));
}

vector<unique_ptr<TrsConnection>> CreateTrsConnections(size_t num_servers, size_t num_unresponsive) {
  vector<unique_ptr<TrsConnection>> mock_servers;
  for (size_t i = 0; i < num_servers; ++i) {
    auto conn = new MockTrsConnection();
    if (num_unresponsive > 0) {
      SetMockServerUnresponsive(conn);
      num_unresponsive--;
    }
    auto server = dynamic_cast<TrsConnection*>(conn);
    mock_servers.push_back(unique_ptr<TrsConnection>(server));
  }
  return mock_servers;
}

vector<unique_ptr<TrsConnection>> CreateTrsConnections(size_t num_servers,
                                                       shared_ptr<MockDataStreamPreparer> stream_preparer,
                                                       MockOrderedDataStreamHasher& hasher,
                                                       size_t num_unresponsive) {
  vector<unique_ptr<TrsConnection>> mock_servers;
  for (size_t i = 0; i < num_servers; ++i) {
    auto conn = new MockTrsConnection();
    SetMockServerBehavior(conn, stream_preparer, hasher);
    if (num_unresponsive > 0) {
      SetMockServerUnresponsive(conn);
      num_unresponsive--;
    }
    auto server = dynamic_cast<TrsConnection*>(conn);
    mock_servers.push_back(unique_ptr<TrsConnection>(server));
  }
  return mock_servers;
}

InitialStateFabricator::InitialStateFabricator(shared_ptr<MockDataStreamPreparer> fabricated_data_preparer,
                                               size_t num_faulty_servers)
    : fabricated_data_preparer_(fabricated_data_preparer), num_faulty_servers_(num_faulty_servers) {}

InitialStateFabricator::~InitialStateFabricator() {}

ClientReaderInterface<Data>* InitialStateFabricator::ReadStateRaw(size_t server_index,
                                                                  ClientContext* context,
                                                                  const ReadStateRequest& request,
                                                                  ClientReaderInterface<Data>* correct_data) {
  if (MakeByzantineFaulty(server_index, num_faulty_servers_)) {
    delete correct_data;
    return fabricated_data_preparer_->ReadStateRaw(context, request);
  }
  return correct_data;
}

UpdateDataFabricator::UpdateDataFabricator(shared_ptr<MockDataStreamPreparer> fabricated_data_preparer,
                                           size_t num_faulty_servers)
    : fabricated_data_preparer_(fabricated_data_preparer), num_faulty_servers_(num_faulty_servers) {}

UpdateDataFabricator::~UpdateDataFabricator() {}

ClientReaderInterface<Data>* UpdateDataFabricator::SubscribeToUpdatesRaw(size_t server_index,
                                                                         ClientContext* context,
                                                                         const SubscriptionRequest& request,
                                                                         ClientReaderInterface<Data>* correct_data) {
  if (MakeByzantineFaulty(server_index, num_faulty_servers_)) {
    delete correct_data;
    return fabricated_data_preparer_->SubscribeToUpdatesRaw(context, request);
  }
  return correct_data;
}

UpdateHashFabricator::UpdateHashFabricator(shared_ptr<MockDataStreamPreparer> fabricated_data_preparer,
                                           size_t num_faulty_servers)
    : fabricated_update_hasher_(new MockOrderedDataStreamHasher(fabricated_data_preparer)),
      num_faulty_servers_(num_faulty_servers) {}

UpdateHashFabricator::~UpdateHashFabricator() {}

ClientReaderInterface<Hash>* UpdateHashFabricator::SubscribeToUpdateHashesRaw(
    size_t server_index,
    ClientContext* context,
    const SubscriptionRequest& request,
    ClientReaderInterface<Hash>* correct_hashes) {
  if (MakeByzantineFaulty(server_index, num_faulty_servers_)) {
    delete correct_hashes;
    return fabricated_update_hasher_->SubscribeToUpdateHashesRaw(context, request);
  }
  return correct_hashes;
}

StateStreamDelayer::DelayedStreamState::DelayedStreamState(ClientReaderInterface<Data>* data, size_t delay_after)
    : undelayed_data_(data), updates_until_delay_(delay_after) {}

StateStreamDelayer::DelayedStreamState::~DelayedStreamState() {}

Status StateStreamDelayer::DelayedStreamState::Finish() {
  if (updates_until_delay_ <= 0) {
    sleep_for(kTestingTimeout * 2);
  }
  return undelayed_data_->Finish();
}

bool StateStreamDelayer::DelayedStreamState::Read(Data* data) {
  if (updates_until_delay_ <= 0) {
    sleep_for(kTestingTimeout * 2);
  } else {
    --updates_until_delay_;
  }
  return undelayed_data_->Read(data);
}

StateStreamDelayer::StateStreamDelayer(size_t delay_after) : delay_after_(delay_after) {}

StateStreamDelayer::~StateStreamDelayer() {}

ClientReaderInterface<Data>* StateStreamDelayer::ReadStateRaw(size_t server_index,
                                                              ClientContext* context,
                                                              const ReadStateRequest& request,
                                                              ClientReaderInterface<Data>* correct_data) {
  if (MakeByzantineFaulty(server_index, 1)) {
    MockThinReplicaStream<Data>* data_stream = new MockThinReplicaStream<Data>();
    DelayedStreamState* stream_state = new DelayedStreamState(correct_data, delay_after_);
    data_stream->state.reset(stream_state);

    ON_CALL(*data_stream, Finish).WillByDefault(Invoke(stream_state, &DelayedStreamState::Finish));
    ON_CALL(*data_stream, Read).WillByDefault(Invoke(stream_state, &DelayedStreamState::Read));
    return data_stream;
  }
  return correct_data;
}

DataStreamDelayer::DelayedStreamState::DelayedStreamState(ClientReaderInterface<Data>* data, size_t delay_after)
    : undelayed_data_(data), updates_until_delay_(delay_after) {}

DataStreamDelayer::DelayedStreamState::~DelayedStreamState() {}

Status DataStreamDelayer::DelayedStreamState::Finish() {
  if (updates_until_delay_ <= 0) {
    sleep_for(kTestingTimeout * 2);
  }
  return undelayed_data_->Finish();
}

bool DataStreamDelayer::DelayedStreamState::Read(Data* data) {
  if (updates_until_delay_ <= 0) {
    sleep_for(kTestingTimeout * 2);
  } else {
    --updates_until_delay_;
  }
  return undelayed_data_->Read(data);
}

DataStreamDelayer::DataStreamDelayer(size_t delay_after) : delay_after_(delay_after) {}

DataStreamDelayer::~DataStreamDelayer() {}

ClientReaderInterface<Data>* DataStreamDelayer::SubscribeToUpdatesRaw(size_t server_index,
                                                                      ClientContext* context,
                                                                      const SubscriptionRequest& request,
                                                                      ClientReaderInterface<Data>* correct_data) {
  if (MakeByzantineFaulty(server_index, 1)) {
    MockThinReplicaStream<Data>* data_stream = new MockThinReplicaStream<Data>();
    DelayedStreamState* stream_state = new DelayedStreamState(correct_data, delay_after_);
    data_stream->state.reset(stream_state);

    ON_CALL(*data_stream, Finish).WillByDefault(Invoke(stream_state, &DelayedStreamState::Finish));
    ON_CALL(*data_stream, Read).WillByDefault(Invoke(stream_state, &DelayedStreamState::Read));
    return data_stream;
  }
  return correct_data;
}

HashStreamDelayer::DelayedStreamState::DelayedStreamState(ClientReaderInterface<Hash>* hashes, size_t delay_after)
    : undelayed_hashes_(hashes), hashes_until_delay_(delay_after) {}

HashStreamDelayer::DelayedStreamState::~DelayedStreamState() {}

Status HashStreamDelayer::DelayedStreamState::Finish() {
  if (hashes_until_delay_ <= 0) {
    sleep_for(kTestingTimeout * 2);
  }
  return undelayed_hashes_->Finish();
}

bool HashStreamDelayer::DelayedStreamState::Read(Hash* hash) {
  if (hashes_until_delay_ <= 0) {
    sleep_for(kTestingTimeout * 2);
  } else {
    --hashes_until_delay_;
  }
  return undelayed_hashes_->Read(hash);
}

HashStreamDelayer::HashStreamDelayer(size_t delay_after) : delay_after_(delay_after) {}

HashStreamDelayer::~HashStreamDelayer() {}

ClientReaderInterface<Hash>* HashStreamDelayer::SubscribeToUpdateHashesRaw(
    size_t server_index,
    ClientContext* context,
    const SubscriptionRequest& request,
    ClientReaderInterface<Hash>* correct_hashes) {
  if (MakeByzantineFaulty(server_index, 1)) {
    MockThinReplicaStream<Hash>* hash_stream = new MockThinReplicaStream<Hash>();
    DelayedStreamState* stream_state = new DelayedStreamState(correct_hashes, delay_after_);
    hash_stream->state.reset(stream_state);

    ON_CALL(*hash_stream, Finish).WillByDefault(Invoke(stream_state, &DelayedStreamState::Finish));
    ON_CALL(*hash_stream, Read).WillByDefault(Invoke(stream_state, &DelayedStreamState::Read));
    return hash_stream;
  }
  return correct_hashes;
}

UpdateHashCorrupter::CorruptedStreamState::CorruptedStreamState(
    ClientReaderInterface<Hash>* hashes,
    shared_ptr<UpdateHashCorrupter::CorruptHash> corrupt_hash,
    size_t corrupt_after)
    : uncorrupted_hashes_(hashes),
      corrupt_hash_(corrupt_hash),
      corruption_applied_(false),
      hashes_until_corruption_(corrupt_after) {}

UpdateHashCorrupter::CorruptedStreamState::~CorruptedStreamState() {}

Status UpdateHashCorrupter::CorruptedStreamState::Finish() { return uncorrupted_hashes_->Finish(); }

bool UpdateHashCorrupter::CorruptedStreamState::Read(Hash* hash) {
  if (!corruption_applied_) {
    if (hashes_until_corruption_ > 0) {
      --hashes_until_corruption_;
    } else {
      bool read_result = uncorrupted_hashes_->Read(hash);
      (*corrupt_hash_)(hash);
      corruption_applied_ = true;
      return read_result;
    }
  }
  return uncorrupted_hashes_->Read(hash);
}

UpdateHashCorrupter::UpdateHashCorrupter(shared_ptr<UpdateHashCorrupter::CorruptHash> corrupt_hash,
                                         size_t corrupt_after)
    : corrupt_hash_(corrupt_hash), corrupt_after_(corrupt_after) {}

UpdateHashCorrupter::~UpdateHashCorrupter() {}

ClientReaderInterface<Hash>* UpdateHashCorrupter::SubscribeToUpdateHashesRaw(
    size_t server_index,
    ClientContext* context,
    const SubscriptionRequest& request,
    ClientReaderInterface<Hash>* correct_hashes) {
  if (MakeByzantineFaulty(server_index, 1)) {
    MockThinReplicaStream<Hash>* hash_stream = new MockThinReplicaStream<Hash>();
    CorruptedStreamState* stream_state = new CorruptedStreamState(correct_hashes, corrupt_hash_, corrupt_after_);
    hash_stream->state.reset(stream_state);

    ON_CALL(*hash_stream, Finish).WillByDefault(Invoke(stream_state, &CorruptedStreamState::Finish));
    ON_CALL(*hash_stream, Read).WillByDefault(Invoke(stream_state, &CorruptedStreamState::Read));
    return hash_stream;
  }
  return correct_hashes;
}

size_t ClusterResponsivenessLimiter::GetMaxFaulty() const { return max_faulty_; }

size_t ClusterResponsivenessLimiter::GetClusterSize() const { return cluster_size_; }

bool ClusterResponsivenessLimiter::IsUnresponsive(size_t server_index) const {
  assert(server_index < server_responsiveness_.size());
  return !(server_responsiveness_[server_index]);
}

size_t ClusterResponsivenessLimiter::GetNumUnresponsiveServers() const {
  size_t num_unresponsive = 0;
  for (size_t i = 0; i < server_responsiveness_.size(); ++i) {
    if (!(server_responsiveness_[i])) {
      ++num_unresponsive;
    }
  }
  return num_unresponsive;
}

void ClusterResponsivenessLimiter::SetResponsive(size_t server_index) {
  assert(server_index < server_responsiveness_.size());
  server_responsiveness_[server_index] = true;
}

void ClusterResponsivenessLimiter::SetUnresponsive(size_t server_index) {
  assert(server_index < server_responsiveness_.size());
  server_responsiveness_[server_index] = false;
}

ClusterResponsivenessLimiter::ResponsivenessManager::~ResponsivenessManager() {}

ClusterResponsivenessLimiter::DataStreamState::DataStreamState(
    grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* data,
    ClusterResponsivenessLimiter& responsiveness_limiter,
    size_t server_index)
    : data_(data), responsiveness_limiter_(responsiveness_limiter), server_index_(server_index) {}

ClusterResponsivenessLimiter::DataStreamState::~DataStreamState() {}

Status ClusterResponsivenessLimiter::DataStreamState::Finish() {
  bool delay;
  {
    unique_lock<mutex> responsiveness_lock(responsiveness_limiter_.responsiveness_change_mutex_);
    delay = !(responsiveness_limiter_.server_responsiveness_[server_index_]);
  }
  if (delay) {
    sleep_for(kTestingTimeout * 2);
  }
  return data_->Finish();
}

bool ClusterResponsivenessLimiter::DataStreamState::Read(Data* data) {
  bool delay;
  bool result = data_->Read(data);
  {
    unique_lock<mutex> responsiveness_lock(responsiveness_limiter_.responsiveness_change_mutex_);
    if (result) {
      responsiveness_limiter_.responsiveness_manager_->UpdateResponsiveness(
          responsiveness_limiter_, server_index_, data->block_id());
    }
    delay = !(responsiveness_limiter_.server_responsiveness_[server_index_]);
  }
  if (delay) {
    sleep_for(kTestingTimeout * 2);
  }
  return result;
}

ClusterResponsivenessLimiter::HashStreamState::HashStreamState(
    grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Hash>* hashes,
    ClusterResponsivenessLimiter& responsiveness_limiter,
    size_t server_index)
    : hashes_(hashes), responsiveness_limiter_(responsiveness_limiter), server_index_(server_index) {}

ClusterResponsivenessLimiter::HashStreamState::~HashStreamState() {}

Status ClusterResponsivenessLimiter::HashStreamState::Finish() {
  bool delay;
  {
    unique_lock<mutex> responsiveness_lock(responsiveness_limiter_.responsiveness_change_mutex_);
    delay = !(responsiveness_limiter_.server_responsiveness_[server_index_]);
  }
  if (delay) {
    sleep_for(kTestingTimeout * 2);
  }
  return hashes_->Finish();
}

bool ClusterResponsivenessLimiter::HashStreamState::Read(Hash* hash) {
  bool delay;
  bool result = hashes_->Read(hash);
  uint64_t block_id = result ? hash->block_id() : UINT64_MAX;
  {
    unique_lock<mutex> responsiveness_lock(responsiveness_limiter_.responsiveness_change_mutex_);
    if (result) {
      responsiveness_limiter_.responsiveness_manager_->UpdateResponsiveness(
          responsiveness_limiter_, server_index_, hash->block_id());
    }
    delay = !(responsiveness_limiter_.server_responsiveness_[server_index_]);
  }
  if (delay) {
    sleep_for(kTestingTimeout * 2);
  }
  return result;
}

ClusterResponsivenessLimiter::ClusterResponsivenessLimiter(size_t max_faulty,
                                                           size_t num_servers,
                                                           unique_ptr<ResponsivenessManager>&& responsiveness_manager)
    : max_faulty_(max_faulty),
      cluster_size_(num_servers),
      responsiveness_manager_(move(responsiveness_manager)),
      server_responsiveness_(num_servers, true),
      responsiveness_change_mutex_() {}

ClusterResponsivenessLimiter::~ClusterResponsivenessLimiter() {}

ClientReaderInterface<Data>* ClusterResponsivenessLimiter::ReadStateRaw(size_t server_index,
                                                                        ClientContext* context,
                                                                        const ReadStateRequest& request,
                                                                        ClientReaderInterface<Data>* correct_data) {
  bool delay;
  {
    unique_lock<mutex> responsiveness_lock(responsiveness_change_mutex_);
    responsiveness_manager_->UpdateResponsiveness(*this, server_index, 0);
    delay = !(server_responsiveness_[server_index]);
  }
  if (delay) {
    sleep_for(kTestingTimeout * 2);
  }
  MockThinReplicaStream<Data>* state_stream = new MockThinReplicaStream<Data>();
  DataStreamState* stream_state = new DataStreamState(correct_data, *this, server_index);
  state_stream->state.reset(stream_state);
  ON_CALL(*state_stream, Finish).WillByDefault(Invoke(stream_state, &DataStreamState::Finish));
  ON_CALL(*state_stream, Read).WillByDefault(Invoke(stream_state, &DataStreamState::Read));
  return state_stream;
}

Status ClusterResponsivenessLimiter::ReadStateHash(size_t server_index,
                                                   ClientContext* context,
                                                   const ReadStateHashRequest& request,
                                                   Hash* response,
                                                   Status correct_status) {
  bool delay;
  {
    unique_lock<mutex> responsiveness_lock(responsiveness_change_mutex_);
    responsiveness_manager_->UpdateResponsiveness(*this, server_index, request.block_id());
    delay = !(server_responsiveness_[server_index]);
  }
  if (delay) {
    sleep_for(kTestingTimeout * 2);
  }
  return correct_status;
}

ClientReaderInterface<Data>* ClusterResponsivenessLimiter::SubscribeToUpdatesRaw(
    size_t server_index,
    ClientContext* context,
    const SubscriptionRequest& request,
    ClientReaderInterface<Data>* correct_data) {
  bool delay;
  {
    unique_lock<mutex> responsiveness_lock(responsiveness_change_mutex_);
    responsiveness_manager_->UpdateResponsiveness(*this, server_index, request.block_id());
    delay = !(server_responsiveness_[server_index]);
  }
  if (delay) {
    sleep_for(kTestingTimeout * 2);
  }
  MockThinReplicaStream<Data>* update_stream = new MockThinReplicaStream<Data>();
  DataStreamState* stream_state = new DataStreamState(correct_data, *this, server_index);
  update_stream->state.reset(stream_state);
  ON_CALL(*update_stream, Finish).WillByDefault(Invoke(stream_state, &DataStreamState::Finish));
  ON_CALL(*update_stream, Read).WillByDefault(Invoke(stream_state, &DataStreamState::Read));
  return update_stream;
}

ClientReaderInterface<Hash>* ClusterResponsivenessLimiter::SubscribeToUpdateHashesRaw(
    size_t server_index,
    ClientContext* context,
    const SubscriptionRequest& request,
    ClientReaderInterface<Hash>* correct_hashes) {
  bool delay;
  {
    unique_lock<mutex> responsiveness_lock(responsiveness_change_mutex_);
    responsiveness_manager_->UpdateResponsiveness(*this, server_index, request.block_id());
    delay = !(server_responsiveness_[server_index]);
  }
  if (delay) {
    sleep_for(kTestingTimeout * 2);
  }
  MockThinReplicaStream<Hash>* hash_stream = new MockThinReplicaStream<Hash>();
  HashStreamState* stream_state = new HashStreamState(correct_hashes, *this, server_index);
  hash_stream->state.reset(stream_state);
  ON_CALL(*hash_stream, Finish).WillByDefault(Invoke(stream_state, &HashStreamState::Finish));
  ON_CALL(*hash_stream, Read).WillByDefault(Invoke(stream_state, &HashStreamState::Read));
  return hash_stream;
}

ComprehensiveDataFabricator::ComprehensiveDataFabricator(shared_ptr<MockDataStreamPreparer> fabricated_data_preparer,
                                                         size_t num_faulty_servers)
    : fabricated_data_preparer_(fabricated_data_preparer),
      fabricated_update_hasher_(new MockOrderedDataStreamHasher(fabricated_data_preparer)),
      num_faulty_servers_(num_faulty_servers) {}

ComprehensiveDataFabricator::~ComprehensiveDataFabricator() {}

ClientReaderInterface<Data>* ComprehensiveDataFabricator::ReadStateRaw(size_t server_index,
                                                                       ClientContext* context,
                                                                       const ReadStateRequest& request,
                                                                       ClientReaderInterface<Data>* correct_data) {
  if (MakeByzantineFaulty(server_index, num_faulty_servers_)) {
    delete correct_data;
    return fabricated_data_preparer_->ReadStateRaw(context, request);
  }
  return correct_data;
}

Status ComprehensiveDataFabricator::ReadStateHash(size_t server_index,
                                                  ClientContext* context,
                                                  const ReadStateHashRequest& request,
                                                  Hash* response,
                                                  Status correct_status) {
  if (MakeByzantineFaulty(server_index, num_faulty_servers_)) {
    return fabricated_update_hasher_->ReadStateHash(context, request, response);
  }
  return correct_status;
}

ClientReaderInterface<Data>* ComprehensiveDataFabricator::SubscribeToUpdatesRaw(
    size_t server_index,
    ClientContext* context,
    const SubscriptionRequest& request,
    ClientReaderInterface<Data>* correct_data) {
  if (MakeByzantineFaulty(server_index, num_faulty_servers_)) {
    delete correct_data;
    return fabricated_data_preparer_->SubscribeToUpdatesRaw(context, request);
  }
  return correct_data;
}

ClientReaderInterface<Hash>* ComprehensiveDataFabricator::SubscribeToUpdateHashesRaw(
    size_t server_index,
    ClientContext* context,
    const SubscriptionRequest& request,
    ClientReaderInterface<Hash>* correct_hashes) {
  if (MakeByzantineFaulty(server_index, num_faulty_servers_)) {
    delete correct_hashes;
    return fabricated_update_hasher_->SubscribeToUpdateHashesRaw(context, request);
  }
  return correct_hashes;
}
