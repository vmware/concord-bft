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

#pragma once

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "thin_replica_mock.grpc.pb.h"
#include "client/thin-replica-client/trs_connection.hpp"

const std::chrono::milliseconds kTestingTimeout(10);

class MockTrsConnection : public client::thin_replica_client::TrsConnection {
 public:
  MockTrsConnection();
  ~MockTrsConnection() override {}
  com::vmware::concord::thin_replica::MockThinReplicaStub* GetStub();
  bool isConnected() override;
};

template <class DataType>
class MockThinReplicaStream : public grpc::ClientReaderInterface<DataType> {
 public:
  // Functions the Thin Replica Client Library is known to currently use.
  MOCK_METHOD0_T(Finish, grpc::Status());
  MOCK_METHOD1_T(Read, bool(DataType* msg));

  // Other virtual StubInterface functions currently thought to be unused by the
  // Thin Replica Client Library.
  MOCK_METHOD1_T(NextMessageSize, bool(uint32_t* sz));
  MOCK_METHOD0_T(WaitForInitialMetadata, void());

  // Abstract state type and unique_ptr to state of that type that testing code
  // may attach to this MockThinReplicaStream. This can be used to tie the
  // lifespan and destruction of auxiliary state the testing code is using with
  // a MockThinReplicaStream object to the lifespan and destruction of that mock
  // object.
  class State {
   public:
    virtual ~State() {}
  };
  std::unique_ptr<State> state = std::unique_ptr<State>(nullptr);
};

// Abstract class specifying an interface for handlig mocked calls to ReadState
// and SubscribeToUpdates.
class MockDataStreamPreparer {
 public:
  virtual ~MockDataStreamPreparer() {}
  virtual grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* ReadStateRaw(
      grpc::ClientContext* context, const com::vmware::concord::thin_replica::ReadStateRequest& request) const = 0;
  virtual grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* SubscribeToUpdatesRaw(
      grpc::ClientContext* context, const com::vmware::concord::thin_replica::SubscriptionRequest& request) const = 0;
};

// Prepares mock data streams to return the contents of the vector it is
// constructed with in order; the block IDs given in the vector for the Data
// entries are preserved.
class VectorMockDataStreamPreparer : public MockDataStreamPreparer {
 private:
  class DataQueue : public MockThinReplicaStream<com::vmware::concord::thin_replica::Data>::State {
   private:
    std::list<com::vmware::concord::thin_replica::Data> queue_;
    std::mutex queue_mutex_;
    std::condition_variable empty_condition_;
    std::mutex empty_condition_mutex_;

   public:
    DataQueue(const std::list<com::vmware::concord::thin_replica::Data>& data);
    ~DataQueue() override;
    grpc::Status Finish();
    bool Read(com::vmware::concord::thin_replica::Data* msg);
  };
  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* PrepareInitialStateDataStream() const;
  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* PrepareSubscriptionDataStream(
      uint64_t block_id) const;

  const std::vector<com::vmware::concord::thin_replica::Data> data_;
  size_t num_updates_in_initial_state_;

 public:
  VectorMockDataStreamPreparer(const std::vector<com::vmware::concord::thin_replica::Data>& data,
                               size_t initial_state_size = 1);
  ~VectorMockDataStreamPreparer() override;
  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* ReadStateRaw(
      grpc::ClientContext* context, const com::vmware::concord::thin_replica::ReadStateRequest& request) const override;
  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* SubscribeToUpdatesRaw(
      grpc::ClientContext* context,
      const com::vmware::concord::thin_replica::SubscriptionRequest& request) const override;
};

// Prepares mock data streams that repeatedly return the same update.
// Automatically increments the Block IDs of sequentially returned Data objects,
// beginning from the block ID the Data object this
// RepeatedMJockDataStreamPreparer was constructed with (for requests that do
// not specify a block ID), or beginning with the specified block ID (for
// requests that do specify a block ID).
class RepeatedMockDataStreamPreparer : public MockDataStreamPreparer {
 private:
  class DataRepeater : public MockThinReplicaStream<com::vmware::concord::thin_replica::Data>::State {
   private:
    com::vmware::concord::thin_replica::Data data_;
    uint64_t current_block_id_;
    std::mutex block_id_mutex_;

    bool finite_length_;
    size_t num_updates_ = 0;

    std::condition_variable finished_condition_;
    std::mutex finished_condition_mutex_;

   public:
    DataRepeater(const com::vmware::concord::thin_replica::Data& data, uint64_t starting_block_id);
    DataRepeater(const com::vmware::concord::thin_replica::Data& data, uint64_t starting_block_id, size_t num_updates);
    ~DataRepeater() override;
    grpc::Status Finish();
    bool Read(com::vmware::concord::thin_replica::Data* msg);
  };

  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* PrepareInitialStateDataStream() const;

  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* PrepareSubscriptionDataStream(
      uint64_t block_id) const;

  com::vmware::concord::thin_replica::Data data_;
  size_t num_updates_in_initial_state_;

 public:
  RepeatedMockDataStreamPreparer(const com::vmware::concord::thin_replica::Data& data, size_t initial_state_length = 1);
  ~RepeatedMockDataStreamPreparer() override;
  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* ReadStateRaw(
      grpc::ClientContext* context, const com::vmware::concord::thin_replica::ReadStateRequest& request) const override;
  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* SubscribeToUpdatesRaw(
      grpc::ClientContext* context,
      const com::vmware::concord::thin_replica::SubscriptionRequest& request) const override;
};

// Decorator of other MockDataStreamPreparers that waits on an external
// condition before sending each update (excluding those that are part of the
// initial state).
class DelayedMockDataStreamPreparer : public MockDataStreamPreparer {
 private:
  class DataDelayer : public MockThinReplicaStream<com::vmware::concord::thin_replica::Data>::State {
   private:
    std::unique_ptr<grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>> undelayed_data_;

    std::shared_ptr<std::condition_variable> waiting_condition_;
    std::shared_ptr<std::atomic_bool> spurious_wakeup_;
    std::shared_ptr<std::mutex> condition_mutex_;

   public:
    DataDelayer(grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* data,
                const std::shared_ptr<std::condition_variable>& delay_condition,
                const std::shared_ptr<std::mutex>& delay_mutex,
                const std::shared_ptr<std::atomic_bool>& spurious_wakeup);
    ~DataDelayer() override;
    grpc::Status Finish();
    bool Read(com::vmware::concord::thin_replica::Data* msg);
  };

  std::shared_ptr<MockDataStreamPreparer> undelayed_data_preparer_;
  std::shared_ptr<std::condition_variable> delay_condition_;
  std::shared_ptr<std::atomic_bool> spurious_wakeup_;
  std::shared_ptr<std::mutex> condition_mutex_;

 public:
  DelayedMockDataStreamPreparer(std::shared_ptr<MockDataStreamPreparer>& data,
                                std::shared_ptr<std::condition_variable> condition,
                                std::shared_ptr<std::atomic_bool> spurious_wakeup_indicator,
                                std::shared_ptr<std::mutex> condition_mutex);
  virtual ~DelayedMockDataStreamPreparer();
  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* ReadStateRaw(
      grpc::ClientContext* context, const com::vmware::concord::thin_replica::ReadStateRequest& request) const override;
  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* SubscribeToUpdatesRaw(
      grpc::ClientContext* context,
      const com::vmware::concord::thin_replica::SubscriptionRequest& request) const override;
};

// Mock hasher class which provides a hashing implementation corresponding to
// what hashes a Thin Replica Server (or at least a Thin Replica Server that is
// not Byzantine-faulty in such a way that it returns hashes disagreeing with
// its own data) would return, given a MockDataStreamPreparer object returning
// the update data that server returns for any MockDataStreamPreparer object
// that is guaranteed to return its updates in a consistent and monotonically
// increasing order.
class MockOrderedDataStreamHasher {
 private:
  typedef size_t StateHashType;
  typedef size_t UpdateHashType;

  class StreamHasher : public MockThinReplicaStream<com::vmware::concord::thin_replica::Hash>::State {
   private:
    std::unique_ptr<grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>> data_stream_;

   public:
    StreamHasher(grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* data);
    ~StreamHasher() override;
    grpc::Status Finish();
    bool Read(com::vmware::concord::thin_replica::Hash* msg);
  };

  std::shared_ptr<MockDataStreamPreparer> data_preparer_;
  uint64_t base_block_id_;

 public:
  MockOrderedDataStreamHasher(std::shared_ptr<MockDataStreamPreparer>& data);

  grpc::Status ReadStateHash(grpc::ClientContext* context,
                             const com::vmware::concord::thin_replica::ReadStateHashRequest& request,
                             com::vmware::concord::thin_replica::Hash* response) const;
  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Hash>* SubscribeToUpdateHashesRaw(
      grpc::ClientContext* context, const com::vmware::concord::thin_replica::SubscriptionRequest& request) const;
};

// Class used for storing records of RPC calls to Thin Replica Servers made by a
// ThinReplicaClient; this is intended for use in testing that the
// ThinReplicaClient implementation complies with the intended use(s) of these
// RPC calls as described in the Thin Replica Mechanism design. Note this class
// handles synchronization of writes to these records across multiple threads.
// Call records are structured as lists of pairs of size_ts (representing the
// server the call was made to) and Protobuf messages (representing the
// parameter given for the call) or as lists of just size_ts (representing the
// server the call was made to) for RPC calls that do not use parameters.
class ThinReplicaCommunicationRecord {
 private:
  std::list<std::pair<size_t, com::vmware::concord::thin_replica::ReadStateRequest>> read_state_calls_;
  std::list<std::pair<size_t, com::vmware::concord::thin_replica::ReadStateHashRequest>> read_state_hash_calls_;
  std::list<std::pair<size_t, com::vmware::concord::thin_replica::SubscriptionRequest>> subscribe_to_updates_calls_;
  std::list<std::pair<size_t, com::vmware::concord::thin_replica::BlockId>> ack_update_calls_;
  std::list<std::pair<size_t, com::vmware::concord::thin_replica::SubscriptionRequest>>
      subscribe_to_update_hashes_calls_;
  std::list<size_t> unsubscribe_calls_;

  std::mutex record_mutex_;

 public:
  void ClearRecords();

  void RecordReadState(size_t server_index, const com::vmware::concord::thin_replica::ReadStateRequest& request);
  void RecordReadStateHash(size_t server_index,
                           const com::vmware::concord::thin_replica::ReadStateHashRequest& request);
  void RecordSubscribeToUpdates(size_t server_index,
                                const com::vmware::concord::thin_replica::SubscriptionRequest& request);
  void RecordAckUpdate(size_t server_index, const com::vmware::concord::thin_replica::BlockId& block_id);
  void RecordSubscribeToUpdateHashes(size_t server_index,
                                     const com::vmware::concord::thin_replica::SubscriptionRequest& request);
  void RecordUnsubscribe(size_t server_index);

  const std::list<std::pair<size_t, com::vmware::concord::thin_replica::ReadStateRequest>>& GetReadStateCalls() const;
  const std::list<std::pair<size_t, com::vmware::concord::thin_replica::ReadStateHashRequest>>& GetReadStateHashCalls()
      const;
  const std::list<std::pair<size_t, com::vmware::concord::thin_replica::SubscriptionRequest>>&
  GetSubscribeToUpdatesCalls() const;
  const std::list<std::pair<size_t, com::vmware::concord::thin_replica::BlockId>>& GetAckUpdateCalls() const;
  const std::list<std::pair<size_t, com::vmware::concord::thin_replica::SubscriptionRequest>>&
  GetSubscribeToUpdateHashesCalls() const;
  const std::list<size_t>& GetUnsubscribeCalls() const;
  size_t GetTotalCallCount() const;
};

// Class for recording Thin Replica RPC calls to a given mocked Thin Replica
// Server, given the data stream preparer and hasher for that server and a
// ThinReplicaCommunicationRecord to record the calls to.
class MockThinReplicaServerRecorder {
 private:
  std::shared_ptr<MockDataStreamPreparer> data_preparer_;
  std::shared_ptr<MockOrderedDataStreamHasher> hasher_;
  std::shared_ptr<ThinReplicaCommunicationRecord> record_;
  size_t server_index_;

 public:
  MockThinReplicaServerRecorder(std::shared_ptr<MockDataStreamPreparer> data,
                                std::shared_ptr<MockOrderedDataStreamHasher> hasher,
                                std::shared_ptr<ThinReplicaCommunicationRecord> record,
                                size_t server_index);

  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* ReadStateRaw(
      grpc::ClientContext* context, const com::vmware::concord::thin_replica::ReadStateRequest& request);
  grpc::Status ReadStateHash(grpc::ClientContext* context,
                             const com::vmware::concord::thin_replica::ReadStateHashRequest& request,
                             com::vmware::concord::thin_replica::Hash* response);
  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* SubscribeToUpdatesRaw(
      grpc::ClientContext* context, const com::vmware::concord::thin_replica::SubscriptionRequest& request);
  grpc::Status AckUpdate(grpc::ClientContext* context,
                         const com::vmware::concord::thin_replica::BlockId& block_id,
                         google::protobuf::Empty* response);
  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Hash>* SubscribeToUpdateHashesRaw(
      grpc::ClientContext* context, const com::vmware::concord::thin_replica::SubscriptionRequest& request);
  grpc::Status Unsubscribe(grpc::ClientContext* context,
                           const google::protobuf::Empty& request,
                           google::protobuf::Empty* response);
};

// Class for managing mocked server behavior in test cases involving
// Byzantine-faulty servers.
//
// A ByzantineMockThinReplicaServerPreparer object itself is effectively a
// factory for mock thin replica servers (of the type
// ByzantineMockThinReplicaServerPreparer::ByzantineMockServer) for a mocked
// cluster including some servers with Byzantine-faulty behavior.
//
// At its construction, a ByzantineMockThinReplicaServerPreparer takes
// specifications of the faulty and non-faulty behavior to be used in a mock
// cluster. The non-faulty behavior is to be specified via shared_ptrs to
// MockDataStreamPreparer and MockOrderedDataStreamHasher objects, which are the
// same objects we use for describing server behavior in test cases involving
// homogenous, non-Byzantine-faulty mocked servers (see definitions of these
// classes above). For specifying behavior of Byzantine-faulty servers, a
// shared_ptr to an object of type
// ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior is accepted.
//
// ByzantineServerBehavior is a public, abstract member class of
// ByzantineMockThinReplicaServerPreparer defining an interface for managing
// Byzantine behavior (see ByzantineServerBehavior's declaration below). A
// ByzantineServerBehavior object decides, possibly dynamically, what server(s)
// will be Byzantine-faulty, when they will start showing Byzantine-faulty
// behavior, and what that faulty behavior will be. It is expected unit test
// cases with server(s) with Byzantine-faulty behavior will use some extension
// of ByzantineServerBehavior to describe that faulty behavior. Examples of such
// test cases, such as test_read_state_fabricated_data, can be found in
// thin-replica-client/test/trc_byzantine_test.cpp, and examples of such
// extensions of ByzantineServerBehavior, such as InitialStateFabricator, are
// declared later in this file.
class ByzantineMockThinReplicaServerPreparer {
 public:
  // Abstract class for describing Byzantine-Faulty server behavior to a
  // ByzantineMockThinReplicaServerPreparer; note a single
  // ByzantineServerBehavior object is intended to be shared for an entire mock
  // cluster with faulty and non-faulty servers.
  //
  // ByzantineServerBehavior has a virtual function for mocking each RPC call a
  // Thin Replica Server supports. ByzantineMockServers created by a
  // ByzantineMockThinReplicaServerPreparer will invoke the
  // ByzantineServerBehavior object they are using for each RPC call they mock
  // to determine the behavior used for that call; the ByzantineServerBehavior
  // should choose whether, and, if so, how, behavior for that call will be
  // faulty. ByzantineServerBehavior itself comes with an implementation for
  // each of these functions that always chooses non-faulty behavior; it is
  // expected ByzantineServerBehavior extensions will only override the behavior
  // for RPC calls whose behavior is desired to be Byzantine in the specific
  // test case(s) the extension is used for.
  //
  // The ByzantineServerBeahvior class also keeps a record of what servers are
  // Byzantine-faulty by server index. This alows extensions of this abstract
  // class to dynamically configure which servers are Byzantine-faulty at
  // runtime, which can allow test cases to make servers Byzantine faulty as
  // they see them called, freeing them of the need to predict or even know what
  // order the ThinReplicaClient implementation tries servers in.
  class ByzantineServerBehavior {
   private:
    std::unordered_set<size_t> byzantine_faulty_servers_;
    std::mutex faulty_server_record_mutex_;

   protected:
    ByzantineServerBehavior();

    // Protected functions for managing which servers are Byzantine-faulty. Note
    // multiple concurrent calls to one or more of IsByzantineFaulty,
    // GetNumFaultyServers, and MakeByzantineFaulty are thread safe.
    bool IsByzantineFaulty(size_t index);
    size_t GetNumFaultyServers();

    // Checks whether the number of servers that are already Byzantine faulty is
    // strictly less than max_faulty, and, if so, records that the server with
    // index index is Byzantine faulty if it was not already. This check and
    // write is done atomically with respect to this ByzantineServerBehavior's
    // record of which servers are Byzantine faulty. Note the max_faulty
    // parameter for this function can be any number and need not match the F
    // (i.e. maximum number of Byzantine-faulty nodes the cluster can tolerate)
    // value for the cluster being mocked.
    //
    // Returns whether the server with index index is listed as Byzantine faulty
    // after this call completes (this includes returning true when it was
    // already listed as faulty before this call).
    bool MakeByzantineFaulty(size_t index, size_t max_faulty);

   public:
    virtual ~ByzantineServerBehavior();

    // Virtual functions called by ByzantineMockServers to decide behavior for
    // RPC calls they are mocking. It is the responsibility of the
    // ByzantineServerBehavior object to decide whether, and, if so, how,
    // behavior will be faulty when these functions are called. Note the
    // ByzantineServerBehavior abstract class's own default implementations of
    // these functions always choose non-faulty behavior, so extensions need
    // only override those that may have faulty behavior in the specific test
    // case(s) they are used for.
    //
    // Note multiple calls to the same or different RPC behavior-mocking
    // functions may be made concurrently from the same or different
    // ByzantineMockServers, so ByzantineServerBehavior extensions are expected
    // to handle (as necessary) synchronization of any shared state that could
    // be written to by calls to these functions.
    //
    // In terms of signature and expected behavior (at least in the
    // non-Byzantine-faulty case) these funcitions are somewhat similar to the
    // com::vmware::concord::thin_replica::MockThinReplicaStub functions
    // ByzantineMockServer uses them to mock behavior for, but with the
    // following specific differences:
    //
    //  - The size_t parameter server_index is added to the start of the
    //  function's parameter list; the calling ByzantineMockServer will give the
    //  index of the server for which behavior is being mocked for this
    //  parameter. It is anticipated this parameter can be used in tracking
    //  which servers it has made Byzantine-faulty.
    //
    //  - A parameter with type matching the function's return type is appended
    //  to the end of each function's parameter list; the calling
    //  ByzantineMockServer will give the return value a non-faulty server would
    //  give for this parameter. It is anticipated this parameter can be used in
    //  both mocking non-faulty behavior and mocking faulty behavior that is
    //  implemented by making specific modifications to non-faulty behavior.
    //
    //  - In cases where the function has a raw object pointer return type and a
    //  pointer to a newly allocated object would be returned in the case the
    //  server handling the request is not faulty, the calling
    //  ByzantineMockServer will in fact allocate a new object matching the
    //  object that would be returned when using such a non-faulty server and
    //  pass it to the ByzantineServerBehavior for its last parameter; in this
    //  case the ByzantineServerBehavior assumes ownership of the non-faulty
    //  object the ByzantineMockServer allocated, and should either free this
    //  pointer or ensure ownership is transfered somewhere else (note returning
    //  the provided pointer or a pointer to an object that itself has been
    //  given ownership of the provided pointer can satisfy this transfer of
    //  ownership).
    //
    //  - If a function has a pointer in its signature that the
    //  com::vmware::concord::thin_replica::MockThinReplicaStub would write a
    //  response to (for example, response in ReadStateHash), and a non-faulty
    //  server would write back to that pointer for a particular call, then the
    //  calling ByzantineMockServer will write the non-faulty response to that
    //  pointer before calling its ByzantineServerBehavior object, which may
    //  then choose to either leave or overwrite the non-faulty value.
    virtual grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* ReadStateRaw(
        size_t server_index,
        grpc::ClientContext* context,
        const com::vmware::concord::thin_replica::ReadStateRequest& request,
        grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* correct_data);
    virtual grpc::Status ReadStateHash(size_t server_index,
                                       grpc::ClientContext* context,
                                       const com::vmware::concord::thin_replica::ReadStateHashRequest& request,
                                       com::vmware::concord::thin_replica::Hash* response,
                                       grpc::Status correct_status);
    virtual grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* SubscribeToUpdatesRaw(
        size_t server_index,
        grpc::ClientContext* context,
        const com::vmware::concord::thin_replica::SubscriptionRequest& request,
        grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* correct_data);
    virtual grpc::Status AckUpdate(size_t server_index,
                                   grpc::ClientContext* context,
                                   const com::vmware::concord::thin_replica::BlockId& block_id,
                                   google::protobuf::Empty* response,
                                   grpc::Status correct_status);
    virtual grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Hash>* SubscribeToUpdateHashesRaw(
        size_t server_index,
        grpc::ClientContext* context,
        const com::vmware::concord::thin_replica::SubscriptionRequest& request,
        grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Hash>* correct_hashes);
    virtual grpc::Status Unsubscribe(size_t server_index,
                                     grpc::ClientContext* context,
                                     const google::protobuf::Empty& request,
                                     google::protobuf::Empty* response,
                                     grpc::Status correct_status);
  };

  // Mock server type for servers in a mock cluster managed by a
  // ByzantineMockThinReplicaServerPreparer; a
  // ByzantineMockThinReplicaServerPreparer effectively serves as a factory for
  // ByzantineMockServerObjects.
  class ByzantineMockServer : public com::vmware::concord::thin_replica::MockThinReplicaStub {
   private:
    std::shared_ptr<MockDataStreamPreparer> non_faulty_data_;
    std::shared_ptr<MockOrderedDataStreamHasher> non_faulty_hasher_;
    std::shared_ptr<ByzantineServerBehavior> byzantine_behavior_;
    size_t index_;

   public:
    ByzantineMockServer(std::shared_ptr<MockDataStreamPreparer> non_faulty_data,
                        std::shared_ptr<MockOrderedDataStreamHasher> non_faulty_hasher,
                        std::shared_ptr<ByzantineServerBehavior> byzantine_behavior,
                        size_t index);

    grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* ReadStateRaw(
        grpc::ClientContext* context, const com::vmware::concord::thin_replica::ReadStateRequest& request);
    grpc::Status ReadStateHash(grpc::ClientContext* context,
                               const com::vmware::concord::thin_replica::ReadStateHashRequest& request,
                               com::vmware::concord::thin_replica::Hash* response);
    grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* SubscribeToUpdatesRaw(
        grpc::ClientContext* context, const com::vmware::concord::thin_replica::SubscriptionRequest& request);
    grpc::Status AckUpdate(grpc::ClientContext* context,
                           const com::vmware::concord::thin_replica::BlockId& block_id,
                           google::protobuf::Empty* response);
    grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Hash>* SubscribeToUpdateHashesRaw(
        grpc::ClientContext* context, const com::vmware::concord::thin_replica::SubscriptionRequest& request);
    grpc::Status Unsubscribe(grpc::ClientContext* context,
                             const google::protobuf::Empty& request,
                             google::protobuf::Empty* response);
  };

 private:
  std::shared_ptr<MockDataStreamPreparer> non_faulty_data_;
  std::shared_ptr<MockOrderedDataStreamHasher> non_faulty_hasher_;
  std::shared_ptr<ByzantineServerBehavior> byzantine_behavior_;

 public:
  ByzantineMockThinReplicaServerPreparer(std::shared_ptr<MockDataStreamPreparer> non_faulty_data,
                                         std::shared_ptr<MockOrderedDataStreamHasher> non_faulty_hasher,
                                         std::shared_ptr<ByzantineServerBehavior> byzantine_behavior);

  // Factory method for creating mock server objects; note
  // ByzantineMockThinReplicaServerPreparer itself does not track which servers
  // it has already created mock servers for, so it is possible to have multiple
  // mock servers sharing an index, either intentionally or unintentionally.
  ByzantineMockServer* CreateByzantineMockServer(size_t index);
};

std::vector<std::unique_ptr<MockThinReplicaServerRecorder>> CreateMockServerRecorders(
    size_t num_servers,
    std::shared_ptr<MockDataStreamPreparer> data,
    std::shared_ptr<MockOrderedDataStreamHasher> hasher,
    std::shared_ptr<ThinReplicaCommunicationRecord> record);
std::vector<std::unique_ptr<ByzantineMockThinReplicaServerPreparer::ByzantineMockServer>> CreateByzantineMockServers(
    size_t num_servers, ByzantineMockThinReplicaServerPreparer& server_preparer);

void SetMockServerBehavior(MockTrsConnection* server,
                           const std::shared_ptr<MockDataStreamPreparer>& data_preparer,
                           const MockOrderedDataStreamHasher& hasher);

void SetMockServerUnresponsive(client::thin_replica_client::TrsConnection* server);

std::vector<std::unique_ptr<client::thin_replica_client::TrsConnection>> CreateTrsConnections(
    size_t num_servers, size_t num_unresponsive = 0);
std::vector<std::unique_ptr<client::thin_replica_client::TrsConnection>> CreateTrsConnections(
    size_t num_servers,
    std::shared_ptr<MockDataStreamPreparer> stream_preparer,
    MockOrderedDataStreamHasher& hasher,
    size_t num_unresponsive = 0);

// Templated versions of SetMockServerBehavior and CreateTrsConnections. To use
// these specific template functions, the template parameter
// MockThinReplicaServer needs to be a type that implements all the RPC calls a
// normal Thin Replica Server would support.
//
// Note we template these functions rather than defining and having them use an
// interface type because SetMockServerBehavior needs to refer to a specific
// class's implementation of the RPC call functions when using gmock's
// testing::Invoke to configure the mock connection behavior.
template <class MockThinReplicaServer>
void SetMockServerBehavior(MockTrsConnection* server, MockThinReplicaServer& mock_server) {
  ON_CALL(*(server->GetStub()), ReadStateRaw)
      .WillByDefault(testing::Invoke(&mock_server, &MockThinReplicaServer::ReadStateRaw));
  ON_CALL(*(server->GetStub()), ReadStateHash)
      .WillByDefault(testing::Invoke(&mock_server, &MockThinReplicaServer::ReadStateHash));
  ON_CALL(*(server->GetStub()), SubscribeToUpdatesRaw)
      .WillByDefault(testing::Invoke(&mock_server, &MockThinReplicaServer::SubscribeToUpdatesRaw));
  ON_CALL(*(server->GetStub()), AckUpdate)
      .WillByDefault(testing::Invoke(&mock_server, &MockThinReplicaServer::AckUpdate));
  ON_CALL(*(server->GetStub()), SubscribeToUpdateHashesRaw)
      .WillByDefault(testing::Invoke(&mock_server, &MockThinReplicaServer::SubscribeToUpdateHashesRaw));
  ON_CALL(*(server->GetStub()), Unsubscribe)
      .WillByDefault(testing::Invoke(&mock_server, &MockThinReplicaServer::Unsubscribe));
}

template <class MockThinReplicaServer>
std::vector<std::unique_ptr<client::thin_replica_client::TrsConnection>> CreateTrsConnections(
    std::vector<std::unique_ptr<MockThinReplicaServer>>& mock_servers) {
  std::vector<std::unique_ptr<client::thin_replica_client::TrsConnection>> mock_connections;
  for (size_t i = 0; i < mock_servers.size(); ++i) {
    auto conn = new MockTrsConnection();
    SetMockServerBehavior(conn, *(mock_servers[i]));
    auto server = dynamic_cast<client::thin_replica_client::TrsConnection*>(conn);
    mock_connections.push_back(std::unique_ptr<client::thin_replica_client::TrsConnection>(server));
  }
  return mock_connections;
}

std::vector<std::unique_ptr<client::thin_replica_client::TrsConnection>> CreateTrsConnections(
    std::vector<MockThinReplicaServerRecorder>& mock_server_recorders);
std::vector<std::unique_ptr<client::thin_replica_client::TrsConnection>> CreateTrsConnections(
    std::vector<std::unique_ptr<ByzantineMockThinReplicaServerPreparer::ByzantineMockServer>>& mock_servers);

template <class DataType>
grpc::ClientReaderInterface<DataType>* CreateUnresponsiveMockStream() {
  auto stream = new MockThinReplicaStream<DataType>();
  ON_CALL(*stream, Finish)
      .WillByDefault(testing::Return(grpc::Status(grpc::StatusCode::UNAVAILABLE, "This server is non-responsive")));
  ON_CALL(*stream, Read).WillByDefault(testing::Return(false));
  return stream;
}

// ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior extensions
// for providing specific behavior in Byzantine testing.

// Simulates Byzantine server behavior which can be modeled by using an
// alternative MockDataStreamPreparer for handling faulty ReadState calls.
class InitialStateFabricator : public ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior {
 private:
  std::shared_ptr<MockDataStreamPreparer> fabricated_data_preparer_;
  size_t num_faulty_servers_;

 public:
  InitialStateFabricator(std::shared_ptr<MockDataStreamPreparer> fabricated_data_preparer,
                         size_t num_faulty_servers = 1);
  ~InitialStateFabricator() override;
  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* ReadStateRaw(
      size_t server_index,
      grpc::ClientContext* context,
      const com::vmware::concord::thin_replica::ReadStateRequest& request,
      grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* correct_data) override;
};

// Simulates Byzantine server behavior which can be modeled by using an
// alternative MockDataStreamPreparer for handling faulty SubscribeToUpdates
// calls.
class UpdateDataFabricator : public ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior {
 private:
  std::shared_ptr<MockDataStreamPreparer> fabricated_data_preparer_;
  size_t num_faulty_servers_;

 public:
  UpdateDataFabricator(std::shared_ptr<MockDataStreamPreparer> fabricated_data_preparer, size_t num_faulty_servers = 1);
  ~UpdateDataFabricator() override;
  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* SubscribeToUpdatesRaw(
      size_t server_index,
      grpc::ClientContext* context,
      const com::vmware::concord::thin_replica::SubscriptionRequest& request,
      grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* correct_data) override;
};

// Simulates Byzantine server behavior which can be modeled by hasing updates
// from an alternative MockDataStreamPreparer for faulty SubscribeToUpdateHashes
// calls.
class UpdateHashFabricator : public ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior {
 private:
  std::shared_ptr<MockOrderedDataStreamHasher> fabricated_update_hasher_;
  size_t num_faulty_servers_;

 public:
  UpdateHashFabricator(std::shared_ptr<MockDataStreamPreparer> fabricated_data_preparer, size_t num_faulty_servers = 1);
  ~UpdateHashFabricator() override;
  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Hash>* SubscribeToUpdateHashesRaw(
      size_t server_index,
      grpc::ClientContext* context,
      const com::vmware::concord::thin_replica::SubscriptionRequest& request,
      grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Hash>* correct_data) override;
};

// Simulates a slow server that slows down at some point when streaming data in
// response to ReadState.
class StateStreamDelayer : public ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior {
 private:
  class DelayedStreamState : public MockThinReplicaStream<com::vmware::concord::thin_replica::Data>::State {
   private:
    std::unique_ptr<grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>> undelayed_data_;
    size_t updates_until_delay_;

   public:
    DelayedStreamState(grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* data, size_t delay_after);
    ~DelayedStreamState() override;
    grpc::Status Finish();
    bool Read(com::vmware::concord::thin_replica::Data* data);
  };

  size_t delay_after_;

 public:
  StateStreamDelayer(size_t delay_after);
  ~StateStreamDelayer() override;
  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* ReadStateRaw(
      size_t server_index,
      grpc::ClientContext* context,
      const com::vmware::concord::thin_replica::ReadStateRequest& request,
      grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* correct_data) override;
};

// Simulates a slow server that slows down at some point when streaming data in
// response to SubscribeToUpdates.
class DataStreamDelayer : public ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior {
 private:
  class DelayedStreamState : public MockThinReplicaStream<com::vmware::concord::thin_replica::Data>::State {
   private:
    std::unique_ptr<grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>> undelayed_data_;
    size_t updates_until_delay_;

   public:
    DelayedStreamState(grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* data, size_t delay_after);
    ~DelayedStreamState() override;
    grpc::Status Finish();
    bool Read(com::vmware::concord::thin_replica::Data* data);
  };

  size_t delay_after_;

 public:
  DataStreamDelayer(size_t delay_after);
  ~DataStreamDelayer() override;
  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* SubscribeToUpdatesRaw(
      size_t server_index,
      grpc::ClientContext* context,
      const com::vmware::concord::thin_replica::SubscriptionRequest& request,
      grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* correct_data) override;
};

// Simulates a slow server that slows down at some point when streaming hashes
// in response to SubscribeToUpdateHashes.
class HashStreamDelayer : public ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior {
 private:
  class DelayedStreamState : public MockThinReplicaStream<com::vmware::concord::thin_replica::Hash>::State {
   private:
    std::unique_ptr<grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Hash>> undelayed_hashes_;
    size_t hashes_until_delay_;

   public:
    DelayedStreamState(grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Hash>* hashes,
                       size_t delay_after);
    ~DelayedStreamState() override;
    grpc::Status Finish();
    bool Read(com::vmware::concord::thin_replica::Hash* hash);
  };

  size_t delay_after_;

 public:
  HashStreamDelayer(size_t delay_after);
  ~HashStreamDelayer() override;
  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Hash>* SubscribeToUpdateHashesRaw(
      size_t server_index,
      grpc::ClientContext* context,
      const com::vmware::concord::thin_replica::SubscriptionRequest& request,
      grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Hash>* correct_hashes) override;
};

// Simulates Byzantine server behavior which can be described by a function
// (passed to the UpdateHashCorrupter via a functor object) to be applied to a
// Hash value streamed after a particular number of responses streamed in
// response to SubscribeToUpdateHashes.
class UpdateHashCorrupter : public ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior {
 public:
  // Functor interface to be implemented in order to describe how an
  // UpdateHashCorrupter object should corrupt its target hash update.
  class CorruptHash {
   public:
    virtual ~CorruptHash() {}
    virtual void operator()(com::vmware::concord::thin_replica::Hash* hash) = 0;
  };

 private:
  class CorruptedStreamState : public MockThinReplicaStream<com::vmware::concord::thin_replica::Hash>::State {
   private:
    std::unique_ptr<grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Hash>> uncorrupted_hashes_;
    std::shared_ptr<CorruptHash> corrupt_hash_;
    bool corruption_applied_;
    size_t hashes_until_corruption_;

   public:
    CorruptedStreamState(grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Hash>* hashes,
                         std::shared_ptr<CorruptHash> corrupt_hash,
                         size_t corrupt_after);
    ~CorruptedStreamState() override;
    grpc::Status Finish();
    bool Read(com::vmware::concord::thin_replica::Hash* hash);
  };

  std::shared_ptr<CorruptHash> corrupt_hash_;
  size_t corrupt_after_;

 public:
  // Construct an UpdateHashCorrupter that will apply the corruption described
  // by corrupt_hash to the corrupt_after-th Hash object (starting from 0)
  // returned by hash streams this UpdateHashCorrupter gives in response to
  // SubscribeToUpdateHashes calls to Byzantine-faulty mocked Thin Replica
  // Servers.
  UpdateHashCorrupter(std::shared_ptr<CorruptHash> corrupt_hash, size_t corrupt_after);

  ~UpdateHashCorrupter() override;
  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Hash>* SubscribeToUpdateHashesRaw(
      size_t server_index,
      grpc::ClientContext* context,
      const com::vmware::concord::thin_replica::SubscriptionRequest& request,
      grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Hash>* correct_hashes) override;
};

// ByzantineServerBehavior implementation for simulating scenarios where not all
// servers are responsive all the time, especially scenarios involving changes
// in responsiveness over time. Server unresponsiveness is simulated by having
// unresponsive servers wait for a period exceeding the timeout used in testing
// before responding to calls. Note unresponsiveness ClusterResponsivnessLimiter
// can simulate includes ClientReaderInterface<T>-type streams provided by a
// mocked Thin Replica Server not responding to calls to Read in addition to the
// mocked Thin Replica Server not responding to new RPC calls made to it.
//
// To use this class, it is expected test cases will implement the abstract
// ClusterResponsivenessLimiter::ResponsivenessManager class to describe which
// server(s) will be unresponsive when.
class ClusterResponsivenessLimiter : public ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior {
 public:
  // Getter functions for information a ClusterResponsivenessLimiter is
  // constructed with that may be relevant to ResponsivenessManager
  // implementations.
  size_t GetMaxFaulty() const;
  size_t GetClusterSize() const;

  // Functions for checking and configuring mock server responsiveness for use
  // by ResponsivenessManager implementations. Note these functions are not
  // themselves thread safe; it is expected synchronization of calls to these
  // functions will addressed by the fact that a ClusterResponsivenessLimiter
  // synchronizes all calls to its ResponsivenessManager for
  // UpdateResponsiveness.
  bool IsUnresponsive(size_t server_index) const;
  size_t GetNumUnresponsiveServers() const;
  void SetResponsive(size_t server_index);
  void SetUnresponsive(size_t server_index);

  // Abstract class that code constructing a ClusterResponsivenessLimiter should
  // implement in order to describe the responsiveness behavior for the cluster
  // under the ClusterResponsivenessLimiter's management.
  class ResponsivenessManager {
   public:
    virtual ~ResponsivenessManager();

    // Function called by the ClusterResponsivenessLimiter that
    // ResponsivenessManager implementations should implement to describe
    // responsiveness. A ClusterResponsivenessLimiter will call
    // UpdateResponsiveness upon each call to ReadState, ReadStateHash,
    // SubscribeToUpdates, and SubscribeToUpdateHashes, as well as on each call
    // to Read that will return true on stream(s) returned by calls to
    // ReadState, ReadStateHash, SubscribeToUpdates, and
    // SubscribeToUpdatesHashes. Note UpdateResponsiveness will not be called
    // for Read calls that will return false or for Finish calls (this decision
    // was made primarily because there is not a meaningful Block ID to
    // associate with such calls). When a ClusterResponsivenessLimiter calls
    // UpdateResponsiveness, it passes a reference to itself for
    // responsiveness_limiter, the index of the server that is being called for
    // server_called, and either 0 in the event this call is being triggered by
    // a ReadStateRaw call, the Block ID requested in the relevant client
    // request for block_id in the event this call to UpdateResponsiveness is
    // being triggered by an initial RPC call other than ReadStateRaw, or the
    // Block ID that will be returned in response to a streamed update or update
    // hash in the event this call to UpdateResponsiveness is being triggered by
    // a Read call on an open stream that is going to return true.
    //
    // Note all calls to UpdateResponsiveness made by a single
    // ClusterResponsivenessLimiter instance will be synchronized such that no
    // two such calls will run concurrently even if the ThinReplicaClient
    // implementation is using multiple threads to make multiple calls to mocked
    // server(s) concurrently.
    // triggered them.
    //
    // ResponsivenessManager implementations should use
    // ClusterResponsivenessLimiter's functions IsUnresponsive,
    // GetNumUnresponsiveServers, SetResponsive, and SetUnresponsive to manage
    // cluster responsiveness. Note changes made to responsiveness will apply
    // immediately, that is, if the ResponsivenessManager changes the
    // responsiveness of server_called, its new responsiveness will apply in
    // resolution of whatever call triggered ClusterResponsivenessLimiter to
    // call UpdateResponsiveness.
    virtual void UpdateResponsiveness(ClusterResponsivenessLimiter& responsivness_limiter,
                                      size_t server_called,
                                      uint64_t block_id) = 0;
  };

 private:
  class DataStreamState : public MockThinReplicaStream<com::vmware::concord::thin_replica::Data>::State {
   private:
    std::unique_ptr<grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>> data_;
    ClusterResponsivenessLimiter& responsiveness_limiter_;
    size_t server_index_;

   public:
    DataStreamState(grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* data,
                    ClusterResponsivenessLimiter& responsiveness_limiter,
                    size_t server_index);
    ~DataStreamState() override;
    grpc::Status Finish();
    bool Read(com::vmware::concord::thin_replica::Data* data);
  };
  class HashStreamState : public MockThinReplicaStream<com::vmware::concord::thin_replica::Hash>::State {
   private:
    std::unique_ptr<grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Hash>> hashes_;
    ClusterResponsivenessLimiter& responsiveness_limiter_;
    size_t server_index_;

   public:
    HashStreamState(grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Hash>* hashes,
                    ClusterResponsivenessLimiter& responsiveness_limiter,
                    size_t server_index);
    ~HashStreamState() override;
    grpc::Status Finish();
    bool Read(com::vmware::concord::thin_replica::Hash* hash);
  };

  size_t max_faulty_;
  size_t cluster_size_;
  std::unique_ptr<ResponsivenessManager> responsiveness_manager_;
  std::vector<bool> server_responsiveness_;
  std::mutex responsiveness_change_mutex_;

 public:
  ClusterResponsivenessLimiter(size_t max_faulty,
                               size_t num_servers,
                               std::unique_ptr<ResponsivenessManager>&& responsiveness_manager);
  ~ClusterResponsivenessLimiter() override;

  // Note that, for mocked RPC calls returning streams (i.e. with return types
  // of ClientReaderInterface<T>*), since the stream implementations returned by
  // a ClusterResponsivenessLimiter will use that ClusterResponsivenessLimiter
  // to track their own responsiveness, behavior is undefined if any call to
  // such a stream is made after the end of the life of the
  // ClusterResponsivenessLimiter that created it.
  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* ReadStateRaw(
      size_t server_index,
      grpc::ClientContext* context,
      const com::vmware::concord::thin_replica::ReadStateRequest& request,
      grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* correct_data) override;
  grpc::Status ReadStateHash(size_t server_index,
                             grpc::ClientContext* context,
                             const com::vmware::concord::thin_replica::ReadStateHashRequest& request,
                             com::vmware::concord::thin_replica::Hash* response,
                             grpc::Status correct_status) override;
  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* SubscribeToUpdatesRaw(
      size_t server_index,
      grpc::ClientContext* context,
      const com::vmware::concord::thin_replica::SubscriptionRequest& request,
      grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* correct_data) override;
  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Hash>* SubscribeToUpdateHashesRaw(
      size_t server_index,
      grpc::ClientContext* context,
      const com::vmware::concord::thin_replica::SubscriptionRequest& request,
      grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Hash>* correct_hashes) override;
};

// Simulates Byzantine server behavior which can be modeled by using an
// alternative MockDataStreamPreparer and an alternative
// MockOrderedDataStreamHasher constructed from that preparer for handling all
// calls to a faulty server.
class ComprehensiveDataFabricator : public ByzantineMockThinReplicaServerPreparer::ByzantineServerBehavior {
 private:
  std::shared_ptr<MockDataStreamPreparer> fabricated_data_preparer_;
  std::shared_ptr<MockOrderedDataStreamHasher> fabricated_update_hasher_;
  size_t num_faulty_servers_;

 public:
  ComprehensiveDataFabricator(std::shared_ptr<MockDataStreamPreparer> fabricated_data_preparer,
                              size_t num_faulty_servers = 1);
  ~ComprehensiveDataFabricator() override;

  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* ReadStateRaw(
      size_t server_index,
      grpc::ClientContext* context,
      const com::vmware::concord::thin_replica::ReadStateRequest& request,
      grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* correct_data) override;
  grpc::Status ReadStateHash(size_t server_index,
                             grpc::ClientContext* context,
                             const com::vmware::concord::thin_replica::ReadStateHashRequest& request,
                             com::vmware::concord::thin_replica::Hash* response,
                             grpc::Status correct_status) override;
  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* SubscribeToUpdatesRaw(
      size_t server_index,
      grpc::ClientContext* context,
      const com::vmware::concord::thin_replica::SubscriptionRequest& request,
      grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>* correct_data) override;
  grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Hash>* SubscribeToUpdateHashesRaw(
      size_t server_index,
      grpc::ClientContext* context,
      const com::vmware::concord::thin_replica::SubscriptionRequest& request,
      grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Hash>* correct_hashes) override;
};