// Concord
//
// Copyright (c) 2021-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <random>
#include "client/thin-replica-client/replica_stream_snapshot_client.hpp"
#include "client/thin-replica-client/grpc_connection.hpp"
#include "client/concordclient/remote_update_queue.hpp"
#include "client/concordclient/concord_client_exceptions.hpp"
#include "thin_replica_mock.grpc.pb.h"
#include "replica_state_snapshot_mock.grpc.pb.h"

#include "gtest/gtest.h"

using vmware::concord::replicastatesnapshot::StreamSnapshotRequest;
using vmware::concord::replicastatesnapshot::StreamSnapshotResponse;
using std::make_shared;
using std::make_unique;
using std::atomic_bool;
using std::shared_ptr;
using std::string;
using std::to_string;
using std::unique_ptr;
using std::vector;
using std::chrono::milliseconds;
using concord::util::ThreadPool;
using concord::client::concordclient::SnapshotKVPair;
using concord::client::concordclient::StreamUpdateQueue;
using client::concordclient::GrpcConnection;
using client::concordclient::GrpcConnectionConfig;
using com::vmware::concord::thin_replica::MockThinReplicaStub;
using vmware::concord::replicastatesnapshot::ReplicaStateSnapshotService;
using vmware::concord::replicastatesnapshot::MockReplicaStateSnapshotServiceStub;
using client::replica_state_snapshot_client::ReplicaStreamSnapshotClient;
using client::replica_state_snapshot_client::ReplicaStateSnapshotClientConfig;

const string kTestingClientID = "mock_client_id";

static std::string getRandomStringOfLength(size_t len) {
  std::vector<char> alphabet{'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
                             'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
                             'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
                             'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', ' '};

  std::mt19937_64 eng{std::random_device{}()};
  std::uniform_int_distribution<> dist{0, static_cast<int>(alphabet.size() - 1)};
  std::string res;
  while (len > 0) {
    res += alphabet[dist(eng)];
    len--;
  }
  return res;
}

namespace vmware {
namespace concord {
namespace replicastatesnapshot {

class FakeReplicaStateSnapshotService : public ReplicaStateSnapshotService::Service {
 public:
  ::grpc::Status StreamSnapshot(
      ::grpc::ServerContext* context,
      const ::vmware::concord::replicastatesnapshot::StreamSnapshotRequest* request,
      ::grpc::ServerWriter<::vmware::concord::replicastatesnapshot::StreamSnapshotResponse>* writer) override {
    auto num_req_to_send = request->snapshot_id();

    while (num_req_to_send > 0) {
      auto resp = StreamSnapshotResponse{};
      auto kv = resp.mutable_key_value();
      auto resp_key = kv->mutable_key();
      auto resp_value = kv->mutable_value();
      *resp_key = getRandomStringOfLength(10);
      *resp_value = getRandomStringOfLength(50);
      writer->Write(resp);
      num_req_to_send--;
    }
    return ::grpc::Status::OK;
  }
};

}  // namespace replicastatesnapshot
}  // namespace concord
}  // namespace vmware

namespace {

class FakeGrpcConnection : public GrpcConnection {
 public:
  FakeGrpcConnection(bool full_fake,
                     const std::string& address,
                     const std::string& client_id,
                     uint16_t data_operation_timeout_seconds,
                     uint16_t hash_operation_timeout_seconds,
                     uint16_t snapshot_operation_timeout_seconds)
      : GrpcConnection(address,
                       client_id,
                       data_operation_timeout_seconds,
                       hash_operation_timeout_seconds,
                       snapshot_operation_timeout_seconds),
        full_fake_(full_fake) {}

  virtual ~FakeGrpcConnection() {}

  virtual void connect(std::unique_ptr<GrpcConnectionConfig>& config) override {
    if (full_fake_) {
      createFakeStub();
    } else {
      if (!channel_) {
        createFakeChannel();
        createFakeStub();
      } else if (!trc_stub_) {
        createFakeStub();
      }
      // Initiate connection
      channel_->GetState(true);
    }
  }

  virtual bool isConnected() override { return true; }

 protected:
  bool full_fake_;
  void createFakeStub() {
    if (full_fake_) {
      this->trc_stub_.reset(new MockThinReplicaStub());
      this->rss_stub_.reset(new MockReplicaStateSnapshotServiceStub());
    } else {
      this->trc_stub_.reset(new MockThinReplicaStub());
      rss_stub_ = ReplicaStateSnapshotService::NewStub(channel_);
    }
  }
  void createFakeChannel() {
    int max_channel_msg_size = 1024 * 1024;
    grpc::ChannelArguments ch_args;
    ch_args.SetMaxSendMessageSize(max_channel_msg_size);
    ch_args.SetMaxReceiveMessageSize(max_channel_msg_size);
    channel_ = ::grpc::CreateCustomChannel(address_, ::grpc::InsecureChannelCredentials(), ch_args);
  }
};

void getGrpcConnections(bool full_fake, vector<shared_ptr<GrpcConnection>>& grpc_connections, int num_replicas) {
  int port = 50001;
  for (int i = 0; i < num_replicas; i++) {
    auto addr = "127.0.0.1:" + std::to_string(port + i);
    std::shared_ptr<GrpcConnection> grpc_conn =
        std::make_shared<FakeGrpcConnection>(full_fake, addr, kTestingClientID, 3, 3, 5);
    auto trsc_config = std::make_unique<GrpcConnectionConfig>(false, "Dummy01", "Dummy02", "Dummy03");
    grpc_conn->connect(trsc_config);
    grpc_connections.push_back(std::move(grpc_conn));
  }
}

TEST(replica_stream_snapshot_client_test, test_destructor_always_successful) {
  vector<shared_ptr<GrpcConnection>> grpc_connections;
  getGrpcConnections(true, grpc_connections, 7);
  ThreadPool thread_pool{10};
  auto read_snapshot = [&grpc_connections]() {
    auto rss_config = std::make_unique<ReplicaStateSnapshotClientConfig>(grpc_connections, 8);
    auto rss = std::make_unique<ReplicaStreamSnapshotClient>(std::move(rss_config));

    auto remote_queue = std::make_shared<StreamUpdateQueue>();
    ASSERT_EQ(remote_queue->size(), 0);
    ::client::replica_state_snapshot_client::SnapshotRequest rss_request;
    rss_request.snapshot_id = 1;
    rss_request.last_received_key = "";
    rss->readSnapshotStream(rss_request, remote_queue);
    ASSERT_EQ(remote_queue->size(), 0);
    EXPECT_THROW(remote_queue->pop(), concord::client::concordclient::InternalError);
  };
  vector<std::future<void>> results;
  results.reserve(100);
  for (int i = 0; i < 100; ++i) {
    results.push_back(thread_pool.async(read_snapshot));
  }
  for (const auto& r : results) {
    r.wait();
  }
}

TEST(replica_stream_snapshot_client_test, test_real_action) {
  vector<unique_ptr<::grpc::Server>> servers(7);
  std::atomic_uint64_t server_hello_count{0};
  auto run_server = [&servers, &server_hello_count](size_t i) {
    string server_address = "0.0.0.0:" + std::to_string(50001 + i);
    vmware::concord::replicastatesnapshot::FakeReplicaStateSnapshotService service;
    ::grpc::ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    builder.SetMaxReceiveMessageSize(10 * 1024 * 1024);
    builder.SetMaxSendMessageSize(10 * 1024 * 1024);
    // Finally assemble the server.
    servers[i] = builder.BuildAndStart();
    ++server_hello_count;
    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    (servers[i])->Wait();
  };
  ThreadPool server_thread_pool{7};
  vector<std::future<void>> server_results;
  for (size_t i = 0; i < 7; ++i) {
    server_results.push_back(server_thread_pool.async(run_server, i));
  }

  while (true) {
    if (server_hello_count.load() == 7) break;
  }

  sleep(1);

  vector<shared_ptr<GrpcConnection>> grpc_connections;
  getGrpcConnections(false, grpc_connections, 7);
  ThreadPool thread_pool{10};
  auto read_snapshot = [&grpc_connections](size_t len) {
    auto rss_config = std::make_unique<ReplicaStateSnapshotClientConfig>(grpc_connections, 8);
    auto rss = std::make_unique<ReplicaStreamSnapshotClient>(std::move(rss_config));

    auto remote_queue = std::make_shared<StreamUpdateQueue>();
    ASSERT_EQ(remote_queue->size(), 0);
    ::client::replica_state_snapshot_client::SnapshotRequest rss_request;
    rss_request.snapshot_id = len;
    rss_request.last_received_key = "";
    rss->readSnapshotStream(rss_request, remote_queue);
    ThreadPool read_thread_pool{1};
    read_thread_pool.async(
        [&remote_queue](size_t l) {
          size_t num_received = 0;
          while (true) {
            std::unique_ptr<SnapshotKVPair> update;
            try {
              update = remote_queue->pop();
            } catch (...) {
              break;
            }
            if (update) {
              num_received++;
            }
          }
          ASSERT_EQ(num_received, l);
        },
        len);
  };
  vector<std::future<void>> results;
  std::mt19937_64 eng{std::random_device{}()};
  std::uniform_int_distribution<size_t> dist{0, static_cast<size_t>(60)};
  results.reserve(20);
  for (int i = 0; i < 20; ++i) {
    results.push_back(thread_pool.async(read_snapshot, dist(eng)));
  }
  for (const auto& r : results) {
    r.wait();
  }

  for (const auto& s : servers) {
    s->Shutdown();
  }

  for (const auto& r : server_results) {
    r.wait();
  }
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}