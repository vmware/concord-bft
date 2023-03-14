// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "categorization/db_categories.h"
#include "kvbc_adapter/replica_adapter.hpp"
#include "kvbc_key_types.hpp"
#include "storage/test/storage_test_common.h"
#include "thin-replica-server/replica_state_snapshot_service_impl.hpp"

#include <grpcpp/channel.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace {

using namespace ::testing;
using namespace concord::kvbc;
using namespace concord::kvbc::categorization;
using namespace concord::thin_replica;
using bftEngine::impl::DbCheckpointManager;
using concord::kvbc::adapter::ReplicaBlockchain;
using concord::storage::rocksdb::NativeClient;
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::CreateChannel;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::StatusCode;
using vmware::concord::replicastatesnapshot::ReplicaStateSnapshotService;
using vmware::concord::replicastatesnapshot::StreamSnapshotRequest;
using vmware::concord::replicastatesnapshot::StreamSnapshotResponse;

class replica_state_snapshot_service_test : public Test {
  void SetUp() override {
    destroyDb();
    service_.setStateValueConverter([](std::string &&v) -> std::string { return std::move(v); });
    db_ = TestRocksDb::createNative();
    const auto link_st_chain = false;
    kvbc_ = std::make_unique<ReplicaBlockchain>(
        db_,
        link_st_chain,
        std::map<std::string, CATEGORY_TYPE>{{kExecutionProvableCategory, CATEGORY_TYPE::block_merkle},
                                             {kConcordInternalCategoryId, CATEGORY_TYPE::versioned_kv}});
  }

  void TearDown() override {
    destroyDb();
    shutdownServer();
  }

 protected:
  void destroyDb() {
    db_.reset();
    ASSERT_EQ(0, db_.use_count());
    cleanup();
  }

  void startServer() {
    builder_.AddListeningPort(grpc_uri_, grpc::InsecureServerCredentials());
    builder_.RegisterService(&service_);
    server_ = builder_.BuildAndStart();
  }

  void shutdownServer() {
    server_->Shutdown();
    server_->Wait();
  }

  void addPublicState() {
    auto updates = Updates{};
    auto merkle = BlockMerkleUpdates{};
    merkle.addUpdate("a", "va");
    merkle.addUpdate("b", "vb");
    merkle.addUpdate("c", "vc");
    merkle.addUpdate("d", "vd");
    auto versioned = VersionedUpdates{};
    const auto public_state = PublicStateKeys{std::vector<std::string>{"a", "b", "c", "d"}};
    const auto ser_public_state = detail::serialize(public_state);
    versioned.addUpdate(std::string{keyTypes::state_public_key_set},
                        std::string{ser_public_state.cbegin(), ser_public_state.cend()});
    updates.add(kExecutionProvableCategory, std::move(merkle));
    updates.add(kConcordInternalCategoryId, std::move(versioned));
    ASSERT_EQ(kvbc_->add(std::move(updates)), 1);
  }

 protected:
  const std::string grpc_uri_{"127.0.0.1:50051"};
  std::shared_ptr<NativeClient> db_;
  ServerBuilder builder_;
  ReplicaStateSnapshotServiceImpl service_;
  std::unique_ptr<Server> server_;
  std::shared_ptr<Channel> channel_ = grpc::CreateChannel(grpc_uri_, grpc::InsecureChannelCredentials());
  std::unique_ptr<ReplicaStateSnapshotService::Stub> stub_ = ReplicaStateSnapshotService::NewStub(channel_);
  std::unique_ptr<ReplicaBlockchain> kvbc_;
};

TEST_F(replica_state_snapshot_service_test, non_existent_snapshot_id) {
  startServer();
  auto context = ClientContext{};
  auto request = StreamSnapshotRequest{};
  request.set_snapshot_id(42);
  auto response = StreamSnapshotResponse{};
  auto reader = std::unique_ptr<ClientReader<StreamSnapshotResponse>>{stub_->StreamSnapshot(&context, request)};
  auto kvs = std::vector<std::pair<std::string, std::string>>{};
  while (reader->Read(&response)) {
    kvs.push_back(std::make_pair(response.key_value().key(), response.key_value().value()));
  }
  const auto status = reader->Finish();
  ASSERT_EQ(status.error_code(), StatusCode::NOT_FOUND);
  ASSERT_TRUE(kvs.empty());
}

TEST_F(replica_state_snapshot_service_test, no_last_received_key) {
  addPublicState();
  service_.overrideCheckpointPathForTest(db_->path());
  startServer();
  auto context = ClientContext{};
  auto request = StreamSnapshotRequest{};
  request.set_snapshot_id(42);  // ignored, because we override the DB path and, hence, the DbCheckpointManager
  auto response = StreamSnapshotResponse{};
  auto reader = std::unique_ptr<ClientReader<StreamSnapshotResponse>>{stub_->StreamSnapshot(&context, request)};
  auto kvs = std::vector<std::pair<std::string, std::string>>{};
  while (reader->Read(&response)) {
    kvs.push_back(std::make_pair(response.key_value().key(), response.key_value().value()));
  }
  const auto status = reader->Finish();
  ASSERT_EQ(status.error_code(), StatusCode::OK);
  ASSERT_THAT(kvs,
              ContainerEq(std::vector<std::pair<std::string, std::string>>{
                  {"a", "va"}, {"b", "vb"}, {"c", "vc"}, {"d", "vd"}}));
}

TEST_F(replica_state_snapshot_service_test, valid_last_received_key) {
  addPublicState();
  service_.overrideCheckpointPathForTest(db_->path());
  startServer();
  auto context = ClientContext{};
  auto request = StreamSnapshotRequest{};
  request.set_snapshot_id(42);  // ignored, because we override the DB path and, hence, the DbCheckpointManager
  request.set_last_received_key("b");
  auto response = StreamSnapshotResponse{};
  auto reader = std::unique_ptr<ClientReader<StreamSnapshotResponse>>{stub_->StreamSnapshot(&context, request)};
  auto kvs = std::vector<std::pair<std::string, std::string>>{};
  while (reader->Read(&response)) {
    kvs.push_back(std::make_pair(response.key_value().key(), response.key_value().value()));
  }
  const auto status = reader->Finish();
  ASSERT_EQ(status.error_code(), StatusCode::OK);
  ASSERT_THAT(kvs, ContainerEq(std::vector<std::pair<std::string, std::string>>{{"c", "vc"}, {"d", "vd"}}));
}

TEST_F(replica_state_snapshot_service_test, last_key_as_last_received_key) {
  addPublicState();
  service_.overrideCheckpointPathForTest(db_->path());
  startServer();
  auto context = ClientContext{};
  auto request = StreamSnapshotRequest{};
  request.set_snapshot_id(42);  // ignored, because we override the DB path and, hence, the DbCheckpointManager
  request.set_last_received_key("d");
  auto response = StreamSnapshotResponse{};
  auto reader = std::unique_ptr<ClientReader<StreamSnapshotResponse>>{stub_->StreamSnapshot(&context, request)};
  auto kvs = std::vector<std::pair<std::string, std::string>>{};
  while (reader->Read(&response)) {
    kvs.push_back(std::make_pair(response.key_value().key(), response.key_value().value()));
  }
  const auto status = reader->Finish();
  ASSERT_EQ(status.error_code(), StatusCode::OK);
  ASSERT_TRUE(kvs.empty());
}

TEST_F(replica_state_snapshot_service_test, invalid_last_received_key) {
  addPublicState();
  service_.overrideCheckpointPathForTest(db_->path());
  startServer();
  auto context = ClientContext{};
  auto request = StreamSnapshotRequest{};
  request.set_snapshot_id(42);  // ignored, because we override the DB path and, hence, the DbCheckpointManager
  request.set_last_received_key("e");
  auto response = StreamSnapshotResponse{};
  auto reader = std::unique_ptr<ClientReader<StreamSnapshotResponse>>{stub_->StreamSnapshot(&context, request)};
  auto kvs = std::vector<std::pair<std::string, std::string>>{};
  while (reader->Read(&response)) {
    kvs.push_back(std::make_pair(response.key_value().key(), response.key_value().value()));
  }
  const auto status = reader->Finish();
  ASSERT_EQ(status.error_code(), StatusCode::INVALID_ARGUMENT);
  ASSERT_TRUE(kvs.empty());
}

TEST_F(replica_state_snapshot_service_test, pending_checkpoint_creation) {
  addPublicState();
  service_.overrideCheckpointStateForTest(DbCheckpointManager::CheckpointState::kPending);
  startServer();
  auto context = ClientContext{};
  auto request = StreamSnapshotRequest{};
  request.set_snapshot_id(42);  // ignored, because we override the checkpoint state
  auto response = StreamSnapshotResponse{};
  auto reader = std::unique_ptr<ClientReader<StreamSnapshotResponse>>{stub_->StreamSnapshot(&context, request)};
  auto kvs = std::vector<std::pair<std::string, std::string>>{};
  while (reader->Read(&response)) {
    kvs.push_back(std::make_pair(response.key_value().key(), response.key_value().value()));
  }
  const auto status = reader->Finish();
  ASSERT_EQ(status.error_code(), StatusCode::UNAVAILABLE);
  ASSERT_TRUE(kvs.empty());
}

TEST_F(replica_state_snapshot_service_test, exception_thrown_while_streaming) {
  addPublicState();
  service_.overrideCheckpointPathForTest(db_->path());
  service_.throwExceptionForTest();
  startServer();
  auto context = ClientContext{};
  auto request = StreamSnapshotRequest{};
  request.set_snapshot_id(42);  // ignored, because we override the DB path and, hence, the DbCheckpointManager
  auto response = StreamSnapshotResponse{};
  auto reader = std::unique_ptr<ClientReader<StreamSnapshotResponse>>{stub_->StreamSnapshot(&context, request)};
  auto kvs = std::vector<std::pair<std::string, std::string>>{};
  while (reader->Read(&response)) {
    kvs.push_back(std::make_pair(response.key_value().key(), response.key_value().value()));
  }
  const auto status = reader->Finish();
  ASSERT_EQ(status.error_code(), StatusCode::UNKNOWN);
  ASSERT_TRUE(kvs.empty());
}

}  // namespace
