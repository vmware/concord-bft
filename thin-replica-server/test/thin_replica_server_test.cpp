// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <grpcpp/impl/codegen/server_context.h>

#include <iterator>
#include <map>
#include <sstream>
#include <string>
#include "thin-replica-server/grpc_services.hpp"
#include "Logger.hpp"

#include "categorization/db_categories.h"
#include "kv_types.hpp"
#include "thin-replica-server/subscription_buffer.hpp"
#include "thin-replica-server/thin_replica_impl.hpp"

#include "db_interfaces.h"
#include "gtest/gtest.h"

namespace {

using concord::kvbc::BlockId;
using concord::kvbc::categorization::ImmutableInput;

using com::vmware::concord::thin_replica::Data;
using com::vmware::concord::thin_replica::Hash;
using com::vmware::concord::thin_replica::ReadStateHashRequest;
using com::vmware::concord::thin_replica::ReadStateRequest;
using com::vmware::concord::thin_replica::SubscriptionRequest;

using concord::thin_replica::SubUpdateBuffer;

using Block = std::pair<BlockId, ImmutableInput>;
using BlockMap = std::map<BlockId, ImmutableInput>;

constexpr uint64_t kLastBlockId{5u};

Block generate_block(BlockId block_id) {
  concord::kvbc::categorization::ImmutableValueUpdate data;
  data.data = "value block#" + std::to_string(block_id);
  concord::kvbc::categorization::ImmutableInput input;
  auto key = std::string{"key block#"} + std::to_string(block_id);
  input.kv.insert({key, data});
  return {block_id, input};
}

BlockMap generate_kvp(BlockId start, BlockId end) {
  BlockMap blocks;
  for (BlockId block = start; block <= end; ++block) {
    blocks.emplace(generate_block(block));
  }
  return blocks;
}

class FakeStorage : public concord::kvbc::IReader {
 public:
  FakeStorage(BlockMap&& db) : db_(std::move(db)), block_id_(db_.size()) {}

  void addBlocks(const BlockMap& db) {
    std::scoped_lock sl(mtx_);
    db_.insert(std::cbegin(db), std::cend(db));
    block_id_ = db_.size();
  }

  std::optional<concord::kvbc::categorization::Value> get(const std::string& category_id,
                                                          const std::string& key,
                                                          BlockId block_id) const override {
    ADD_FAILURE() << "get() should not be called by this test";
    return {};
  }

  std::optional<concord::kvbc::categorization::Value> getLatest(const std::string& category_id,
                                                                const std::string& key) const override {
    ADD_FAILURE() << "getLatest() should not be called by this test";
    return {};
  }

  void multiGet(const std::string& category_id,
                const std::vector<std::string>& keys,
                const std::vector<BlockId>& versions,
                std::vector<std::optional<concord::kvbc::categorization::Value>>& values) const override {
    ADD_FAILURE() << "multiGet() should not be called by this test";
    values = {};
  }

  void multiGetLatest(const std::string& category_id,
                      const std::vector<std::string>& keys,
                      std::vector<std::optional<concord::kvbc::categorization::Value>>& values) const override {
    ADD_FAILURE() << "multiGetLatest() should not be called by this test";
    values = {};
  }

  std::optional<concord::kvbc::categorization::TaggedVersion> getLatestVersion(const std::string& category_id,
                                                                               const std::string& key) const override {
    ADD_FAILURE() << "getLatestVersion() should not be called by this test";
    return {};
  }

  void multiGetLatestVersion(
      const std::string& category_id,
      const std::vector<std::string>& keys,
      std::vector<std::optional<concord::kvbc::categorization::TaggedVersion>>& versions) const override {
    ADD_FAILURE() << "multiGetLatestVersion() should not be called by this test";
  }

  std::optional<concord::kvbc::categorization::Updates> getBlockUpdates(BlockId block_id) const override {
    std::scoped_lock sl(mtx_);
    if (block_id >= 0 && block_id <= block_id_) {
      auto data = db_.at(block_id);
      concord::kvbc::categorization::Updates updates{};
      concord::kvbc::categorization::ImmutableUpdates immutable{};
      for (auto& [k, v] : data.kv) {
        std::string key(k);
        std::set<std::string> tags(v.tags.begin(), v.tags.end());
        concord::kvbc::categorization::ImmutableUpdates::ImmutableValue value{std::move(v.data), std::move(tags)};
        immutable.addUpdate(std::move(key), std::move(value));
      }
      updates.add(concord::kvbc::categorization::kExecutionEventsCategory, std::move(immutable));
      return {updates};
    }
    // The actual storage implementation (ReplicaImpl.cpp) expects us to
    // handle an invalid block range; let's simulate this here.
    ADD_FAILURE() << "Provide a valid block range " << block_id << " " << block_id_;
    return {};
  }

  BlockId getGenesisBlockId() const override {
    ADD_FAILURE() << "get() should not be called by this test";
    return 0;
  }

  BlockId getLastBlockId() const override { return block_id_; }

 private:
  BlockMap db_;
  BlockId block_id_;
  mutable std::mutex mtx_;
};

class TestServerContext {
  std::multimap<std::string, std::string> metadata_ = {{"client_id", "TEST ID"}};

  class AuthContext {
   public:
    bool IsPeerAuthenticated() const { return false; }

    std::vector<std::string> GetPeerIdentity() const {
      std::vector<std::string> temp_vector;
      return temp_vector;
    }

    std::vector<std::string> FindPropertyValues(const std::string& temp) const {
      std::vector<std::string> temp_vector;
      const char* cert =
          "-----BEGIN CERTIFICATE-----\n"
          "MIICZjCCAeygAwIBAgIUUDYNatCx+fxZ5JSnlPYr4VByumgwCgYIKoZIzj0EAwIw\n"
          "ajELMAkGA1UEBhMCTkExCzAJBgNVBAgMAk5BMQswCQYDVQQHDAJOQTELMAkGA1UE\n"
          "CgwCTkExGTAXBgNVBAsMEGRhbWxfbGVkZ2VyX2FwaTExGTAXBgNVBAMMEGRhbWxf\n"
          "bGVkZ2VyX2FwaTEwHhcNMjAxMjA0MDA1NzM1WhcNMjExMjA0MDA1NzM1WjBqMQsw\n"
          "CQYDVQQGEwJOQTELMAkGA1UECAwCTkExCzAJBgNVBAcMAk5BMQswCQYDVQQKDAJO\n"
          "QTEZMBcGA1UECwwQZGFtbF9sZWRnZXJfYXBpMTEZMBcGA1UEAwwQZGFtbF9sZWRn\n"
          "ZXJfYXBpMTB2MBAGByqGSM49AgEGBSuBBAAiA2IABIwKwDQ+BrBG+8Bjx1TSkjmj\n"
          "XhMfEU/0z6GHPvlcdqYM/23AsZqu/l19egPxGciN2JuTvazxaLi/QC7PlN8C6pxE\n"
          "FQkwOBxG8wkOODic7sObq9kkQ9ho7axUzdsXgur1u6NTMFEwHQYDVR0OBBYEFFJH\n"
          "7VYeFWg9UXPgUUcR3wdv4P4AMB8GA1UdIwQYMBaAFFJH7VYeFWg9UXPgUUcR3wdv\n"
          "4P4AMA8GA1UdEwEB/wQFMAMBAf8wCgYIKoZIzj0EAwIDaAAwZQIxALJ1wKliQWCZ\n"
          "qW1sDuktNuyiv1Mf5GN/tyQWbBUaWKf3SigHsTFJsHat8lAvzBEIKQIwMrs90u4Q\n"
          "bCcOw5GqAHtmOSt1ZQRKJsid/TFphtDCZOw0BI3PFII+yCKcgqZQpD6c\n"
          "-----END CERTIFICATE-----\n";
      std::string cert_str(cert);
      temp_vector.push_back(cert_str);
      return temp_vector;
    }
  };

 public:
  const std::multimap<std::string, std::string>& client_metadata() const { return metadata_; }
  std::shared_ptr<const AuthContext> auth_context() const { return nullptr; }
  void erase_client_metadata() { metadata_.clear(); }
  bool IsCancelled() { return false; }
};

template <typename DataT>
class TestStateMachine {
 public:
  TestStateMachine(FakeStorage& storage, const BlockMap& live_update_blocks, uint64_t start_block_id)
      : storage_(storage),
        live_update_blocks_(std::cbegin(live_update_blocks), std::cend(live_update_blocks)),
        current_block_to_send_(start_block_id) {
    last_block_to_send_ = storage_.getLastBlockId();
    if (live_update_blocks_.size()) {
      last_block_to_send_ += live_update_blocks_.size() - 1;
      // the gap blocks
      last_block_to_send_ += live_update_blocks_.begin()->first - storage_.getLastBlockId();
    }
    // the last pushed block
    last_block_to_send_++;
  }

  ~TestStateMachine() { EXPECT_EQ(last_block_to_send_, current_block_to_send_); }

  void set_expected_last_block_to_send(BlockId block_id) { last_block_to_send_ = block_id; }

  void on_live_update_buffer_added(std::shared_ptr<SubUpdateBuffer> buffer) {
    for (const auto& block : live_update_blocks_) {
      buffer->Push({block.first, "cid", block.second});
    }
    live_buffer_ = buffer;
  }

  void return_false_on_last_block(bool on) { return_false_on_last_block_ = on; }

  bool on_server_write(const DataT& data) {
    EXPECT_EQ(current_block_to_send_, data.events().block_id());
    if (current_block_to_send_ == last_block_to_send_) {
      return !return_false_on_last_block_;
    }

    if (live_buffer_) {
      if (current_block_to_send_ == last_block_to_send_ - 1) {
        on_finished_dropping_blocks();
      } else if (live_buffer_->Full() || live_buffer_->oldestBlockId() > (storage_.getLastBlockId() + 1)) {
        // There is a gap that is supposed to be filled with blocks from the
        // storage
        on_sync_with_kvb_finished();
      }
    }
    ++current_block_to_send_;
    return true;
  }

  void on_sync_with_kvb_finished() {
    auto gap_blocks = generate_kvp(storage_.getLastBlockId() + 1, live_buffer_->newestBlockId());
    storage_.addBlocks(gap_blocks);
  }

  void on_finished_dropping_blocks() {
    auto block = generate_block(storage_.getLastBlockId() + 1);
    live_buffer_->Push({block.first, "cid", block.second});
  }

 private:
  FakeStorage& storage_;
  BlockMap live_update_blocks_;
  std::shared_ptr<SubUpdateBuffer> live_buffer_;
  size_t current_block_to_send_{0u};
  size_t last_block_to_send_{0};
  bool return_false_on_last_block_{true};
};

template <typename T>
class TestServerWriter {
  TestStateMachine<T>& state_machine_;

 public:
  TestServerWriter(TestStateMachine<T>& state_machine) : state_machine_(state_machine) {}
  bool Write(T& msg) { return state_machine_.on_server_write(msg); }
};

template <typename DataT>
class TestSubBufferList : public concord::thin_replica::SubBufferList {
  TestStateMachine<DataT>& state_machine_;

 public:
  TestSubBufferList(TestStateMachine<DataT>& state_machine) : state_machine_(state_machine) {}

  // Add a subscriber
  bool addBuffer(std::shared_ptr<SubUpdateBuffer> elem) override {
    state_machine_.on_live_update_buffer_added(elem);
    return concord::thin_replica::SubBufferList::addBuffer(elem);
  }
};

TEST(thin_replica_server_test, SubscribeToUpdatesAlreadySynced) {
  FakeStorage storage{generate_kvp(1, kLastBlockId)};
  EXPECT_EQ(storage.getLastBlockId(), 5);
  auto live_update_blocks = generate_kvp(kLastBlockId + 1, kLastBlockId + 5);
  TestStateMachine<Data> state_machine{storage, live_update_blocks, 1};
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;

  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_events()->set_block_id(1u);
  auto status =
      replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::OK);
}

TEST(thin_replica_server_test, SubscribeToUpdatesWithGap) {
  FakeStorage storage{generate_kvp(1, kLastBlockId)};
  auto live_update_blocks = generate_kvp(kLastBlockId + 2, kLastBlockId + 5);
  TestStateMachine<Data> state_machine{storage, live_update_blocks, 1};
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;

  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_events()->set_block_id(1u);
  auto status =
      replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::OK);
}

TEST(thin_replica_server_test, SubscribeToUpdatesWithGapFromTheMiddleBlock) {
  FakeStorage storage{generate_kvp(1, kLastBlockId)};
  auto live_update_blocks = generate_kvp(kLastBlockId + 2, kLastBlockId + 5);
  TestStateMachine<Data> state_machine{storage, live_update_blocks, 3};
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;

  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_events()->set_block_id(3u);
  auto status =
      replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::OK);
}

TEST(thin_replica_server_test, SubscribeToUpdateHashesAlreadySynced) {
  FakeStorage storage{generate_kvp(1, kLastBlockId)};
  auto live_update_blocks = generate_kvp(kLastBlockId + 1, kLastBlockId + 5);
  TestStateMachine<Hash> state_machine{storage, live_update_blocks, 1};
  TestSubBufferList<Hash> buffer{state_machine};
  TestServerWriter<Hash> stream{state_machine};

  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;

  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_events()->set_block_id(1u);
  auto status =
      replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Hash>, Hash>(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::OK);
}

TEST(thin_replica_server_test, SubscribeToUpdateHashesWithGap) {
  FakeStorage storage{generate_kvp(1, kLastBlockId)};
  auto live_update_blocks = generate_kvp(kLastBlockId + 2, kLastBlockId + 5);
  TestStateMachine<Hash> state_machine{storage, live_update_blocks, 1};
  TestSubBufferList<Hash> buffer{state_machine};
  TestServerWriter<Hash> stream{state_machine};

  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;

  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_events()->set_block_id(1u);
  auto status =
      replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Hash>, Hash>(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::OK);
}

TEST(thin_replica_server_test, ReadState) {
  FakeStorage storage{generate_kvp(1, kLastBlockId)};
  auto live_update_blocks = generate_kvp(0, 0);
  TestStateMachine<Data> state_machine{storage, live_update_blocks, 1};
  state_machine.set_expected_last_block_to_send(kLastBlockId);
  state_machine.return_false_on_last_block(false);
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;

  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());
  TestServerContext context;
  ReadStateRequest request;
  auto status = replica.ReadState(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::OK);
}

TEST(thin_replica_server_test, ReadStateHash) {
  FakeStorage storage{generate_kvp(1, kLastBlockId)};
  auto live_update_blocks = generate_kvp(0, 0);
  TestStateMachine<Hash> state_machine{storage, live_update_blocks, 0u};
  state_machine.set_expected_last_block_to_send(0u);
  TestSubBufferList<Hash> buffer{state_machine};
  TestServerWriter<Hash> stream{state_machine};

  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;

  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());
  TestServerContext context;
  ReadStateHashRequest request;
  request.mutable_events()->set_block_id(kLastBlockId);
  Hash hash;
  auto status = replica.ReadStateHash(&context, &request, &hash);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(hash.events().block_id(), kLastBlockId);
}

TEST(thin_replica_server_test, AckUpdate) {
  FakeStorage storage{generate_kvp(0, 0)};
  auto live_update_blocks = generate_kvp(0, 0);
  TestStateMachine<Hash> state_machine{storage, live_update_blocks, 1};
  TestSubBufferList<Hash> buffer{state_machine};
  TestServerWriter<Hash> stream{state_machine};

  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;

  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_events()->set_block_id(kLastBlockId);

  com::vmware::concord::thin_replica::BlockId block_id;
  block_id.set_block_id(1u);
  auto status = replica.AckUpdate(&context, &block_id, nullptr);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNIMPLEMENTED);
}

TEST(thin_replica_server_test, Unsubscribe) {
  FakeStorage storage{generate_kvp(0, 0)};
  auto live_update_blocks = generate_kvp(0, 0);
  TestStateMachine<Hash> state_machine{storage, live_update_blocks, 1};
  TestSubBufferList<Hash> buffer{state_machine};
  TestServerWriter<Hash> stream{state_machine};

  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;

  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_events()->set_block_id(kLastBlockId);

  com::vmware::concord::thin_replica::BlockId block_id;
  block_id.set_block_id(1u);
  auto status = replica.Unsubscribe(&context, nullptr, nullptr);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNIMPLEMENTED);
}

TEST(thin_replica_server_test, ContextWithoutClientIdData) {
  FakeStorage storage{generate_kvp(0, 0)};
  auto live_update_blocks = generate_kvp(0, 0);
  TestStateMachine<Data> data_state_machine{storage, live_update_blocks, 1};
  TestSubBufferList<Data> data_buffer{data_state_machine};
  TestServerWriter<Data> data_stream{data_state_machine};
  TestStateMachine<Hash> hash_state_machine{storage, live_update_blocks, 1};
  TestServerWriter<Hash> hash_stream{hash_state_machine};
  TestServerContext context;
  context.erase_client_metadata();
  ReadStateHashRequest read_state_hash_request;
  ReadStateRequest read_state_request;
  SubscriptionRequest subscription_request;
  Hash hash;

  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;

  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, data_buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());
  EXPECT_EQ(replica.ReadState(&context, &read_state_request, &data_stream).error_code(), grpc::StatusCode::UNKNOWN);
  EXPECT_EQ(replica.ReadStateHash(&context, &read_state_hash_request, &hash).error_code(), grpc::StatusCode::UNKNOWN);
  auto status = replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(
      &context, &subscription_request, &data_stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNKNOWN);
  status = replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Hash>, Hash>(
      &context, &subscription_request, &hash_stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNKNOWN);
}

TEST(thin_replica_server_test, SubscribeWithWrongBlockId) {
  FakeStorage storage{generate_kvp(0, 0)};
  auto live_update_blocks = generate_kvp(0, 0);
  TestStateMachine<Data> state_machine{storage, live_update_blocks, 1};
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;

  TestServerContext context;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());
  SubscriptionRequest request;
  request.mutable_events()->set_block_id(kLastBlockId + 100);
  auto status =
      replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::FAILED_PRECONDITION);
}

TEST(thin_replica_server_test, GetClientIdFromCertSubjectField) {
  FakeStorage storage{generate_kvp(0, 0)};
  auto live_update_blocks = generate_kvp(0, 0);
  TestStateMachine<Data> state_machine{storage, live_update_blocks, 1};
  TestSubBufferList<Data> buffer{state_machine};

  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;

  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());
  std::string subject_str =
      "subject=C = NA, ST = NA, L = NA, O = NA, OU = daml_ledger_api1, CN = "
      "daml_ledger_api1";
  std::string client_id = "daml_ledger_api1";
  std::string parsed_client_id = replica.parseClientIdFromSubject(subject_str);
  EXPECT_EQ(client_id, parsed_client_id);
}

TEST(thin_replica_server_test, GetClientIdSetFromRootCert) {
  FakeStorage storage{generate_kvp(0, 0)};
  auto live_update_blocks = generate_kvp(0, 0);
  TestStateMachine<Data> state_machine{storage, live_update_blocks, 1};
  TestSubBufferList<Data> buffer{state_machine};
  auto logger = logging::getLogger("thin_replica_server_test");
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::string root_cert_path = "resources/trs_trc_tls_certs/concord1/client.cert";
  std::unordered_set<std::string> parsed_client_id_set;
  std::unordered_set<std::string> client_id_set(
      {"daml_ledger_api1", "daml_ledger_api2", "daml_ledger_api3", "daml_ledger_api4", "trutil"});
  uint16_t update_metrics_aggregator_thresh = 100;

  concord::thin_replica::ThinReplicaImpl::getClientIdFromRootCert(logger, root_cert_path, parsed_client_id_set);
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, parsed_client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());
  EXPECT_GT(parsed_client_id_set.size(), 0);
  for (auto& client_id : client_id_set) {
    auto parsed_client_id_it = parsed_client_id_set.find(client_id);
    EXPECT_NE(parsed_client_id_it, parsed_client_id_set.end());
    EXPECT_EQ(*parsed_client_id_it, client_id);
  }
}

TEST(thin_replica_server_test, getClientIdFromClientCert) {
  FakeStorage storage{generate_kvp(0, 0)};
  auto live_update_blocks = generate_kvp(0, 0);
  TestStateMachine<Data> state_machine{storage, live_update_blocks, 1};
  TestSubBufferList<Data> buffer{state_machine};

  TestServerContext context;

  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;

  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  std::string expected_client_id = "daml_ledger_api1";
  std::string client_id = replica.getClientIdFromClientCert<TestServerContext>(&context);
  EXPECT_EQ(expected_client_id, client_id);
}
}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  auto logger = logging::getLogger("thin_replica_server_test");
  return RUN_ALL_TESTS();
}
