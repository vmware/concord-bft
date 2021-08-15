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

#include "kv_types.hpp"
#include "thin-replica-server/subscription_buffer.hpp"
#include "thin-replica-server/thin_replica_impl.hpp"

#include "db_interfaces.h"
#include "gtest/gtest.h"

namespace {

using concord::kvbc::BlockId;
using concord::kvbc::EventGroupId;
using concord::kvbc::categorization::ImmutableInput;
using concord::kvbc::categorization::Event;
using concord::kvbc::categorization::EventGroup;

using com::vmware::concord::thin_replica::Data;
using com::vmware::concord::thin_replica::Hash;
using com::vmware::concord::thin_replica::ReadStateHashRequest;
using com::vmware::concord::thin_replica::ReadStateRequest;
using com::vmware::concord::thin_replica::SubscriptionRequest;

using concord::thin_replica::SubUpdateBuffer;

using Block = std::pair<BlockId, ImmutableInput>;
using BlockMap = std::map<BlockId, ImmutableInput>;
using EventGroupMap = std::map<std::string, EventGroup>;

constexpr uint64_t kLastBlockId{5u};
constexpr uint64_t kLastEventGroupId{5u};

static inline const std::string kGlobalEgIdKey{"_global_eg_id"};

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

EventGroup generateEventGroup(EventGroupId eg_id) {
  concord::kvbc::categorization::Event event;
  EventGroup event_group;
  // we are adding just one event per event group
  event.data = "val eg_id#" + std::to_string(eg_id);
  event.tags = {"TEST ID"};
  event_group.events.emplace_back(event);
  return event_group;
}

EventGroupMap generateEventGroupMap(EventGroupId start, EventGroupId end) {
  EventGroupMap event_group_map;
  for (EventGroupId eg_id = start; eg_id <= end; ++eg_id) {
    auto event_group = generateEventGroup(eg_id);
    auto key = concordUtils::toBigEndianStringBuffer(eg_id);
    event_group_map[key] = event_group;
  }
  return event_group_map;
}

class FakeStorage : public concord::kvbc::IReader {
 public:
  FakeStorage(BlockMap&& db) : db_(std::move(db)), block_id_(db_.size()) {}
  FakeStorage(EventGroupMap&& db) : eg_db_(std::move(db)), eg_id_(eg_db_.size()) {
    updateEventGroupStorageMaps(eg_db_);
  }

  void addBlocks(const BlockMap& db) {
    std::scoped_lock sl(mtx_);
    db_.insert(std::cbegin(db), std::cend(db));
    block_id_ = db_.size();
  }

  void addEventGroups(const EventGroupMap& db) {
    std::scoped_lock sl(mtx_);
    eg_db_.insert(std::cbegin(db), std::cend(db));
    eg_id_ = eg_db_.size();
  }

  std::optional<concord::kvbc::categorization::Value> get(const std::string& category_id,
                                                          const std::string& key,
                                                          BlockId block_id) const override {
    ADD_FAILURE() << "get() should not be called by this test";
    return {};
  }

  std::optional<concord::kvbc::categorization::Value> getLatest(const std::string& category_id,
                                                                const std::string& key) const override {
    BlockId block_id = 4;
    if (category_id == concord::kvbc::categorization::kExecutionEventGroupIdsCategory) {
      // get latest trid event_group_id
      if (latest_eg_id.find(key) == latest_eg_id.end()) {
        throw std::runtime_error(" The key: " + key +
                                 "for category kExecutionEventGroupIdsCategory doesn't exist in storage!");
      }
      return concord::kvbc::categorization::VersionedValue{{block_id, latest_eg_id.at(key)}};
    } else if (category_id == concord::kvbc::categorization::kExecutionTridEventGroupsCategory) {
      // get global event_group_id corresponding to trid event_group_id
      if (trid_event_group_id.find(key) == trid_event_group_id.end())
        throw std::runtime_error(" The key: " + key +
                                 "for category kExecutionTridEventGroupsCategory doesn't exist in storage!");
      return concord::kvbc::categorization::ImmutableValue{{block_id, trid_event_group_id.at(key)}};
    } else if (category_id == concord::kvbc::categorization::kExecutionGlobalEventGroupsCategory) {
      // get event group
      std::vector<uint8_t> output;
      if (concordUtils::fromBigEndianBuffer<uint64_t>(key.data()) - 1 >= eg_db_.size())
        throw std::runtime_error(" The key: " + key +
                                 "for category kExecutionGlobalEventGroupsCategory doesn't exist in storage!");
      auto event_group_input = eg_db_.at(key);
      concord::kvbc::categorization::serialize(output, event_group_input);
      return concord::kvbc::categorization::ImmutableValue{{block_id, std::string(output.begin(), output.end())}};
    } else {
      ADD_FAILURE() << "getLatest() should not be called by this test";
      return {};
    }
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
    if (category_id == concord::kvbc::categorization::kExecutionGlobalEventGroupsCategory) {
      // Event groups not enabled
      return std::nullopt;
    }
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
  EventGroupId getLastEventGroupId() const { return eg_id_; }

  void updateLatestGlobalEgId(uint64_t global_event_group_id) {
    // Let's save the latest global event group id in storage
    latest_eg_id[kGlobalEgIdKey] = concordUtils::toBigEndianStringBuffer(global_event_group_id);
  }

  void updateLatestTridEgId(const std::string& trid) {
    // Let's save the latest private/trid specific event group id
    if (latest_eg_id[trid].empty()) {
      uint64_t trid_eg_id_start = 1;
      latest_eg_id[trid] = concordUtils::toBigEndianStringBuffer(trid_eg_id_start);
    } else {
      auto latest_id = concordUtils::fromBigEndianBuffer<uint64_t>(latest_eg_id[trid].data());
      latest_eg_id[trid] = concordUtils::toBigEndianStringBuffer(++latest_id);
    }
  }

  void updateTridToGlobalEgIdMapping(uint64_t global_event_group_id, const std::string& trid) {
    // We need to be able to map the global event_group_id to the trid specific event_group_id
    trid_event_group_id[trid + "#" + latest_eg_id[trid]] = concordUtils::toBigEndianStringBuffer(global_event_group_id);
  }

  // Update the following category maps in storage
  // 1. latest_eg_id (trid -> latest_trid_event_group_id)
  // 2. trid_event_group_id (trid_event_group_id -> global_event_group_id)
  void updateEventGroupStorageMaps(const EventGroupMap& event_group_map) {
    for (const auto& [eg_id_str, event_group] : event_group_map) {
      EventGroupId eg_id(concordUtils::fromBigEndianBuffer<uint64_t>(eg_id_str.data()));
      for (const auto& event : event_group.events) {
        if (not event.tags.empty()) {
          for (const auto& tag : event.tags) {
            updateLatestTridEgId(tag);
            updateTridToGlobalEgIdMapping(eg_id, tag);
          }
        }
        updateLatestGlobalEgId(eg_id);
      }
    }
  }

 private:
  BlockMap db_;
  BlockId block_id_;
  mutable std::mutex mtx_;
  EventGroupMap eg_db_;
  EventGroupId eg_id_;
  // trid -> latest_trid_event_group_id map
  std::map<std::string, std::string> latest_eg_id;
  // trid_event_group_id -> global_event_group_id map
  std::map<std::string, std::string> trid_event_group_id;
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

  TestStateMachine(FakeStorage& storage, const EventGroupMap& live_update_event_groups, uint64_t start_event_group_id)
      : storage_(storage),
        live_update_event_groups_(std::cbegin(live_update_event_groups), std::cend(live_update_event_groups)),
        current_event_group_to_send_(start_event_group_id) {
    is_event_group_sm = true;
    last_event_group_to_send_ = storage_.getLastEventGroupId();
    if (live_update_event_groups_.size()) {
      last_event_group_to_send_ += live_update_event_groups_.size() - 1;
      // the gap event_groups
      EventGroupId eg_id(concordUtils::fromBigEndianBuffer<uint64_t>(live_update_event_groups_.begin()->first.data()));
      last_event_group_to_send_ += eg_id - storage_.getLastEventGroupId();
    }
    // the last pushed event group
    last_event_group_to_send_++;
  }

  ~TestStateMachine() {
    if (not is_event_group_sm) {
      EXPECT_EQ(last_block_to_send_, current_block_to_send_);
    } else {
      EXPECT_EQ(last_event_group_to_send_, current_event_group_to_send_);
    }
  }

  void set_expected_last_block_to_send(BlockId block_id) { last_block_to_send_ = block_id; }
  void set_expected_last_event_group_to_send(EventGroupId eg_id) { last_event_group_to_send_ = eg_id; }

  void on_live_update_buffer_added(std::shared_ptr<SubUpdateBuffer> buffer) {
    for (const auto& block : live_update_blocks_) {
      buffer->Push({block.first, "cid", block.second});
    }
    live_buffer_ = buffer;
  }

  void on_live_eg_update_buffer_added(std::shared_ptr<SubUpdateBuffer> buffer) {
    for (const auto& event_group : live_update_event_groups_) {
      EventGroupId eg_id(concordUtils::fromBigEndianBuffer<uint64_t>(event_group.first.data()));
      buffer->PushEventGroup({eg_id, event_group.second});
    }
    live_buffer_ = buffer;
  }

  void return_false_on_last_block(bool on) { return_false_on_last_block_ = on; }
  void return_false_on_last_event_group(bool on) { return_false_on_last_event_group_ = on; }

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

  bool on_server_write_event_group(const DataT& data) {
    if constexpr (std::is_same<DataT, com::vmware::concord::thin_replica::Data>()) {
      EXPECT_EQ(current_event_group_to_send_, data.event_group().id());
    } else if constexpr (std::is_same<DataT, com::vmware::concord::thin_replica::Hash>()) {
      EXPECT_EQ(current_event_group_to_send_, data.event_group().event_group_id());
    }
    if (current_event_group_to_send_ == last_event_group_to_send_) {
      return !return_false_on_last_event_group_;
    }

    if (live_buffer_) {
      if (current_event_group_to_send_ == last_event_group_to_send_ - 1) {
        on_finished_dropping_event_groups();
      } else if (live_buffer_->Full() || live_buffer_->oldestEventGroupId() > (storage_.getLastEventGroupId() + 1)) {
        // There is a gap that is supposed to be filled with event groups from the
        // storage
        on_sync_with_event_groups_finished();
      }
    }
    ++current_event_group_to_send_;
    return true;
  }

  void on_sync_with_kvb_finished() {
    auto gap_blocks = generate_kvp(storage_.getLastBlockId() + 1, live_buffer_->newestBlockId());
    storage_.addBlocks(gap_blocks);
  }

  void on_sync_with_event_groups_finished() {
    auto eg_id = storage_.getLastEventGroupId() + 1;
    auto gap_event_groups = generateEventGroupMap(eg_id, live_buffer_->newestEventGroupId());
    storage_.updateEventGroupStorageMaps(gap_event_groups);
    storage_.addEventGroups(gap_event_groups);
  }

  void on_finished_dropping_blocks() {
    auto block = generate_block(storage_.getLastBlockId() + 1);
    live_buffer_->Push({block.first, "cid", block.second});
  }

  void on_finished_dropping_event_groups() {
    auto eg_id = storage_.getLastEventGroupId() + 1;
    auto event_group = generateEventGroup(eg_id);
    live_buffer_->PushEventGroup({eg_id, event_group});
  }

 public:
  bool is_event_group_sm = false;

 private:
  FakeStorage& storage_;
  BlockMap live_update_blocks_;
  EventGroupMap live_update_event_groups_;
  std::shared_ptr<SubUpdateBuffer> live_buffer_;
  size_t current_block_to_send_{0u};
  size_t last_block_to_send_{0};
  size_t current_event_group_to_send_{0u};
  size_t last_event_group_to_send_{0};
  bool return_false_on_last_block_{true};
  bool return_false_on_last_event_group_{true};
};

template <typename T>
class TestServerWriter {
  TestStateMachine<T>& state_machine_;

 public:
  TestServerWriter(TestStateMachine<T>& state_machine) : state_machine_(state_machine) {}
  bool Write(T& msg) {
    if (not state_machine_.is_event_group_sm) {
      return state_machine_.on_server_write(msg);
    }
    return state_machine_.on_server_write_event_group(msg);
  }
};

template <typename DataT>
class TestSubBufferList : public concord::thin_replica::SubBufferList {
  TestStateMachine<DataT>& state_machine_;

 public:
  TestSubBufferList(TestStateMachine<DataT>& state_machine) : state_machine_(state_machine) {}

  // Add a subscriber
  bool addBuffer(std::shared_ptr<SubUpdateBuffer> elem) override {
    if (not state_machine_.is_event_group_sm) {
      state_machine_.on_live_update_buffer_added(elem);
    } else {
      state_machine_.on_live_eg_update_buffer_added(elem);
    }
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

TEST(thin_replica_server_test, SubscribeToEventGroupUpdatesAlreadySynced) {
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId));
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId);
  auto live_update_event_groups = generateEventGroupMap(kLastEventGroupId + 1, kLastEventGroupId + 5);
  TestStateMachine<Data> state_machine{storage, live_update_event_groups, 1};
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
  request.mutable_event_groups()->set_event_group_id(1u);
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

TEST(thin_replica_server_test, SubscribeToEventGroupUpdatesWithGap) {
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId));
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId);
  auto live_update_event_groups = generateEventGroupMap(kLastEventGroupId + 2, kLastEventGroupId + 5);
  TestStateMachine<Data> state_machine{storage, live_update_event_groups, 1};
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
  request.mutable_event_groups()->set_event_group_id(1u);
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

TEST(thin_replica_server_test, SubscribeToEventGroupUpdatesWithGapFromTheMiddleBlock) {
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId));
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId);
  auto live_update_event_groups = generateEventGroupMap(kLastEventGroupId + 2, kLastEventGroupId + 5);
  TestStateMachine<Data> state_machine{storage, live_update_event_groups, 3};
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
  request.mutable_event_groups()->set_event_group_id(3u);
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

TEST(thin_replica_server_test, SubscribeToEventGroupUpdateHashesAlreadySynced) {
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId));
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId);
  auto live_update_event_groups = generateEventGroupMap(kLastEventGroupId + 1, kLastEventGroupId + 5);
  TestStateMachine<Hash> state_machine{storage, live_update_event_groups, 1};
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
  request.mutable_event_groups()->set_event_group_id(1u);
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

TEST(thin_replica_server_test, SubscribeToEventGroupUpdateHashesWithGap) {
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId));
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId);
  auto live_update_event_groups = generateEventGroupMap(kLastEventGroupId + 2, kLastEventGroupId + 5);
  TestStateMachine<Hash> state_machine{storage, live_update_event_groups, 1};
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
  request.mutable_event_groups()->set_event_group_id(1u);
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

TEST(thin_replica_server_test, SubscribeWithWrongEventGroupId) {
  FakeStorage storage(generateEventGroupMap(0, 0));
  auto live_update_event_groups = generateEventGroupMap(0, 0);
  TestStateMachine<Data> state_machine{storage, live_update_event_groups, 1};
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
  request.mutable_event_groups()->set_event_group_id(kLastEventGroupId + 100);
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
  return RUN_ALL_TESTS();
}
