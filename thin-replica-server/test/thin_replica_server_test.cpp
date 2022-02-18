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
#include "concord_kvbc.pb.h"

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
using com::vmware::concord::kvbc::ValueWithTrids;
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
static inline const std::string kPublicEgIdKey{"_public_eg_id"};
static inline const std::string kClientId{"TEST_ID"};
static inline const std::string kTagTableKeySeparator{"#"};

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
  if (start == end && end == 0) return blocks;
  for (BlockId block = start; block <= end; ++block) {
    blocks.emplace(generate_block(block));
  }
  return blocks;
}

enum EventGroupType {
  PublicEventGroupsOnly,       // all the event groups generated are public, i.e., visible to all the clients
  PrivateEventGroupsOnly,      // all the event groups generated are private, i.e., visible to the clients with matching
                               // tag/trid
  PublicAndPrivateEventGroups  // both private and public event groups are generated
};

std::string CreateTridKvbValue(const std::string& value, const std::vector<std::string>& trid_list) {
  ValueWithTrids proto;
  proto.set_value(value);
  for (const auto& trid : trid_list) {
    proto.add_trid(trid);
  }

  size_t size = proto.ByteSizeLong();
  std::string ret(size, '\0');
  proto.SerializeToArray(ret.data(), ret.size());

  return ret;
}

EventGroup generateEventGroup(EventGroupId eg_id, bool is_public) {
  concord::kvbc::categorization::Event event;
  EventGroup event_group;
  // we are adding just one event per event group
  const std::string data = "val eg_id#" + std::to_string(eg_id);
  std::vector<std::string> trid_list = {};
  if (!is_public) trid_list.emplace_back(kClientId);
  event.data = CreateTridKvbValue(data, trid_list);
  event.tags = trid_list;
  event_group.events.emplace_back(event);
  return event_group;
}

EventGroupMap generateEventGroupMap(EventGroupId start, EventGroupId end, EventGroupType eg_type) {
  EventGroupMap event_group_map;
  if (start == end && end == 0) return event_group_map;
  if (eg_type == EventGroupType::PublicAndPrivateEventGroups) {
    for (EventGroupId eg_id = start; eg_id <= end; ++eg_id) {
      bool is_public = false;
      if (eg_id % 2 == 0) {
        is_public = true;
      }
      EventGroup event_group = generateEventGroup(eg_id, is_public);
      auto key = concordUtils::toBigEndianStringBuffer(eg_id);
      event_group_map[key] = event_group;
    }
    return event_group_map;
  }
  bool is_public = false;
  if (eg_type == EventGroupType::PublicEventGroupsOnly) is_public = true;
  for (EventGroupId eg_id = start; eg_id <= end; ++eg_id) {
    EventGroup event_group = generateEventGroup(eg_id, is_public);
    auto key = concordUtils::toBigEndianStringBuffer(eg_id);
    event_group_map[key] = event_group;
  }
  return event_group_map;
}

class FakeStorage : public concord::kvbc::IReader {
 public:
  FakeStorage(BlockMap&& db) : db_(std::move(db)), block_id_(db_.size()) {}
  FakeStorage(EventGroupMap&& db) {
    addEventGroups(db);
    updateEventGroupStorageMaps(db);
  }
  FakeStorage(BlockMap&& blocks, EventGroupMap&& event_groups) {
    addBlocks(blocks);
    addEventGroups(event_groups);
    updateEventGroupStorageMaps(event_groups);
  }

  void addBlocks(const BlockMap& db) {
    std::scoped_lock sl(mtx_);
    db_.insert(std::cbegin(db), std::cend(db));
    block_id_ += db.size();
  }

  void addEventGroups(const EventGroupMap& db) {
    std::scoped_lock sl(mtx_);
    eg_db_.insert(std::cbegin(db), std::cend(db));
    first_event_group_block_id_ = first_event_group_block_id_ ? first_event_group_block_id_ : block_id_ + 1;
    block_id_ += db.size();
  }

  EventGroupMap getEventGroups() { return eg_db_; }

  std::optional<concord::kvbc::categorization::Value> get(const std::string& category_id,
                                                          const std::string& key,
                                                          BlockId block_id) const override {
    ADD_FAILURE() << "get() should not be called by this test";
    return {};
  }

  std::optional<concord::kvbc::categorization::Value> getLatest(const std::string& category_id,
                                                                const std::string& key) const override {
    std::scoped_lock sl(mtx_);
    BlockId block_id = 4;
    auto logger = logging::getLogger("thin_replica_server_test");
    if (category_id == concord::kvbc::categorization::kExecutionEventGroupLatestCategory) {
      // get latest trid event_group_id
      if (latest_table.find(key) == latest_table.end()) {
        // In case there are no public or private event groups for a client, return 0.
        // Note: `0` is an invalid event group id
        uint64_t eg_id = 0;
        LOG_DEBUG(logger, "getLatest: key: " << key.data() << " ,category_id:  LATEST table, val: " << eg_id);
        return concord::kvbc::categorization::VersionedValue{{block_id, concordUtils::toBigEndianStringBuffer(eg_id)}};
      }
      LOG_DEBUG(logger,
                "getLatest: key: " << key.data() << " ,category_id:  LATEST table, val: "
                                   << concordUtils::fromBigEndianBuffer<uint64_t>(latest_table.at(key).data()));
      return concord::kvbc::categorization::VersionedValue{{block_id, latest_table.at(key)}};
    } else if (category_id == concord::kvbc::categorization::kExecutionEventGroupTagCategory) {
      // get global event_group_id corresponding to trid event_group_id
      if (tag_table.find(key) == tag_table.end()) {
        std::stringstream msg;
        msg << "The kExecutionEventGroupTagCategory category key: " << key << " doesn't exist in storage!";
        throw std::runtime_error(msg.str());
      }
      LOG_DEBUG(logger,
                "getLatest: key: " << key.data() << " ,category_id: TAG table"
                                   << " val: " << tag_table.at(key));
      return concord::kvbc::categorization::ImmutableValue{{block_id, tag_table.at(key)}};
    } else if (category_id == concord::kvbc::categorization::kExecutionEventGroupDataCategory) {
      // get event group
      std::vector<uint8_t> output;
      if (concordUtils::fromBigEndianBuffer<uint64_t>(key.data()) - 1 >= eg_db_.size()) {
        std::stringstream msg;
        msg << "The kExecutionEventGroupDataCategory category key: " << key << " doesn't exist in storage!";
        throw std::runtime_error(msg.str());
      }
      auto event_group_input = eg_db_.at(key);
      concord::kvbc::categorization::serialize(output, event_group_input);
      LOG_DEBUG(
          logger,
          "getLatest: key: " << concordUtils::fromBigEndianBuffer<uint64_t>(key.data()) << " ,category_id: DATA table");
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
    if (category_id == concord::kvbc::categorization::kExecutionEventGroupDataCategory) {
      if (first_event_group_block_id_) {
        return {concord::kvbc::categorization::TaggedVersion{false, first_event_group_block_id_}};
      }
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

  BlockId getGenesisBlockId() const override { return genesis_block_id; }

  BlockId getLastBlockId() const override { return block_id_; }
  EventGroupId getLastEventGroupId() const { return eg_db_.size(); }
  BlockId getOldestEventGroupBlockId() const { return first_event_group_block_id_; }

  void updateLatestTable(const std::string& key, uint64_t id = 1) {
    // Let's save the latest private/trid specific event group id
    auto logger = logging::getLogger("thin_replica_server_test");
    if (latest_table[key + "_oldest"].empty()) {
      latest_table[key + "_newest"] = concordUtils::toBigEndianStringBuffer(id);
      latest_table[key + "_oldest"] = concordUtils::toBigEndianStringBuffer(id);
      LOG_DEBUG(logger, "key: " << key << "_oldest: " << id << ", _newest: " << id);
    } else {
      auto latest_id = concordUtils::fromBigEndianBuffer<uint64_t>(latest_table[key + "_newest"].data());
      latest_table[key + "_newest"] = concordUtils::toBigEndianStringBuffer(++latest_id);
      LOG_DEBUG(logger,
                "key: " << key << "_oldest: "
                        << concordUtils::fromBigEndianBuffer<uint64_t>(latest_table[key + "_oldest"].data())
                        << ", _newest: " << latest_id);
    }
  }

  void updateLatestTableAfterPruning(const std::string& key, uint64_t id) {
    // ensure that the category being updated had corresponding updates in the first place
    ConcordAssert(!latest_table[key + "_oldest"].empty());
    // the updated eg_id will always be greater than previous eg_id
    ConcordAssertGT(id, concordUtils::fromBigEndianBuffer<uint64_t>(latest_table[key + "_oldest"].data()));
    latest_table[key + "_oldest"] = concordUtils::toBigEndianStringBuffer(id);
  }

  void updateTagTable(const std::string& trid, const uint64_t global_event_group_id, uint64_t external_tag_eg_id = 0) {
    // We need to be able to map the global event_group_id to the trid specific event_group_id
    auto logger = logging::getLogger("thin_replica_server_test");
    if (trid != kPublicEgIdKey) {
      if (latest_table.find(kPublicEgIdKey + "_oldest") != latest_table.end()) {
        external_tag_eg_id =
            concordUtils::fromBigEndianBuffer<uint64_t>(latest_table[trid + "_newest"].data()) +
            concordUtils::fromBigEndianBuffer<uint64_t>(latest_table[kPublicEgIdKey + "_newest"].data());
      } else {
        external_tag_eg_id = concordUtils::fromBigEndianBuffer<uint64_t>(latest_table[trid + "_newest"].data());
      }
    }
    tag_table[trid + kTagTableKeySeparator + latest_table[trid + "_newest"]] =
        concordUtils::toBigEndianStringBuffer(global_event_group_id) + kTagTableKeySeparator +
        concordUtils::toBigEndianStringBuffer(external_tag_eg_id);
    LOG_DEBUG(logger,
              "key: " << trid + kTagTableKeySeparator
                      << concordUtils::fromBigEndianBuffer<uint64_t>(latest_table[trid + "_newest"].data())
                      << ", global_event_group_id: " << global_event_group_id);
  }

  // Update the following category maps in storage
  // 1. latest_table (trid -> latest_tag_table)
  // 2. tag_table (tag_table -> global_event_group_id)
  void updateEventGroupStorageMaps(const EventGroupMap& event_group_map) {
    for (const auto& [eg_id_str, event_group] : event_group_map) {
      EventGroupId eg_id(concordUtils::fromBigEndianBuffer<uint64_t>(eg_id_str.data()));
      for (const auto& event : event_group.events) {
        if (not event.tags.empty()) {
          for (const auto& tag : event.tags) {
            updateLatestTable(tag);
            updateTagTable(tag, eg_id);
          }
        } else {
          // update latest and tag tables for public event groups
          updateLatestTable(kPublicEgIdKey);
          updateTagTable(kPublicEgIdKey, eg_id);
        }
      }
      updateLatestTable(kGlobalEgIdKey, eg_id);
    }
  }

 public:
  BlockId genesis_block_id{0};

 private:
  BlockMap db_;
  BlockId block_id_{0};
  BlockId first_event_group_block_id_{0};
  mutable std::mutex mtx_;
  EventGroupMap eg_db_;
  // given trid as key, the map returns the latest event_group_id
  std::map<std::string, std::string> latest_table;
  // given trid#<event_group_id> as key, the map returns the global_event_group_id
  std::map<std::string, std::string> tag_table;
};

class TestServerContext {
  std::multimap<std::string, std::string> metadata_ = {{"client_id", kClientId}};
  bool context_cancelled_ = false;

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
  bool IsCancelled() { return context_cancelled_; }
  void TryCancel() { context_cancelled_ = true; }
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

  TestStateMachine(FakeStorage& storage,
                   const EventGroupMap& live_update_event_groups,
                   uint64_t start_id,
                   bool more_egs_to_add = false)
      : storage_(storage),
        live_update_event_groups_(std::cbegin(live_update_event_groups), std::cend(live_update_event_groups)),
        current_block_to_send_(start_id),
        current_event_group_to_send_(start_id),
        more_event_groups_to_add_(more_egs_to_add) {
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
  void toggle_more_event_groups_to_add_(bool val) { more_event_groups_to_add_ = val; }

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

  // The server can stream either legacy events or event groups
  bool on_server_write(const DataT& data) {
    if (data.has_events()) {
      EXPECT_EQ(current_block_to_send_, data.events().block_id());
      if (current_block_to_send_ == last_block_to_send_) {
        return !return_false_on_last_block_;
      }

      if (not is_event_group_sm && live_buffer_) {
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
    } else {
      EXPECT_TRUE(data.has_event_group());
      if constexpr (std::is_same<DataT, com::vmware::concord::thin_replica::Data>()) {
        EXPECT_EQ(current_event_group_to_send_, data.event_group().id());
      } else if constexpr (std::is_same<DataT, com::vmware::concord::thin_replica::Hash>()) {
        EXPECT_EQ(current_event_group_to_send_, data.event_group().event_group_id());
      }
      if (current_event_group_to_send_ == last_event_group_to_send_) {
        return !return_false_on_last_event_group_;
      }

      if (live_buffer_) {
        if ((current_event_group_to_send_ == (last_event_group_to_send_ - 1)) && not more_event_groups_to_add_) {
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
  }

  void on_sync_with_kvb_finished() {
    auto gap_blocks = generate_kvp(storage_.getLastBlockId() + 1, live_buffer_->newestBlockId());
    storage_.addBlocks(gap_blocks);
  }

  void on_sync_with_event_groups_finished() {
    std::scoped_lock sl(mtx_);
    auto eg_id = current_event_group_to_send_ + 1;
    auto gap_event_groups = generateEventGroupMap(eg_id, last_event_group_to_send_, current_eg_type);
    storage_.addEventGroups(gap_event_groups);
    storage_.updateEventGroupStorageMaps(gap_event_groups);
  }

  void on_finished_dropping_blocks() {
    auto block = generate_block(storage_.getLastBlockId() + 1);
    live_buffer_->Push({block.first, "cid", block.second});
  }

  void on_finished_dropping_event_groups() {
    std::scoped_lock sl(mtx_);
    auto eg_id = current_event_group_to_send_ + 1;
    auto live_update = generateEventGroupMap(eg_id, eg_id, current_eg_type);
    storage_.addEventGroups(live_update);
    storage_.updateEventGroupStorageMaps(live_update);
    for (const auto& [key, val] : live_update) {
      EventGroupId eg_id(concordUtils::fromBigEndianBuffer<uint64_t>(key.data()));
      concord::thin_replica::SubEventGroupUpdate update{eg_id, val};
      live_buffer_->PushEventGroup(update);
    }
  }

  uint64_t numUpdatesReceived() { return current_block_to_send_ + current_event_group_to_send_ - 2; }
  uint64_t numLegacyBlocksReceived() { return current_block_to_send_ - 1; }
  uint64_t numEventGroupsReceived() { return current_event_group_to_send_ - 1; }

 public:
  bool is_event_group_sm = false;
  EventGroupType current_eg_type;

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
  bool more_event_groups_to_add_{false};
  std::mutex mtx_;
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
    if (not state_machine_.is_event_group_sm) {
      state_machine_.on_live_update_buffer_added(elem);
    } else {
      state_machine_.on_live_eg_update_buffer_added(elem);
    }
    return concord::thin_replica::SubBufferList::addBuffer(elem);
  }
};

TEST(thin_replica_server_test, SubscribeToFirstBlockNotInStorage) {
  FakeStorage storage{generate_kvp(0, 0)};
  storage.genesis_block_id = 1;
  EXPECT_EQ(storage.getLastBlockId(), 0);
  auto live_update_blocks = generate_kvp(0, 0);
  EXPECT_EQ(live_update_blocks.size(), 0);
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

  std::chrono::seconds data_timeout_ = 3s;
  auto stream_out = async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = stream_out.wait_for(data_timeout_);
  EXPECT_EQ(status, std::future_status::timeout);
  context.TryCancel();
}

TEST(thin_replica_server_test, SubscribeToNextBlockNotInStorage) {
  FakeStorage storage{generate_kvp(1, kLastBlockId)};
  storage.genesis_block_id = 1;
  EXPECT_EQ(storage.getLastBlockId(), kLastBlockId);
  auto live_update_blocks = generate_kvp(0, 0);
  EXPECT_EQ(live_update_blocks.size(), 0);
  TestStateMachine<Data> state_machine{storage, live_update_blocks, 6};
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
  request.mutable_events()->set_block_id(6u);

  std::chrono::seconds data_timeout_ = 3s;
  auto stream_out = async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = stream_out.wait_for(data_timeout_);
  EXPECT_EQ(status, std::future_status::timeout);
  context.TryCancel();
}

TEST(thin_replica_server_test, SubscribeToFirstEventGroupNotInStorage) {
  FakeStorage storage(generateEventGroupMap(0, 0, EventGroupType::PublicAndPrivateEventGroups));
  EXPECT_EQ(storage.getLastEventGroupId(), 0);
  auto live_update_event_groups = generateEventGroupMap(0, 0, EventGroupType::PublicAndPrivateEventGroups);
  EXPECT_EQ(live_update_event_groups.size(), 0);
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

  std::chrono::seconds data_timeout_ = 3s;
  auto stream_out = async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = stream_out.wait_for(data_timeout_);
  EXPECT_EQ(status, std::future_status::timeout);
  context.TryCancel();
}

TEST(thin_replica_server_test, SubscribeToNextEventGroupNotInStorage) {
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId, EventGroupType::PublicAndPrivateEventGroups));
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId);
  auto live_update_event_groups = generateEventGroupMap(0, 0, EventGroupType::PublicAndPrivateEventGroups);
  EXPECT_EQ(live_update_event_groups.size(), 0);
  TestStateMachine<Data> state_machine{storage, live_update_event_groups, 6};
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
  request.mutable_event_groups()->set_event_group_id(6u);

  std::chrono::seconds data_timeout_ = 3s;
  auto stream_out = async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = stream_out.wait_for(data_timeout_);
  EXPECT_EQ(status, std::future_status::timeout);
  context.TryCancel();
}

TEST(thin_replica_server_test, SubscribeToUpdatesAlreadySynced) {
  FakeStorage storage{generate_kvp(1, kLastBlockId)};
  storage.genesis_block_id = 1;
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

TEST(thin_replica_server_test, SubscribeToPrivateEventGroupUpdatesAlreadySynced) {
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId + 5, EventGroupType::PrivateEventGroupsOnly));
  auto storage_egs = storage.getEventGroups();
  EventGroupMap live_updates = storage_egs;
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Data> state_machine{storage, live_updates, 1, true};
  state_machine.current_eg_type = EventGroupType::PrivateEventGroupsOnly;
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
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);
  auto more_live_updates =
      generateEventGroupMap(kLastEventGroupId + 6, kLastEventGroupId + 6, EventGroupType::PrivateEventGroupsOnly);
  storage.addEventGroups(more_live_updates);
  storage.updateEventGroupStorageMaps(more_live_updates);
  state_machine.toggle_more_event_groups_to_add_(false);
  for (const auto& [key, val] : more_live_updates) {
    EventGroupId eg_id(concordUtils::fromBigEndianBuffer<uint64_t>(key.data()));
    concord::thin_replica::SubEventGroupUpdate update{eg_id, val};
    buffer.updateEventGroupSubBuffers(update);
  }
  state_machine.set_expected_last_event_group_to_send(12u);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5 + 1;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicEventGroupUpdatesAlreadySynced) {
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId + 5, EventGroupType::PublicEventGroupsOnly));
  auto storage_egs = storage.getEventGroups();
  EventGroupMap live_updates = storage_egs;
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Data> state_machine{storage, live_updates, 1, true};
  state_machine.current_eg_type = EventGroupType::PublicEventGroupsOnly;
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
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);
  auto more_live_updates =
      generateEventGroupMap(kLastEventGroupId + 6, kLastEventGroupId + 6, EventGroupType::PrivateEventGroupsOnly);
  storage.addEventGroups(more_live_updates);
  storage.updateEventGroupStorageMaps(more_live_updates);
  state_machine.toggle_more_event_groups_to_add_(false);
  for (const auto& [key, val] : more_live_updates) {
    EventGroupId eg_id(concordUtils::fromBigEndianBuffer<uint64_t>(key.data()));
    concord::thin_replica::SubEventGroupUpdate update{eg_id, val};
    buffer.updateEventGroupSubBuffers(update);
  }
  state_machine.set_expected_last_event_group_to_send(12u);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5 + 1;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicAndPrivateEventGroupUpdatesAlreadySynced) {
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId + 5, EventGroupType::PublicAndPrivateEventGroups));
  auto storage_egs = storage.getEventGroups();
  EventGroupMap live_updates = storage_egs;
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Data> state_machine{storage, live_updates, 1, true};
  state_machine.current_eg_type = EventGroupType::PublicAndPrivateEventGroups;
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
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);
  auto more_live_updates =
      generateEventGroupMap(kLastEventGroupId + 6, kLastEventGroupId + 6, EventGroupType::PrivateEventGroupsOnly);
  storage.addEventGroups(more_live_updates);
  storage.updateEventGroupStorageMaps(more_live_updates);
  state_machine.toggle_more_event_groups_to_add_(false);
  for (const auto& [key, val] : more_live_updates) {
    EventGroupId eg_id(concordUtils::fromBigEndianBuffer<uint64_t>(key.data()));
    concord::thin_replica::SubEventGroupUpdate update{eg_id, val};
    buffer.updateEventGroupSubBuffers(update);
  }
  state_machine.set_expected_last_event_group_to_send(12u);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5 + 1;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToUpdatesWithGap) {
  FakeStorage storage{generate_kvp(1, kLastBlockId)};
  storage.genesis_block_id = 1;
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

TEST(thin_replica_server_test, SubscribeToPrivateEventGroupUpdatesWithGap) {
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId + 5, EventGroupType::PrivateEventGroupsOnly));
  auto storage_egs = storage.getEventGroups();
  EventGroupMap live_updates = storage_egs;
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Data> state_machine{storage, live_updates, 1, true};
  state_machine.current_eg_type = EventGroupType::PrivateEventGroupsOnly;
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
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);
  state_machine.set_expected_last_event_group_to_send(14u);
  auto more_live_updates =
      generateEventGroupMap(kLastEventGroupId + 6, kLastEventGroupId + 8, EventGroupType::PrivateEventGroupsOnly);
  storage.addEventGroups(more_live_updates);
  storage.updateEventGroupStorageMaps(more_live_updates);
  state_machine.toggle_more_event_groups_to_add_(false);
  for (const auto& [key, val] : more_live_updates) {
    EventGroupId eg_id(concordUtils::fromBigEndianBuffer<uint64_t>(key.data()));
    concord::thin_replica::SubEventGroupUpdate update{eg_id, val};
    buffer.updateEventGroupSubBuffers(update);
  }
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5 + 3;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicEventGroupUpdatesWithGap) {
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId + 5, EventGroupType::PublicEventGroupsOnly));
  auto storage_egs = storage.getEventGroups();
  EventGroupMap live_updates = storage_egs;
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Data> state_machine{storage, live_updates, 1, true};
  state_machine.current_eg_type = EventGroupType::PublicEventGroupsOnly;
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
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);
  state_machine.set_expected_last_event_group_to_send(14u);
  auto more_live_updates =
      generateEventGroupMap(kLastEventGroupId + 6, kLastEventGroupId + 8, EventGroupType::PublicEventGroupsOnly);
  storage.addEventGroups(more_live_updates);
  storage.updateEventGroupStorageMaps(more_live_updates);
  state_machine.toggle_more_event_groups_to_add_(false);
  for (const auto& [key, val] : more_live_updates) {
    EventGroupId eg_id(concordUtils::fromBigEndianBuffer<uint64_t>(key.data()));
    concord::thin_replica::SubEventGroupUpdate update{eg_id, val};
    buffer.updateEventGroupSubBuffers(update);
  }
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5 + 3;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicAndPrivateEventGroupUpdatesWithGap) {
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId + 5, EventGroupType::PublicAndPrivateEventGroups));
  auto storage_egs = storage.getEventGroups();
  EventGroupMap live_updates = storage_egs;
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Data> state_machine{storage, live_updates, 1, true};
  state_machine.current_eg_type = EventGroupType::PublicAndPrivateEventGroups;
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
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);
  state_machine.set_expected_last_event_group_to_send(14u);
  auto more_live_updates =
      generateEventGroupMap(kLastEventGroupId + 6, kLastEventGroupId + 8, EventGroupType::PublicAndPrivateEventGroups);
  storage.addEventGroups(more_live_updates);
  storage.updateEventGroupStorageMaps(more_live_updates);
  state_machine.toggle_more_event_groups_to_add_(false);
  for (const auto& [key, val] : more_live_updates) {
    EventGroupId eg_id(concordUtils::fromBigEndianBuffer<uint64_t>(key.data()));
    concord::thin_replica::SubEventGroupUpdate update{eg_id, val};
    buffer.updateEventGroupSubBuffers(update);
  }
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5 + 3;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToUpdatesWithGapFromTheMiddleBlock) {
  FakeStorage storage{generate_kvp(1, kLastBlockId)};
  storage.genesis_block_id = 1;
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

TEST(thin_replica_server_test, SubscribeToPrivateEventGroupUpdatesWithGapFromTheMiddleBlock) {
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId, EventGroupType::PrivateEventGroupsOnly));
  auto storage_egs = storage.getEventGroups();
  EventGroupMap live_updates = storage_egs;
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId);
  TestStateMachine<Data> state_machine{storage, live_updates, 3, true};
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
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);
  state_machine.set_expected_last_event_group_to_send(11u);
  auto more_storage_updates =
      generateEventGroupMap(kLastEventGroupId + 1, kLastEventGroupId + 5, EventGroupType::PrivateEventGroupsOnly);
  EventGroupMap more_live_updates;
  more_live_updates.insert(std::next(more_storage_updates.begin()), more_storage_updates.end());
  storage.addEventGroups(more_storage_updates);
  storage.updateEventGroupStorageMaps(more_storage_updates);
  state_machine.toggle_more_event_groups_to_add_(false);
  for (const auto& [key, val] : more_live_updates) {
    EventGroupId eg_id(concordUtils::fromBigEndianBuffer<uint64_t>(key.data()));
    concord::thin_replica::SubEventGroupUpdate update{eg_id, val};
    buffer.updateEventGroupSubBuffers(update);
  }
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicEventGroupUpdatesWithGapFromTheMiddleBlock) {
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId, EventGroupType::PublicEventGroupsOnly));
  auto storage_egs = storage.getEventGroups();
  EventGroupMap live_updates = storage_egs;
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId);
  TestStateMachine<Data> state_machine{storage, live_updates, 3, true};
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
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);
  state_machine.set_expected_last_event_group_to_send(11u);
  auto more_storage_updates =
      generateEventGroupMap(kLastEventGroupId + 1, kLastEventGroupId + 5, EventGroupType::PublicEventGroupsOnly);
  EventGroupMap more_live_updates;
  more_live_updates.insert(std::next(more_storage_updates.begin()), more_storage_updates.end());
  storage.addEventGroups(more_storage_updates);
  storage.updateEventGroupStorageMaps(more_storage_updates);
  state_machine.toggle_more_event_groups_to_add_(false);
  for (const auto& [key, val] : more_live_updates) {
    EventGroupId eg_id(concordUtils::fromBigEndianBuffer<uint64_t>(key.data()));
    concord::thin_replica::SubEventGroupUpdate update{eg_id, val};
    buffer.updateEventGroupSubBuffers(update);
  }
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicAndPrivateEventGroupUpdatesWithGapFromTheMiddleBlock) {
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId, EventGroupType::PublicAndPrivateEventGroups));
  auto storage_egs = storage.getEventGroups();
  EventGroupMap live_updates = storage_egs;
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId);
  TestStateMachine<Data> state_machine{storage, live_updates, 3, true};
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
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);
  state_machine.set_expected_last_event_group_to_send(11u);
  auto more_storage_updates =
      generateEventGroupMap(kLastEventGroupId + 1, kLastEventGroupId + 5, EventGroupType::PublicAndPrivateEventGroups);
  EventGroupMap more_live_updates;
  more_live_updates.insert(std::next(more_storage_updates.begin()), more_storage_updates.end());
  storage.addEventGroups(more_storage_updates);
  storage.updateEventGroupStorageMaps(more_storage_updates);
  state_machine.toggle_more_event_groups_to_add_(false);
  for (const auto& [key, val] : more_live_updates) {
    EventGroupId eg_id(concordUtils::fromBigEndianBuffer<uint64_t>(key.data()));
    concord::thin_replica::SubEventGroupUpdate update{eg_id, val};
    buffer.updateEventGroupSubBuffers(update);
  }
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToUpdateHashesAlreadySynced) {
  FakeStorage storage{generate_kvp(1, kLastBlockId)};
  storage.genesis_block_id = 1;
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

TEST(thin_replica_server_test, SubscribeToPrivateEventGroupUpdateHashesAlreadySynced) {
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId + 5, EventGroupType::PrivateEventGroupsOnly));
  auto storage_egs = storage.getEventGroups();
  EventGroupMap live_updates = storage_egs;
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Hash> state_machine{storage, live_updates, 1, true};
  state_machine.current_eg_type = EventGroupType::PrivateEventGroupsOnly;
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
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Hash>, Hash>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);
  auto more_live_updates =
      generateEventGroupMap(kLastEventGroupId + 6, kLastEventGroupId + 6, EventGroupType::PrivateEventGroupsOnly);
  storage.addEventGroups(more_live_updates);
  storage.updateEventGroupStorageMaps(more_live_updates);
  state_machine.toggle_more_event_groups_to_add_(false);
  for (const auto& [key, val] : more_live_updates) {
    EventGroupId eg_id(concordUtils::fromBigEndianBuffer<uint64_t>(key.data()));
    concord::thin_replica::SubEventGroupUpdate update{eg_id, val};
    buffer.updateEventGroupSubBuffers(update);
  }
  state_machine.set_expected_last_event_group_to_send(12u);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5 + 1;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicEventGroupUpdateHashesAlreadySynced) {
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId + 5, EventGroupType::PublicEventGroupsOnly));
  auto storage_egs = storage.getEventGroups();
  EventGroupMap live_updates = storage_egs;
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Hash> state_machine{storage, live_updates, 1, true};
  state_machine.current_eg_type = EventGroupType::PublicEventGroupsOnly;
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
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Hash>, Hash>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);
  auto more_live_updates =
      generateEventGroupMap(kLastEventGroupId + 6, kLastEventGroupId + 6, EventGroupType::PrivateEventGroupsOnly);
  storage.addEventGroups(more_live_updates);
  storage.updateEventGroupStorageMaps(more_live_updates);
  state_machine.toggle_more_event_groups_to_add_(false);
  for (const auto& [key, val] : more_live_updates) {
    EventGroupId eg_id(concordUtils::fromBigEndianBuffer<uint64_t>(key.data()));
    concord::thin_replica::SubEventGroupUpdate update{eg_id, val};
    buffer.updateEventGroupSubBuffers(update);
  }
  state_machine.set_expected_last_event_group_to_send(12u);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5 + 1;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicAndPrivateEventGroupUpdateHashesAlreadySynced) {
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId + 5, EventGroupType::PublicAndPrivateEventGroups));
  auto storage_egs = storage.getEventGroups();
  EventGroupMap live_updates = storage_egs;
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Hash> state_machine{storage, live_updates, 1, true};
  state_machine.current_eg_type = EventGroupType::PublicAndPrivateEventGroups;
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
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Hash>, Hash>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);
  auto more_live_updates =
      generateEventGroupMap(kLastEventGroupId + 6, kLastEventGroupId + 6, EventGroupType::PrivateEventGroupsOnly);
  storage.addEventGroups(more_live_updates);
  storage.updateEventGroupStorageMaps(more_live_updates);
  state_machine.toggle_more_event_groups_to_add_(false);
  for (const auto& [key, val] : more_live_updates) {
    EventGroupId eg_id(concordUtils::fromBigEndianBuffer<uint64_t>(key.data()));
    concord::thin_replica::SubEventGroupUpdate update{eg_id, val};
    buffer.updateEventGroupSubBuffers(update);
  }
  state_machine.set_expected_last_event_group_to_send(12u);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5 + 1;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToUpdateHashesWithGap) {
  FakeStorage storage{generate_kvp(1, kLastBlockId)};
  storage.genesis_block_id = 1;
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

TEST(thin_replica_server_test, SubscribeToPrivateEventGroupUpdateHashesWithGap) {
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId + 5, EventGroupType::PrivateEventGroupsOnly));
  auto storage_egs = storage.getEventGroups();
  EventGroupMap live_updates = storage_egs;
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Hash> state_machine{storage, live_updates, 1, true};
  state_machine.current_eg_type = EventGroupType::PrivateEventGroupsOnly;
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
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Hash>, Hash>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);
  auto more_live_updates =
      generateEventGroupMap(kLastEventGroupId + 6, kLastEventGroupId + 8, EventGroupType::PrivateEventGroupsOnly);
  storage.addEventGroups(more_live_updates);
  storage.updateEventGroupStorageMaps(more_live_updates);
  state_machine.toggle_more_event_groups_to_add_(false);
  for (const auto& [key, val] : more_live_updates) {
    EventGroupId eg_id(concordUtils::fromBigEndianBuffer<uint64_t>(key.data()));
    concord::thin_replica::SubEventGroupUpdate update{eg_id, val};
    buffer.updateEventGroupSubBuffers(update);
  }
  state_machine.set_expected_last_event_group_to_send(14u);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5 + 3;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicEventGroupUpdateHashesSyncedWithGap) {
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId + 5, EventGroupType::PublicEventGroupsOnly));
  auto storage_egs = storage.getEventGroups();
  EventGroupMap live_updates = storage_egs;
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Hash> state_machine{storage, live_updates, 1, true};
  state_machine.current_eg_type = EventGroupType::PublicEventGroupsOnly;
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
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Hash>, Hash>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);
  auto more_live_updates =
      generateEventGroupMap(kLastEventGroupId + 6, kLastEventGroupId + 8, EventGroupType::PublicEventGroupsOnly);
  storage.addEventGroups(more_live_updates);
  storage.updateEventGroupStorageMaps(more_live_updates);
  state_machine.toggle_more_event_groups_to_add_(false);
  for (const auto& [key, val] : more_live_updates) {
    EventGroupId eg_id(concordUtils::fromBigEndianBuffer<uint64_t>(key.data()));
    concord::thin_replica::SubEventGroupUpdate update{eg_id, val};
    buffer.updateEventGroupSubBuffers(update);
  }
  state_machine.set_expected_last_event_group_to_send(14u);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5 + 3;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicAndPrivateEventGroupUpdateHashesWithGap) {
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId + 5, EventGroupType::PublicAndPrivateEventGroups));
  auto storage_egs = storage.getEventGroups();
  EventGroupMap live_updates = storage_egs;
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Hash> state_machine{storage, live_updates, 1, true};
  state_machine.current_eg_type = EventGroupType::PublicAndPrivateEventGroups;
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
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Hash>, Hash>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);
  auto more_live_updates =
      generateEventGroupMap(kLastEventGroupId + 6, kLastEventGroupId + 8, EventGroupType::PublicAndPrivateEventGroups);
  storage.addEventGroups(more_live_updates);
  storage.updateEventGroupStorageMaps(more_live_updates);
  state_machine.toggle_more_event_groups_to_add_(false);
  for (const auto& [key, val] : more_live_updates) {
    EventGroupId eg_id(concordUtils::fromBigEndianBuffer<uint64_t>(key.data()));
    concord::thin_replica::SubEventGroupUpdate update{eg_id, val};
    buffer.updateEventGroupSubBuffers(update);
  }
  state_machine.set_expected_last_event_group_to_send(14u);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5 + 3;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, ReadState) {
  FakeStorage storage{generate_kvp(1, kLastBlockId)};
  storage.genesis_block_id = 1;
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
  storage.genesis_block_id = 1;
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

TEST(thin_replica_server_test, SubscribeWithOutOfRangeBlockId) {
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
  EXPECT_EQ(status.error_code(), grpc::StatusCode::OUT_OF_RANGE);
}

TEST(thin_replica_server_test, SubscribeWithOutOfRangeEventGroupId) {
  FakeStorage storage(generateEventGroupMap(0, 0, EventGroupType::PrivateEventGroupsOnly));
  auto live_update_event_groups = generateEventGroupMap(0, 0, EventGroupType::PrivateEventGroupsOnly);
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
  EXPECT_EQ(status.error_code(), grpc::StatusCode::OUT_OF_RANGE);
}

TEST(thin_replica_server_test, SubscribeWithPrunedBlockId) {
  FakeStorage storage{generate_kvp(1, 10)};
  storage.genesis_block_id = 5;
  auto live_update_blocks = generate_kvp(0, 0);
  TestStateMachine<Data> state_machine{storage, live_update_blocks, 11};
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
  request.mutable_events()->set_block_id(1);
  auto status =
      replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

TEST(thin_replica_server_test, SubscribeWithPrunedEventGroupIdPrivate) {
  FakeStorage storage(generateEventGroupMap(1, 10, EventGroupType::PrivateEventGroupsOnly));
  storage.updateLatestTableAfterPruning(kClientId, 5);
  auto live_update_event_groups = generateEventGroupMap(0, 0, EventGroupType::PrivateEventGroupsOnly);
  TestStateMachine<Data> state_machine{storage, live_update_event_groups, 11};
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
  request.mutable_event_groups()->set_event_group_id(1);
  auto status =
      replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

TEST(thin_replica_server_test, SubscribeWithPrunedEventGroupIdPublic) {
  FakeStorage storage(generateEventGroupMap(1, 10, EventGroupType::PublicEventGroupsOnly));
  storage.updateLatestTableAfterPruning(kPublicEgIdKey, 5);
  auto live_update_event_groups = generateEventGroupMap(0, 0, EventGroupType::PublicEventGroupsOnly);
  TestStateMachine<Data> state_machine{storage, live_update_event_groups, 11};
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
  request.mutable_event_groups()->set_event_group_id(1);
  auto status =
      replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

TEST(thin_replica_server_test, SubscribeWithPrunedEventGroupIdPublicAndPrivate) {
  FakeStorage storage(generateEventGroupMap(1, 10, EventGroupType::PublicAndPrivateEventGroups));
  storage.updateLatestTableAfterPruning(kPublicEgIdKey, 2);
  storage.updateLatestTableAfterPruning(kClientId, 3);
  auto live_update_event_groups = generateEventGroupMap(0, 0, EventGroupType::PublicAndPrivateEventGroups);
  TestStateMachine<Data> state_machine{storage, live_update_event_groups, 11};
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
  request.mutable_event_groups()->set_event_group_id(1);
  auto status =
      replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
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

// Subscribing to legacy events will eventually return event groups
TEST(thin_replica_server_test, SubscribeToUpdatesLegacyTransition) {
  // Storage contains legacy events and event groups
  FakeStorage storage(generate_kvp(1, kLastBlockId),
                      generateEventGroupMap(1, kLastEventGroupId + 5, EventGroupType::PrivateEventGroupsOnly));
  EXPECT_EQ(storage.getLastBlockId(), kLastBlockId + kLastEventGroupId + 5);
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  EXPECT_EQ(storage.getOldestEventGroupBlockId(), kLastBlockId + 1);

  // Live updates can contain event groups only
  auto live_update_event_groups =
      generateEventGroupMap(kLastEventGroupId + 1, kLastEventGroupId + 5, EventGroupType::PrivateEventGroupsOnly);
  TestStateMachine<Data> state_machine{storage, live_update_event_groups, 1, true};
  state_machine.current_eg_type = EventGroupType::PublicEventGroupsOnly;
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  auto total_legacy_blocks = kLastBlockId;
  auto total_event_groups = kLastEventGroupId + 5;
  auto total_updates = total_legacy_blocks + total_event_groups;

  // Setup ThinReplicaServer
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;

  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());
  TestServerContext context;

  // ThinReplica legacy events request
  SubscriptionRequest request;
  request.mutable_events()->set_block_id(1);

  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);
  auto live_updates =
      generateEventGroupMap(kLastEventGroupId + 6, kLastEventGroupId + 6, EventGroupType::PrivateEventGroupsOnly);
  storage.addEventGroups(live_updates);
  storage.updateEventGroupStorageMaps(live_updates);
  total_updates++;
  total_event_groups++;
  state_machine.toggle_more_event_groups_to_add_(false);
  for (const auto& [key, val] : live_updates) {
    EventGroupId eg_id(concordUtils::fromBigEndianBuffer<uint64_t>(key.data()));
    concord::thin_replica::SubEventGroupUpdate update{eg_id, val};
    buffer.updateEventGroupSubBuffers(update);
  }
  state_machine.set_expected_last_event_group_to_send(12u);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numUpdatesReceived(), total_updates);
  EXPECT_EQ(state_machine.numLegacyBlocksReceived(), total_legacy_blocks);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

// Subscribing to legacy events with storage that contains event groups only returns event groups
TEST(thin_replica_server_test, SubscribeToUpdatesLegacyRequestEventGroups) {
  // Storage contains event groups only
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId + 10, EventGroupType::PrivateEventGroupsOnly));
  auto storage_egs = storage.getEventGroups();
  // Live updates can contain event groups only
  EventGroupMap live_updates = storage_egs;
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 10);
  EXPECT_EQ(storage.getOldestEventGroupBlockId(), 1);
  TestStateMachine<Data> state_machine{storage, live_updates, 1, true};
  state_machine.current_eg_type = EventGroupType::PrivateEventGroupsOnly;
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  auto total_event_groups = kLastEventGroupId + 10;

  // Setup ThinReplicaServer
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;

  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());
  TestServerContext context;

  // ThinReplica legacy events request
  SubscriptionRequest request;
  request.mutable_events()->set_block_id(3);

  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);
  auto more_live_updates =
      generateEventGroupMap(kLastEventGroupId + 11, kLastEventGroupId + 11, EventGroupType::PrivateEventGroupsOnly);
  storage.addEventGroups(more_live_updates);
  storage.updateEventGroupStorageMaps(more_live_updates);
  state_machine.set_expected_last_event_group_to_send(17u);
  total_event_groups++;
  state_machine.toggle_more_event_groups_to_add_(false);
  for (const auto& [key, val] : more_live_updates) {
    EventGroupId eg_id(concordUtils::fromBigEndianBuffer<uint64_t>(key.data()));
    concord::thin_replica::SubEventGroupUpdate update{eg_id, val};
    buffer.updateEventGroupSubBuffers(update);
  }
  state_machine.set_expected_last_event_group_to_send(17u);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numLegacyBlocksReceived(), 0);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}
}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
