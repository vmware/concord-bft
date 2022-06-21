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

#include <grpcpp/impl/codegen/server_context.h>

#include <iterator>
#include <map>
#include <sstream>
#include <string>
#include "thin-replica-server/grpc_services.hpp"
#include "Logger.hpp"
#include "concord_kvbc.pb.h"

#include "kv_types.hpp"
#include "kvbc_app_filter/kvbc_key_types.h"
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
static inline const std::string kClientId1{"TEST_ID1"};
static inline const std::string kClientId2{"TEST_ID2"};
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

EventGroup generateEventGroup(EventGroupId eg_id, bool is_public, const std::string& trid) {
  concord::kvbc::categorization::Event event;
  EventGroup event_group;
  // we are adding just one event per event group
  const std::string data = "val eg_id#" + std::to_string(eg_id);
  std::vector<std::string> trid_list = {};
  if (!is_public) trid_list.emplace_back(trid);
  event.data = CreateTridKvbValue(data, trid_list);
  event.tags = trid_list;
  event_group.events.emplace_back(event);
  return event_group;
}

EventGroupMap generateEventGroupMap(EventGroupId start,
                                    EventGroupId end,
                                    EventGroupType eg_type,
                                    const std::string& trid = kClientId1) {
  EventGroupMap event_group_map;
  if (start == end && end == 0) return event_group_map;
  if (eg_type == EventGroupType::PublicAndPrivateEventGroups) {
    for (EventGroupId eg_id = start; eg_id <= end; ++eg_id) {
      bool is_public = false;
      if (eg_id % 2 == 0) {
        is_public = true;
      }
      EventGroup event_group = generateEventGroup(eg_id, is_public, trid);
      auto key = concordUtils::toBigEndianStringBuffer(eg_id);
      event_group_map[key] = event_group;
    }
    return event_group_map;
  }
  bool is_public = false;
  if (eg_type == EventGroupType::PublicEventGroupsOnly) is_public = true;
  for (EventGroupId eg_id = start; eg_id <= end; ++eg_id) {
    EventGroup event_group = generateEventGroup(eg_id, is_public, trid);
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
        msg << "The kExecutionEventGroupTagCategory category key: " << key.data() << " doesn't exist in storage!";
        throw std::runtime_error(msg.str());
      }
      LOG_DEBUG(logger,
                "getLatest: key: " << key.data() << " ,category_id: TAG table"
                                   << " val: " << tag_table.at(key));
      return concord::kvbc::categorization::ImmutableValue{{block_id, tag_table.at(key)}};
    } else if (category_id == concord::kvbc::categorization::kExecutionEventGroupDataCategory) {
      // get event group
      std::vector<uint8_t> output;
      const auto key_idx = concordUtils::fromBigEndianBuffer<uint64_t>(key.data());
      if (key_idx == 0 || (key_idx - 1 >= eg_db_.size() && !is_pruned_)) {
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
      // add cid
      std::string batch_cid = "temp_batch_cid" + std::to_string(block_id);
      concord::kvbc::categorization::VersionedUpdates internal;
      internal.addUpdate(std::string(cid_key_), std::move(batch_cid));
      updates.add(concord::kvbc::categorization::kConcordInternalCategoryId, std::move(internal));
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

  uint64_t getValueFromLatestTable(const std::string& key) const {
    auto logger = logging::getLogger("thin_replica_server_test");
    const auto opt = getLatest(concord::kvbc::categorization::kExecutionEventGroupLatestCategory, key);
    if (not opt) {
      LOG_DEBUG(logger, "External event group ID for key \"" << key << "\" doesn't exist yet");
      // In case there are no public or private event groups for a client, return 0.
      // Note: `0` is an invalid event group id
      return 0;
    }
    auto val = std::get_if<concord::kvbc::categorization::VersionedValue>(&(opt.value()));
    if (not val) {
      std::stringstream msg;
      msg << "Failed to convert stored external event group id for key \"" << key << "\" to versioned value";
      throw std::runtime_error(msg.str());
    }
    return concordUtils::fromBigEndianBuffer<uint64_t>(val->data.data());
  }

  void updateLatestTable(const std::string& key, uint64_t id = 1) {
    // Let's save the latest private/trid specific event group id
    bool is_pruned = concordUtils::fromBigEndianBuffer<uint64_t>(latest_table[key + "_oldest"].data()) == 0;
    auto logger = logging::getLogger("thin_replica_server_test");
    if (latest_table[key + "_oldest"].empty() || is_pruned) {
      if (is_pruned && !latest_table[key + "_newest"].empty()) {
        id = concordUtils::fromBigEndianBuffer<uint64_t>(latest_table[key + "_newest"].data()) + 1;
      }
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

  // Note: The caller needs to know that all requested event groups are either public or private
  void prune(uint8_t num_egs, const std::string& trid) {
    is_pruned_ = true;
    auto oldest = concordUtils::fromBigEndianBuffer<uint64_t>(latest_table[trid + "_oldest"].data());
    auto num_available =
        concordUtils::fromBigEndianBuffer<uint64_t>(latest_table[trid + "_newest"].data()) - oldest + 1;
    if (num_egs > num_available) {
      ADD_FAILURE() << "Test shouldn't request to prune more than available " << num_available;
      return;
    }
    // Prune data table
    auto num_egs_to_rem = num_egs;
    auto it = eg_db_.begin();
    while (it != eg_db_.end() && num_egs_to_rem > 0) {
      eg_db_.erase(it);
      num_egs_to_rem--;
      it++;
    }
    // Prune tag table
    for (unsigned i = oldest; i < (oldest + num_egs); ++i) {
      tag_table.erase(trid + kTagTableKeySeparator + concordUtils::toBigEndianStringBuffer<uint64_t>(i));
    }
    // Update latest table
    if (num_egs == num_available) {
      latest_table[trid + "_oldest"] = concordUtils::toBigEndianStringBuffer<uint64_t>(0);
    } else {
      latest_table[trid + "_oldest"] = concordUtils::toBigEndianStringBuffer<uint64_t>(oldest + num_egs);
    }
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
      EventGroupId global_eg_id(concordUtils::fromBigEndianBuffer<uint64_t>(eg_id_str.data()));
      for (const auto& event : event_group.events) {
        if (not event.tags.empty()) {
          for (const auto& tag : event.tags) {
            updateLatestTable(tag);
            updateTagTable(tag, global_eg_id);
          }
          if (std::find(event.tags.begin(), event.tags.end(), kClientId2) != event.tags.end()) {
            total_egs_for_other_clients++;
          }
        } else {
          // update latest and tag tables for public event groups
          updateLatestTable(kPublicEgIdKey);
          updateTagTable(kPublicEgIdKey, global_eg_id);
        }
      }
      updateLatestTable(kGlobalEgIdKey, global_eg_id);
    }
  }

 public:
  BlockId genesis_block_id{0};
  size_t total_egs_for_other_clients{0};

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
  const std::string cid_key_{concord::kvbc::kKvbKeyCorrelationId};
  bool is_pruned_ = false;
};

class TestServerContext {
  std::multimap<std::string, std::string> metadata_ = {{"client_id", kClientId1}};
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
  TestStateMachine(FakeStorage& storage,
                   const BlockMap& live_update_blocks,
                   uint64_t start_block_id,
                   bool more_blocks_to_add = false)
      : storage_(storage),
        live_update_blocks_(std::cbegin(live_update_blocks), std::cend(live_update_blocks)),
        current_block_to_send_(start_block_id),
        more_blocks_to_add_(more_blocks_to_add) {
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
    last_event_group_to_send_ = storage_.getValueFromLatestTable(kGlobalEgIdKey + "_newest");
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
      EXPECT_EQ(last_event_group_to_send_, current_event_group_to_send_ + storage_.total_egs_for_other_clients);
    }
  }

  void set_expected_last_block_to_send(BlockId block_id) { last_block_to_send_ = block_id; }
  void set_expected_last_event_group_to_send(EventGroupId eg_id) { last_event_group_to_send_ = eg_id; }
  void toggle_more_blocks_to_add(bool val) { more_blocks_to_add_ = val; }
  void toggle_more_event_groups_to_add(bool val) { more_event_groups_to_add_ = val; }

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
        if ((current_block_to_send_ == last_block_to_send_ - 1) && not more_blocks_to_add_) {
          on_finished_dropping_blocks();
        } else if (live_buffer_->Full() ||
                   (!(live_buffer_->Empty()) && (live_buffer_->oldestBlockId() > (storage_.getLastBlockId() + 1)))) {
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
      if (current_event_group_to_send_ + storage_.total_egs_for_other_clients == last_event_group_to_send_) {
        return !return_false_on_last_event_group_;
      }

      if (live_buffer_) {
        if ((current_event_group_to_send_ + storage_.total_egs_for_other_clients == (last_event_group_to_send_ - 1)) &&
            not more_event_groups_to_add_) {
          on_finished_dropping_event_groups();
        } else if (live_buffer_->Full() ||
                   (!(live_buffer_->EmptyEventGroupQueue()) &&
                    live_buffer_->oldestEventGroupId() > (storage_.getLastEventGroupId() + 1))) {
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
    auto eg_id = current_event_group_to_send_ + storage_.total_egs_for_other_clients + 1;
    auto gap_event_groups = generateEventGroupMap(eg_id, last_event_group_to_send_, current_eg_type);
    storage_.addEventGroups(gap_event_groups);
    storage_.updateEventGroupStorageMaps(gap_event_groups);
  }

  void on_finished_dropping_blocks() {
    auto block = generate_block(storage_.getLastBlockId() + 1);
    live_buffer_->Push({block.first, "cid", block.second});
  }

  void on_finished_dropping_event_groups() {
    auto logger = logging::getLogger("thin_replica_server_test");
    std::scoped_lock sl(mtx_);
    auto eg_id = current_event_group_to_send_ + storage_.total_egs_for_other_clients + 1;
    auto live_update = generateEventGroupMap(eg_id, eg_id, current_eg_type);
    storage_.addEventGroups(live_update);
    storage_.updateEventGroupStorageMaps(live_update);
    for (const auto& [key, val] : live_update) {
      EventGroupId eg_id(concordUtils::fromBigEndianBuffer<uint64_t>(key.data()));
      concord::thin_replica::SubEventGroupUpdate update{eg_id, val};
      live_buffer_->PushEventGroup(update);
      LOG_INFO(logger, " Pushed to live update queue eg: " << eg_id);
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
  bool more_blocks_to_add_{false};
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

// Note that, if a test case uses addMoreEventGroups, it should call set_expected_last_event_group_to_send on the
// case's TestStateMachine instance with a parameter of ("end" + 1), where "end" is the highest value that the test
// case calls addMoreEventGroups with. This is necessary to enable the TestStateMachine to detect where the test case
// should stop the subscription. Furthermore, this set_expected_last_event_group_to_send call must be completed before
// the test casee begins the thin replica subscription to avoid race conditions with it.
template <typename T>
void addMoreEventGroups(const EventGroupId start,
                        const EventGroupId end,
                        const EventGroupType type,
                        FakeStorage& storage,
                        TestStateMachine<T>& state_machine,
                        TestSubBufferList<T>& buffer,
                        const std::string& trid = kClientId1,
                        bool even_more_event_groups_to_add_after_these = false) {
  auto more_live_updates = generateEventGroupMap(start, end, type, trid);
  storage.addEventGroups(more_live_updates);
  storage.updateEventGroupStorageMaps(more_live_updates);
  state_machine.toggle_more_event_groups_to_add(even_more_event_groups_to_add_after_these);
  for (const auto& [key, val] : more_live_updates) {
    EventGroupId eg_id(concordUtils::fromBigEndianBuffer<uint64_t>(key.data()));
    concord::thin_replica::SubEventGroupUpdate update{eg_id, val};
    buffer.updateEventGroupSubBuffers(update);
  }
}

TEST(thin_replica_server_test, SubscribeToFirstBlockNotInStorage) {
  // Initialize storage and live update queue
  FakeStorage storage{generate_kvp(0, 0)};
  storage.genesis_block_id = 1;
  EXPECT_EQ(storage.getLastBlockId(), 0);
  auto live_update_blocks = generate_kvp(0, 0);
  EXPECT_EQ(live_update_blocks.size(), 0);
  TestStateMachine<Data> state_machine{storage, live_update_blocks, 1};
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_events()->set_block_id(1u);

  // subscribe
  std::chrono::seconds data_timeout = 3s;
  auto stream_out = async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = stream_out.wait_for(data_timeout);
  EXPECT_EQ(status, std::future_status::timeout);
  context.TryCancel();
}

TEST(thin_replica_server_test, SubscribeToNextBlockNotInStorage) {
  // Initialize storage and live update queue
  FakeStorage storage{generate_kvp(1, kLastBlockId)};
  storage.genesis_block_id = 1;
  EXPECT_EQ(storage.getLastBlockId(), kLastBlockId);
  auto live_update_blocks = generate_kvp(5, 5);
  TestStateMachine<Data> state_machine{storage, live_update_blocks, 6, true};
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_events()->set_block_id(6u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);
  // add block ID `end + 1` i.e., 6th block to storage and live update queue
  auto more_live_updates = generate_kvp(kLastBlockId + 1, kLastBlockId + 1);
  storage.addBlocks(more_live_updates);
  state_machine.toggle_more_blocks_to_add(false);
  for (const auto& [key, val] : more_live_updates) {
    concord::thin_replica::SubUpdate update{key, "cid", val};
    buffer.updateSubBuffers(update);
  }
  state_machine.set_expected_last_block_to_send(7u);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
}

TEST(thin_replica_server_test, SubscribeToNextBlockNotInStorageTimeout) {
  // Initialize storage and live update queue
  FakeStorage storage{generate_kvp(1, kLastBlockId)};
  storage.genesis_block_id = 1;
  EXPECT_EQ(storage.getLastBlockId(), kLastBlockId);
  auto live_update_blocks = generate_kvp(5, 5);
  TestStateMachine<Data> state_machine{storage, live_update_blocks, 6};
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_events()->set_block_id(6u);

  // subscribe
  std::chrono::seconds data_timeout = 3s;
  auto stream_out = async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = stream_out.wait_for(data_timeout);
  EXPECT_EQ(status, std::future_status::timeout);
  context.TryCancel();
}

TEST(thin_replica_server_test, SubscribeToFirstEventGroupNotInStorage) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(0, 0, EventGroupType::PublicAndPrivateEventGroups));
  EXPECT_EQ(storage.getLastEventGroupId(), 0);
  auto live_update_event_groups = generateEventGroupMap(0, 0, EventGroupType::PublicAndPrivateEventGroups);
  EXPECT_EQ(live_update_event_groups.size(), 0);
  TestStateMachine<Data> state_machine{storage, live_update_event_groups, 1};
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1u);

  // subscribe
  std::chrono::seconds data_timeout = 3s;
  auto stream_out = async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = stream_out.wait_for(data_timeout);
  EXPECT_EQ(status, std::future_status::timeout);
  context.TryCancel();
}

TEST(thin_replica_server_test, SubscribeToNextEventGroupNotInStorage) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId, EventGroupType::PublicAndPrivateEventGroups));
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId);
  auto live_update_event_groups = generateEventGroupMap(0, 0, EventGroupType::PublicAndPrivateEventGroups);
  EXPECT_EQ(live_update_event_groups.size(), 0);
  TestStateMachine<Data> state_machine{storage, live_update_event_groups, 6};
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(6u);

  // subscribe
  std::chrono::seconds data_timeout = 3s;
  auto stream_out = async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = stream_out.wait_for(data_timeout);
  EXPECT_EQ(status, std::future_status::timeout);
  context.TryCancel();
}

TEST(thin_replica_server_test, SubscribeToUpdatesAlreadySynced) {
  // Initialize storage and live update queue
  FakeStorage storage{generate_kvp(1, kLastBlockId)};
  storage.genesis_block_id = 1;
  EXPECT_EQ(storage.getLastBlockId(), 5);
  auto live_update_blocks = generate_kvp(kLastBlockId + 1, kLastBlockId + 5);
  TestStateMachine<Data> state_machine{storage, live_update_blocks, 1};
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_events()->set_block_id(1u);

  // subscribe
  auto status =
      replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::OK);
}

TEST(thin_replica_server_test, SubscribeToPrivateEventGroupUpdatesAlreadySynced) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId + 5, EventGroupType::PrivateEventGroupsOnly));
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Data> state_machine{storage, storage_egs, 1, true};
  state_machine.current_eg_type = EventGroupType::PrivateEventGroupsOnly;
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 7);
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add 1 more private event group to storage and live update queue
  addMoreEventGroups(kLastEventGroupId + 6,
                     kLastEventGroupId + 6,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1);

  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5 + 1;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPrivateEventGroupUpdatesAlreadySyncedTwoClients) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId, EventGroupType::PrivateEventGroupsOnly, kClientId1));
  auto client2_egs = generateEventGroupMap(
      kLastEventGroupId + 1, kLastEventGroupId + 5, EventGroupType::PrivateEventGroupsOnly, kClientId2);
  storage.addEventGroups(client2_egs);
  storage.updateEventGroupStorageMaps(client2_egs);
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Data> state_machine{storage, storage_egs, 1, true};
  state_machine.current_eg_type = EventGroupType::PrivateEventGroupsOnly;
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 8);
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add 1 more event group for kClientId2
  addMoreEventGroups(kLastEventGroupId + 6,
                     kLastEventGroupId + 6,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId2,
                     true);
  // add 1 more event group for kClientId1
  addMoreEventGroups(kLastEventGroupId + 7,
                     kLastEventGroupId + 7,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1,
                     false);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 1;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicEventGroupUpdatesAlreadySynced) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId + 5, EventGroupType::PublicEventGroupsOnly));
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Data> state_machine{storage, storage_egs, 1, true};
  state_machine.current_eg_type = EventGroupType::PublicEventGroupsOnly;
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 7);
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add 1 more event group for kClientId2
  addMoreEventGroups(kLastEventGroupId + 6,
                     kLastEventGroupId + 6,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5 + 1;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicEventGroupUpdatesAlreadySyncedTwoClients) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId, EventGroupType::PublicEventGroupsOnly));
  auto client2_egs = generateEventGroupMap(
      kLastEventGroupId + 1, kLastEventGroupId + 5, EventGroupType::PrivateEventGroupsOnly, kClientId2);
  storage.addEventGroups(client2_egs);
  storage.updateEventGroupStorageMaps(client2_egs);
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Data> state_machine{storage, storage_egs, 1, true};
  state_machine.current_eg_type = EventGroupType::PublicEventGroupsOnly;
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 8);
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add 1 more event group for kClientId2
  addMoreEventGroups(kLastEventGroupId + 6,
                     kLastEventGroupId + 6,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId2,
                     true);
  // add 1 more public event group
  addMoreEventGroups(kLastEventGroupId + 7,
                     kLastEventGroupId + 7,
                     EventGroupType::PublicEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1,
                     false);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 1;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicAndPrivateEventGroupUpdatesAlreadySynced) {
  // Initialize storage and live update queue
  FakeStorage storage(
      generateEventGroupMap(1, kLastEventGroupId + 5, EventGroupType::PublicAndPrivateEventGroups, kClientId1));
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Data> state_machine{storage, storage_egs, 1, true};
  state_machine.current_eg_type = EventGroupType::PublicAndPrivateEventGroups;
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 7);
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add 1 more event group for kClientId1
  addMoreEventGroups(kLastEventGroupId + 6,
                     kLastEventGroupId + 6,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5 + 1;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicAndPrivateEventGroupUpdatesAlreadySyncedTwoClients) {
  // Initialize storage and live update queue
  FakeStorage storage(
      generateEventGroupMap(1, kLastEventGroupId, EventGroupType::PublicAndPrivateEventGroups, kClientId1));
  auto client2_egs = generateEventGroupMap(
      kLastEventGroupId + 1, kLastEventGroupId + 5, EventGroupType::PrivateEventGroupsOnly, kClientId2);
  storage.addEventGroups(client2_egs);
  storage.updateEventGroupStorageMaps(client2_egs);
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Data> state_machine{storage, storage_egs, 1, true};
  state_machine.current_eg_type = EventGroupType::PublicAndPrivateEventGroups;
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 8);
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add 1 more event group for kClientId2
  addMoreEventGroups(kLastEventGroupId + 6,
                     kLastEventGroupId + 6,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId2,
                     true);
  // add 1 more event group for kClientId1
  addMoreEventGroups(kLastEventGroupId + 7,
                     kLastEventGroupId + 7,
                     EventGroupType::PublicAndPrivateEventGroups,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1,
                     false);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 1;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToUpdatesWithGap) {
  // Initialize storage and live update queue
  FakeStorage storage{generate_kvp(1, kLastBlockId)};
  storage.genesis_block_id = 1;
  auto live_update_blocks = generate_kvp(kLastBlockId + 2, kLastBlockId + 5);
  TestStateMachine<Data> state_machine{storage, live_update_blocks, 1};
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_events()->set_block_id(1u);

  // subscribe
  auto status =
      replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::OK);
}

TEST(thin_replica_server_test, SubscribeToPrivateEventGroupUpdatesWithGap) {
  // Initialize storage and live update queue
  FakeStorage storage(
      generateEventGroupMap(1, kLastEventGroupId + 5, EventGroupType::PrivateEventGroupsOnly, kClientId1));
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Data> state_machine{storage, storage_egs, 1, true};
  state_machine.current_eg_type = EventGroupType::PrivateEventGroupsOnly;
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 9);
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add 1 gap update yo storage
  auto gap_updates =
      generateEventGroupMap(kLastEventGroupId + 6, kLastEventGroupId + 6, EventGroupType::PrivateEventGroupsOnly);
  storage.addEventGroups(gap_updates);
  storage.updateEventGroupStorageMaps(gap_updates);

  // add 2 more event group for kClientId1
  addMoreEventGroups(kLastEventGroupId + 7,
                     kLastEventGroupId + 8,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5 + 3;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPrivateEventGroupUpdatesWithGapTwoClients) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId, EventGroupType::PrivateEventGroupsOnly, kClientId1));
  auto client2_egs = generateEventGroupMap(
      kLastEventGroupId + 1, kLastEventGroupId + 5, EventGroupType::PrivateEventGroupsOnly, kClientId2);
  storage.addEventGroups(client2_egs);
  storage.updateEventGroupStorageMaps(client2_egs);
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Data> state_machine{storage, storage_egs, 1, true};
  state_machine.current_eg_type = EventGroupType::PrivateEventGroupsOnly;
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 14);
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add 1 gap update to storage
  auto gap_updates = generateEventGroupMap(
      kLastEventGroupId + 6, kLastEventGroupId + 6, EventGroupType::PrivateEventGroupsOnly, kClientId1);
  storage.addEventGroups(gap_updates);
  storage.updateEventGroupStorageMaps(gap_updates);

  // add 2 more event groups for kClientId2
  addMoreEventGroups(kLastEventGroupId + 7,
                     kLastEventGroupId + 8,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId2,
                     true);
  // add 2 more event groups for kClientId1
  addMoreEventGroups(kLastEventGroupId + 9,
                     kLastEventGroupId + 10,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1,
                     true);
  // add 2 more event groups for kClientId2
  addMoreEventGroups(kLastEventGroupId + 11,
                     kLastEventGroupId + 12,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId2,
                     true);
  // add 1 more event group for kClientId1
  addMoreEventGroups(kLastEventGroupId + 13,
                     kLastEventGroupId + 13,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1,
                     false);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 4;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicEventGroupUpdatesWithGap) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId + 5, EventGroupType::PublicEventGroupsOnly));
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Data> state_machine{storage, storage_egs, 1, true};
  state_machine.current_eg_type = EventGroupType::PublicEventGroupsOnly;
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 9);
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add 1 gap update to storage
  auto gap_updates =
      generateEventGroupMap(kLastEventGroupId + 6, kLastEventGroupId + 6, EventGroupType::PublicEventGroupsOnly);
  storage.addEventGroups(gap_updates);
  storage.updateEventGroupStorageMaps(gap_updates);

  // add 2 more public event groups
  addMoreEventGroups(kLastEventGroupId + 7,
                     kLastEventGroupId + 8,
                     EventGroupType::PublicEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5 + 3;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicEventGroupUpdatesWithGapTwoClients) {
  // Initialize storage and live update queue with public event groups and private event groups for kClientId2
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId, EventGroupType::PublicEventGroupsOnly));
  auto client2_egs = generateEventGroupMap(
      kLastEventGroupId + 1, kLastEventGroupId + 5, EventGroupType::PrivateEventGroupsOnly, kClientId2);
  storage.addEventGroups(client2_egs);
  storage.updateEventGroupStorageMaps(client2_egs);
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Data> state_machine{storage, storage_egs, 1, true};
  state_machine.current_eg_type = EventGroupType::PublicEventGroupsOnly;
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 14);
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add gap updates to storage
  auto gap_updates =
      generateEventGroupMap(kLastEventGroupId + 6, kLastEventGroupId + 6, EventGroupType::PublicEventGroupsOnly);
  storage.addEventGroups(gap_updates);
  storage.updateEventGroupStorageMaps(gap_updates);
  // add 2 more event groups for kClientId2
  addMoreEventGroups(kLastEventGroupId + 7,
                     kLastEventGroupId + 8,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId2,
                     true);
  // add 2 more public event groups
  addMoreEventGroups(kLastEventGroupId + 9,
                     kLastEventGroupId + 10,
                     EventGroupType::PublicEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1,
                     true);
  // add 2 more event groups for kClientId2
  addMoreEventGroups(kLastEventGroupId + 11,
                     kLastEventGroupId + 12,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId2,
                     true);
  // add 2 more public event groups
  addMoreEventGroups(kLastEventGroupId + 13,
                     kLastEventGroupId + 13,
                     EventGroupType::PublicEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1,
                     false);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 4;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicAndPrivateEventGroupUpdatesWithGap) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId + 5, EventGroupType::PublicAndPrivateEventGroups));
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Data> state_machine{storage, storage_egs, 1, true};
  state_machine.current_eg_type = EventGroupType::PublicAndPrivateEventGroups;
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 9);
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add gap event groups to storage
  auto gap_updates =
      generateEventGroupMap(kLastEventGroupId + 6, kLastEventGroupId + 6, EventGroupType::PublicAndPrivateEventGroups);
  storage.addEventGroups(gap_updates);
  storage.updateEventGroupStorageMaps(gap_updates);

  // add 2 more public and private event groups
  addMoreEventGroups(kLastEventGroupId + 7,
                     kLastEventGroupId + 8,
                     EventGroupType::PublicAndPrivateEventGroups,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5 + 3;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicAndPrivateEventGroupUpdatesWithGapTwoClients) {
  // Initialize storage and live update queue
  FakeStorage storage(
      generateEventGroupMap(1, kLastEventGroupId, EventGroupType::PublicAndPrivateEventGroups, kClientId1));
  auto client2_egs = generateEventGroupMap(
      kLastEventGroupId + 1, kLastEventGroupId + 5, EventGroupType::PrivateEventGroupsOnly, kClientId2);
  storage.addEventGroups(client2_egs);
  storage.updateEventGroupStorageMaps(client2_egs);
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Data> state_machine{storage, storage_egs, 1, true};
  state_machine.current_eg_type = EventGroupType::PublicAndPrivateEventGroups;
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 14);
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add gap event groups
  auto gap_updates = generateEventGroupMap(
      kLastEventGroupId + 6, kLastEventGroupId + 6, EventGroupType::PublicAndPrivateEventGroups, kClientId1);
  storage.addEventGroups(gap_updates);
  storage.updateEventGroupStorageMaps(gap_updates);

  // add 2 more event groups for kClientId2
  addMoreEventGroups(kLastEventGroupId + 7,
                     kLastEventGroupId + 8,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId2,
                     true);
  // add 2 more public and private event groups
  addMoreEventGroups(kLastEventGroupId + 9,
                     kLastEventGroupId + 10,
                     EventGroupType::PublicAndPrivateEventGroups,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1,
                     true);
  // add 2 more private event groups
  addMoreEventGroups(kLastEventGroupId + 11,
                     kLastEventGroupId + 12,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId2,
                     true);
  // add 1 more public and/or private event group
  addMoreEventGroups(kLastEventGroupId + 13,
                     kLastEventGroupId + 13,
                     EventGroupType::PublicAndPrivateEventGroups,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1,
                     false);

  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 4;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToUpdatesWithGapFromTheMiddleBlock) {
  // Initialize storage and live update queue
  FakeStorage storage{generate_kvp(1, kLastBlockId)};
  storage.genesis_block_id = 1;
  auto live_update_blocks = generate_kvp(kLastBlockId + 2, kLastBlockId + 5);
  TestStateMachine<Data> state_machine{storage, live_update_blocks, 3};
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_events()->set_block_id(3u);

  // subscribe
  auto status =
      replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::OK);
}

TEST(thin_replica_server_test, SubscribeToPrivateEventGroupUpdatesWithGapFromTheMiddleBlock) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId, EventGroupType::PrivateEventGroupsOnly));
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId);
  TestStateMachine<Data> state_machine{storage, storage_egs, 3, true};
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 6);
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(3u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add gap updates to storage
  auto gap_updates = generateEventGroupMap(
      kLastEventGroupId + 1, kLastEventGroupId + 2, EventGroupType::PrivateEventGroupsOnly, kClientId1);
  storage.addEventGroups(gap_updates);
  storage.updateEventGroupStorageMaps(gap_updates);

  // add 4 more private event groups
  addMoreEventGroups(kLastEventGroupId + 2,
                     kLastEventGroupId + 5,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicEventGroupUpdatesWithGapFromTheMiddleBlock) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId, EventGroupType::PublicEventGroupsOnly));
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId);
  TestStateMachine<Data> state_machine{storage, storage_egs, 3, true};
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 6);
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(3u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add gap updates to storage
  auto gap_updates =
      generateEventGroupMap(kLastEventGroupId + 1, kLastEventGroupId + 2, EventGroupType::PublicEventGroupsOnly);
  storage.addEventGroups(gap_updates);
  storage.updateEventGroupStorageMaps(gap_updates);

  // add 4 more public event groups
  addMoreEventGroups(kLastEventGroupId + 2,
                     kLastEventGroupId + 5,
                     EventGroupType::PublicEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicAndPrivateEventGroupUpdatesWithGapFromTheMiddleBlock) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId, EventGroupType::PublicAndPrivateEventGroups));
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId);
  TestStateMachine<Data> state_machine{storage, storage_egs, 3, true};
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 6);
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(3u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add gap updates to storage
  auto gap_updates =
      generateEventGroupMap(kLastEventGroupId + 1, kLastEventGroupId + 2, EventGroupType::PublicAndPrivateEventGroups);
  storage.addEventGroups(gap_updates);
  storage.updateEventGroupStorageMaps(gap_updates);

  // add 4 more public and private event groups
  addMoreEventGroups(kLastEventGroupId + 2,
                     kLastEventGroupId + 5,
                     EventGroupType::PublicAndPrivateEventGroups,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToUpdateHashesAlreadySynced) {
  // Initialize storage and live update queue
  FakeStorage storage{generate_kvp(1, kLastBlockId)};
  storage.genesis_block_id = 1;
  auto live_update_blocks = generate_kvp(kLastBlockId + 1, kLastBlockId + 5);
  TestStateMachine<Hash> state_machine{storage, live_update_blocks, 1};
  TestSubBufferList<Hash> buffer{state_machine};
  TestServerWriter<Hash> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_events()->set_block_id(1u);

  // subscribe
  auto status =
      replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Hash>, Hash>(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::OK);
}

TEST(thin_replica_server_test, SubscribeToPrivateEventGroupUpdateHashesAlreadySynced) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId + 5, EventGroupType::PrivateEventGroupsOnly));
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Hash> state_machine{storage, storage_egs, 1, true};
  state_machine.current_eg_type = EventGroupType::PrivateEventGroupsOnly;
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 7);
  TestSubBufferList<Hash> buffer{state_machine};
  TestServerWriter<Hash> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Hash>, Hash>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add 2 more private event groups
  addMoreEventGroups(kLastEventGroupId + 6,
                     kLastEventGroupId + 6,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5 + 1;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPrivateEventGroupUpdateHashesAlreadySyncedTwoClients) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId, EventGroupType::PrivateEventGroupsOnly, kClientId1));
  auto client2_egs = generateEventGroupMap(
      kLastEventGroupId + 1, kLastEventGroupId + 5, EventGroupType::PrivateEventGroupsOnly, kClientId2);
  storage.addEventGroups(client2_egs);
  storage.updateEventGroupStorageMaps(client2_egs);
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Hash> state_machine{storage, storage_egs, 1, true};
  state_machine.current_eg_type = EventGroupType::PrivateEventGroupsOnly;
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 8);
  TestSubBufferList<Hash> buffer{state_machine};
  TestServerWriter<Hash> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Hash>, Hash>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add 1 more event group for kClientId2
  addMoreEventGroups(kLastEventGroupId + 6,
                     kLastEventGroupId + 6,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId2,
                     true);
  // add 1 more event group for kClientId1
  addMoreEventGroups(kLastEventGroupId + 7,
                     kLastEventGroupId + 7,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1,
                     false);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 1;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicEventGroupUpdateHashesAlreadySynced) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId + 5, EventGroupType::PublicEventGroupsOnly));
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Hash> state_machine{storage, storage_egs, 1, true};
  state_machine.current_eg_type = EventGroupType::PublicEventGroupsOnly;
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 7);
  TestSubBufferList<Hash> buffer{state_machine};
  TestServerWriter<Hash> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Hash>, Hash>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add 1 more event group for kClientId1
  addMoreEventGroups(kLastEventGroupId + 6,
                     kLastEventGroupId + 6,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1);

  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5 + 1;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicEventGroupUpdateHashesAlreadySyncedTwoClients) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId, EventGroupType::PublicEventGroupsOnly));
  auto client2_egs = generateEventGroupMap(
      kLastEventGroupId + 1, kLastEventGroupId + 5, EventGroupType::PrivateEventGroupsOnly, kClientId2);
  storage.addEventGroups(client2_egs);
  storage.updateEventGroupStorageMaps(client2_egs);
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Hash> state_machine{storage, storage_egs, 1, true};
  state_machine.current_eg_type = EventGroupType::PublicEventGroupsOnly;
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 8);
  TestSubBufferList<Hash> buffer{state_machine};
  TestServerWriter<Hash> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Hash>, Hash>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add 1 more event group for kClientId2
  addMoreEventGroups(kLastEventGroupId + 6,
                     kLastEventGroupId + 6,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId2,
                     true);

  // add 1 more public event group
  addMoreEventGroups(kLastEventGroupId + 7,
                     kLastEventGroupId + 7,
                     EventGroupType::PublicEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1,
                     false);

  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 1;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicAndPrivateEventGroupUpdateHashesAlreadySynced) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId + 5, EventGroupType::PublicAndPrivateEventGroups));
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Hash> state_machine{storage, storage_egs, 1, true};
  state_machine.current_eg_type = EventGroupType::PublicAndPrivateEventGroups;
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 7);
  TestSubBufferList<Hash> buffer{state_machine};
  TestServerWriter<Hash> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Hash>, Hash>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add 1 more event group for kClientId1
  addMoreEventGroups(kLastEventGroupId + 6,
                     kLastEventGroupId + 6,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5 + 1;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicAndPrivateEventGroupUpdateHashesAlreadySyncedTwoClients) {
  // Initialize storage and live update queue
  FakeStorage storage(
      generateEventGroupMap(1, kLastEventGroupId, EventGroupType::PublicAndPrivateEventGroups, kClientId1));
  auto client2_egs = generateEventGroupMap(
      kLastEventGroupId + 1, kLastEventGroupId + 5, EventGroupType::PrivateEventGroupsOnly, kClientId2);
  storage.addEventGroups(client2_egs);
  storage.updateEventGroupStorageMaps(client2_egs);
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Hash> state_machine{storage, storage_egs, 1, true};
  state_machine.current_eg_type = EventGroupType::PublicAndPrivateEventGroups;
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 8);
  TestSubBufferList<Hash> buffer{state_machine};
  TestServerWriter<Hash> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Hash>, Hash>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add 1 more event group for kClientId2
  addMoreEventGroups(kLastEventGroupId + 6,
                     kLastEventGroupId + 6,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId2,
                     true);
  // add 1 more event group for kClientId1
  addMoreEventGroups(kLastEventGroupId + 7,
                     kLastEventGroupId + 7,
                     EventGroupType::PublicAndPrivateEventGroups,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1,
                     false);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 1;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToUpdateHashesWithGap) {
  // Initialize storage and live update queue
  FakeStorage storage{generate_kvp(1, kLastBlockId)};
  storage.genesis_block_id = 1;
  auto live_update_blocks = generate_kvp(kLastBlockId + 2, kLastBlockId + 5);
  TestStateMachine<Hash> state_machine{storage, live_update_blocks, 1};
  TestSubBufferList<Hash> buffer{state_machine};
  TestServerWriter<Hash> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_events()->set_block_id(1u);

  // subscribe
  auto status =
      replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Hash>, Hash>(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::OK);
}

TEST(thin_replica_server_test, SubscribeToPrivateEventGroupUpdateHashesWithGap) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId + 5, EventGroupType::PrivateEventGroupsOnly));
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Hash> state_machine{storage, storage_egs, 1, true};
  state_machine.current_eg_type = EventGroupType::PrivateEventGroupsOnly;
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 9);
  TestSubBufferList<Hash> buffer{state_machine};
  TestServerWriter<Hash> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Hash>, Hash>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add 1 gap update yo storage
  auto gap_updates =
      generateEventGroupMap(kLastEventGroupId + 6, kLastEventGroupId + 6, EventGroupType::PrivateEventGroupsOnly);
  storage.addEventGroups(gap_updates);
  storage.updateEventGroupStorageMaps(gap_updates);

  // add 2 more event group for kClientId1
  addMoreEventGroups(kLastEventGroupId + 7,
                     kLastEventGroupId + 8,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5 + 3;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPrivateEventGroupUpdateHashesWithGapTwoClients) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId, EventGroupType::PrivateEventGroupsOnly, kClientId1));
  auto client2_egs = generateEventGroupMap(
      kLastEventGroupId + 1, kLastEventGroupId + 5, EventGroupType::PrivateEventGroupsOnly, kClientId2);
  storage.addEventGroups(client2_egs);
  storage.updateEventGroupStorageMaps(client2_egs);
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Hash> state_machine{storage, storage_egs, 1, true};
  state_machine.current_eg_type = EventGroupType::PrivateEventGroupsOnly;
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 14);
  TestSubBufferList<Hash> buffer{state_machine};
  TestServerWriter<Hash> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Hash>, Hash>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add 1 gap update to storage
  auto gap_updates = generateEventGroupMap(
      kLastEventGroupId + 6, kLastEventGroupId + 6, EventGroupType::PrivateEventGroupsOnly, kClientId1);
  storage.addEventGroups(gap_updates);
  storage.updateEventGroupStorageMaps(gap_updates);

  // add 2 more event groups for kClientId2
  addMoreEventGroups(kLastEventGroupId + 7,
                     kLastEventGroupId + 8,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId2,
                     true);
  // add 2 more event groups for kClientId1
  addMoreEventGroups(kLastEventGroupId + 9,
                     kLastEventGroupId + 10,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1,
                     true);
  // add 2 more event groups for kClientId2
  addMoreEventGroups(kLastEventGroupId + 11,
                     kLastEventGroupId + 12,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId2,
                     true);
  // add 1 more event group for kClientId1
  addMoreEventGroups(kLastEventGroupId + 13,
                     kLastEventGroupId + 13,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1,
                     false);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 4;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicEventGroupUpdateHashesWithGap) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId + 5, EventGroupType::PublicEventGroupsOnly));
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Hash> state_machine{storage, storage_egs, 1, true};
  state_machine.current_eg_type = EventGroupType::PublicEventGroupsOnly;
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 9);
  TestSubBufferList<Hash> buffer{state_machine};
  TestServerWriter<Hash> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Hash>, Hash>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  auto gap_updates =
      generateEventGroupMap(kLastEventGroupId + 6, kLastEventGroupId + 6, EventGroupType::PublicEventGroupsOnly);
  storage.addEventGroups(gap_updates);
  storage.updateEventGroupStorageMaps(gap_updates);
  // add 2 more event groups for kClientId2
  addMoreEventGroups(kLastEventGroupId + 7,
                     kLastEventGroupId + 8,
                     EventGroupType::PublicEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5 + 3;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicEventGroupUpdateHashesWithGapTwoClients) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId, EventGroupType::PublicEventGroupsOnly));
  auto client2_egs = generateEventGroupMap(
      kLastEventGroupId + 1, kLastEventGroupId + 5, EventGroupType::PrivateEventGroupsOnly, kClientId2);
  storage.addEventGroups(client2_egs);
  storage.updateEventGroupStorageMaps(client2_egs);
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Hash> state_machine{storage, storage_egs, 1, true};
  state_machine.current_eg_type = EventGroupType::PublicEventGroupsOnly;
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 14);
  TestSubBufferList<Hash> buffer{state_machine};
  TestServerWriter<Hash> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Hash>, Hash>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add gap updates to storage
  auto gap_updates =
      generateEventGroupMap(kLastEventGroupId + 6, kLastEventGroupId + 6, EventGroupType::PublicEventGroupsOnly);
  storage.addEventGroups(gap_updates);
  storage.updateEventGroupStorageMaps(gap_updates);
  // add 2 more event groups for kClientId2
  addMoreEventGroups(kLastEventGroupId + 7,
                     kLastEventGroupId + 8,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId2,
                     true);
  // add 2 more public event groups
  addMoreEventGroups(kLastEventGroupId + 9,
                     kLastEventGroupId + 10,
                     EventGroupType::PublicEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1,
                     true);
  // add 2 more event groups for kClientId2
  addMoreEventGroups(kLastEventGroupId + 11,
                     kLastEventGroupId + 12,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId2,
                     true);
  // add 2 more public event groups
  addMoreEventGroups(kLastEventGroupId + 13,
                     kLastEventGroupId + 13,
                     EventGroupType::PublicEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1,
                     false);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 4;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicAndPrivateEventGroupUpdateHashesWithGap) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId + 5, EventGroupType::PublicAndPrivateEventGroups));
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Hash> state_machine{storage, storage_egs, 1, true};
  state_machine.current_eg_type = EventGroupType::PublicAndPrivateEventGroups;
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 9);
  TestSubBufferList<Hash> buffer{state_machine};
  TestServerWriter<Hash> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Hash>, Hash>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add gap event groups to storage
  auto gap_updates =
      generateEventGroupMap(kLastEventGroupId + 6, kLastEventGroupId + 6, EventGroupType::PublicAndPrivateEventGroups);
  storage.addEventGroups(gap_updates);
  storage.updateEventGroupStorageMaps(gap_updates);

  // add 2 more public and private event groups
  addMoreEventGroups(kLastEventGroupId + 7,
                     kLastEventGroupId + 8,
                     EventGroupType::PublicAndPrivateEventGroups,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 5 + 3;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeToPublicAndPrivateEventGroupUpdateHashesWithGapTwoClients) {
  // Initialize storage and live update queue
  FakeStorage storage(
      generateEventGroupMap(1, kLastEventGroupId, EventGroupType::PublicAndPrivateEventGroups, kClientId1));
  auto client2_egs = generateEventGroupMap(
      kLastEventGroupId + 1, kLastEventGroupId + 5, EventGroupType::PrivateEventGroupsOnly, kClientId2);
  storage.addEventGroups(client2_egs);
  storage.updateEventGroupStorageMaps(client2_egs);
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 5);
  TestStateMachine<Hash> state_machine{storage, storage_egs, 1, true};
  state_machine.current_eg_type = EventGroupType::PublicAndPrivateEventGroups;
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 14);
  TestSubBufferList<Hash> buffer{state_machine};
  TestServerWriter<Hash> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1u);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Hash>, Hash>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add gap updates to storage
  auto gap_updates = generateEventGroupMap(
      kLastEventGroupId + 6, kLastEventGroupId + 6, EventGroupType::PublicAndPrivateEventGroups, kClientId1);
  storage.addEventGroups(gap_updates);
  storage.updateEventGroupStorageMaps(gap_updates);
  // add 2 more event groups for kClientId2
  addMoreEventGroups(kLastEventGroupId + 7,
                     kLastEventGroupId + 8,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId2,
                     true);
  // add 2 more public and private event groups
  addMoreEventGroups(kLastEventGroupId + 9,
                     kLastEventGroupId + 10,
                     EventGroupType::PublicAndPrivateEventGroups,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1,
                     true);
  // add 2 more private event groups
  addMoreEventGroups(kLastEventGroupId + 11,
                     kLastEventGroupId + 12,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId2,
                     true);
  // add 1 more public and/or private event group
  addMoreEventGroups(kLastEventGroupId + 13,
                     kLastEventGroupId + 13,
                     EventGroupType::PublicAndPrivateEventGroups,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1,
                     false);
  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  auto total_event_groups = kLastEventGroupId + 4;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, ReadState) {
  // Initialize storage and live update queue
  FakeStorage storage{generate_kvp(1, kLastBlockId)};
  storage.genesis_block_id = 1;
  auto live_update_blocks = generate_kvp(0, 0);
  TestStateMachine<Data> state_machine{storage, live_update_blocks, 1};
  state_machine.set_expected_last_block_to_send(kLastBlockId);
  state_machine.return_false_on_last_block(false);
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  ReadStateRequest request;
  auto status = replica.ReadState(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::OK);
}

TEST(thin_replica_server_test, ReadStateHash) {
  // Initialize storage and live update queue
  FakeStorage storage{generate_kvp(1, kLastBlockId)};
  storage.genesis_block_id = 1;
  auto live_update_blocks = generate_kvp(0, 0);
  TestStateMachine<Hash> state_machine{storage, live_update_blocks, 0u};
  state_machine.set_expected_last_block_to_send(0u);
  TestSubBufferList<Hash> buffer{state_machine};
  TestServerWriter<Hash> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  ReadStateHashRequest request;
  request.mutable_events()->set_block_id(kLastBlockId);
  Hash hash;
  auto status = replica.ReadStateHash(&context, &request, &hash);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(hash.events().block_id(), kLastBlockId);
}

TEST(thin_replica_server_test, AckUpdate) {
  // Initialize storage and live update queue
  FakeStorage storage{generate_kvp(0, 0)};
  auto live_update_blocks = generate_kvp(0, 0);
  TestStateMachine<Hash> state_machine{storage, live_update_blocks, 1};
  TestSubBufferList<Hash> buffer{state_machine};
  TestServerWriter<Hash> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_events()->set_block_id(kLastBlockId);

  com::vmware::concord::thin_replica::BlockId block_id;
  block_id.set_block_id(1u);
  auto status = replica.AckUpdate(&context, &block_id, nullptr);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNIMPLEMENTED);
}

TEST(thin_replica_server_test, Unsubscribe) {
  // Initialize storage and live update queue
  FakeStorage storage{generate_kvp(0, 0)};
  auto live_update_blocks = generate_kvp(0, 0);
  TestStateMachine<Hash> state_machine{storage, live_update_blocks, 1};
  TestSubBufferList<Hash> buffer{state_machine};
  TestServerWriter<Hash> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_events()->set_block_id(kLastBlockId);

  com::vmware::concord::thin_replica::BlockId block_id;
  block_id.set_block_id(1u);
  auto status = replica.Unsubscribe(&context, nullptr, nullptr);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNIMPLEMENTED);
}

TEST(thin_replica_server_test, ContextWithoutClientIdData) {
  // Initialize storage and live update queue
  FakeStorage storage{generate_kvp(0, 0)};
  auto live_update_blocks = generate_kvp(0, 0);
  TestStateMachine<Data> data_state_machine{storage, live_update_blocks, 1};
  TestSubBufferList<Data> data_buffer{data_state_machine};
  TestServerWriter<Data> data_stream{data_state_machine};
  TestStateMachine<Hash> hash_state_machine{storage, live_update_blocks, 1};
  TestServerWriter<Hash> hash_stream{hash_state_machine};

  // create subscription request
  TestServerContext context;
  context.erase_client_metadata();
  ReadStateHashRequest read_state_hash_request;
  ReadStateRequest read_state_request;
  SubscriptionRequest subscription_request;
  Hash hash;

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, data_buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  EXPECT_EQ(replica.ReadState(&context, &read_state_request, &data_stream).error_code(), grpc::StatusCode::UNKNOWN);
  EXPECT_EQ(replica.ReadStateHash(&context, &read_state_hash_request, &hash).error_code(), grpc::StatusCode::UNKNOWN);

  // subscribe
  auto status = replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(
      &context, &subscription_request, &data_stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNKNOWN);
  status = replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Hash>, Hash>(
      &context, &subscription_request, &hash_stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNKNOWN);
}

TEST(thin_replica_server_test, SubscribeWithOutOfRangeBlockId) {
  // Initialize storage and live update queue
  FakeStorage storage{generate_kvp(0, 0)};
  auto live_update_blocks = generate_kvp(0, 0);
  TestStateMachine<Data> state_machine{storage, live_update_blocks, 1};
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_events()->set_block_id(kLastBlockId + 100);

  // subscribe
  auto status =
      replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::OUT_OF_RANGE);
}

TEST(thin_replica_server_test, SubscribeWithOutOfRangeEventGroupId) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(0, 0, EventGroupType::PrivateEventGroupsOnly));
  auto live_update_event_groups = generateEventGroupMap(0, 0, EventGroupType::PrivateEventGroupsOnly);
  TestStateMachine<Data> state_machine{storage, live_update_event_groups, 1};
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(kLastEventGroupId + 100);

  // subscribe
  auto status =
      replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::OUT_OF_RANGE);
}

TEST(thin_replica_server_test, SubscribeWithPrunedBlockId) {
  // Initialize storage and live update queue
  FakeStorage storage{generate_kvp(1, 10)};
  storage.genesis_block_id = 5;
  auto live_update_blocks = generate_kvp(0, 0);
  TestStateMachine<Data> state_machine{storage, live_update_blocks, 11};
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_events()->set_block_id(1);

  // subscribe
  auto status =
      replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

TEST(thin_replica_server_test, SubscribeWithPrunedEventGroupIdPrivate) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, 10, EventGroupType::PrivateEventGroupsOnly));
  storage.prune(10, kClientId1);
  auto live_update_event_groups = generateEventGroupMap(0, 0, EventGroupType::PrivateEventGroupsOnly);
  TestStateMachine<Data> state_machine{storage, live_update_event_groups, 11};
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1);

  // subscribe
  auto status =
      replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

TEST(thin_replica_server_test, SubscribeWithPrunedEventGroupIdPrivateCorrectly) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, 10, EventGroupType::PrivateEventGroupsOnly));
  storage.prune(10, kClientId1);
  auto live_update_event_groups = generateEventGroupMap(0, 0, EventGroupType::PrivateEventGroupsOnly);
  TestStateMachine<Data> state_machine{storage, live_update_event_groups, 11};
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(11);

  // subscribe
  std::chrono::seconds data_timeout = 1s;
  auto stream_out = async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = stream_out.wait_for(data_timeout);
  EXPECT_EQ(status, std::future_status::timeout);
  context.TryCancel();
}

TEST(thin_replica_server_test, SubscribeWithPrunedEventGroupIdPrivateNewEgsAdded) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, 10, EventGroupType::PrivateEventGroupsOnly));
  storage.prune(10, kClientId1);
  auto live_update_event_groups = generateEventGroupMap(0, 0, EventGroupType::PrivateEventGroupsOnly);
  TestStateMachine<Data> state_machine{storage, live_update_event_groups, 11, true};
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 7);
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // add 1 more event group for kClientId1
  addMoreEventGroups(kLastEventGroupId + 6,
                     kLastEventGroupId + 6,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1);
  state_machine.set_expected_last_event_group_to_send(11);

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1);

  // subscribe
  auto status =
      replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

TEST(thin_replica_server_test, SubscribeWithPrunedEventGroupIdPrivateNewEgsAddedCorrectly) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, 10, EventGroupType::PrivateEventGroupsOnly));
  storage.prune(10, kClientId1);
  auto live_update_event_groups = generateEventGroupMap(0, 0, EventGroupType::PrivateEventGroupsOnly);
  TestStateMachine<Data> state_machine{storage, live_update_event_groups, 11, true};
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 7);
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // add 1 more event group for kClientId1
  addMoreEventGroups(kLastEventGroupId + 6,
                     kLastEventGroupId + 6,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1);

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(11);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  // we assume the client has already received pruned event groups
  auto total_event_groups = 11;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeWithPrunedEventGroupIdPublic) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, 10, EventGroupType::PublicEventGroupsOnly));
  storage.prune(10, kPublicEgIdKey);
  auto live_update_event_groups = generateEventGroupMap(0, 0, EventGroupType::PublicEventGroupsOnly);
  TestStateMachine<Data> state_machine{storage, live_update_event_groups, 11};
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1);

  // subscribe
  auto status =
      replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

TEST(thin_replica_server_test, SubscribeWithPrunedEventGroupIdPublicCorrectly) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, 10, EventGroupType::PublicEventGroupsOnly));
  storage.prune(10, kPublicEgIdKey);
  auto live_update_event_groups = generateEventGroupMap(0, 0, EventGroupType::PublicEventGroupsOnly);
  TestStateMachine<Data> state_machine{storage, live_update_event_groups, 11};
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(11);

  // subscribe
  std::chrono::seconds data_timeout = 1s;
  auto stream_out = async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = stream_out.wait_for(data_timeout);
  EXPECT_EQ(status, std::future_status::timeout);
  context.TryCancel();
}

TEST(thin_replica_server_test, SubscribeWithPrunedEventGroupIdPublicNewEgsAdded) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, 10, EventGroupType::PublicEventGroupsOnly));
  storage.prune(10, kPublicEgIdKey);
  auto live_update_event_groups = generateEventGroupMap(0, 0, EventGroupType::PublicEventGroupsOnly);
  TestStateMachine<Data> state_machine{storage, live_update_event_groups, 11, true};
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 7);
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // add 1 more public event group
  addMoreEventGroups(kLastEventGroupId + 6,
                     kLastEventGroupId + 6,
                     EventGroupType::PublicEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer);
  state_machine.set_expected_last_event_group_to_send(11);

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1);

  // subscribe
  auto status =
      replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

TEST(thin_replica_server_test, SubscribeWithPrunedEventGroupIdPublicNewEgsAddedCorrectly) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, 10, EventGroupType::PublicEventGroupsOnly));
  storage.prune(10, kPublicEgIdKey);
  auto live_update_event_groups = generateEventGroupMap(0, 0, EventGroupType::PublicEventGroupsOnly);
  TestStateMachine<Data> state_machine{storage, live_update_event_groups, 11, true};
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 7);
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // add 1 more public event group
  addMoreEventGroups(kLastEventGroupId + 6,
                     kLastEventGroupId + 6,
                     EventGroupType::PublicEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer);

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(11);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  // we assume the client has already received pruned event groups
  auto total_event_groups = 11;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, SubscribeWithPrunedEventGroupIdPublicAndPrivate) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, 10, EventGroupType::PublicAndPrivateEventGroups));
  storage.prune(5, kPublicEgIdKey);
  storage.prune(5, kClientId1);
  auto live_update_event_groups = generateEventGroupMap(0, 0, EventGroupType::PublicAndPrivateEventGroups);
  TestStateMachine<Data> state_machine{storage, live_update_event_groups, 11};
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1);

  // subscribe
  auto status =
      replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

TEST(thin_replica_server_test, SubscribeWithPrunedEventGroupIdPublicAndPrivateCorrectly) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, 10, EventGroupType::PublicAndPrivateEventGroups));
  storage.prune(5, kPublicEgIdKey);
  storage.prune(5, kClientId1);
  auto live_update_event_groups = generateEventGroupMap(0, 0, EventGroupType::PublicAndPrivateEventGroups);
  TestStateMachine<Data> state_machine{storage, live_update_event_groups, 11};
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(11);

  // susbcribe
  std::chrono::seconds data_timeout = 1s;
  auto stream_out = async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = stream_out.wait_for(data_timeout);
  EXPECT_EQ(status, std::future_status::timeout);
  context.TryCancel();
}

TEST(thin_replica_server_test, SubscribeWithPrunedEventGroupIdPublicAndPrivateNewEgsAdded) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, 10, EventGroupType::PublicAndPrivateEventGroups));
  storage.prune(5, kPublicEgIdKey);
  storage.prune(5, kClientId1);
  auto live_update_event_groups = generateEventGroupMap(0, 0, EventGroupType::PublicAndPrivateEventGroups);
  TestStateMachine<Data> state_machine{storage, live_update_event_groups, 11, true};
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 7);
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // add 1 more event group for kClientId1
  addMoreEventGroups(kLastEventGroupId + 6,
                     kLastEventGroupId + 6,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1);
  state_machine.set_expected_last_event_group_to_send(11);

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(1);

  // susbcribe
  auto status =
      replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

TEST(thin_replica_server_test, SubscribeWithPrunedEventGroupIdPublicAndPrivateNewEgsAddedCorrectly) {
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, 10, EventGroupType::PublicAndPrivateEventGroups));
  storage.prune(5, kPublicEgIdKey);
  storage.prune(5, kClientId1);
  auto live_update_event_groups = generateEventGroupMap(0, 0, EventGroupType::PublicAndPrivateEventGroups);
  TestStateMachine<Data> state_machine{storage, live_update_event_groups, 11, true};
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 7);
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // add 1 more event group for kClientId1
  addMoreEventGroups(kLastEventGroupId + 6,
                     kLastEventGroupId + 6,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1);

  // create subscription request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_event_groups()->set_event_group_id(11);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  if (status != std::future_status::ready) {
    out_stream.wait();
  }
  // we assume the client has already received pruned event groups
  auto total_event_groups = 11;
  EXPECT_EQ(out_stream.get().error_code(), grpc::StatusCode::OK);
  EXPECT_EQ(state_machine.numEventGroupsReceived(), total_event_groups);
}

TEST(thin_replica_server_test, GetClientIdFromCertSubjectField) {
  // Initialize storage and live update queue
  FakeStorage storage{generate_kvp(0, 0)};
  auto live_update_blocks = generate_kvp(0, 0);
  TestStateMachine<Data> state_machine{storage, live_update_blocks, 1};
  TestSubBufferList<Data> buffer{state_machine};

  // generate TRS config and create ThinReplicaImpl object
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
  std::string parsed_client_id = replica.parseClientIdFromSubject(subject_str, "OU = ");
  EXPECT_EQ(client_id, parsed_client_id);
}

TEST(thin_replica_server_test, GetClientIdFromCertsSubjectField) {
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
      "subject=C = NA, ST = NA, L = NA, O = clientservice1, OU = 39, CN = "
      "node39";
  std::string client_id = "clientservice1";
  std::string parsed_client_id = replica.parseClientIdFromSubject(subject_str, "O = ");
  EXPECT_EQ(client_id, parsed_client_id);
}

TEST(thin_replica_server_test, GetClientIdSetFromRootCert) {
  // Initialize storage and live update queue
  FakeStorage storage{generate_kvp(0, 0)};
  auto live_update_blocks = generate_kvp(0, 0);
  TestStateMachine<Data> state_machine{storage, live_update_blocks, 1};
  TestSubBufferList<Data> buffer{state_machine};
  auto logger = logging::getLogger("thin_replica_server_test");

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::string root_cert_path = "resources/trs_trc_tls_certs/concord1/client.cert";
  std::unordered_set<std::string> parsed_client_id_set;
  std::unordered_set<std::string> client_id_set(
      {"daml_ledger_api1", "daml_ledger_api2", "daml_ledger_api3", "daml_ledger_api4", "trutil"});
  uint16_t update_metrics_aggregator_thresh = 100;

  concord::thin_replica::ThinReplicaImpl::getClientIdFromRootCert(logger, root_cert_path, parsed_client_id_set, false);
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

TEST(thin_replica_server_test, GetClientIdSetFromRootCerts) {
  FakeStorage storage{generate_kvp(0, 0)};
  auto live_update_blocks = generate_kvp(0, 0);
  TestStateMachine<Data> state_machine{storage, live_update_blocks, 1};
  TestSubBufferList<Data> buffer{state_machine};
  auto logger = logging::getLogger("thin_replica_server_test");
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::string root_cert_path = "resources/tls_certs";
  std::unordered_set<std::string> parsed_client_id_set;
  std::unordered_set<std::string> client_id_set({"clientservice1", "clientservice2", "trutil"});
  uint16_t update_metrics_aggregator_thresh = 100;

  concord::thin_replica::ThinReplicaImpl::getClientIdSetFromRootCert(logger, root_cert_path, parsed_client_id_set);
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
  // Initialize storage and live update queue
  FakeStorage storage{generate_kvp(0, 0)};
  auto live_update_blocks = generate_kvp(0, 0);
  TestStateMachine<Data> state_machine{storage, live_update_blocks, 1};
  TestSubBufferList<Data> buffer{state_machine};

  TestServerContext context;

  // generate TRS config and create ThinReplicaImpl object
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
  // Initialize storage and live update queue
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
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 7);
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  auto total_legacy_blocks = kLastBlockId;
  auto total_event_groups = kLastEventGroupId + 5;
  auto total_updates = total_legacy_blocks + total_event_groups;

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  // ThinReplica legacy events request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_events()->set_block_id(1);

  // susbcribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add 1 more event group for kClientId1
  addMoreEventGroups(kLastEventGroupId + 6,
                     kLastEventGroupId + 6,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1);
  total_updates++;
  total_event_groups++;
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
  // Initialize storage and live update queue
  FakeStorage storage(generateEventGroupMap(1, kLastEventGroupId + 10, EventGroupType::PrivateEventGroupsOnly));
  auto storage_egs = storage.getEventGroups();
  EXPECT_EQ(storage.getLastEventGroupId(), kLastEventGroupId + 10);
  EXPECT_EQ(storage.getOldestEventGroupBlockId(), 1);
  // Live updates can contain event groups only (storage_egs = live_updates)
  TestStateMachine<Data> state_machine{storage, storage_egs, 1, true};
  state_machine.current_eg_type = EventGroupType::PrivateEventGroupsOnly;
  state_machine.set_expected_last_event_group_to_send(kLastEventGroupId + 12);
  TestSubBufferList<Data> buffer{state_machine};
  TestServerWriter<Data> stream{state_machine};

  auto total_event_groups = kLastEventGroupId + 10;

  // generate TRS config and create ThinReplicaImpl object
  bool is_insecure_trs = true;
  std::string tls_trs_cert_path;
  std::unordered_set<std::string> client_id_set;
  uint16_t update_metrics_aggregator_thresh = 100;
  auto trs_config = std::make_unique<concord::thin_replica::ThinReplicaServerConfig>(
      is_insecure_trs, tls_trs_cert_path, &storage, buffer, client_id_set, update_metrics_aggregator_thresh);
  concord::thin_replica::ThinReplicaImpl replica(std::move(trs_config), std::make_shared<concordMetrics::Aggregator>());

  // create subscription request
  // ThinReplica legacy events request
  TestServerContext context;
  SubscriptionRequest request;
  request.mutable_events()->set_block_id(3);

  // subscribe
  auto out_stream = std::async(std::launch::async, [&] {
    return replica.SubscribeToUpdates<TestServerContext, TestServerWriter<Data>, Data>(&context, &request, &stream);
  });
  auto status = out_stream.wait_for(1s);

  // add 1 more event group for kClientId1
  addMoreEventGroups(kLastEventGroupId + 11,
                     kLastEventGroupId + 11,
                     EventGroupType::PrivateEventGroupsOnly,
                     storage,
                     state_machine,
                     buffer,
                     kClientId1);
  total_event_groups++;
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
