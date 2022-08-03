// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#include <map>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>
#include <random>
#include <sstream>
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "util/filesystem.hpp"
#include "categorization/base_types.h"
#include "categorization/db_categories.h"
#include "categorization/updates.h"
#include "categorized_kvbc_msgs.cmf.hpp"
#include "db_interfaces.h"
#include "endianness.hpp"
#include "event_group_msgs.cmf.hpp"
#include "kvbc_app_filter/kvbc_key_types.h"
#include "concord_kvbc.pb.h"
#include "storage/test/storage_test_common.h"
#include "tools/db_editor/include/kv_blockchain_db_editor.hpp"
#include "tools/migrations/v4migration_tool/include/blockchain_adapter.hpp"
#include "tools/migrations/v4migration_tool/include/migration_bookeeper.hpp"
using namespace ::testing;
namespace {
using keys = std::vector<std::string>;
using values = std::vector<std::optional<concord::kvbc::categorization::Value>>;
using versions = std::vector<std::optional<concord::kvbc::categorization::TaggedVersion>>;

// Prefix the key with the immutable index to make it unique and ordered.
inline std::string prefixImmutableKey(const std::string& key, uint64_t immutable_index) {
  return concordUtils::toBigEndianStringBuffer(immutable_index) + key;
}

class AccumulatedBlock {
 public:
  AccumulatedBlock(const uint64_t immutable_index) : immutable_index_(immutable_index) {}

  std::size_t immutable_size() const {
    const auto& immutable_data = immutables.getData();
    return immutable_data.kv.size();
  }

  std::size_t versioned_size() const {
    const auto& versioned_data = versioned.getData();
    return versioned_data.kv.size() + versioned_data.deletes.size();
  }

  std::size_t block_merkle_size() const {
    const auto& block_merkle_data = block_merkle.getData();
    return block_merkle_data.kv.size() + block_merkle_data.deletes.size();
  }

  std::size_t internal_size() const {
    const auto& internal_data = internal.getData();
    return internal_data.kv.size() + internal_data.deletes.size();
  }

  std::size_t event_group_data_table_size() const {
    const auto& event_group_data_table_data = event_group_data_table.getData();
    return event_group_data_table_data.kv.size();
  }

  std::size_t event_group_latest_table_size() const {
    const auto& event_group_latest_table_data = event_group_latest_table.getData();
    return event_group_latest_table_data.kv.size() + event_group_latest_table_data.deletes.size();
  }

  std::size_t event_group_tag_table_size() const {
    const auto& event_group_tag_table_data = event_group_tag_table.getData();
    return event_group_tag_table_data.kv.size();
  }

  std::size_t size() const {
    size_t time_update = time_val.empty() ? 0 : 1;
    return immutable_size() + versioned_size() + block_merkle_size() + internal_size() + event_group_data_table_size() +
           event_group_latest_table_size() + event_group_tag_table_size() + time_update;
  }

  bool empty() const { return size() == 0; }

  // Prefix immutable keys with index to make them unique and ordered.
  void addImmutable(const std::string& key, concord::kvbc::categorization::ImmutableUpdates::ImmutableValue&& value) {
    immutable_index_++;
    immutables.addUpdate(prefixImmutableKey(key, immutable_index_), std::move(value));
  }

  void moveEventGroupToImmutables(uint64_t& event_group_id, concord::kvbc::categorization::EventGroup& event_group) {
    std::string serialized_event_group;
    concord::kvbc::categorization::serialize(serialized_event_group, event_group);
    concord::kvbc::categorization::ImmutableUpdates::ImmutableValue imm_value{std::move(serialized_event_group),
                                                                              std::set<std::string>{}};
    event_group_id++;
    event_group_data_table.addUpdate(concordUtils::toBigEndianStringBuffer(event_group_id), std::move(imm_value));
    event_group = {};
  }

  std::optional<std::string> findState(const std::string& key) const {
    if (auto it = versioned.getData().kv.find(key); it != versioned.getData().kv.end()) {
      return it->second.data;
    }
    if (auto it = block_merkle.getData().kv.find(key); it != block_merkle.getData().kv.end()) {
      return it->second;
    }
    return std::nullopt;
  }

  // In theory it's possible to have two events with the same key in an acc
  // block. We'll return the first occurrence.
  std::optional<concord::kvbc::categorization::ImmutableValueUpdate> findImmutable(
      const std::string& lookup_key) const {
    for (auto [prefixed_key, v] : immutables.getData().kv) {
      ConcordAssertGE(prefixed_key.size(), sizeof(uint64_t));
      const auto key = prefixed_key.size() == sizeof(uint64_t) ? std::string{} : prefixed_key.substr(sizeof(uint64_t));
      if (key == lookup_key) return v;
    }

    return std::nullopt;
  }

  std::optional<concord::kvbc::categorization::ImmutableValueUpdate> findEventGroupImmutable(
      const std::string& lookup_key) const {
    auto it = event_group_data_table.getData().kv.find(lookup_key);
    if (it != event_group_data_table.getData().kv.end()) {
      return it->second;
    }
    return std::nullopt;
  }

  std::optional<std::string> findAny(const std::string& key) const {
    if (auto v = findImmutable(key); v.has_value()) {
      return v->data;
    }
    if (auto v = findEventGroupImmutable(key); v.has_value()) {
      return v->data;
    }
    if (auto it = versioned.getData().kv.find(key); it != versioned.getData().kv.end()) {
      return it->second.data;
    }
    if (auto it = internal.getData().kv.find(key); it != internal.getData().kv.end()) {
      return it->second.data;
    }
    if (auto it = block_merkle.getData().kv.find(key); it != block_merkle.getData().kv.end()) {
      return it->second;
    }
    return std::nullopt;
  }

  keys get_merkle_keys() {
    keys res;
    for (auto& it : block_merkle.getData().kv) {
      res.push_back(it.first);
    }
    return res;
  }

  keys get_private_keys() {
    keys res;
    for (auto& it : versioned.getData().kv) {
      res.push_back(it.first);
    }
    return res;
  }

  bool isStateDeleted(const std::string& key) const {
    {
      auto itr = std::find(std::begin(versioned.getData().deletes), std::end(versioned.getData().deletes), key);
      if (itr != std::end(versioned.getData().deletes)) return true;
    }
    {
      auto itr = std::find(std::begin(block_merkle.getData().deletes), std::end(block_merkle.getData().deletes), key);
      if (itr != std::end(block_merkle.getData().deletes)) return true;
    }
    return false;
  }

  void insertClientIDs(const std::vector<std::string>& trids_for_time_updates) {
    clients_.insert(trids_for_time_updates);
  }

  const std::set<std::string>& getClientIDs() { return clients_.IDs; }

  // After using this method the object is undefined.
  // A new instance should be created to accmulate new updates.
  concord::kvbc::categorization::Updates moveUpdates(const bool event_group_enabled = false) {
    if (has_moved_) throw std::runtime_error("Trying to move an AccumulatedBlock that has already been moved");
    has_moved_ = true;
    concord::kvbc::categorization::Updates updates;
    immutables.calculateRootHash(true);
    if (event_group_enabled) {
      if (event_group_data_table_size() > 0)
        updates.add(concord::kvbc::categorization::kExecutionEventGroupDataCategory, std::move(event_group_data_table));
      if (event_group_latest_table_size() > 0)
        updates.add(concord::kvbc::categorization::kExecutionEventGroupLatestCategory,
                    std::move(event_group_latest_table));
      if (event_group_tag_table_size() > 0)
        updates.add(concord::kvbc::categorization::kExecutionEventGroupTagCategory, std::move(event_group_tag_table));
    } else {
      // insert time
      if (!time_key.empty() && !time_val.empty()) {
        addImmutable(time_key, {std::move(time_val), std::set<std::string>{clients_.IDs}});
      }
      if (immutable_size() > 0)
        updates.add(concord::kvbc::categorization::kExecutionEventsCategory, std::move(immutables));
    }
    if (versioned_size() > 0)
      updates.add(concord::kvbc::categorization::kExecutionPrivateCategory, std::move(versioned));
    if (block_merkle_size() > 0)
      updates.add(concord::kvbc::categorization::kExecutionProvableCategory, std::move(block_merkle));
    // Updates shuold always have the internal category
    updates.add(concord::kvbc::categorization::kConcordInternalCategoryId, std::move(internal));
    return updates;
  }

  concord::kvbc::categorization::ImmutableUpdates immutables;
  concord::kvbc::categorization::VersionedUpdates versioned;
  concord::kvbc::categorization::VersionedUpdates internal;
  concord::kvbc::categorization::BlockMerkleUpdates block_merkle;
  // Time accumulation
  std::string time_key;
  std::string time_val;
  // Accumulates event_groups
  // Key: global_event_group_id
  // Value: event_group
  concord::kvbc::categorization::ImmutableUpdates event_group_data_table;
  // Accumulates global latest event_group_id and latest/oldest event_group_id per TRID
  // Key: <trid> + "_newest/_oldest"
  // Value: latest_trid_event_group_id
  // Note that "_global_eg_id_oldest", "_global_eg_id_newest", "_public_eg_id_oldest", "_public_eg_id_newest" are
  // special keys; the values at these keys represent the oldest/newest global/public event group ids respectively
  concord::kvbc::categorization::VersionedUpdates event_group_latest_table;
  // Accumulates global event_group_id per TRID_event_group_id
  // Key: trid + # + trid_event_group_id
  // Value: global_event_group_id
  // For e.g., given a trid - `trid1`, trid_event_group_id - 120, we get `trid1#120`as key,
  // and the value is the global_event_group_id associated with the trid specific event_group_id
  concord::kvbc::categorization::ImmutableUpdates event_group_tag_table;

  // Keep track of public block merkle key additions.
  std::set<std::string> public_block_merkle_key_adds;

  struct ClientIDs {
    std::set<std::string> IDs;
    bool is_public{false};
    void insert(const std::vector<std::string>& trids_for_time_updates) {
      if (is_public) return;
      if (trids_for_time_updates.empty()) {
        is_public = true;
        IDs.clear();
        return;
      }
      IDs.insert(trids_for_time_updates.begin(), trids_for_time_updates.end());
    }
  };

  // Note: currently this class is used from a single thread,
  // if this class will be used from several threads, this index should be
  // changed to be atomic.
  uint64_t immutable_index_{0};

 private:
  bool has_moved_{false};
  ClientIDs clients_;
};
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

class BlockchainRandomPopulator {
 public:
  ~BlockchainRandomPopulator() = default;
  BlockchainRandomPopulator(const std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client_4v1,
                            bool link_st_chain_4v1,
                            const std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client_4v4,
                            bool link_st_chain_4v4)
      : native_client_4v1_(native_client_4v1),
        native_client_4v4_(native_client_4v4),
        link_st_chain_4v1_(link_st_chain_4v1),
        link_st_chain_4v4_(link_st_chain_4v4) {}

  void randomly_populate_both_bc(uint32_t max_in_each_cat, uint32_t max_num_blocks) {
    BlockchainAdapter<concord::kvbc::CATEGORIZED_BLOCKCHAIN> cat_kvbc(native_client_4v1_, link_st_chain_4v1_);
    BlockchainAdapter<concord::kvbc::V4_BLOCKCHAIN> v4_kvbc(native_client_4v4_, link_st_chain_4v4_);
    std::string raw_key_v1;
    std::string value_v1;
    std::string raw_key_v4;
    std::string value_v4;
    std::string tag_v1;
    std::string tag_v4;
    std::set<std::string> g_tags_v1;
    std::set<std::string> g_tags_v4;
    std::mt19937_64 eng{std::random_device{}()};
    std::uniform_int_distribution<> key_dist{5, 20};
    std::uniform_int_distribution<> val_dist{1024, 10240};
    std::uniform_int_distribution<> tag_dist{1, 5};

    for (concord::kvbc::BlockId block_id = 1; block_id <= max_num_blocks; ++block_id) {
      auto last_reachable_block_id_v1 = (cat_kvbc.getAdapter())->getLastBlockId();
      auto last_reachable_block_id_v4 = (v4_kvbc.getAdapter())->getLastBlockId();
      ASSERT_EQ(last_reachable_block_id_v1, last_reachable_block_id_v4);
      AccumulatedBlock accumulated_block_v1(last_reachable_block_id_v1 + 1);
      AccumulatedBlock accumulated_block_v4(last_reachable_block_id_v4 + 1);
      for (uint32_t i = 0; i < max_in_each_cat; ++i) {
        raw_key_v1 = raw_key_v4 = getRandomStringOfLength(key_dist(eng));
        value_v1 = value_v4 = getRandomStringOfLength(val_dist(eng));
        accumulated_block_v1.block_merkle.addUpdate(std::move(raw_key_v1), std::move(value_v1));
        accumulated_block_v4.block_merkle.addUpdate(std::move(raw_key_v4), std::move(value_v4));
      }

      for (uint32_t i = 0; i < max_in_each_cat; ++i) {
        raw_key_v1 = raw_key_v4 = getRandomStringOfLength(key_dist(eng));
        value_v1 = value_v4 = getRandomStringOfLength(val_dist(eng));
        uint32_t max_tags = key_dist(eng);
        auto& tags_v1 = g_tags_v1;
        auto& tags_v4 = g_tags_v4;
        for (uint32_t ti = 0; ti < max_tags; ++ti) {
          tag_v1 = tag_v4 = getRandomStringOfLength(tag_dist(eng));
          tags_v1.emplace(std::move(tag_v1));
          tags_v4.emplace(std::move(tag_v4));
        }
        concord::kvbc::categorization::ImmutableUpdates::ImmutableValue immutable_value_v1(std::move(value_v1),
                                                                                           std::move(tags_v1));
        concord::kvbc::categorization::ImmutableUpdates::ImmutableValue immutable_value_v4(std::move(value_v4),
                                                                                           std::move(tags_v4));
        accumulated_block_v1.immutables.addUpdate(std::move(raw_key_v1), std::move(immutable_value_v1));
        accumulated_block_v4.immutables.addUpdate(std::move(raw_key_v4), std::move(immutable_value_v4));
      }

      for (uint32_t i = 0; i < max_in_each_cat; ++i) {
        raw_key_v1 = raw_key_v4 = getRandomStringOfLength(key_dist(eng));
        value_v1 = value_v4 = getRandomStringOfLength(val_dist(eng));
        accumulated_block_v1.versioned.addUpdate(std::move(raw_key_v1), std::move(value_v1));
        accumulated_block_v4.versioned.addUpdate(std::move(raw_key_v4), std::move(value_v4));
      }

      for (uint32_t i = 0; i < max_in_each_cat; ++i) {
        raw_key_v1 = raw_key_v4 = getRandomStringOfLength(key_dist(eng));
        value_v1 = value_v4 = getRandomStringOfLength(val_dist(eng));
        accumulated_block_v1.internal.addUpdate(std::move(raw_key_v1), std::move(value_v1));
        accumulated_block_v4.internal.addUpdate(std::move(raw_key_v4), std::move(value_v4));
      }
      (cat_kvbc.getAdapter())->add(accumulated_block_v1.moveUpdates());
      (v4_kvbc.getAdapter())->add(accumulated_block_v4.moveUpdates());
    }
  }

  template <concord::kvbc::BLOCKCHAIN_VERSION V>
  void randomly_populate_single_bc(uint32_t max_in_each_cat, uint32_t max_num_blocks) {
    if (V == concord::kvbc::CATEGORIZED_BLOCKCHAIN) {
      ASSERT_NE(native_client_4v1_, nullptr);
    } else if (V == concord::kvbc::V4_BLOCKCHAIN) {
      ASSERT_NE(native_client_4v4_, nullptr);
    }
    BlockchainAdapter<V> some_kvbc(native_client_4v1_ != nullptr ? native_client_4v1_ : native_client_4v4_,
                                   native_client_4v1_ != nullptr ? link_st_chain_4v1_ : link_st_chain_4v4_);

    std::string raw_key;
    std::string value;
    std::string tag;
    std::set<std::string> g_tags;
    std::mt19937_64 eng{std::random_device{}()};
    std::uniform_int_distribution<> key_dist{5, 20};
    std::uniform_int_distribution<> val_dist{1024, 10240};
    std::uniform_int_distribution<> tag_dist{1, 5};

    for (concord::kvbc::BlockId block_id = 1; block_id <= max_num_blocks; ++block_id) {
      auto last_reachable_block_id = (some_kvbc.getAdapter())->getLastBlockId();
      AccumulatedBlock accumulated_block(last_reachable_block_id + 1);
      for (uint32_t i = 0; i < max_in_each_cat; ++i) {
        raw_key = getRandomStringOfLength(key_dist(eng));
        value = getRandomStringOfLength(val_dist(eng));
        accumulated_block.block_merkle.addUpdate(std::move(raw_key), std::move(value));
      }

      for (uint32_t i = 0; i < max_in_each_cat; ++i) {
        raw_key = getRandomStringOfLength(key_dist(eng));
        value = getRandomStringOfLength(val_dist(eng));
        uint32_t max_tags = key_dist(eng);

        auto& tags = g_tags;
        for (uint32_t ti = 0; ti < max_tags; ++ti) {
          tag = getRandomStringOfLength(tag_dist(eng));
          tags.emplace(std::move(tag));
        }
        concord::kvbc::categorization::ImmutableUpdates::ImmutableValue immutable_value(std::move(value),
                                                                                        std::move(tags));
        accumulated_block.immutables.addUpdate(std::move(raw_key), std::move(immutable_value));
      }

      for (uint32_t i = 0; i < max_in_each_cat; ++i) {
        raw_key = getRandomStringOfLength(key_dist(eng));
        value = getRandomStringOfLength(val_dist(eng));
        accumulated_block.versioned.addUpdate(std::move(raw_key), std::move(value));
      }

      for (uint32_t i = 0; i < max_in_each_cat; ++i) {
        raw_key = getRandomStringOfLength(key_dist(eng));
        value = getRandomStringOfLength(val_dist(eng));
        accumulated_block.internal.addUpdate(std::move(raw_key), std::move(value));
      }
      (some_kvbc.getAdapter())->add(accumulated_block.moveUpdates());
    }
  }

 private:
  std::shared_ptr<concord::storage::rocksdb::NativeClient> native_client_4v1_;
  std::shared_ptr<concord::storage::rocksdb::NativeClient> native_client_4v4_;
  bool link_st_chain_4v1_;
  bool link_st_chain_4v4_;
};

class MigratorAndVerifier {
 public:
  MigratorAndVerifier() = default;
  ~MigratorAndVerifier() = default;
  void migrate_db(const std::string& from_path, const std::string& to_path, bool v4_to_v1) {
    static logging::Logger logger(logging::getLogger("concord.migration.v4.main"));
    BookKeeper v4_migration;
    std::vector<std::string> argv = {{"v4migration_tool"},
                                     {"--input-rocksdb-path"},
                                     {""},
                                     {"--output-rocksdb-path"},
                                     {""},
                                     {"--point-lookup-batch-size"},
                                     {"500"},
                                     {"--point-lookup-threads"},
                                     {"8"},
                                     {"--max-point-lookup-batches"},
                                     {"50"}};
    argv[2] += from_path;
    argv[4] += to_path;
    if (v4_to_v1) {
      argv.push_back("--migrate-to-v1");
      argv.push_back("true");
    }

    char** p_argv = new char*[argv.size()];
    for (size_t i = 0; i < argv.size(); ++i) {
      p_argv[i] = const_cast<char*>((argv[i]).c_str());
    }

    ASSERT_NO_THROW(ASSERT_EQ(EXIT_SUCCESS, v4_migration.migrate(argv.size(), p_argv)));
    delete[] p_argv;
  }

  void verify_DBs(const std::string& from_path, const std::string& to_path, bool& result) {
    std::vector<std::string> argv = {{"kv_blockchain_db_editor"}, {""}, {"compareTo"}, {""}};
    argv[1] += from_path;
    argv[3] += to_path;

    char** p_argv = new char*[argv.size()];
    for (size_t i = 0; i < argv.size(); ++i) {
      p_argv[i] = const_cast<char*>((argv[i]).c_str());
    }

    std::ostringstream out;
    std::ostringstream err;
    result = false;
    ASSERT_NO_THROW(
        ASSERT_EQ(EXIT_SUCCESS,
                  concord::kvbc::tools::db_editor::run(
                      concord::kvbc::tools::db_editor::command_line_arguments(argv.size(), p_argv), out, err)));
    if (out.str().find("equivalent") != std::string::npos) {
      result = true;
    }
    delete[] p_argv;
  }
};

TEST(v4_migration_test, check_e2e_flow_1) {
  std::mt19937_64 eng{std::random_device{}()};
  std::uniform_int_distribution<> num_cat_dist{7, 57};
  std::uniform_int_distribution<> num_block_dist{111, 555};
  std::string v1_db_path;
  std::string v4_db_path;
  std::string v1_db_dsh_path;
  std::string v4_db_dsh_path;
  {
    auto v1_db = TestRocksDb::createNative(1);
    v1_db_path = v1_db->path();
    auto v4_db = TestRocksDb::createNative(2);
    v4_db_path = v4_db->path();
    auto v1_db_dsh = TestRocksDb::createNative(3);
    v1_db_dsh_path = v1_db_dsh->path();
    auto v4_db_dsh = TestRocksDb::createNative(4);
    v4_db_dsh_path = v4_db_dsh->path();
    BlockchainRandomPopulator kvbc_populator_verifier(v1_db, true, v4_db, true);
    kvbc_populator_verifier.randomly_populate_both_bc(num_cat_dist(eng), num_block_dist(eng));
  }

  MigratorAndVerifier migrator_verifier;
  migrator_verifier.migrate_db(v1_db_path, v4_db_dsh_path, false);
  migrator_verifier.migrate_db(v4_db_path, v1_db_dsh_path, true);

  bool result = false;
  migrator_verifier.verify_DBs(v1_db_path, v1_db_dsh_path, result);
  ASSERT_TRUE(result);

  result = false;
  migrator_verifier.verify_DBs(v4_db_path, v4_db_dsh_path, result);
  ASSERT_TRUE(result);

  ASSERT_NO_THROW(fs::remove_all(v1_db_path));
  ASSERT_NO_THROW(fs::remove_all(v4_db_path));
  ASSERT_NO_THROW(fs::remove_all(v1_db_dsh_path));
  ASSERT_NO_THROW(fs::remove_all(v4_db_dsh_path));
}

TEST(v4_migration_test, check_e2e_flow_2) {
  std::mt19937_64 eng{std::random_device{}()};
  std::uniform_int_distribution<> num_cat_dist{7, 57};
  std::uniform_int_distribution<> num_block_dist{111, 555};
  std::string v1_db_path;
  std::string v4_db_path;
  std::string v1_db_dsh_path;
  std::string v4_db_dsh_path;
  {
    auto v1_db = TestRocksDb::createNative(1);
    v1_db_path = v1_db->path();
    auto v4_db = TestRocksDb::createNative(2);
    v4_db_path = v4_db->path();
    auto v1_db_dsh = TestRocksDb::createNative(3);
    v1_db_dsh_path = v1_db_dsh->path();
    auto v4_db_dsh = TestRocksDb::createNative(4);
    v4_db_dsh_path = v4_db_dsh->path();
    BlockchainRandomPopulator kvbc_populator_verifier(v1_db, true, nullptr, false);
    kvbc_populator_verifier.randomly_populate_single_bc<concord::kvbc::CATEGORIZED_BLOCKCHAIN>(num_cat_dist(eng),
                                                                                               num_block_dist(eng));
  }

  MigratorAndVerifier migrator_verifier;
  migrator_verifier.migrate_db(v1_db_path, v4_db_path, false);
  migrator_verifier.migrate_db(v4_db_path, v1_db_dsh_path, true);
  migrator_verifier.migrate_db(v1_db_dsh_path, v4_db_dsh_path, false);

  bool result = false;
  migrator_verifier.verify_DBs(v1_db_path, v1_db_dsh_path, result);
  ASSERT_TRUE(result);

  result = false;
  migrator_verifier.verify_DBs(v4_db_path, v4_db_dsh_path, result);
  ASSERT_TRUE(result);

  ASSERT_NO_THROW(fs::remove_all(v1_db_path));
  ASSERT_NO_THROW(fs::remove_all(v4_db_path));
  ASSERT_NO_THROW(fs::remove_all(v1_db_dsh_path));
  ASSERT_NO_THROW(fs::remove_all(v4_db_dsh_path));
}

TEST(v4_migration_test, check_e2e_flow_3) {
  std::mt19937_64 eng{std::random_device{}()};
  std::uniform_int_distribution<> num_cat_dist{7, 57};
  std::uniform_int_distribution<> num_block_dist{111, 555};
  std::string v1_db_path;
  std::string v4_db_path;
  std::string v1_db_dsh_path;
  std::string v4_db_dsh_path;
  {
    auto v1_db = TestRocksDb::createNative(1);
    v1_db_path = v1_db->path();
    auto v4_db = TestRocksDb::createNative(2);
    v4_db_path = v4_db->path();
    auto v1_db_dsh = TestRocksDb::createNative(3);
    v1_db_dsh_path = v1_db_dsh->path();
    auto v4_db_dsh = TestRocksDb::createNative(4);
    v4_db_dsh_path = v4_db_dsh->path();
    BlockchainRandomPopulator kvbc_populator_verifier(nullptr, false, v4_db, true);
    kvbc_populator_verifier.randomly_populate_single_bc<concord::kvbc::V4_BLOCKCHAIN>(num_cat_dist(eng),
                                                                                      num_block_dist(eng));
  }

  MigratorAndVerifier migrator_verifier;
  migrator_verifier.migrate_db(v4_db_path, v1_db_path, true);
  migrator_verifier.migrate_db(v1_db_path, v4_db_dsh_path, false);
  migrator_verifier.migrate_db(v4_db_dsh_path, v1_db_dsh_path, true);

  bool result = false;
  migrator_verifier.verify_DBs(v1_db_path, v1_db_dsh_path, result);
  ASSERT_TRUE(result);

  result = false;
  migrator_verifier.verify_DBs(v4_db_path, v4_db_dsh_path, result);
  ASSERT_TRUE(result);

  ASSERT_NO_THROW(fs::remove_all(v1_db_path));
  ASSERT_NO_THROW(fs::remove_all(v4_db_path));
  ASSERT_NO_THROW(fs::remove_all(v1_db_dsh_path));
  ASSERT_NO_THROW(fs::remove_all(v4_db_dsh_path));
}

}  // namespace

int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}