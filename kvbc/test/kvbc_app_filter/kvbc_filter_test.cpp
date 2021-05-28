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

#include <boost/detail/endian.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <cassert>
#include <exception>
#include <memory>
#include <string>
#include <vector>
#include "Logger.hpp"
#include "categorization/updates.h"
#include "categorization/db_categories.h"
#include "endianness.hpp"
#include "gtest/gtest.h"
#include "kv_types.hpp"
#include "memorydb/client.h"
#include "memorydb/key_comparator.h"
#include "openssl_crypto.hpp"
#include "status.hpp"
#include "concord_kvbc.pb.h"

#include <cassert>
#include <exception>
#include <memory>
#include <string>
#include <vector>
#include "kvbc_app_filter/kvbc_app_filter.h"
#include "storage/test/storage_test_common.h"

using com::vmware::concord::kvbc::ValueWithTrids;

using boost::lockfree::spsc_queue;
using concord::kvbc::BlockId;
using concord::kvbc::InvalidBlockRange;
using concord::kvbc::KvbAppFilter;
using concord::kvbc::KvbFilteredUpdate;
using concord::kvbc::KvbUpdate;
using concord::util::openssl_utils::computeSHA256Hash;

namespace {

constexpr auto kLastBlockId = BlockId{150};

std::string CreateTridKvbValue(const std::string &value, const std::vector<std::string> &trid_list) {
  ValueWithTrids proto;
  proto.set_value(value);
  for (const auto &trid : trid_list) {
    proto.add_trid(trid);
  }

  size_t size = proto.ByteSizeLong();
  std::string ret(size, '\0');
  proto.SerializeToArray(ret.data(), ret.size());

  return ret;
}

class FakeStorage : public concord::kvbc::IReader {
 public:
  std::vector<KvbUpdate> data_;

  std::optional<concord::kvbc::categorization::Value> get(const std::string &category_id,
                                                          const std::string &key,
                                                          BlockId block_id) const override {
    ADD_FAILURE() << "get() should not be called by this test";
    return {};
  }

  std::optional<concord::kvbc::categorization::Value> getLatest(const std::string &category_id,
                                                                const std::string &key) const override {
    ADD_FAILURE() << "getLatest() should not be called by this test";
    return {};
  }
  void multiGet(const std::string &category_id,
                const std::vector<std::string> &keys,
                const std::vector<BlockId> &versions,
                std::vector<std::optional<concord::kvbc::categorization::Value>> &values) const override {
    ADD_FAILURE() << "multiGet() should not be called by this test";
    values = {};
  }

  void multiGetLatest(const std::string &category_id,
                      const std::vector<std::string> &keys,
                      std::vector<std::optional<concord::kvbc::categorization::Value>> &values) const override {
    ADD_FAILURE() << "multiGetLatest() should not be called by this test";
    values = {};
  }

  std::optional<concord::kvbc::categorization::TaggedVersion> getLatestVersion(const std::string &category_id,
                                                                               const std::string &key) const override {
    ADD_FAILURE() << "getLatestVersion() should not be called by this test";
    return {};
  }

  void multiGetLatestVersion(
      const std::string &category_id,
      const std::vector<std::string> &keys,
      std::vector<std::optional<concord::kvbc::categorization::TaggedVersion>> &versions) const override {
    ADD_FAILURE() << "multiGetLatestVersion() should not be called by this test";
  }

  std::optional<concord::kvbc::categorization::Updates> getBlockUpdates(BlockId block_id) const override {
    if (block_id >= 0 && block_id < blockId_) {
      auto data = data_.at(block_id).immutable_kv_pairs;
      concord::kvbc::categorization::Updates updates{};
      concord::kvbc::categorization::ImmutableUpdates immutable{};
      for (auto &[k, v] : data.kv) {
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
    ADD_FAILURE() << "Provide a valid block range " << block_id << " " << blockId_;
    return {};
  }

  BlockId getGenesisBlockId() const override {
    ADD_FAILURE() << "get() should not be called by this test";
    return 0;
  }

  BlockId getLastBlockId() const override { return blockId_; }

  // Dummy method to fill DB for testing, each client id can watch only the
  // block id that equals to his client id.
  void fillWithData(BlockId num_of_blocks) {
    blockId_ = num_of_blocks;
    std::string key{"Key"};
    for (BlockId i = 0; i <= num_of_blocks; i++) {
      concord::kvbc::categorization::ImmutableValueUpdate data;
      data.data = "TridVal" + std::to_string(i);
      data.tags.push_back(std::to_string(i));
      concord::kvbc::categorization::ImmutableInput input;
      input.kv.insert({concordUtils::toBigEndianStringBuffer(i) + key, data});
      data_.push_back({i, "cid", std::move(input)});
    }
  }

 private:
  BlockId blockId_{kLastBlockId};
};

// Helper function to test cases involving computation of expected hash
// value(s).
std::string blockIdToByteStringLittleEndian(const BlockId &block_id) {
#ifdef BOOST_LITTLE_ENDIAN
  return std::string(reinterpret_cast<const char *>(&block_id), sizeof(block_id));
#else  // BOOST_LITTLE_ENDIAN not defined in this case
#ifndef BOOST_BIG_ENDIAN
  static_assert(false,
                "Cannot determine endianness (needed for computing expected "
                "Thin Replica hash values).");
#endif  // BOOST_BIG_ENDIAN defined
  const char *block_id_as_bytes = reinterpret_cast<const char *>(block_id);
  string block_id_little_endian;
  for (size_t i = 1; i <= sizeof(block_id); ++i) {
    block_id_little_endian += *(block_id_as_bytes + sizeof(block_id) - i);
  }
  return block_id_little_endian;
#endif  // BOOST_LITTLE_ENDIAN defined/else
}

// Prefix the key with the immutable index to make it unique and ordered.
inline std::string prefixImmutableKey(const std::string &key, uint64_t immutable_index) {
  return concordUtils::toBigEndianStringBuffer(immutable_index) + key;
}

TEST(kvbc_filter_test, kvbfilter_update_success) {
  FakeStorage storage;
  int client_id = 1;
  std::string key_prefix = "Ke";
  const auto block_id = BlockId{1};
  auto kvb_filter = KvbAppFilter(&storage, std::to_string(client_id), key_prefix);

  auto val_trid1 = CreateTridKvbValue("TridVal1", {"0", "1"});
  auto val_trid2 = CreateTridKvbValue("TridVal2", {"0"});

  std::string key1{"Key1"};
  std::string key2{"Key2"};
  concord::kvbc::categorization::ImmutableInput immutable{};
  concord::kvbc::categorization::ImmutableValueUpdate value1;
  value1.data = val_trid1;
  value1.tags = {"0", "1"};
  concord::kvbc::categorization::ImmutableValueUpdate value2;
  value2.data = val_trid2;
  value2.tags = {"0"};
  immutable.kv.insert({prefixImmutableKey(key1, block_id), value1});
  immutable.kv.insert({prefixImmutableKey(key2, block_id), value2});

  const auto &filtered = kvb_filter.filterUpdate({block_id, "cid", immutable});

  EXPECT_EQ(filtered.block_id, block_id);
  EXPECT_EQ(filtered.kv_pairs.size(), 1);
  for (auto &[k, v] : filtered.kv_pairs) {
    EXPECT_EQ(k, "Key1");
    EXPECT_EQ(v, "TridVal1");
  }
}

TEST(kvbc_filter_test, kvbfilter_update_message_prefix_only_one_matched) {
  FakeStorage storage;
  int client_id = 0;
  std::string key_prefix = "Ke";
  const auto block_id = BlockId{1};
  auto kvb_filter = KvbAppFilter(&storage, std::to_string(client_id), key_prefix);

  auto val_trid1 = CreateTridKvbValue("TridVal", {"0"});

  std::string key1{"Rey"};
  std::string key2{"Key"};
  concord::kvbc::categorization::ImmutableInput immutable{};
  concord::kvbc::categorization::ImmutableValueUpdate value1;
  value1.data = val_trid1;
  value1.tags = {"0"};
  concord::kvbc::categorization::ImmutableValueUpdate value2 = value1;
  immutable.kv.insert({prefixImmutableKey(key1, block_id), value1});
  immutable.kv.insert({prefixImmutableKey(key2, block_id), value2});

  const auto &filtered = kvb_filter.filterUpdate({block_id, "cid", immutable});

  EXPECT_EQ(filtered.block_id, block_id);
  const auto &filtered_map = filtered.kv_pairs;
  EXPECT_EQ(filtered_map.size(), 1);
  for (auto &[k, v] : filtered_map) {
    EXPECT_EQ(k, "Key");
    EXPECT_EQ(v, "TridVal");
  }
}

TEST(kvbc_filter_testsr, kvbfilter_update_message_empty_prefix) {
  FakeStorage storage;
  int client_id = 0;
  std::string key_prefix = "";
  const BlockId block_id = BlockId{1};
  auto kvb_filter = KvbAppFilter(&storage, std::to_string(client_id), key_prefix);

  auto val_trid1 = CreateTridKvbValue("TridVal", {"0"});

  std::string key1{"Rey"};
  std::string key2{"Key"};
  concord::kvbc::categorization::ImmutableInput immutable{};
  concord::kvbc::categorization::ImmutableValueUpdate value1;
  value1.data = val_trid1;
  value1.tags = {"0"};
  concord::kvbc::categorization::ImmutableValueUpdate value2 = value1;
  immutable.kv.insert({prefixImmutableKey(key1, block_id), value1});
  immutable.kv.insert({prefixImmutableKey(key2, block_id), value2});

  const auto &filtered = kvb_filter.filterUpdate({block_id, "cid", immutable});

  EXPECT_EQ(filtered.block_id, block_id);
  const auto &filtered_kv = filtered.kv_pairs;
  EXPECT_EQ(filtered_kv.size(), 2);
  auto it1 = std::find(filtered_kv.begin(), filtered_kv.end(), std::pair<std::string, std::string>{"Rey", "TridVal"});
  EXPECT_NE(it1, filtered_kv.end());
  auto it2 = std::find(filtered_kv.begin(), filtered_kv.end(), std::pair<std::string, std::string>{"Key", "TridVal"});
  EXPECT_NE(it2, filtered_kv.end());
}

TEST(kvbc_filter_test, kvbfilter_update_client_has_no_trids) {
  FakeStorage storage;
  int client_id = 1;
  std::string key_prefix = "";
  const auto block_id = BlockId{1};
  auto kvb_filter = KvbAppFilter(&storage, std::to_string(client_id), key_prefix);

  std::string key1{"Rey"};
  std::string key2{"Key"};
  concord::kvbc::categorization::ImmutableInput immutable{};
  concord::kvbc::categorization::ImmutableValueUpdate value1;
  value1.data = "TridVal";
  value1.tags = {"0"};
  concord::kvbc::categorization::ImmutableValueUpdate value2 = value1;
  immutable.kv.insert({prefixImmutableKey(key1, block_id), value1});
  immutable.kv.insert({prefixImmutableKey(key2, block_id), value2});

  const auto &filtered = kvb_filter.filterUpdate({block_id, "cid", immutable});

  EXPECT_EQ(filtered.block_id, block_id);
  EXPECT_EQ(filtered.kv_pairs.size(), 0);
}

TEST(kvbc_filter_test, kvbfilter_hash_update_success) {
  FakeStorage storage;
  int client_id = 1;
  std::string key_prefix = "";
  auto kvb_filter = KvbAppFilter(&storage, std::to_string(client_id), key_prefix);

  std::string key{"Restringy"};
  std::string key1{"Key"};
  KvbFilteredUpdate::OrderedKVPairs kv{};
  ValueWithTrids proto;
  proto.add_trid("0");
  proto.set_value("TridVal");
  std::string buf = proto.SerializeAsString();
  kv.push_back({key, buf});
  kv.push_back({key1, buf});

  BlockId block_id = 0;
  auto hash_val = kvb_filter.hashUpdate({block_id, "cid", kv});

  // make hash value for check
  std::string concatenated_entry_hashes;
  concatenated_entry_hashes += blockIdToByteStringLittleEndian(block_id);
  std::map<std::string, std::string> entry_hashes;
  entry_hashes[computeSHA256Hash(key.data(), key.length())] = computeSHA256Hash(buf.data(), buf.length());
  entry_hashes[computeSHA256Hash(key1.data(), key1.length())] = computeSHA256Hash(buf.data(), buf.length());
  for (const auto &kvp_hashes : entry_hashes) {
    concatenated_entry_hashes += kvp_hashes.first;
    concatenated_entry_hashes += kvp_hashes.second;
  }

  EXPECT_EQ(hash_val, computeSHA256Hash(concatenated_entry_hashes));
}

TEST(kvbc_filter_test, kvbfilter_success_get_blocks_in_range) {
  FakeStorage storage;
  size_t client_id = 123;
  std::string key_prefix = "";
  auto kvb_filter = KvbAppFilter(&storage, std::to_string(client_id), key_prefix);
  storage.fillWithData(kLastBlockId);

  BlockId block_id_start = 0;
  BlockId block_id_end = 10;
  KvbFilteredUpdate temporary;
  spsc_queue<KvbFilteredUpdate> queue_out{storage.getLastBlockId()};
  std::atomic_bool stop_exec = false;
  kvb_filter.readBlockRange(block_id_start, block_id_end, queue_out, stop_exec);

  std::vector<KvbFilteredUpdate> filtered_kv_pairs;
  while (queue_out.pop(temporary)) {
    filtered_kv_pairs.push_back(temporary);
  }

  EXPECT_EQ(filtered_kv_pairs.size(), block_id_end - block_id_start + 1);
  for (size_t i = 0; i < filtered_kv_pairs.size(); i++) {
    const auto &x = filtered_kv_pairs.at(i);
    if (i != client_id)
      EXPECT_EQ(x.kv_pairs.size(), 0);
    else
      EXPECT_EQ(x.kv_pairs.size(), 1);
  }
}

TEST(kvbc_filter_test, kvbfilter_stop_exec_in_the_middle) {
  FakeStorage storage;
  int client_id = 1;
  std::string key_prefix = "";
  auto kvb_filter = KvbAppFilter(&storage, std::to_string(client_id), key_prefix);
  storage.fillWithData(1000);
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now() + std::chrono::seconds(30);
  KvbFilteredUpdate temporary;
  spsc_queue<KvbFilteredUpdate> queue_out{storage.getLastBlockId()};
  std::atomic_bool stop_exec = false;
  auto kvb_reader = std::async(std::launch::async,
                               &KvbAppFilter::readBlockRange,
                               kvb_filter,
                               0,
                               kLastBlockId,
                               std::ref(queue_out),
                               std::ref(stop_exec));
  while (queue_out.read_available() < 5) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    if (end < std::chrono::steady_clock::now()) ConcordAssert(false);
  }
  stop_exec = true;
  kvb_reader.get();
  int num_of_blocks = 0;
  while (queue_out.pop(temporary)) {
    num_of_blocks++;
  }
  EXPECT_GT(num_of_blocks, 0);
  EXPECT_LE(num_of_blocks, 1000);
}

TEST(kvbc_filter_test, kvbfilter_block_out_of_range) {
  FakeStorage storage;
  int client_id = 1;
  std::string key_prefix = "";
  auto kvb_filter = KvbAppFilter(&storage, std::to_string(client_id), key_prefix);
  BlockId block_id = kLastBlockId + 5;
  KvbFilteredUpdate temporary;
  spsc_queue<KvbFilteredUpdate> queue_out{storage.getLastBlockId()};
  std::atomic_bool stop_exec = false;
  EXPECT_THROW(kvb_filter.readBlockRange(block_id, block_id, queue_out, stop_exec);, InvalidBlockRange);
}

TEST(kvbc_filter_test, kvbfilter_end_block_greater_then_start_block) {
  FakeStorage storage;
  int client_id = 1;
  std::string key_prefix = "";
  auto kvb_filter = KvbAppFilter(&storage, std::to_string(client_id), key_prefix);
  storage.fillWithData(kLastBlockId);
  BlockId block_id_end = 0;
  BlockId block_id_start = 10;
  KvbFilteredUpdate temporary;
  spsc_queue<KvbFilteredUpdate> queue_out{storage.getLastBlockId()};
  std::atomic_bool stop_exec = false;
  EXPECT_THROW(kvb_filter.readBlockRange(block_id_start, block_id_end, queue_out, stop_exec);, InvalidBlockRange);
}

TEST(kvbc_filter_test, kvbfilter_success_hash_of_blocks_in_range) {
  FakeStorage storage;
  int client_id = 1;
  std::string key_prefix = "";
  auto kvb_filter = KvbAppFilter(&storage, std::to_string(client_id), key_prefix);
  storage.fillWithData(kLastBlockId);

  // add Trid 1 to watch on another value -> now Trid 1 watches on 2 values
  std::string key1 = {"Key"};
  concord::kvbc::categorization::ImmutableInput immutable{};
  concord::kvbc::categorization::ImmutableValueUpdate value1;
  value1.data = "TridVal2";
  value1.tags = {"1", "2"};
  immutable.kv.insert({prefixImmutableKey(key1, 2), value1});

  storage.data_.at(2) = {2, "cid", immutable};

  BlockId block_id_start = 0;
  BlockId block_id_end = 10;
  KvbFilteredUpdate temporary;
  spsc_queue<KvbFilteredUpdate> queue_out{storage.getLastBlockId()};
  std::atomic_bool stop_exec = false;
  kvb_filter.readBlockRange(block_id_start, block_id_end, queue_out, stop_exec);
  std::vector<KvbFilteredUpdate> filtered_kv_pairs;
  while (queue_out.pop(temporary)) {
    filtered_kv_pairs.push_back(temporary);
  }

  auto hash_value = kvb_filter.readBlockRangeHash(block_id_start, block_id_end);
  std::string concatenated_update_hashes;
  for (BlockId i = block_id_start; i <= block_id_end; ++i) {
    concatenated_update_hashes += kvb_filter.hashUpdate(filtered_kv_pairs.at(i));
  }

  EXPECT_EQ(hash_value, computeSHA256Hash(concatenated_update_hashes));
}

TEST(kvbc_filter_test, kvbfilter_success_hash_of_block) {
  FakeStorage storage;
  int client_id = 1;
  std::string key_prefix = "";
  auto kvb_filter = KvbAppFilter(&storage, std::to_string(client_id), key_prefix);
  storage.fillWithData(kLastBlockId);
  BlockId block_id_start = 1;

  auto hash_value = kvb_filter.readBlockHash(block_id_start);
  const auto &[block_id, _, filtered] = kvb_filter.filterUpdate(storage.data_[1]);

  std::string concatenated_entry_hashes;
  concatenated_entry_hashes += blockIdToByteStringLittleEndian(block_id);
  std::map<std::string, std::string> entry_hashes;
  for (const auto &[key, value] : filtered) {
    entry_hashes[computeSHA256Hash(key.data(), key.length())] = computeSHA256Hash(value.data(), value.length());
  }
  for (const auto &kvp_hashes : entry_hashes) {
    concatenated_entry_hashes += kvp_hashes.first;
    concatenated_entry_hashes += kvp_hashes.second;
  }

  EXPECT_EQ(hash_value, computeSHA256Hash(concatenated_entry_hashes));
}

TEST(kvbc_filter_test, kvbfilter_hash_filter_block_out_of_range) {
  FakeStorage storage;
  int client_id = 1;
  std::string key_prefix = "";
  auto kvb_filter = KvbAppFilter(&storage, std::to_string(client_id), key_prefix);
  storage.fillWithData(kLastBlockId);
  BlockId block_id_start = 1;
  BlockId block_id_end = kLastBlockId + 5;

  EXPECT_THROW(kvb_filter.readBlockRangeHash(block_id_start, block_id_end);, InvalidBlockRange);
}

TEST(kvbc_filter_test, kvbfilter_update_empty_kv_pair) {
  FakeStorage storage;
  int client_id = 1;
  auto kvb_filter = KvbAppFilter(&storage, std::to_string(client_id), "");

  concord::kvbc::categorization::ImmutableInput immutable{};
  BlockId block_id = 0;

  const auto &[bid, _, filtered] = kvb_filter.filterUpdate({block_id, "cid", immutable});

  EXPECT_EQ(bid, block_id);
  EXPECT_EQ(filtered.size(), 0);
}

TEST(kvbc_filter_test, updates_order) {
  FakeStorage storage;
  int client_id = 1;
  auto kvb_filter = KvbAppFilter(&storage, std::to_string(client_id), "");

  std::vector<std::string> order{"z", "o", "a", "c"};
  ValueWithTrids proto;
  proto.add_trid("1");
  proto.set_value("imm_value");
  std::string buf = proto.SerializeAsString();

  uint64_t immutable_index = 0;
  concord::kvbc::categorization::ImmutableUpdates immutables;
  immutable_index++;
  immutables.addUpdate(prefixImmutableKey("z", immutable_index),
                       concord::kvbc::categorization::ImmutableUpdates::ImmutableValue{std::string(buf), {"1", "2"}});
  immutable_index++;
  immutables.addUpdate(prefixImmutableKey("o", immutable_index),
                       concord::kvbc::categorization::ImmutableUpdates::ImmutableValue{std::string(buf), {"1", "2"}});
  immutable_index++;
  immutables.addUpdate(prefixImmutableKey("a", immutable_index),
                       concord::kvbc::categorization::ImmutableUpdates::ImmutableValue{std::string(buf), {"1", "2"}});
  immutable_index++;
  immutables.addUpdate(prefixImmutableKey("c", immutable_index),
                       concord::kvbc::categorization::ImmutableUpdates::ImmutableValue{std::string(buf), {"1", "2"}});

  concord::kvbc::categorization::Updates updates;
  concord::kvbc::categorization::VersionedUpdates internal;
  immutables.calculateRootHash(true);
  if (immutables.getData().kv.size() > 0)
    updates.add(concord::kvbc::categorization::kExecutionEventsCategory, std::move(immutables));
  auto imm_var_updates = updates.categoryUpdates(concord::kvbc::categorization::kExecutionEventsCategory);
  EXPECT_TRUE(imm_var_updates != std::nullopt);

  auto &imm_updates = std::get<concord::kvbc::categorization::ImmutableInput>((*imm_var_updates).get());

  auto ordered_kv_pairs = kvb_filter.filterKeyValuePairs(imm_updates);
  EXPECT_EQ(ordered_kv_pairs.size(), order.size());

  for (size_t i = 0; i < order.size(); ++i) {
    EXPECT_EQ(ordered_kv_pairs[i].first, order[i]);
  }
}

}  // anonymous namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  auto logger = logging::getLogger("kvb_filter_test");
  return RUN_ALL_TESTS();
}
