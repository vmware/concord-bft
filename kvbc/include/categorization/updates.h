// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "assertUtils.hpp"
#include "kv_types.hpp"
#include "categorized_kvbc_msgs.cmf.hpp"

#include <ctime>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <variant>

// Categorized key-value updates for KVBC blocks. Every category supports different properties and functionalities.
// Note1: Empty category IDs are invalid and not supported.
// Note2: Using the same category ID for different category types is an error.
namespace concord::kvbc::categorization {

// Shared updates across multiple categories.
// Persists key-values directly in the underlying key-value store. All key-values are marked stale during the update
// itself. Explicit deletes are not supported.
struct SharedKeyValueUpdates {
  SharedKeyValueUpdates() = default;
  SharedKeyValueUpdates(SharedKeyValueUpdates&& other) = default;
  SharedKeyValueUpdates& operator=(SharedKeyValueUpdates&& other) = default;

  // Do not allow copy
  SharedKeyValueUpdates(SharedKeyValueUpdates& other) = delete;
  SharedKeyValueUpdates& operator=(SharedKeyValueUpdates& other) = delete;

  struct SharedValue {
    SharedValue(std::string&& val, std::set<std::string>&& cat_ids) {
      data_.value = std::move(val);
      for (auto it = cat_ids.begin(); it != cat_ids.end();) {
        // Save the iterator as extract() invalidates it.
        auto extracted_it = it;
        ++it;
        data_.category_ids.emplace_back(cat_ids.extract(extracted_it).value());
      }
    }

   private:
    SharedValueData data_;
    friend struct SharedKeyValueUpdates;
  };

  void addUpdate(std::string&& key, SharedValue&& val) { data_.kv.emplace(std::move(key), std::move(val.data_)); }

 private:
  SharedKeyValueUpdatesData data_;
  friend struct Updates;
};

// Updates for a key-value category.
// Persists key-values directly in the underlying key-value store.
// Controls whether a hash of the updated key-values is calculated for this update
struct KeyValueUpdates {
  KeyValueUpdates() = default;
  KeyValueUpdates(KeyValueUpdates&& other) = default;
  KeyValueUpdates& operator=(KeyValueUpdates&& other) = default;

  // Do not allow copy
  KeyValueUpdates(KeyValueUpdates& other) = delete;
  KeyValueUpdates& operator=(KeyValueUpdates& other) = delete;
  struct Value {
    std::string data;

    // If set, the expiry time will be persisted and available on lookups.
    // An implementation might optionally choose to automatically mark the key-value stale at or after the `expire_at`
    // time. If not done automatically, the application needs to manage the lifetime of the key.
    std::optional<std::time_t> expire_at;

    // Mark the key-value stale during the update itself.
    bool stale_on_update{false};
  };

  void addUpdate(std::string&& key, Value&& val) {
    data_.kv.emplace(std::move(key), ValueWithExpirationData{std::move(val.data), val.expire_at, val.stale_on_update});
  }

  // Set a value with no expiration and that is not stale on update
  void addUpdate(std::string&& key, std::string&& val) {
    data_.kv.emplace(std::move(key), ValueWithExpirationData{std::move(val), 0, false});
  }

  void addDelete(std::string&& key) {
    if (const auto [itr, inserted] = unique_deletes_.insert(key); !inserted) {
      *itr;  // disable unused variable
      // Log warn
      return;
    }
    data_.deletes.emplace_back(std::move(key));
  }

  void calculateRootHash(const bool hash) { data_.calculate_root_hash = hash; }

 private:
  KeyValueUpdatesData data_;
  std::set<std::string> unique_deletes_;
  friend struct Updates;
};

// Updates for a merkle tree category.
// Persists key-values in a merkle tree that is constructed on top of the underlying key-value store.
struct MerkleUpdates {
  MerkleUpdates() = default;
  MerkleUpdates(MerkleUpdates&& other) = default;
  MerkleUpdates& operator=(MerkleUpdates&& other) = default;

  // Do not allow copy
  MerkleUpdates(MerkleUpdates& other) = delete;
  MerkleUpdates& operator=(MerkleUpdates& other) = delete;

  void addUpdate(std::string&& key, std::string&& val) { data_.kv.emplace(std::move(key), std::move(val)); }

  void addDelete(std::string&& key) {
    if (const auto [itr, inserted] = unique_deletes_.insert(key); !inserted) {
      *itr;  // disable unused variable
      // Log warn
      return;
    }
    data_.deletes.emplace_back(std::move(key));
  }

  const MerkleUpdatesData& getData() { return data_; }

 private:
  MerkleUpdatesData data_;
  std::set<std::string> unique_deletes_;
  friend struct Updates;
};

// Updates contains a list of updates for different categories.
// Note: Only a single `SharedKeyValueUpdates` is supported per block.
struct Updates {
  void add(std::string&& category_id, MerkleUpdates&& updates) {
    if (const auto [itr, inserted] = category_updates_.try_emplace(std::move(category_id), std::move(updates.data_));
        !inserted) {
      (void)itr;  // disable unused variable
      throw std::logic_error{std::string("Only one update for category is allowed. type: Merkle, category: ") +
                             category_id};
    }
  }

  void add(std::string&& category_id, KeyValueUpdates&& updates) {
    if (const auto [itr, inserted] = category_updates_.try_emplace(std::move(category_id), std::move(updates.data_));
        !inserted) {
      (void)itr;  // disable unused variable
      throw std::logic_error{std::string("Only one update for category is allowed. type: KVHash, category: ") +
                             category_id};
    }
  }

  void add(SharedKeyValueUpdates&& updates) {
    if (shared_update_.has_value()) {
      throw std::logic_error{std::string("Only one update for category is allowed for shared category")};
    }
    shared_update_.emplace(std::move(updates.data_));
  }

 private:
  friend class KeyValueBlockchain;
  std::optional<SharedKeyValueUpdatesData> shared_update_;
  std::map<std::string, std::variant<MerkleUpdatesData, KeyValueUpdatesData>> category_updates_;
};

}  // namespace concord::kvbc::categorization
