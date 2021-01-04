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

// Keys in immutable categories have a single version only and can be tagged. Updating keys in immutable categories is
// undefined behavior. Persists key-values directly in the underlying key-value store. All key-values become stale since
// the block they are being added in and this cannot be turned off. Explicit deletes are not supported. Supports an
// option to calculate a root hash per tag from the key-values in the update. The root hash can be used for key proofs
// per tag.
struct ImmutableUpdates {
  ImmutableUpdates() = default;
  ImmutableUpdates(ImmutableUpdates&& other) = default;
  ImmutableUpdates& operator=(ImmutableUpdates&& other) = default;

  // Do not allow copy
  ImmutableUpdates(ImmutableUpdates& other) = delete;
  ImmutableUpdates& operator=(ImmutableUpdates& other) = delete;

  struct ImmutableValue {
    ImmutableValue(std::string&& val, std::set<std::string>&& tags) {
      update_.data = std::move(val);
      for (auto it = tags.begin(); it != tags.end();) {
        // Save the iterator as extract() invalidates it.
        auto extracted_it = it;
        ++it;
        update_.tags.emplace_back(tags.extract(extracted_it).value());
      }
    }

   private:
    ImmutableValueUpdate update_;
    friend struct ImmutableUpdates;
  };

  void addUpdate(std::string&& key, ImmutableValue&& val) { data_.kv.emplace(std::move(key), std::move(val.update_)); }

 private:
  ImmutableUpdatesData data_;
  friend struct Updates;
};

// Updates for a key-value category.
// Persists versioned (by block ID) key-values directly in the underlying key-value store.
// Supports an option to calculate a root hash from the key-values in the update. The root hash can be used for key
// proofs.
struct KeyValueUpdates {
  KeyValueUpdates() = default;
  KeyValueUpdates(KeyValueUpdates&& other) = default;
  KeyValueUpdates(KeyValueUpdatesData&& data) : data_{std::move(data)} {}
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
    data_.kv.emplace(std::move(key), ValueWithExpirationData{std::move(val), std::nullopt, false});
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
  MerkleUpdates(MerkleUpdatesData&& data) : data_{std::move(data)} {}
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
struct Updates {
  Updates() = default;
  Updates(CategoryUpdatesData&& updates) : category_updates_{std::move(updates)} {}
  void add(const std::string& category_id, MerkleUpdates&& updates) {
    if (const auto [itr, inserted] = category_updates_.kv.try_emplace(category_id, std::move(updates.data_));
        !inserted) {
      (void)itr;  // disable unused variable
      throw std::logic_error{std::string("Only one update for category is allowed. type: Merkle, category: ") +
                             category_id};
    }
  }

  void add(const std::string& category_id, KeyValueUpdates&& updates) {
    if (const auto [itr, inserted] = category_updates_.kv.try_emplace(category_id, std::move(updates.data_));
        !inserted) {
      (void)itr;  // disable unused variable
      throw std::logic_error{std::string("Only one update for category is allowed. type: KVHash, category: ") +
                             category_id};
    }
  }

  void add(const std::string& category_id, ImmutableUpdates&& updates) {
    if (const auto [itr, inserted] = category_updates_.kv.try_emplace(category_id, std::move(updates.data_));
        !inserted) {
      (void)itr;  // disable unused variable
      throw std::logic_error{std::string("Only one update for category is allowed. type: Immutable, category: ") +
                             category_id};
    }
  }

 private:
  friend class KeyValueBlockchain;
  CategoryUpdatesData category_updates_;
};

}  // namespace concord::kvbc::categorization
