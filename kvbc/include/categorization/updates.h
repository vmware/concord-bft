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

// Updates for multiple categories.
// Persists key-values directly in the underlying key-value store. All key-values are marked stale during the update
// itself. Explicit removals are not supported.
struct CategorizedKeyValueUpdate {
  struct CategorizedValue {
    Value value;

    // Persist the key-value into this set of categories.
    std::set<std::string> category_ids;
  };
  std::map<Key, CategorizedValue> updates;

  // Controls whether a hash of the key-values is calculated per category for this update.
  bool calculate_hash{true};

  // All key-values are marked stale during the update itself. Cannot be turned off for this category.
  static constexpr bool stale_on_update{true};

  bool operator<(const CategorizedKeyValueUpdate &) const {
    // Implies that a single categorized update per block is supported.
    return false;
  }

  template <typename T>
  bool operator<(const T &) const {
    // Since empty category IDs are not supported, use the empty one to designate a categorized update. Implies that the
    // categorized update is always `less` than other types and that a single categorized update per block is supported.
    return true;
  }
};

// Updates for a merkle tree category.
// Persists key-values in a merkle tree that is constructed on top of the underlying key-value store.
struct MerkleUpdate {
  MerkleUpdate(const std::string &category_id) : category_id{category_id} { ConcordAssert(!category_id.empty()); }

  OrderedSetOfKeyValuePairs updates;
  OrderedKeysSet removals;

  const std::string category_id;

  bool operator<(const CategorizedKeyValueUpdate &) const { return false; }

  template <typename T>
  bool operator<(const T &other) const {
    return category_id < other.category_id;
  }
};

// Updates for a key-value category.
// Persists key-values directly in the underlying key-value store.
struct KeyValueUpdate {
  KeyValueUpdate(const std::string &category_id) : category_id{category_id} { ConcordAssert(!category_id.empty()); }

  struct ValueData {
    Value value;

    // If set, the expiry time will be persisted and available on lookups.
    // An implementation might optionally choose to automatically mark the key-value stale at or after the `expire_at`
    // time. If not done automatically, the application needs to manage the lifetime of the key.
    std::optional<std::time_t> expire_at;

    // Mark the key-value stale during the update itself.
    bool stale_on_update{false};
  };
  std::map<Key, ValueData> updates;
  OrderedKeysSet removals;

  const std::string category_id;

  // Controls whether a hash of the updated key-values is calculated for this update.
  bool calculate_hash{false};

  bool operator<(const CategorizedKeyValueUpdate &) const { return false; }

  template <typename T>
  bool operator<(const T &other) const {
    return category_id < other.category_id;
  }
};

// A block update is a list of updates for different categories.
// Note: Only a single `CategorizedKeyValueUpdate` is supported per block.
using CategorizedUpdates = std::set<std::variant<MerkleUpdate, KeyValueUpdate, CategorizedKeyValueUpdate>>;

}  // namespace concord::kvbc::categorization
