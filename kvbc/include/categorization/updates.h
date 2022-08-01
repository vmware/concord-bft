// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
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
#include "details.h"
#include "kv_types.hpp"
#include "categorized_kvbc_msgs.cmf.hpp"

#include <ctime>
#include <functional>
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

// Do not allow copy outside of benchmarks.
#ifndef KVBCBENCH
  ImmutableUpdates(ImmutableUpdates& other) = delete;
  ImmutableUpdates& operator=(const ImmutableUpdates& other) = delete;
#else
  ImmutableUpdates(ImmutableUpdates& other) = default;
  ImmutableUpdates& operator=(const ImmutableUpdates& other) = default;
#endif

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
    friend struct Updates;
  };

  using ValueType = ImmutableValue;

  void addUpdate(std::string&& key, ImmutableValue&& val) { data_.kv[std::move(key)] = std::move(val.update_); }

  void calculateRootHash(const bool hash) { data_.calculate_root_hash = hash; }

  size_t size() const { return data_.kv.size(); }

  const ImmutableInput& getData() const { return data_; }

 private:
  ImmutableInput data_;
  friend struct Updates;
};

// Updates for a versioned key-value category.
// Persists versioned (by block ID) key-values directly in the underlying key-value store.
// Supports an option to calculate a root hash from the key-values in the update. The root hash can be used for key
// proofs.
struct VersionedUpdates {
  VersionedUpdates() = default;
  VersionedUpdates(VersionedUpdates&& other) = default;
  VersionedUpdates& operator=(VersionedUpdates&& other) = default;

  // Do not allow copy outside of benchmarks.
#ifndef KVBCBENCH
  VersionedUpdates(const VersionedUpdates& other) = delete;
  VersionedUpdates& operator=(const VersionedUpdates& other) = delete;
#else
  VersionedUpdates(const VersionedUpdates& other) = default;
  VersionedUpdates& operator=(const VersionedUpdates& other) = default;
#endif

  struct Value {
    std::string data;

    // Mark the key-value stale during the update itself.
    bool stale_on_update{false};
  };

  using ValueType = Value;

  void addUpdate(std::string&& key, Value&& val) {
    data_.kv[std::move(key)] = ValueWithFlags{std::move(val.data), val.stale_on_update};
  }

  // Set a value with no flags set
  void addUpdate(std::string&& key, std::string&& val) {
    data_.kv[std::move(key)] = ValueWithFlags{std::move(val), false};
  }

  void addDelete(std::string&& key) {
    deletes_sorted_and_duplicates_removed = false;
    data_.deletes.emplace_back(std::move(key));
  }

  void calculateRootHash(const bool hash) { data_.calculate_root_hash = hash; }

  std::size_t size() const { return data_.kv.size(); }

  const VersionedInput& getData() const {
    sortAndRemoveDuplicateDeletes();
    return data_;
  }

 private:
  void sortAndRemoveDuplicateDeletes() const {
    if (!deletes_sorted_and_duplicates_removed) {
      detail::sortAndRemoveDuplicates(data_.deletes);
      deletes_sorted_and_duplicates_removed = true;
    }
  }

 private:
  mutable VersionedInput data_;
  mutable bool deletes_sorted_and_duplicates_removed{false};
  friend struct Updates;
};

// Updates for a merkle tree category.
// Persists key-values in a merkle tree that is constructed on top of the underlying key-value store.
struct BlockMerkleUpdates {
  BlockMerkleUpdates() = default;
  BlockMerkleUpdates(BlockMerkleUpdates&& other) = default;
  BlockMerkleUpdates(BlockMerkleInput&& data) : data_{std::move(data)} {}
  BlockMerkleUpdates& operator=(BlockMerkleUpdates&& other) = default;

  // Do not allow copy
  BlockMerkleUpdates(BlockMerkleUpdates& other) = delete;
  BlockMerkleUpdates& operator=(BlockMerkleUpdates& other) = delete;

  using ValueType = std::string;

  void addUpdate(std::string&& key, std::string&& val) { data_.kv[std::move(key)] = std::move(val); }

  void addDelete(std::string&& key) {
    deletes_sorted_and_duplicates_removed = false;
    data_.deletes.emplace_back(std::move(key));
  }

  const BlockMerkleInput& getData() const {
    sortAndRemoveDuplicateDeletes();
    return data_;
  }

  std::size_t size() const { return data_.kv.size(); }

 private:
  void sortAndRemoveDuplicateDeletes() const {
    if (!deletes_sorted_and_duplicates_removed) {
      detail::sortAndRemoveDuplicates(data_.deletes);
      deletes_sorted_and_duplicates_removed = true;
    }
  }

 private:
  mutable BlockMerkleInput data_;
  mutable bool deletes_sorted_and_duplicates_removed{false};
  friend struct Updates;
};

// Updates contains a list of updates for different categories.
struct Updates {
  Updates() = default;
  Updates(CategoryInput&& updates) : category_updates_{std::move(updates)} {}
  void add(const std::string& category_id, BlockMerkleUpdates&& updates) {
    block_merkle_size += updates.size();
    updates.sortAndRemoveDuplicateDeletes();
    if (const auto [itr, inserted] = category_updates_.kv.try_emplace(category_id, std::move(updates.data_));
        !inserted) {
      (void)itr;  // disable unused variable
      throw std::logic_error{std::string("Only one update for category is allowed. type: BlockMerkle, category: ") +
                             category_id};
    }
  }

  void add(const std::string& category_id, VersionedUpdates&& updates) {
    versioned_kv_size += updates.size();
    updates.sortAndRemoveDuplicateDeletes();
    if (const auto [itr, inserted] = category_updates_.kv.try_emplace(category_id, std::move(updates.data_));
        !inserted) {
      (void)itr;  // disable unused variable
      throw std::logic_error{std::string("Only one update for category is allowed. type: Versioned, category: ") +
                             category_id};
    }
  }

  void add(const std::string& category_id, ImmutableUpdates&& updates) {
    immutable_size += updates.size();
    if (const auto [itr, inserted] = category_updates_.kv.try_emplace(category_id, std::move(updates.data_));
        !inserted) {
      (void)itr;  // disable unused variable
      throw std::logic_error{std::string("Only one update for category is allowed. type: Immutable, category: ") +
                             category_id};
    }
  }

  std::optional<std::reference_wrapper<const std::variant<BlockMerkleInput, VersionedInput, ImmutableInput>>>
  categoryUpdates(const std::string& category_id) const {
    auto it = category_updates_.kv.find(category_id);
    if (it == category_updates_.kv.cend()) {
      return std::nullopt;
    }
    return it->second;
  }

  const CategoryInput& categoryUpdates() const { return category_updates_; }

  CategoryInput&& categoryUpdates() { return std::move(category_updates_); }

  // Appends a key-value of an `Update` type to already existing key-values for that category.
  // Precondition: The given `category_id` is of the same type as the passed updates.
  // Returns true on success or false if the given `category_id` doesn't exist.
  template <typename Update>
  bool appendKeyValue(const std::string& category_id, std::string&& key, typename Update::ValueType&& value);

  template <typename InputType>
  void addCategoryIfNotExisting(const std::string& category_id) {
    category_updates_.kv.try_emplace(category_id, InputType{});
  }

  std::size_t size() const { return block_merkle_size + versioned_kv_size + immutable_size; }
  bool empty() const { return size() == 0; }
  std::size_t block_merkle_size{};
  std::size_t versioned_kv_size{};
  std::size_t immutable_size{};

 private:
  std::optional<std::reference_wrapper<std::variant<BlockMerkleInput, VersionedInput, ImmutableInput>>>
  mutableCategoryUpdates(const std::string& category_id) {
    auto it = category_updates_.kv.find(category_id);
    if (it == category_updates_.kv.end()) {
      return std::nullopt;
    }
    return it->second;
  }

 private:
  friend bool operator==(const Updates&, const Updates&);
  CategoryInput category_updates_;
};

inline bool operator==(const Updates& l, const Updates& r) { return l.category_updates_ == r.category_updates_; }

template <>
inline bool Updates::appendKeyValue<ImmutableUpdates>(const std::string& category_id,
                                                      std::string&& key,
                                                      ImmutableUpdates::ValueType&& val) {
  auto updates = mutableCategoryUpdates(category_id);
  if (!updates) {
    return false;
  }
  std::get<ImmutableInput>(updates->get()).kv[std::move(key)] = std::move(val.update_);
  immutable_size++;
  return true;
}

template <>
inline bool Updates::appendKeyValue<VersionedUpdates>(const std::string& category_id,
                                                      std::string&& key,
                                                      VersionedUpdates::ValueType&& val) {
  auto updates = mutableCategoryUpdates(category_id);
  if (!updates) {
    return false;
  }
  std::get<VersionedInput>(updates->get()).kv[std::move(key)] =
      ValueWithFlags{std::move(val.data), val.stale_on_update};
  versioned_kv_size++;
  return true;
}

template <>
inline bool Updates::appendKeyValue<BlockMerkleUpdates>(const std::string& category_id,
                                                        std::string&& key,
                                                        BlockMerkleUpdates::ValueType&& val) {
  auto updates = mutableCategoryUpdates(category_id);
  if (!updates) {
    return false;
  }
  std::get<BlockMerkleInput>(updates->get()).kv[std::move(key)] = std::move(val);
  block_merkle_size++;
  return true;
}

}  // namespace concord::kvbc::categorization
