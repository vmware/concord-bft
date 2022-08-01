// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "v4blockchain/detail/categories.h"
#include "v4blockchain/detail/column_families.h"
#include "Logger.hpp"
#include "v4blockchain/detail/detail.h"

using namespace concord::kvbc;
namespace concord::kvbc::v4blockchain::detail {

Categories::Categories(const std::shared_ptr<concord::storage::rocksdb::NativeClient>& client,
                       const std::optional<std::map<std::string, categorization::CATEGORY_TYPE>>& category_types)
    : native_client_(client) {
  if (native_client_->createColumnFamilyIfNotExisting(v4blockchain::detail::CATEGORIES_CF)) {
    LOG_INFO(V4_BLOCK_LOG, "Created [" << v4blockchain::detail::CATEGORIES_CF << "]");
  }
  loadCategories();
  if (category_to_prefix_.empty()) {
    initNewBlockchainCategories(category_types);

  } else {
    initExistingBlockchainCategories(category_types);
  }
}

void Categories::loadCategories() {
  auto itr = native_client_->getIterator(detail::CATEGORIES_CF);
  itr.first();
  while (itr) {
    if (itr.valueView().size() != 2) {
      LOG_FATAL(V4_BLOCK_LOG, "Category type value of [" << itr.key() << "] is invalid (bigger than one).");
      ConcordAssertEQ(itr.valueView().size(), 1);
    }
    auto cat_type = static_cast<categorization::CATEGORY_TYPE>(itr.valueView()[0]);
    auto cat_prefix = std::string(1, static_cast<char>(itr.valueView()[1]));
    switch (cat_type) {
      case categorization::CATEGORY_TYPE::block_merkle:
        category_types_[itr.key()] = categorization::CATEGORY_TYPE::block_merkle;
        category_to_prefix_[itr.key()] = cat_prefix;
        LOG_INFO(V4_BLOCK_LOG,
                 "Created category [" << itr.key() << "] as type BlockMerkleCategory with prefix " << cat_prefix);
        break;
      case categorization::CATEGORY_TYPE::immutable:
        category_types_[itr.key()] = categorization::CATEGORY_TYPE::immutable;
        category_to_prefix_[itr.key()] = cat_prefix;
        LOG_INFO(V4_BLOCK_LOG,
                 "Created category [" << itr.key() << "] as type ImmutableKeyValueCategory with prefix " << cat_prefix);
        break;
      case categorization::CATEGORY_TYPE::versioned_kv:
        category_types_[itr.key()] = categorization::CATEGORY_TYPE::versioned_kv;
        category_to_prefix_[itr.key()] = cat_prefix;
        LOG_INFO(V4_BLOCK_LOG,
                 "Created category [" << itr.key() << "] as type VersionedKeyValueCategory with prefix " << cat_prefix);
        break;
      default:
        ConcordAssert(false);
        break;
    }
    itr.next();
  }
}

void Categories::initNewBlockchainCategories(
    const std::optional<std::map<std::string, categorization::CATEGORY_TYPE>>& category_types) {
  if (!category_types) {
    const auto msg = "Category types needed when constructing a KeyValueBlockchain for a new blockchain";
    LOG_ERROR(V4_BLOCK_LOG, msg);
    throw std::invalid_argument{msg};
  }
  // Add all categories passed by the user.
  for (const auto& [category_id, type] : *category_types) {
    addNewCategory(category_id, type);
  }
}

void Categories::addNewCategory(const std::string& cat_id, categorization::CATEGORY_TYPE type) {
  // cache the type in memory and store it in DB
  auto inserted = category_types_.try_emplace(cat_id, type).second;
  if (!inserted) {
    LOG_FATAL(V4_BLOCK_LOG, "Category [" << cat_id << "] already exists in type map");
    ConcordAssert(false);
  }

  auto prefix = std::string(1, static_cast<char>(PREFIX_START + category_types_.size()));
  for (const auto& [k, v] : category_to_prefix_) {
    (void)k;  // unused
    ConcordAssertNE(prefix, v);
  }
  inserted = category_to_prefix_.try_emplace(cat_id, prefix).second;
  if (!inserted) {
    LOG_FATAL(V4_BLOCK_LOG, "Category [" << cat_id << "] already exists in prefix map");
    ConcordAssert(false);
  }
  ConcordAssertLE(category_types_.size(), MAX_NUM_CATEGORIES);
  std::string value = {static_cast<char>(type), static_cast<char>(PREFIX_START + category_types_.size())};
  native_client_->put(detail::CATEGORIES_CF, cat_id, value);
}

void Categories::initExistingBlockchainCategories(
    const std::optional<std::map<std::string, categorization::CATEGORY_TYPE>>& category_types) {
  if (!category_types) {
    return;
  }

  // Make sure the user passed all existing categories on disk with their correct types.
  for (const auto& [category_id, type] : category_types_) {
    auto it = category_types->find(category_id);
    if (it == category_types->cend()) {
      const auto msg =
          "Category ID [" + category_id + "] exists on disk, but is missing from the given category types parameter";
      LOG_ERROR(V4_BLOCK_LOG, msg);
      throw std::invalid_argument{msg};
    } else if (it->second != type) {
      const auto msg = "Category ID [" + category_id + "] parameter with type [" + categoryStringType(it->second) +
                       "] differs from type on disk [" + categoryStringType(type) + "]";
      LOG_ERROR(V4_BLOCK_LOG, msg);
      throw std::invalid_argument{msg};
    }
  }

  // If the user passed a new category, add it.
  for (const auto& [category_id, type] : *category_types) {
    if (category_types_.find(category_id) == category_types_.cend()) {
      addNewCategory(category_id, type);
    }
  }
}

}  // namespace concord::kvbc::v4blockchain::detail
