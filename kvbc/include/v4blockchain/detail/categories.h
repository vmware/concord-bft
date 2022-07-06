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

#pragma once
#include <string>
#include <memory>
#include <unordered_map>
#include "rocksdb/native_client.h"
#include <optional>
#include "categorization/base_types.h"

namespace concord::kvbc::v4blockchain::detail {
/*
  To support the notion of categories, introduced in categorized storage.
  A category is mapped to a char that is used as the prefix of the key in the latest CF.
*/
class Categories {
 public:
  // Magic number, from this version new categories won't be defined, therefore it's sufficient.
  static constexpr int8_t MAX_NUM_CATEGORIES = 100;
  // It's more convinient that the prefix is printable.
  static constexpr char PREFIX_START = 0x21;
  Categories(const std::shared_ptr<concord::storage::rocksdb::NativeClient>&,
             const std::optional<std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE>>&);

  Categories() = delete;

  // Throws if the category does not exist
  const std::string& categoryPrefix(const std::string& cat_id) const {
    if (category_to_prefix_.count(cat_id) == 0) {
      throw std::runtime_error("No prefix found for category " + cat_id);
    }
    return category_to_prefix_.at(cat_id);
  }
  concord::kvbc::categorization::CATEGORY_TYPE categoryType(const std::string& cat_id) const {
    return category_types_.at(cat_id);
  }

  const std::unordered_map<std::string, std::string>& prefixMap() const { return category_to_prefix_; }

  std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE> getCategories() const { return category_types_; }

  std::string getCategoryFromPrefix(const std::string& p) const {
    for (const auto& [k, v] : category_to_prefix_) {
      if (v == p) {
        return k;
      }
    }
    return "";
  }

 private:
  void loadCategories();
  void initNewBlockchainCategories(
      const std::optional<std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE>>&);
  void initExistingBlockchainCategories(
      const std::optional<std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE>>&);
  void addNewCategory(const std::string&, concord::kvbc::categorization::CATEGORY_TYPE);

 private:
  std::shared_ptr<concord::storage::rocksdb::NativeClient> native_client_;
  std::unordered_map<std::string, std::string> category_to_prefix_;
  std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE> category_types_;
};

}  // namespace concord::kvbc::v4blockchain::detail
