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

#include "kv_types.hpp"
#include "sparse_merkle/base_types.h"

#include <optional>
#include <set>
#include <unordered_map>
#include <variant>

namespace concord::kvbc::categorization::detail {

struct MerkleUpdateInfo {
  struct KeyFlags {
    bool deleted{false};
  };

  std::unordered_map<Key, KeyFlags> keys;
  std::array<std::uint8_t, 32> hash;
  sparse_merkle::Version version;
  std::string category_id;

  template <typename T>
  bool operator<(const T &other) const {
    return category_id < other.category_id;
  }
};

struct KeyValueUpdateInfo {
  struct KeyFlags {
    bool deleted{false};

    // Applicable to non-deleted keys only.
    bool stale_on_update{false};
  };

  std::unordered_map<Key, KeyFlags> keys;
  std::optional<std::array<std::uint8_t, 32>> hash;
  std::string category_id;

  template <typename T>
  bool operator<(const T &other) const {
    return category_id < other.category_id;
  }
};

struct CategorizedKeyValueUpdateInfo {
  std::set<Key> keys;
  std::optional<std::array<std::uint8_t, 32>> hash;
  std::string category_id;

  template <typename T>
  bool operator<(const T &other) const {
    return category_id < other.category_id;
  }
};

// Represents updates returned by categories and consumbed by the blockchain (for use in blocks).
using CategoryUpdateInfos = std::set<std::variant<MerkleUpdateInfo, KeyValueUpdateInfo, CategorizedKeyValueUpdateInfo>>;

}  // namespace concord::kvbc::categorization::detail
