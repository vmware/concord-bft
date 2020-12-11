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
#include "sha_hash.hpp"

#include <cstddef>
#include <ctime>
#include <optional>
#include <string>

namespace concord::kvbc::categorization {

struct Value {
  std::string data;
  BlockId block_id{0};
  std::optional<std::time_t> expire_at;
};

using Hasher = concord::util::SHA3_256;
using Hash = Hasher::Digest;

struct KeyValueProof {
  std::string key;
  Value value;

  // The index at which the key-value hash is to be combined with the complement ones.
  std::size_t key_value_index{0};

  // Ordeded hashes of the other key-values in the category.
  std::vector<Hash> ordered_complement_kv_hashes;

  Hash calculateRootHash() const {
    auto hasher = Hasher{};
    const auto key_hash = hasher.digest(key.data(), key.size());
    const auto value_hash = hasher.digest(value.data.data(), value.data.size());

    // root_hash = h((h(k1) || h(v1)) || (h(k2) || h(v2)) || ... || (h(k3) || h(v3)))
    hasher.init();
    if (ordered_complement_kv_hashes.empty()) {
      hasher.update(key_hash.data(), key_hash.size());
      hasher.update(value_hash.data(), value_hash.size());
    } else {
      for (auto i = 0ul; i < ordered_complement_kv_hashes.size(); ++i) {
        if (i == key_value_index) {
          hasher.update(key_hash.data(), key_hash.size());
          hasher.update(value_hash.data(), value_hash.size());
        }
        const auto &hash_i = ordered_complement_kv_hashes[i];
        hasher.update(hash_i.data(), hash_i.size());
      }
    }
    return hasher.finish();
  }
};

}  // namespace concord::kvbc::categorization
