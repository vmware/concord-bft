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

using Hasher = concord::util::SHA3_256;
using Hash = Hasher::Digest;

struct KeyValueProof {
  BlockId block_id{0};
  std::string key;
  std::string value;

  // The index at which the key-value hash is to be combined with the complement ones.
  std::size_t key_value_index{0};

  // Ordeded hashes of the other key-values in the category.
  std::vector<Hash> ordered_complement_kv_hashes;

  Hash calculateRootHash() const {
    const auto key_hash = Hasher{}.digest(key.data(), key.size());
    const auto value_hash = Hasher{}.digest(value.data(), value.size());

    // root_hash = h(h(k1) || h(v1) || h(k2) || h(v2) || ... || h(kn) || h(vn))
    auto hasher = Hasher{};
    hasher.init();
    const auto update = [&hasher](const auto &in) { hasher.update(in.data(), in.size()); };

    if (ordered_complement_kv_hashes.empty()) {
      update(key_hash);
      update(value_hash);
    } else {
      for (auto i = 0ul; i < ordered_complement_kv_hashes.size(); ++i) {
        if (i == key_value_index) {
          update(key_hash);
          update(value_hash);
        }
        update(ordered_complement_kv_hashes[i]);
      }
      if (key_value_index == ordered_complement_kv_hashes.size()) {
        update(key_hash);
        update(value_hash);
      }
    }
    return hasher.finish();
  }
};

}  // namespace concord::kvbc::categorization
