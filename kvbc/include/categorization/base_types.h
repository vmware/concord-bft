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

#include "kv_types.hpp"
#include "sha_hash.hpp"

#include <cstddef>
#include <ctime>
#include <limits>
#include <optional>
#include <string>
#include <variant>

namespace concord::kvbc::categorization {

using Hasher = concord::util::SHA3_256;
using Hash = Hasher::Digest;

struct BasicValue {
  BlockId block_id{0};
  std::string data;
};

inline bool operator==(const BasicValue &lhs, const BasicValue &rhs) {
  return (lhs.block_id == rhs.block_id && lhs.data == rhs.data);
}

struct MerkleValue : BasicValue {};
struct ImmutableValue : BasicValue {};
struct VersionedValue : BasicValue {};

inline bool operator==(const MerkleValue &lhs, const MerkleValue &rhs) {
  return (lhs.block_id == rhs.block_id && lhs.data == rhs.data);
}

using Value = std::variant<MerkleValue, ImmutableValue, VersionedValue>;

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
      return hasher.finish();
    }

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
    return hasher.finish();
  }
};

struct TaggedVersion {
  // The high bit contains a flag indicating whether the key was deleted or not.
  TaggedVersion(uint64_t masked_version) {
    static_assert(!std::numeric_limits<BlockId>::is_signed);
    static_assert(std::numeric_limits<BlockId>::digits == 64);
    deleted = 0x8000000000000000 & masked_version;
    version = 0x7FFFFFFFFFFFFFFF & masked_version;
  }

  TaggedVersion(bool deleted, BlockId version) : deleted(deleted), version(version) {}

  // Return a BlockId that sets the high bit to 1 if the version is deleted
  BlockId encode() const {
    if (deleted) {
      return version | 0x8000000000000000;
    }
    return version;
  }

  bool deleted;
  BlockId version;
};

inline bool operator==(const TaggedVersion &lhs, const TaggedVersion &rhs) {
  return (lhs.deleted == rhs.deleted && lhs.version == rhs.version);
}

enum class CATEGORY_TYPE : char { block_merkle = 0, immutable = 1, versioned_kv = 2, end_of_types };

inline std::string categoryStringType(CATEGORY_TYPE t) {
  switch (t) {
    case CATEGORY_TYPE::block_merkle:
      return "block_merkle";
    case CATEGORY_TYPE::immutable:
      return "immutable";
    case CATEGORY_TYPE::versioned_kv:
      return "versioned_kv";
    default:
      ConcordAssert(false);
  }
}

}  // namespace concord::kvbc::categorization
