// Concord
//
// Copyright (c) 2019-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#pragma once

#include "util/sliver.hpp"
#include "sparse_merkle/base_types.h"

namespace concord {
namespace kvbc {
namespace sparse_merkle {

// An InternalNodeKey points to a BatchedInternalNode. It's always on a
// nibble boundary, hence it's keyed by NibblePath. We retrieve children that
// are part of the batch by pointing to their containing BatchedInternalNode.
class InternalNodeKey {
 public:
  template <class T>
  InternalNodeKey(T&& custom_prefix, Version version, const NibblePath& path)
      : custom_prefix_(std::forward<T>(custom_prefix)), version_(version), path_(path) {}

  // Return the root of a sparse merkle tree at a given version.
  template <class T>
  static InternalNodeKey root(T&& custom_prefix, Version version) {
    return InternalNodeKey(std::forward<T>(custom_prefix), version, NibblePath());
  }

  bool operator==(const InternalNodeKey& other) const {
    return custom_prefix_ == other.custom_prefix_ && version_ == other.version_ && path_ == other.path_;
  }

  // Compare by address, version and then by path
  bool operator<(const InternalNodeKey& other) const {
    if (custom_prefix_ != other.custom_prefix_) {
      return custom_prefix_ < other.custom_prefix_;
    }
    if (version_ != other.version_) {
      return version_ < other.version_;
    }
    return path_ < other.path_;
  }

  std::string toString() const {
    if (path_.empty()) {
      return custom_prefix_.value() + std::string("-<ROOT>") + "-" + version_.toString();
    }
    return custom_prefix_.value() + "-" + path_.toString() + "-" + version_.toString();
  }

  Version version() const { return version_; }

  const CustomPrefix& customPrefix() const { return custom_prefix_; }

  const NibblePath& path() const { return path_; }
  NibblePath& path() { return path_; }

 private:
  CustomPrefix custom_prefix_;
  Version version_;
  NibblePath path_;
};

// The key for a leaf node of a sparse merkle tree.
//
// We identify this by the full key hash rather than NibblePath so that we can
// do direct DB lookups rather than having to walk from the root of the sparse
// merkle tree.
class LeafKey {
 public:
  static constexpr auto SIZE_IN_BYTES = PaddedCustomPrefix::TOTAL_SIZE + Hash::SIZE_IN_BYTES + Version::SIZE_IN_BYTES;

 public:
  template <class T>
  LeafKey(Hash key, Version version, T&& address) : key_(key), version_(version), prefix_(std::forward<T>(address)) {}
  LeafKey(Hash key, Version version) : key_(key), version_(version), prefix_("") {}

  bool operator==(const LeafKey& other) const {
    return prefix_ == other.prefix_ && key_ == other.key_ && version_ == other.version_;
  }
  bool operator!=(const LeafKey& other) const { return !(*this == other); }

  // Compare by prefix_, key_ and then version_
  bool operator<(const LeafKey& other) const {
    if (prefix_ != other.prefix_) {
      return prefix_ < other.prefix_;
    }
    if (key_ != other.key_) {
      return key_ < other.key_;
    }
    return version_ < other.version_;
  }

  std::string toString() const { return key_.toString() + "-" + version_.toString() + "-" + prefix_.value(); }

  Nibble getNibble(const size_t n) const {
    ConcordAssert(n < Hash::MAX_NIBBLES);
    return key_.getNibble(n);
  }

  const PaddedCustomPrefix& customPrefix() const { return prefix_; }

  Version version() const { return version_; }

  const Hash& hash() const { return key_; }

 private:
  Hash key_;
  Version version_;
  PaddedCustomPrefix prefix_;
};

std::ostream& operator<<(std::ostream& os, const LeafKey&);

// A binary blob value
struct LeafNode {
  concordUtils::Sliver value;
};

inline bool operator==(const LeafNode& lhs, const LeafNode& rhs) { return (lhs.value == rhs.value); }

}  // namespace sparse_merkle
}  // namespace kvbc
}  // namespace concord
