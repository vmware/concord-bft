// Concord
//
// Copyright (c) 2019-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#pragma once

#include "sliver.hpp"
#include "sparse_merkle/base_types.h"

namespace concord {
namespace storage {
namespace sparse_merkle {

// An InternalNodeKey points to a BatchedInternalNode. It's always on a
// nibble boundary, hence it's keyed by NibblePath. We retrieve children that
// are part of the batch by pointing to their containing BatchedInternalNode.
class InternalNodeKey {
 public:
  InternalNodeKey(Version version, NibblePath path) : version_(version), path_(path) {}

  // Return the root of a sparse merkle tree at a given version.
  static InternalNodeKey root(Version version) { return InternalNodeKey(version, NibblePath()); }

  bool operator==(const InternalNodeKey& other) const { return version_ == other.version_ && path_ == other.path_; }

  Version version() const { return version_; }

 private:
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
  LeafKey(Hash key, Version version) : key_(key), version_(version) {}

  bool operator==(const LeafKey& other) const { return key_ == other.key_ && version_ == other.version_; }

  Nibble getNibble(const size_t n) const {
    Assert(n < Hash::MAX_NIBBLES);
    return key_.getNibble(n);
  }

  Version version() const { return version_; }

  const Hash& hash() const { return key_; }

 private:
  Hash key_;
  Version version_;
};

}  // namespace sparse_merkle
}  // namespace storage
}  // namespace concord
