
// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#pragma once

#include <set>

#include "sparse_merkle/base_types.h"
#include "sparse_merkle/keys.h"
#include "sparse_merkle/internal_node.h"

namespace concord {
namespace kvbc {
namespace sparse_merkle {

// A set of old tree nodes that are no longer reachable from the new root of
// the tree.
struct StaleNodeIndexes {
  // All the following keys became stale at this version.
  Version stale_since_version;
  std::set<InternalNodeKey> internal_keys;
  std::set<LeafKey> leaf_keys;
};

// Every mutation of the tree returns a BatchUpdate containing nodes to be written
// to the database.
struct UpdateBatch {
  StaleNodeIndexes stale;
  std::vector<std::pair<InternalNodeKey, BatchedInternalNode>> internal_nodes;
  std::vector<std::pair<LeafKey, LeafNode>> leaf_nodes;
};

}  // namespace sparse_merkle
}  // namespace kvbc
}  // namespace concord
