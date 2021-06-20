// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#include "sparse_merkle/update_cache.h"

namespace concord::kvbc::sparse_merkle::detail {

BatchedInternalNode UpdateCache::getInternalNode(const InternalNodeKey& key) {
  auto it = internal_nodes_.find(key.path());
  if (it != internal_nodes_.end()) {
    return it->second;
  }
  return db_reader_->get_internal(key);
}

void UpdateCache::putStale(const std::optional<LeafKey>& key) {
  if (key) {
    stale_.leaf_keys.insert(key.value());
  }
}

const BatchedInternalNode& UpdateCache::getRoot() {
  auto it = internal_nodes_.find(NibblePath());
  if (it != internal_nodes_.end()) {
    return it->second;
  }
  return original_root_;
}

void UpdateCache::put(const NibblePath& path, const BatchedInternalNode& node) { internal_nodes_[path] = node; }

void UpdateCache::putStale(const InternalNodeKey& key) { stale_.internal_keys.insert(key); }

void UpdateCache::remove(const NibblePath& path) { internal_nodes_.erase(path); }

}  // namespace concord::kvbc::sparse_merkle::detail
