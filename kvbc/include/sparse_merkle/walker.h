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

#pragma once

#include <stack>
#include "sparse_merkle/internal_node.h"
#include "sparse_merkle/update_cache.h"

namespace concord {
namespace kvbc {
namespace sparse_merkle {

// This is an implementation detail of the Tree::update call
namespace detail {

typedef std::stack<BatchedInternalNode, std::vector<BatchedInternalNode>> NodeStack;

// A class for ascending and descending the tree. This is used for updates.
class Walker {
 public:
  Walker(UpdateCache& cache) : cache_(cache), current_node_(cache.getRoot()) {
    // Save the version of the root node, in case we only update the root node.
    stale_version_ = current_node_.version();
  }

  size_t depth() const { return nibble_path_.length(); }
  BatchedInternalNode& currentNode() { return current_node_; }
  Version version() { return cache_.version(); }
  bool atRoot() { return nibble_path_.empty(); }
  void appendEmptyNodes(const Hash& key, size_t nodes_to_create);
  void descend(const Hash& key, Version next_version);

  // Walk the stack of internal nodes upwards toward the root, updating the
  // hashes as we go along, and caching the updated nodes.
  void ascendToRoot(const std::optional<LeafKey>& key);
  void ascendToRoot();

  // When a key is removed from a BatchedInternalNode, the node will find it,
  // remove it and inform the caller of the version. It is the callers
  // responsibility to create a leafKey from the key and version and mark it
  // stale.
  void markStale(LeafKey stale) { cache_.putStale(stale); }

  // remove current_node_ and ascend up the tree if not at the root.
  std::optional<Nibble> removeCurrentNode();

  void insertEmptyRootAtCurrentVersion();

 private:
  void ascend();
  std::pair<Nibble, Hash> pop();
  void markCurrentNodeStale();
  void cacheCurrentNode();

  // The version of the current node after a call to `descend`, but prior to
  // user modification. We use it to mark the version prior to modification
  // stale when we ascend back up the tree.
  //
  // If we never ascend, because, for example, a node to delete is "not found",
  // then we never use the stale_version_;
  Version stale_version_;

  UpdateCache& cache_;
  NodeStack stack_;
  BatchedInternalNode current_node_;
  NibblePath nibble_path_;
};

}  // namespace detail

}  // namespace sparse_merkle
}  // namespace kvbc
}  // namespace concord
