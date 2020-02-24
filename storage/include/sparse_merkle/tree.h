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

#include <vector>
#include <cstddef>
#include <optional>
#include <array>
#include <map>
#include <stack>

#include "assertUtils.hpp"
#include "kv_types.hpp"
#include "sliver.hpp"
#include "sparse_merkle/base_types.h"
#include "sparse_merkle/db_reader.h"
#include "sparse_merkle/internal_node.h"

namespace concord {
namespace storage {
namespace sparse_merkle {

// An in-memory representation of a sparse merkle tree.
//
// The tree gets constructed by reading the latest root node from storage.
// Updates are made to the tree by reading in nodes from storage where necessary
// and making the changes in memory. A batch of DB updates of both internal and
// leaf nodes, as well as stale nodes are returned to the caller so that they
// can be written to the DB atomically.
class Tree {
 public:
  // A set of old tree nodes that are no longer reachable from the new root of
  // the tree.
  struct StaleNodeIndexes {
    // All the following keys became stale at this version.
    Version stale_since_version;
    std::vector<InternalNodeKey> internal_keys;
    std::vector<LeafKey> leaf_keys;
  };

  // Every mutation of the tree returns a BatchUpdate containing nodes to be written
  // to the database.
  struct UpdateBatch {
    StaleNodeIndexes stale;
    std::vector<std::pair<InternalNodeKey, BatchedInternalNode>> internal_nodes;
    std::vector<std::pair<LeafKey, LeafNode>> leaf_nodes;
  };

  typedef std::stack<BatchedInternalNode, std::vector<BatchedInternalNode>> NodeStack;

  explicit Tree(std::shared_ptr<IDBReader> db_reader) : db_reader_(db_reader) {
    // Perform a single allocation of the maximum depth of the tree for the
    // node_stack_.
    auto vec = std::vector<BatchedInternalNode>();
    vec.reserve(Hash::MAX_NIBBLES);
    node_stack_ = NodeStack(std::move(vec));
    reset();
  }

  const Hash& get_root_hash() const { return root_.hash(); }
  Version get_version() const { return root_.version(); }

  // Add or update key-value pairs given in `updates`, and remove keys in
  // `deleted_keys`.
  //
  // Return an UpdateBatch to be written to the DB.
  UpdateBatch update(const concordUtils::SetOfKeyValuePairs& updates, const concordUtils::KeysVector& deleted_keys);
  UpdateBatch update(const concordUtils::SetOfKeyValuePairs& updates) {
    concordUtils::KeysVector no_deletes;
    return update(updates, no_deletes);
  }

  // All data that is loaded from the DB, manipulated, and/or returned from a
  // call to `update`.
  //
  // A new instance of this structure is created during every update call.
  class UpdateCache {
   public:
    UpdateCache(const BatchedInternalNode& root, const std::shared_ptr<IDBReader>& db_reader)
        : version_(root.version() + 1), db_reader_(db_reader) {
      internal_nodes_[NibblePath()] = root;
    }

    const BatchedInternalNode& getRoot() { return internal_nodes_[NibblePath()]; }
    const Tree::StaleNodeIndexes& stale() const { return stale_; }
    const auto& internalNodes() const { return internal_nodes_; }
    Version version() const { return version_; }

    // Get a node if it's in the cache, otherwise get it from the DB.
    BatchedInternalNode getInternalNode(const InternalNodeKey& key);

    void putStale(const std::optional<LeafKey>& key);
    void putStale(const InternalNodeKey& key);
    void put(const NibblePath& path, const BatchedInternalNode& node);
    void remove(const NibblePath& path);

   private:
    // The version of the tree after this update is complete.
    Version version_;
    std::shared_ptr<IDBReader> db_reader_;
    Tree::StaleNodeIndexes stale_;

    // All the internal nodes related to the current batch update.
    //
    // These nodes are mutable and are all being updated to a single version.
    // Therefore we key them by their NibblePath alone, without a version.
    std::map<NibblePath, BatchedInternalNode> internal_nodes_;
  };

  // A class for ascending and descending the tree. This is used for updates.
  class Walker {
   public:
    Walker(NodeStack& stack, UpdateCache& cache) : stack_(stack), cache_(cache), current_node_(cache.getRoot()) {
      // Mark the root node stale
      markCurrentNodeStale();
    }

    size_t depth() const { return nibble_path_.length(); }
    BatchedInternalNode& currentNode() { return current_node_; }
    Version version() { return cache_.version(); }
    bool atRoot() { return nibble_path_.empty(); }
    void cacheCurrentNode();
    void putStale(const std::optional<LeafKey>& key) { cache_.putStale(key); }
    void appendEmptyNodes(const Hash& key, int nodes_to_create);
    void descend(const Hash& key, Version next_version);
    void ascend();
    void removeCurrentNode();

   private:
    void markCurrentNodeStale();

    NodeStack& stack_;
    UpdateCache& cache_;
    BatchedInternalNode current_node_;
    NibblePath nibble_path_;
  };

 private:
  // Reset the tree to the latest version.
  //
  // This is necessary to do before updates, as we only allow updating the
  // latest tree.
  void reset() { root_ = db_reader_->get_latest_root(); }

  std::shared_ptr<IDBReader> db_reader_;
  BatchedInternalNode root_;

  // Store internal nodes needed when updating a tree. This is a member variable
  // so that we can allocate only once.
  NodeStack node_stack_;
};

}  // namespace sparse_merkle
}  // namespace storage
}  // namespace concord
