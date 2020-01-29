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

#include "sparse_merkle/tree.h"
#include "hash_defs.h"

using namespace concordUtils;

namespace concord::storage::sparse_merkle {

void insertComplete(Tree::Walker& walker, const BatchedInternalNode::InsertComplete& result) {
  walker.putStale(result.stale_leaf);
  // Save the bottom most internal node for the given leaf_key.
  walker.cacheCurrentNode();

  // Walk the stack of internal nodes upwards toward the root, updating the
  // hashes as we go along, and caching the updated nodes.
  while (!walker.atRoot()) {
    walker.ascend();
  }
}

// We have a collision and need to create new internal nodes so that we reach
// the appropriate depth of the tree to place the new child and stored child
// that caused the collision.
//
// Once we have created the proper depth, we can be sure that the inserts will
// succeed at the attempt, since this is precisely where they belong after
// walking the prefix bits they have in common. Both successful inserts return
// BatchedInternalNode::InsertComplete.
void handleCollision(Tree::Walker& walker, const LeafChild& stored_child, const LeafChild& new_child) {
  int nodes_to_create = new_child.key.hash().prefix_bits_in_common(stored_child.key.hash(), walker.depth()) / 4;
  walker.appendEmptyNodes(new_child.key.hash(), nodes_to_create);
  walker.currentNode().insert(stored_child, walker.depth());
  auto result = walker.currentNode().insert(new_child, walker.depth());
  return insertComplete(walker, std::get<BatchedInternalNode::InsertComplete>(result));
}

// Insert a LeafChild into the proper BatchedInternalNode. Handle all possible
// responses and walk the tree as appropriate to get to the correct node, where
// the insert will succeed.
void insert(Tree::Walker& walker, const LeafChild& child) {
  while (true) {
    Assert(walker.depth() < Hash::MAX_NIBBLES);

    auto result = walker.currentNode().insert(child, walker.depth());

    if (auto rv = std::get_if<BatchedInternalNode::InsertComplete>(&result)) {
      return insertComplete(walker, *rv);
    }

    if (auto rv = std::get_if<BatchedInternalNode::CreateNewBatchedInternalNodes>(&result)) {
      return handleCollision(walker, rv->stored_child, child);
    }

    auto next_node_version = std::get<BatchedInternalNode::InsertIntoExistingNode>(result).next_node_version;
    walker.descend(child.key.hash(), next_node_version);
  }
}

// TODO: Add support for delete. Only insert/update are supported now.
Tree::UpdateBatch Tree::update(const SetOfKeyValuePairs& updates, const KeysVector& deleted_keys) {
  reset();

  UpdateBatch batch;
  UpdateCache cache(root_, db_reader_);
  auto version = cache.version();
  Hasher hasher;
  for (auto&& [key, val] : updates) {
    auto leaf_hash = hasher.hash(val.data(), val.length());
    LeafNode leaf_node{val};
    LeafKey leaf_key{hasher.hash(key.data(), key.length()), version};
    LeafChild child{leaf_hash, leaf_key};
    Walker walker(node_stack_, cache);
    insert(walker, child);
    batch.leaf_nodes.push_back(std::make_pair(leaf_key, leaf_node));
  }

  // Create and return the UpdateBatch
  batch.stale = std::move(cache.stale());
  batch.stale.stale_since_version = version;
  for (auto& it : cache.internalNodes()) {
    batch.internal_nodes.emplace_back(InternalNodeKey(version, it.first), it.second);
  }

  return batch;
}

BatchedInternalNode Tree::UpdateCache::getInternalNode(const InternalNodeKey& key) {
  auto it = internal_nodes_.find(key.path());
  if (it != internal_nodes_.end()) {
    return it->second;
  }
  return db_reader_->get_internal(key);
}

void Tree::UpdateCache::putStale(const std::optional<LeafKey>& key) {
  if (key) {
    stale_.leaf_keys.push_back(key.value());
  }
}

void Tree::UpdateCache::put(const NibblePath& path, const BatchedInternalNode& node) { internal_nodes_[path] = node; }

void Tree::UpdateCache::putStale(const InternalNodeKey& key) { stale_.internal_keys.emplace_back(key); }

void Tree::UpdateCache::remove(const NibblePath& path) { internal_nodes_.erase(path); }

void Tree::Walker::appendEmptyNodes(const Hash& key, int nodes_to_create) {
  stack_.push(current_node_);
  for (int i = 0; i < nodes_to_create - 1; i++) {
    Nibble next_nibble = key.getNibble(depth());
    nibble_path_.append(next_nibble);
    stack_.emplace(BatchedInternalNode());
  }
  Nibble next_nibble = key.getNibble(depth());
  nibble_path_.append(next_nibble);
  current_node_ = BatchedInternalNode();
}

void Tree::Walker::descend(const Hash& key, Version next_version) {
  stack_.push(current_node_);
  Nibble next_nibble = key.getNibble(depth());
  nibble_path_.append(next_nibble);
  InternalNodeKey next_internal_key{next_version, nibble_path_};
  current_node_ = cache_.getInternalNode(next_internal_key);
  markCurrentNodeStale();
}

void Tree::Walker::cacheCurrentNode() { cache_.put(nibble_path_, current_node_); }

void Tree::Walker::ascend() {
  Assert(!stack_.empty());
  // Get the hash of the updated node, and its location in the parent node
  Hash hash = current_node_.hash();
  Nibble child_key = nibble_path_.popBack();

  current_node_ = stack_.top();
  stack_.pop();

  // We want to just update the hash of the leaf child pointing to the next
  // BatchedInternalNode. This triggers the root hash of the BatchedInternalNode
  // to be calculated.
  InternalChild update{hash, version()};
  current_node_.write_internal_child_at_level_0(child_key, update);
  cache_.put(nibble_path_, current_node_);
}

void Tree::Walker::removeCurrentNode() {
  markCurrentNodeStale();
  cache_.remove(nibble_path_);
  if (!stack_.empty()) {
    // TODO: Ascend and delete if necessary
  }
}

void Tree::Walker::markCurrentNodeStale() {
  // Don't mark the initial root stale, and don't mark an already cached node
  // stale.
  if (current_node_.version() != Version(0) && current_node_.version() != version()) {
    cache_.putStale(InternalNodeKey(current_node_.version(), nibble_path_));
  }
}

}  // namespace concord::storage::sparse_merkle
