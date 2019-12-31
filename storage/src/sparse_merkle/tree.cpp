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

// All data that is loaded from the DB, manipulated, and/or returned from a
// call to `update`.
//
// A new instance of this structure is created during every update call.
struct UpdateCache {
  UpdateCache(const BatchedInternalNode& root, const std::shared_ptr<IDBReader>& db_reader) : db_reader_(db_reader) {
    internal_nodes_[NibblePath()] = root;
  }

  // Get a node if it's in the cache, otherwise get it from the DB.
  BatchedInternalNode getInternalNode(const InternalNodeKey& key) {
    auto it = internal_nodes_.find(key.path());
    if (it != internal_nodes_.end()) {
      return it->second;
    }
    return db_reader_->get_internal(key);
  }

  std::shared_ptr<IDBReader> db_reader_;
  Tree::StaleNodeIndexes stale_;

  // All the internal nodes related to the current batch update.
  //
  // These nodes are mutable and are all being updated to a single version.
  // Therefore we key them by their NibblePath alone, without a version.
  std::map<NibblePath, BatchedInternalNode> internal_nodes_;
};

// The state of the insert loop.
//
// The data in here is only relevant to a single insert.
//
// This mainly exists to allow easily splitting up the insert code into separate
// functions based upon the return value of the BatchedInternalNode insert.
struct InsertState {
  InsertState(const BatchedInternalNode& current, Version version, Tree::NodeStack& stack)
      : current_node(current), version(version), stack(stack) {}

  size_t depth = 0;
  NibblePath nibble_path;
  BatchedInternalNode current_node;
  Version version;
  std::stack<BatchedInternalNode, std::vector<BatchedInternalNode>>& stack;
};

// Insert to a BatchedInternalNode completed successfully. Finish processing the
// call to insert.
void handleInsertComplete(const BatchedInternalNode::InsertComplete* result, UpdateCache& cache, InsertState& state) {
  // If there is a stale leaf key then save it.
  if (result->stale_leaf) {
    cache.stale_.leaf_keys.push_back(result->stale_leaf.value());
  }

  // This is bottom most internal node for the given leaf_key.
  cache.internal_nodes_[state.nibble_path] = state.current_node;

  // Walk the stack of internal nodes upwards toward the root, updating the
  // hashes as we go along.
  while (!state.stack.empty()) {
    Hash hash = state.current_node.hash();
    state.current_node = state.stack.top();
    state.stack.pop();

    // We want to just update the hash of leaf child pointing to the next
    // BatchedInternalNode. This triggers the root hash of the
    // BatchedInternalNode to be calculated.
    Nibble child_key = state.nibble_path.popBack();
    InternalChild update{hash, state.version};
    state.current_node.write_internal_child_at_level_0(child_key, update);

    // Write this node into our into our cache of internal nodes
    cache.internal_nodes_[state.nibble_path] = state.current_node;
  }
}

// Save the current InternalNodeKey as stale when it is a valid version, and
// hasn't already been cached.
void cacheStaleInternalNode(UpdateCache& cache, const InsertState& state) {
  if (state.current_node.version() != Version(0) && state.current_node.version() != state.version) {
    cache.stale_.internal_keys.emplace_back(InternalNodeKey(state.current_node.version(), state.nibble_path));
  }
}

// Modify the insert state to walk further down the tree
void descend(Nibble next_nibble, Version next_version, UpdateCache& cache, InsertState& state) {
  state.nibble_path.append(next_nibble);
  InternalNodeKey next_internal_key{next_version, state.nibble_path};
  state.current_node = cache.getInternalNode(next_internal_key);
  cacheStaleInternalNode(cache, state);
  state.depth += 1;
}

// A node split has occurred, due to a collision, triggering the need to create more
// BatchedInternalNodes.
void create_internal_nodes(const LeafChild& stored_child, const LeafChild& child_to_insert, InsertState& state) {
  int nodes_to_create = child_to_insert.key.hash().prefix_bits_in_common(stored_child.key.hash(), state.depth) / 4;
  for (int i = 0; i < nodes_to_create - 1; i++) {
    // Add a bunch of empty intermediate nodes
    state.nibble_path.append(child_to_insert.key.getNibble(state.depth));
    state.stack.emplace(BatchedInternalNode());
    state.depth += 1;
  }
  // Create the bottom most node and make it the current node
  state.nibble_path.append(child_to_insert.key.getNibble(state.depth));
  state.current_node = BatchedInternalNode();
  state.depth += 1;
}

// A node split occurred due to a key collision.
//
// Create a bunch of new internal nodes and then insert the previously stored
// child that collided with the child that was attempting to be inserted.
//
// On the next loop around the `child_to_insert` will be successfully inserted
// inside `state.current_node`.
void handleNodeSplit(BatchedInternalNode::CreateNewBatchedInternalNodes result,
                     const LeafChild& child_to_insert,
                     InsertState& state) {
  create_internal_nodes(result.stored_child, child_to_insert, state);

  // Ignore results. We already know this insert will succeed.
  state.current_node.insert(result.stored_child, state.depth);
}

// Insert a single leaf. This is called as part of `update`.
void insert(UpdateCache& cache, const LeafChild& child, const Version version, Tree::NodeStack& stack) {
  const auto& root = cache.internal_nodes_[NibblePath()];
  InsertState state(root, version, stack);
  cacheStaleInternalNode(cache, state);

  while (true) {
    Assert(state.depth < Hash::MAX_NIBBLES);

    auto result = state.current_node.insert(child, state.depth);

    if (auto rv = std::get_if<BatchedInternalNode::InsertComplete>(&result)) {
      return handleInsertComplete(rv, cache, state);
    }

    // Push the current node onto the stack so that we can update the hash and
    // version when we are done.
    state.stack.push(state.current_node);

    if (auto rv = std::get_if<BatchedInternalNode::InsertIntoExistingNode>(&result)) {
      descend(child.key.getNibble(state.depth), rv->next_node_version, cache, state);
    } else {
      handleNodeSplit(std::get<BatchedInternalNode::CreateNewBatchedInternalNodes>(result), child, state);
    }
  }
}

// TODO: Add support for delete. Only insert/update are supported now.
Tree::UpdateBatch Tree::update(const SetOfKeyValuePairs& updates, const KeysVector& deleted_keys) {
  reset();

  UpdateBatch batch;
  UpdateCache cache(root_, db_reader_);
  auto version = get_version() + 1;
  Hasher hasher;
  for (auto&& [key, val] : updates) {
    auto leaf_hash = hasher.hash(val.data(), val.length());
    LeafNode leaf_node{val};
    LeafKey leaf_key{hasher.hash(key.data(), key.length()), version};
    LeafChild child{leaf_hash, leaf_key};
    insert(cache, child, version, insert_stack_);
    batch.leaf_nodes.push_back(std::make_pair(leaf_key, leaf_node));
  }

  // Create and return the UpdateBatch
  batch.stale = std::move(cache.stale_);
  batch.stale.stale_since_version = version;
  for (auto& it : cache.internal_nodes_) {
    batch.internal_nodes.emplace_back(InternalNodeKey(version, it.first), it.second);
  }

  return batch;
}

}  // namespace concord::storage::sparse_merkle
