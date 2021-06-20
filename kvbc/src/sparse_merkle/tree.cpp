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

#include "sparse_merkle/histograms.h"
#include "sparse_merkle/tree.h"
#include "sparse_merkle/walker.h"

#include <iostream>
using namespace std;

using namespace concordUtils;
using namespace concord::diagnostics;

namespace concord::kvbc::sparse_merkle {
using namespace detail;

void insertComplete(Walker& walker, const BatchedInternalNode::InsertComplete& result) {
  histograms.insert_depth->record(walker.depth());
  walker.ascendToRoot(result.stale_leaf);
}

// We have a collision and need to create new internal nodes so that we reach
// the appropriate depth of the tree to place the new child and stored child
// that caused the collision.
//
// Once we have created the proper depth, we can be sure that the inserts will
// succeed at the attempt, since this is precisely where they belong after
// walking the prefix bits they have in common. Both successful inserts return
// BatchedInternalNode::InsertComplete.
void handleCollision(Walker& walker, const LeafChild& stored_child, const LeafChild& new_child) {
  auto nodes_to_create = new_child.key.hash().prefix_bits_in_common(stored_child.key.hash(), walker.depth()) / 4;
  walker.appendEmptyNodes(new_child.key.hash(), nodes_to_create);
  walker.currentNode().insert(stored_child, walker.depth(), walker.version());
  auto result = walker.currentNode().insert(new_child, walker.depth(), walker.version());
  return insertComplete(walker, std::get<BatchedInternalNode::InsertComplete>(result));
}

// Insert a LeafChild into the proper BatchedInternalNode. Handle all possible
// responses and walk the tree as appropriate to get to the correct node, where
// the insert will succeed.
void insert(Walker& walker, const LeafChild& child) {
  TimeRecorder scoped_timer(*histograms.insert_key);
  while (true) {
    ConcordAssert(walker.depth() < Hash::MAX_NIBBLES);

    auto result = walker.currentNode().insert(child, walker.depth(), walker.version());

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

void removeBatchedInternalNode(Walker& walker, const std::optional<LeafChild>& promoted) {
  auto promoted_after_unlink = promoted;
  while (!walker.atRoot() && walker.currentNode().safeToRemove()) {
    auto child_key = walker.removeCurrentNode().value();
    promoted_after_unlink = walker.currentNode().unlinkChild(child_key, walker.version(), promoted_after_unlink);
  }

  // At this point all empty BatchedInternalNodes have been removed and we
  // have walked back up the tree.
  if (promoted_after_unlink) {
    auto insertResult = walker.currentNode().insert(promoted_after_unlink.value(), walker.depth(), walker.version());
    ConcordAssert(std::holds_alternative<BatchedInternalNode::InsertComplete>(insertResult));
    walker.ascendToRoot();
  } else {
    if (walker.atRoot() && walker.currentNode().safeToRemove()) {
      walker.removeCurrentNode();
      // Ensure monotonically increasing root versions even in the case of the current root being removed (e.g. when all
      // keys are removed) by persisting an empty root at the current version.
      walker.insertEmptyRootAtCurrentVersion();
    } else {
      walker.ascendToRoot();
    }
  }
}

void remove(Walker& walker, const Hash& key_hash) {
  TimeRecorder scoped_timer(*histograms.remove_key);
  while (true) {
    ConcordAssert(walker.depth() < Hash::MAX_NIBBLES);

    auto result = walker.currentNode().remove(key_hash, walker.depth(), walker.version());

    if (auto rv = std::get_if<BatchedInternalNode::RemoveComplete>(&result)) {
      histograms.remove_depth->record(walker.depth());
      auto stale = LeafKey(key_hash, rv->version);
      return walker.ascendToRoot(stale);
    }

    if (std::holds_alternative<BatchedInternalNode::NotFound>(result)) {
      // TODO: Log this?
      return;
    }

    if (auto rv = std::get_if<BatchedInternalNode::RemoveBatchedInternalNode>(&result)) {
      walker.markStale(LeafKey(key_hash, rv->removed_version));
      return removeBatchedInternalNode(walker, rv->promoted);
    }

    auto next_node_version = std::get<BatchedInternalNode::Descend>(result).next_node_version;
    walker.descend(key_hash, next_node_version);
  }
}

static void updateBatchHistograms(const UpdateBatch& batch) {
  histograms.num_batch_internal_nodes->record(batch.internal_nodes.size());
  histograms.num_batch_leaf_nodes->record(batch.leaf_nodes.size());
  histograms.num_stale_internal_keys->record(batch.stale.internal_keys.size());
  histograms.num_stale_leaf_keys->record(batch.stale.leaf_keys.size());
}

UpdateBatch Tree::update(const concord::kvbc::SetOfKeyValuePairs& updates,
                         const concord::kvbc::KeysVector& deleted_keys) {
  histograms.num_updated_keys->record(updates.size());
  histograms.num_deleted_keys->record(deleted_keys.size());
  TimeRecorder scoped_timer(*histograms.update);
  reset();
  UpdateCache cache(root_, db_reader_);
  return update_impl(updates, deleted_keys, cache);
}

std::pair<UpdateBatch, UpdateCache> Tree::update_with_cache(const concord::kvbc::SetOfKeyValuePairs& updates,
                                                            const concord::kvbc::KeysVector& deleted_keys) {
  reset();
  UpdateCache cache(root_, db_reader_);
  auto batch = update_impl(updates, deleted_keys, cache);
  return std::make_pair(batch, cache);
}

UpdateBatch Tree::update_impl(const concord::kvbc::SetOfKeyValuePairs& updates,
                              const concord::kvbc::KeysVector& deleted_keys,
                              UpdateCache& cache) {
  UpdateBatch batch;
  const auto version = cache.version();
  Hasher hasher;

  // Deletes come before inserts because it makes more semantic sense. A user can delete a key and then write a new
  // version, but it makes no sense to add a new version and then delete a key.
  for (auto& key : deleted_keys) {
    Walker walker(cache);
    auto key_hash = hasher.hash(key.data(), key.length());
    sparse_merkle::remove(walker, key_hash);
  }

  for (auto&& [key, val] : updates) {
    histograms.key_size->record(key.length());
    histograms.val_size->record(val.length());
    Hash leaf_hash;
    {
      TimeRecorder scoped_timer(*histograms.hash_val);
      leaf_hash = hasher.hash(val.data(), val.length());
    }
    LeafNode leaf_node{val};
    LeafKey leaf_key{hasher.hash(key.data(), key.length()), version};
    LeafChild child{leaf_hash, leaf_key};
    Walker walker(cache);
    insert(walker, child);
    batch.leaf_nodes.emplace_back(leaf_key, leaf_node);
  }

  // Create and return the UpdateBatch
  batch.stale = cache.stale();
  batch.stale.stale_since_version = version;
  for (auto& it : cache.internalNodes()) {
    batch.internal_nodes.emplace_back(InternalNodeKey(version, it.first), it.second);
  }
  updateBatchHistograms(batch);

  // Set the root after updates so that it is reflected to users in get_root_hash() and get_version() .
  root_ = cache.getRoot();

  return batch;
}

}  // namespace concord::kvbc::sparse_merkle
