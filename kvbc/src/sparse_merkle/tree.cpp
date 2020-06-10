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
#include "sparse_merkle/walker.h"

#include <future>
#include <iterator>
#include <unordered_map>

using namespace concordUtils;

namespace concord::kvbc::sparse_merkle {
using namespace detail;

// Computes value hashes in a separate thread.
class ValueHashes {
 public:
  ValueHashes(const SetOfKeyValuePairs& updates) {
    // A vector of value and hash promise pairs. We pass it to a separate thread that calculates the value hashes and
    // sets them in their corresponding futures. The futures are indexed by key for user convenience.
    auto value_promises = std::vector<std::pair<Sliver, std::promise<Hash>>>{};
    for (const auto& [key, value] : updates) {
      auto promise = std::promise<Hash>{};
      hashes_.emplace(key, promise.get_future());
      value_promises.push_back(std::make_pair(value, std::move(promise)));
    }

    // Compute value hashes in a separate thread. The passed lambda is noexcept, because:
    //  * we don't expect the hashing code to throw
    //  * propagating the exception correctly will make the code unnecessarily complex
    async_future_ = std::async(std::launch::async, [value_promises = std::move(value_promises)]() mutable noexcept {
      auto hasher = Hasher{};
      for (auto& [value, promise] : value_promises) {
        const auto hash = hasher.hash(value.data(), value.length());
        promise.set_value(hash);
      }
    });
  }

  ValueHashes(ValueHashes&&) = default;
  ValueHashes(const ValueHashes&) = delete;
  ValueHashes& operator=(const ValueHashes&) = delete;

  Hash operator[](const Sliver& key) {
    auto it = hashes_.find(key);
    AssertNE(it, std::end(hashes_));
    return it->second.get();
  }

 private:
  // A Key -> Value Hash map.
  std::unordered_map<Sliver, std::future<Hash>> hashes_;
  // The future returned from the std::async() call. Keep as a member so that the returned future's destructor is not
  // called immediately and thus blocking until all hashes are computed.
  std::future<void> async_future_;
};

void insertComplete(Walker& walker, const BatchedInternalNode::InsertComplete& result) {
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
  int nodes_to_create = new_child.key.hash().prefix_bits_in_common(stored_child.key.hash(), walker.depth()) / 4;
  walker.appendEmptyNodes(new_child.key.hash(), nodes_to_create);
  walker.currentNode().insert(stored_child, walker.depth(), walker.version());
  auto result = walker.currentNode().insert(new_child, walker.depth(), walker.version());
  return insertComplete(walker, std::get<BatchedInternalNode::InsertComplete>(result));
}

// Insert a LeafChild into the proper BatchedInternalNode. Handle all possible
// responses and walk the tree as appropriate to get to the correct node, where
// the insert will succeed.
void insert(Walker& walker, const LeafChild& child) {
  while (true) {
    Assert(walker.depth() < Hash::MAX_NIBBLES);

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
    Assert(std::holds_alternative<BatchedInternalNode::InsertComplete>(insertResult));
    walker.ascendToRoot();
  } else {
    if (walker.atRoot() && walker.currentNode().safeToRemove()) {
      walker.removeCurrentNode();
    } else {
      walker.ascendToRoot();
    }
  }
}

void remove(Walker& walker, const Hash& key_hash) {
  while (true) {
    Assert(walker.depth() < Hash::MAX_NIBBLES);

    auto result = walker.currentNode().remove(key_hash, walker.depth(), walker.version());

    if (auto rv = std::get_if<BatchedInternalNode::RemoveComplete>(&result)) {
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

UpdateBatch Tree::update(const concord::kvbc::SetOfKeyValuePairs& updates,
                         const concord::kvbc::KeysVector& deleted_keys) {
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

  // Compute value hashes in parallel with tree updates.
  // Assume the same order for subsequent iterations of SetOfKeyValuePairs objects, provided that we don't mutate them
  // in any way. That, combined with the assumption that the tree update takes longer than value hash calculation,
  // should ensure that this is actually an optimization.
  // Note: The documentation for std::unordered_map (SetOfKeyValuePairs) states that read-only operations do not
  // invalidate iterators. Therefore, we can expect that subsequent iterations will have the same order.
  auto value_hashes = ValueHashes(updates);
  for (auto&& [key, val] : updates) {
    LeafNode leaf_node{val};
    LeafKey leaf_key{hasher.hash(key.data(), key.length()), version};
    LeafChild child{value_hashes[key], leaf_key};
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

  // Set the root after updates so that it is reflected to users in get_root_hash() and get_version() .
  root_ = cache.getRoot();

  return batch;
}

}  // namespace concord::kvbc::sparse_merkle
