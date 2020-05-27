
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

#include <map>

#include "sparse_merkle/keys.h"
#include "sparse_merkle/base_types.h"
#include "sparse_merkle/db_reader.h"
#include "sparse_merkle/internal_node.h"
#include "sparse_merkle/update_batch.h"

namespace concord {
namespace kvbc {
namespace sparse_merkle {

// This is an implementation detail of the Tree::update mechanism.
namespace detail {

// All data that is loaded from the DB, manipulated, and/or returned from a
// call to `update`.
//
// A new instance of this structure is created during every update call.
class UpdateCache {
 public:
  UpdateCache(const BatchedInternalNode& root, const std::shared_ptr<IDBReader>& db_reader)
      : version_(root.version() + 1), db_reader_(db_reader), original_root_(root) {}

  const StaleNodeIndexes& stale() const { return stale_; }
  const auto& internalNodes() const { return internal_nodes_; }
  Version version() const { return version_; }

  // Used for testing purposes only.
  auto& internalNodes() { return internal_nodes_; }

  // Return the root if it's been updated and stored in the cache. Otherwise
  // return the root at the time of cache creation.
  const BatchedInternalNode& getRoot();

  // Get a node if it's in the cache, otherwise get it from the DB.
  // This method assumes the node exists. It is a logic error in the caller if
  // it does not exist.
  BatchedInternalNode getInternalNode(const InternalNodeKey& key);

  void putStale(const std::optional<LeafKey>& key);
  void putStale(const InternalNodeKey& key);
  void put(const NibblePath& path, const BatchedInternalNode& node);
  void remove(const NibblePath& path);

 private:
  // The version of the tree after this update is complete.
  Version version_;
  std::shared_ptr<IDBReader> db_reader_;
  StaleNodeIndexes stale_;

  // The root at the time the cache was created. We don't want to add this to
  // the cache, because the cache only contains "updated" nodes that are
  // supposed to be written to disk.
  BatchedInternalNode original_root_;

  // All the internal nodes related to the current batch update.
  //
  // These nodes are mutable and are all being updated to a single version.
  // Therefore we key them by their NibblePath alone, without a version.
  std::map<NibblePath, BatchedInternalNode> internal_nodes_;
};

}  // namespace detail

}  // namespace sparse_merkle
}  // namespace kvbc
}  // namespace concord
