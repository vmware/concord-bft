// Concord
//
// Copyright (c) 2019-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#pragma once

#include <vector>
#include <cstddef>
#include <optional>
#include <array>
#include <map>
#include <stack>
#include <utility>

#include "assertUtils.hpp"
#include "kv_types.hpp"
#include "sliver.hpp"
#include "sparse_merkle/base_types.h"
#include "sparse_merkle/db_reader.h"
#include "sparse_merkle/internal_node.h"
#include "sparse_merkle/update_batch.h"
#include "sparse_merkle/update_cache.h"

namespace concord {
namespace kvbc {
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
  Tree() = default;
  explicit Tree(std::shared_ptr<IDBReader> db_reader) : db_reader_(db_reader) { reset(); }

  const Hash& get_root_hash() const { return root_.hash(); }
  Version get_version() const { return root_.version(); }
  bool empty() const { return root_.numChildren() == 0; }

  // Add or update key-value pairs given in `updates`, and remove keys in
  // `deleted_keys`.
  //
  // Return an UpdateBatch to be written to the DB.
  UpdateBatch update(const concord::kvbc::SetOfKeyValuePairs& updates, const concord::kvbc::KeysVector& deleted_keys);
  UpdateBatch update(const concord::kvbc::SetOfKeyValuePairs& updates) {
    concord::kvbc::KeysVector no_deletes;
    return update(updates, no_deletes);
  }

  // Perform an update with only delete operations
  UpdateBatch remove(const concord::kvbc::KeysVector& deletes) {
    concord::kvbc::SetOfKeyValuePairs no_updates;
    return update(no_updates, deletes);
  }

  // In addition to the batch, returns the cache object used for the update. Used for testing purposes.
  std::pair<UpdateBatch, detail::UpdateCache> update_with_cache(const concord::kvbc::SetOfKeyValuePairs& updates,
                                                                const concord::kvbc::KeysVector& deleted_keys);

 private:
  // Reset the tree to the latest version.
  //
  // This is necessary to do before updates, as we only allow updating the
  // latest tree.
  void reset() { root_ = db_reader_->get_latest_root(); }

  UpdateBatch update_impl(const concord::kvbc::SetOfKeyValuePairs& updates,
                          const concord::kvbc::KeysVector& deleted_keys,
                          detail::UpdateCache& cache);

  std::shared_ptr<IDBReader> db_reader_;
  BatchedInternalNode root_;
};

}  // namespace sparse_merkle
}  // namespace kvbc
}  // namespace concord
