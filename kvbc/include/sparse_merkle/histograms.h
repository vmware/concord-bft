
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

#include "diagnostics.h"
#include "sparse_merkle/base_types.h"

namespace concord::kvbc::sparse_merkle::detail {

struct Recorders {
  static constexpr int64_t MAX_US = 1000 * 1000 * 60;         // 60s
  static constexpr int64_t MAX_NS = 1000 * 1000 * 1000;       // 1s
  static constexpr int64_t MAX_VAL_SIZE = 1024 * 1024 * 100;  // 100MB

  using Recorder = concord::diagnostics::Recorder;
  using Unit = concord::diagnostics::Unit;

  Recorders() {
    auto& registrar = concord::diagnostics::RegistrarSingleton::getInstance();
    registrar.perf.registerComponent("sparse_merkle",
                                     {update,
                                      insert_key,
                                      remove_key,
                                      num_updated_keys,
                                      num_deleted_keys,
                                      key_size,
                                      val_size,
                                      hash_val,
                                      num_batch_internal_nodes,
                                      num_batch_leaf_nodes,
                                      num_stale_internal_keys,
                                      num_stale_leaf_keys,
                                      insert_depth,
                                      remove_depth,

                                      internal_node_update_hashes,
                                      internal_node_insert,
                                      internal_node_remove,

                                      dba_batch_to_db_updates,
                                      dba_get_value,
                                      dba_create_block_node,
                                      dba_get_raw_block,
                                      dba_last_reachable_block_db_updates,
                                      dba_size_of_updates,
                                      dba_get_internal,
                                      dba_serialize_internal,
                                      dba_serialize_leaf,
                                      dba_deserialize_internal,
                                      dba_deserialize_block,
                                      dba_deserialize_leaf,
                                      dba_link_st_chain,
                                      dba_num_blocks_for_st_link,
                                      dba_add_raw_block,
                                      dba_delete_block,
                                      dba_get_leaf_key_val_at_most_version,
                                      dba_delete_keys_for_block,
                                      dba_has_block,
                                      dba_keys_for_version,
                                      dba_hash_parent_block,
                                      dba_hashed_parent_block_size,
                                      dba_add_block_rocksdb_write,

                                      walker_ascend,
                                      walker_descend});
  }

  // Used in tree.cpp
  DEFINE_SHARED_RECORDER(update, 1, MAX_US, 3, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(insert_key, 1, MAX_US, 3, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(remove_key, 1, MAX_NS, 3, Unit::NANOSECONDS);
  DEFINE_SHARED_RECORDER(num_updated_keys, 1, 1000, 3, Unit::COUNT);
  DEFINE_SHARED_RECORDER(num_deleted_keys, 1, 1000, 3, Unit::COUNT);
  DEFINE_SHARED_RECORDER(key_size, 1, 2048, 3, Unit::BYTES);
  DEFINE_SHARED_RECORDER(val_size, 1, MAX_VAL_SIZE, 3, Unit::BYTES);
  DEFINE_SHARED_RECORDER(hash_val, 1, MAX_NS, 3, Unit::NANOSECONDS);
  DEFINE_SHARED_RECORDER(num_batch_internal_nodes, 1, 1000, 3, Unit::COUNT);
  DEFINE_SHARED_RECORDER(num_batch_leaf_nodes, 1, 1000, 3, Unit::COUNT);
  DEFINE_SHARED_RECORDER(num_stale_internal_keys, 1, 1000, 3, Unit::COUNT);
  DEFINE_SHARED_RECORDER(num_stale_leaf_keys, 1, 1000, 3, Unit::COUNT);
  DEFINE_SHARED_RECORDER(insert_depth, 1, Hash::MAX_NIBBLES, 3, Unit::COUNT);
  DEFINE_SHARED_RECORDER(remove_depth, 1, Hash::MAX_NIBBLES, 3, Unit::COUNT);

  // Used in internal_node.cpp
  DEFINE_SHARED_RECORDER(internal_node_update_hashes, 1, MAX_NS, 3, Unit::NANOSECONDS);
  DEFINE_SHARED_RECORDER(internal_node_insert, 1, MAX_NS, 3, Unit::NANOSECONDS);
  DEFINE_SHARED_RECORDER(internal_node_remove, 1, MAX_NS, 3, Unit::NANOSECONDS);

  // Used in merkle_tree_db_adapter.cpp

  DEFINE_SHARED_RECORDER(dba_batch_to_db_updates, 1, MAX_NS * 5, 3, Unit::NANOSECONDS);
  DEFINE_SHARED_RECORDER(dba_get_value, 1, MAX_NS * 5, 3, Unit::NANOSECONDS);
  DEFINE_SHARED_RECORDER(dba_create_block_node, 1, MAX_NS, 3, Unit::NANOSECONDS);
  DEFINE_SHARED_RECORDER(dba_get_raw_block, 1, MAX_US, 3, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(dba_last_reachable_block_db_updates, 1, MAX_US, 3, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(dba_size_of_updates, 1, MAX_VAL_SIZE, 3, Unit::BYTES);
  DEFINE_SHARED_RECORDER(dba_get_internal, 1, MAX_NS, 3, Unit::NANOSECONDS);
  DEFINE_SHARED_RECORDER(dba_serialize_internal, 1, MAX_NS, 3, Unit::NANOSECONDS);
  DEFINE_SHARED_RECORDER(dba_serialize_leaf, 1, MAX_NS, 3, Unit::NANOSECONDS);
  DEFINE_SHARED_RECORDER(dba_deserialize_internal, 1, MAX_NS, 3, Unit::NANOSECONDS);
  DEFINE_SHARED_RECORDER(dba_deserialize_block, 1, MAX_NS, 3, Unit::NANOSECONDS);
  DEFINE_SHARED_RECORDER(dba_deserialize_leaf, 1, MAX_NS, 3, Unit::NANOSECONDS);
  DEFINE_SHARED_RECORDER(dba_link_st_chain, 1, MAX_US, 3, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(dba_num_blocks_for_st_link, 1, 1000000, 3, Unit::COUNT);
  DEFINE_SHARED_RECORDER(dba_add_raw_block, 1, MAX_NS * 10, 3, Unit::NANOSECONDS);
  DEFINE_SHARED_RECORDER(dba_delete_block, 1, MAX_NS, 3, Unit::NANOSECONDS);
  DEFINE_SHARED_RECORDER(dba_get_leaf_key_val_at_most_version, 1, MAX_US, 3, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(dba_delete_keys_for_block, 1, MAX_NS, 3, Unit::NANOSECONDS);
  DEFINE_SHARED_RECORDER(dba_has_block, 1, MAX_NS, 3, Unit::NANOSECONDS);
  DEFINE_SHARED_RECORDER(dba_keys_for_version, 1, MAX_NS, 3, Unit::NANOSECONDS);
  DEFINE_SHARED_RECORDER(dba_hash_parent_block, 1, MAX_NS, 3, Unit::NANOSECONDS);
  DEFINE_SHARED_RECORDER(dba_hashed_parent_block_size, 1, MAX_VAL_SIZE, 3, Unit::BYTES);
  DEFINE_SHARED_RECORDER(dba_add_block_rocksdb_write, 1, MAX_US, 3, Unit::MICROSECONDS);

  // Used in walker.cpp
  DEFINE_SHARED_RECORDER(walker_descend, 1, MAX_US, 3, Unit::MICROSECONDS);
  DEFINE_SHARED_RECORDER(walker_ascend, 1, MAX_US, 3, Unit::MICROSECONDS);
};

inline const Recorders histograms;

}  // namespace concord::kvbc::sparse_merkle::detail
