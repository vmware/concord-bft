
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
                                     {{"update", update},
                                      {"insert_key", insert_key},
                                      {"remove_key", remove_key},
                                      {"num_update_keys", num_updated_keys},
                                      {"num_deleted_keys", num_deleted_keys},
                                      {"key_size", key_size},
                                      {"val_size", val_size},
                                      {"hash_val", hash_val},
                                      {"num_batch_internal_nodes", num_batch_internal_nodes},
                                      {"num_batch_leaf_nodes", num_batch_leaf_nodes},
                                      {"num_stale_internal_keys", num_stale_internal_keys},
                                      {"num_stale_leaf_keys", num_stale_leaf_keys},
                                      {"insert_depth", insert_depth},
                                      {"remove_depth", remove_depth},

                                      {"internal_node_update_hashes", internal_node_update_hashes},
                                      {"internal_node_insert", internal_node_insert},
                                      {"internal_node_remove", internal_node_remove},

                                      {"dba_batch_to_db_updates", dba_batch_to_db_updates},
                                      {"dba_get_value", dba_get_value},
                                      {"dba_create_block_node", dba_create_block_node},
                                      {"dba_get_raw_block", dba_get_raw_block},
                                      {"dba_last_reachable_block_db_updates", dba_last_reachable_block_db_updates},
                                      {"dba_size_of_updates", dba_size_of_updates},
                                      {"dba_get_internal", dba_get_internal},
                                      {"dba_serialize_internal", dba_serialize_internal},
                                      {"dba_serialize_leaf", dba_serialize_leaf},
                                      {"dba_deserialize_internal", dba_deserialize_internal},
                                      {"dba_deserialize_block", dba_deserialize_block},
                                      {"dba_deserialize_leaf", dba_deserialize_leaf},
                                      {"dba_link_st_chain", dba_link_st_chain},
                                      {"dba_num_blocks_for_st_link", dba_num_blocks_for_st_link},
                                      {"dba_add_raw_block", dba_add_raw_block},
                                      {"dba_delete_block", dba_delete_block},
                                      {"dba_get_leaf_key_val_at_most_version", dba_get_leaf_key_val_at_most_version},
                                      {"dba_delete_keys_for_block", dba_delete_keys_for_block},
                                      {"dba_has_block", dba_has_block},
                                      {"dba_keys_for_version", dba_keys_for_version},
                                      {"dba_hash_parent_block", dba_hash_parent_block},
                                      {"dba_hashed_parent_block_size", dba_hashed_parent_block_size},
                                      {"dba_add_block_rocksdb_write", dba_add_block_rocksdb_write},

                                      {"walker_ascend", walker_ascend},
                                      {"walker_descend", walker_descend}});
  }

  // Used in tree.cpp
  std::shared_ptr<Recorder> update = std::make_shared<Recorder>(1, MAX_US, 3, Unit::MICROSECONDS);
  std::shared_ptr<Recorder> insert_key = std::make_shared<Recorder>(1, MAX_US, 3, Unit::MICROSECONDS);
  std::shared_ptr<Recorder> remove_key = std::make_shared<Recorder>(1, MAX_US, 3, Unit::MICROSECONDS);
  std::shared_ptr<Recorder> num_updated_keys = std::make_shared<Recorder>(1, 1000, 3, Unit::COUNT);
  std::shared_ptr<Recorder> num_deleted_keys = std::make_shared<Recorder>(1, 1000, 3, Unit::COUNT);
  std::shared_ptr<Recorder> key_size = std::make_shared<Recorder>(1, 2048, 3, Unit::BYTES);
  std::shared_ptr<Recorder> val_size = std::make_shared<Recorder>(1, MAX_VAL_SIZE, 3, Unit::BYTES);
  std::shared_ptr<Recorder> hash_val = std::make_shared<Recorder>(1, MAX_NS, 3, Unit::NANOSECONDS);
  std::shared_ptr<Recorder> num_batch_internal_nodes = std::make_shared<Recorder>(1, 1000, 3, Unit::COUNT);
  std::shared_ptr<Recorder> num_batch_leaf_nodes = std::make_shared<Recorder>(1, 1000, 3, Unit::COUNT);
  std::shared_ptr<Recorder> num_stale_internal_keys = std::make_shared<Recorder>(1, 1000, 3, Unit::COUNT);
  std::shared_ptr<Recorder> num_stale_leaf_keys = std::make_shared<Recorder>(1, 1000, 3, Unit::COUNT);
  std::shared_ptr<Recorder> insert_depth = std::make_shared<Recorder>(1, Hash::MAX_NIBBLES, 3, Unit::COUNT);
  std::shared_ptr<Recorder> remove_depth = std::make_shared<Recorder>(1, Hash::MAX_NIBBLES, 3, Unit::COUNT);

  // Used in internal_node.cpp
  std::shared_ptr<Recorder> internal_node_update_hashes = std::make_shared<Recorder>(1, MAX_NS, 3, Unit::NANOSECONDS);
  std::shared_ptr<Recorder> internal_node_insert = std::make_shared<Recorder>(1, MAX_NS, 3, Unit::NANOSECONDS);
  std::shared_ptr<Recorder> internal_node_remove = std::make_shared<Recorder>(1, MAX_NS, 3, Unit::NANOSECONDS);

  // Used in merkle_tree_db_adapter.cpp
  std::shared_ptr<Recorder> dba_batch_to_db_updates = std::make_shared<Recorder>(1, MAX_US * 5, 3, Unit::MICROSECONDS);
  std::shared_ptr<Recorder> dba_get_value = std::make_shared<Recorder>(1, MAX_US * 5, 3, Unit::MICROSECONDS);
  std::shared_ptr<Recorder> dba_create_block_node = std::make_shared<Recorder>(1, MAX_US, 3, Unit::MICROSECONDS);
  std::shared_ptr<Recorder> dba_get_raw_block = std::make_shared<Recorder>(1, MAX_US, 3, Unit::MICROSECONDS);
  std::shared_ptr<Recorder> dba_last_reachable_block_db_updates =
      std::make_shared<Recorder>(1, MAX_US * 5, 3, Unit::MICROSECONDS);
  std::shared_ptr<Recorder> dba_size_of_updates = std::make_shared<Recorder>(1, MAX_VAL_SIZE, 3, Unit::BYTES);
  std::shared_ptr<Recorder> dba_get_internal = std::make_shared<Recorder>(1, MAX_US, 3, Unit::MICROSECONDS);
  std::shared_ptr<Recorder> dba_serialize_internal = std::make_shared<Recorder>(1, MAX_NS, 3, Unit::NANOSECONDS);
  std::shared_ptr<Recorder> dba_serialize_leaf = std::make_shared<Recorder>(1, MAX_NS, 3, Unit::NANOSECONDS);
  std::shared_ptr<Recorder> dba_deserialize_internal = std::make_shared<Recorder>(1, MAX_NS, 3, Unit::NANOSECONDS);
  std::shared_ptr<Recorder> dba_deserialize_block = std::make_shared<Recorder>(1, MAX_NS, 3, Unit::NANOSECONDS);
  std::shared_ptr<Recorder> dba_deserialize_leaf = std::make_shared<Recorder>(1, MAX_NS, 3, Unit::NANOSECONDS);
  std::shared_ptr<Recorder> dba_link_st_chain = std::make_shared<Recorder>(1, MAX_NS, 3, Unit::NANOSECONDS);
  std::shared_ptr<Recorder> dba_num_blocks_for_st_link = std::make_shared<Recorder>(1, 1000000, 3, Unit::COUNT);
  std::shared_ptr<Recorder> dba_add_raw_block = std::make_shared<Recorder>(1, MAX_US * 10, 3, Unit::MICROSECONDS);
  std::shared_ptr<Recorder> dba_delete_block = std::make_shared<Recorder>(1, MAX_NS, 3, Unit::NANOSECONDS);
  std::shared_ptr<Recorder> dba_get_leaf_key_val_at_most_version =
      std::make_shared<Recorder>(1, MAX_US, 3, Unit::MICROSECONDS);
  std::shared_ptr<Recorder> dba_delete_keys_for_block = std::make_shared<Recorder>(1, MAX_NS, 3, Unit::NANOSECONDS);
  std::shared_ptr<Recorder> dba_has_block = std::make_shared<Recorder>(1, MAX_US, 3, Unit::MICROSECONDS);
  std::shared_ptr<Recorder> dba_keys_for_version = std::make_shared<Recorder>(1, MAX_NS, 3, Unit::NANOSECONDS);
  std::shared_ptr<Recorder> dba_hash_parent_block = std::make_shared<Recorder>(1, MAX_US, 3, Unit::MICROSECONDS);
  std::shared_ptr<Recorder> dba_hashed_parent_block_size = std::make_shared<Recorder>(1, MAX_VAL_SIZE, 3, Unit::BYTES);
  std::shared_ptr<Recorder> dba_add_block_rocksdb_write = std::make_shared<Recorder>(1, MAX_US, 3, Unit::MICROSECONDS);

  // Used in walker.cpp
  std::shared_ptr<Recorder> walker_descend = std::make_shared<Recorder>(1, MAX_US, 3, Unit::MICROSECONDS);
  std::shared_ptr<Recorder> walker_ascend = std::make_shared<Recorder>(1, MAX_US, 3, Unit::MICROSECONDS);
};

inline const Recorders histograms;

}  // namespace concord::kvbc::sparse_merkle::detail
