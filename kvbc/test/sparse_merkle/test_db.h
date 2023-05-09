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

#include <map>

#include "sparse_merkle/keys.h"
#include "sparse_merkle/db_reader.h"
#include "sparse_merkle/internal_node.h"
#include "kvbc/include/categorization/details.h"

using namespace std;
using namespace concord::kvbc::sparse_merkle;

class TestDB : public IDBReader {
 public:
  void put(const LeafKey& key, const LeafNode& val) { leaf_nodes_[key] = val; }
  void put(const InternalNodeKey& key, const BatchedInternalNode& val) {
    ConcordAssert(internal_nodes_.find(key) == internal_nodes_.end());

    internal_nodes_[key] = val;
    if (latest_version_[key.customPrefix().value()] < key.version()) {
      latest_version_[key.customPrefix().value()] = key.version();
    }
  }

  bool del(const LeafKey& key) { return leaf_nodes_.erase(key) == 1; }

  bool del(const InternalNodeKey& key) { return internal_nodes_.erase(key) == 1; }

  BatchedInternalNode get_latest_root(std::string custom_prefix = "") const override {
    if (latest_version_.find(custom_prefix) == latest_version_.end()) {
      return BatchedInternalNode();
    }
    auto root_key = InternalNodeKey::root(custom_prefix, latest_version_.at(custom_prefix));
    return internal_nodes_.at(root_key);
  }

  BatchedInternalNode get_internal(const InternalNodeKey& key) const override { return internal_nodes_.at(key); }

 private:
  map<std::string, Version> latest_version_;
  map<LeafKey, LeafNode> leaf_nodes_;
  map<InternalNodeKey, BatchedInternalNode> internal_nodes_;
};
