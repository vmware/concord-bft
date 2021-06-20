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

using namespace std;
using namespace concord::kvbc::sparse_merkle;

class TestDB : public concord::kvbc::sparse_merkle::IDBReader {
 public:
  void put(const LeafKey& key, const LeafNode& val) { leaf_nodes_[key] = val; }
  void put(const InternalNodeKey& key, const BatchedInternalNode& val) {
    internal_nodes_[key] = val;
    if (latest_version_ < key.version()) {
      latest_version_ = key.version();
    }
  }

  BatchedInternalNode get_latest_root() const override {
    if (latest_version_ == 0) {
      return BatchedInternalNode();
    }
    auto root_key = InternalNodeKey::root(latest_version_);
    return internal_nodes_.at(root_key);
  }

  BatchedInternalNode get_internal(const InternalNodeKey& key) const override { return internal_nodes_.at(key); }

 private:
  Version latest_version_ = 0;
  map<LeafKey, LeafNode> leaf_nodes_;
  map<InternalNodeKey, BatchedInternalNode> internal_nodes_;
};
