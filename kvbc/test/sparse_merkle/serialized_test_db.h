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
#include "kvbc/include/merkle_tree_serialization.h"

using namespace std;
using namespace concord::kvbc::sparse_merkle;

class SerializedTestDB : public IDBReader {
 public:
  void put(const LeafKey& key, const LeafNode& val) {
    std::string serializedLeafKey = concord::kvbc::v2MerkleTree::detail::serialize(key);
    std::string serializedLeafNode(val.value.data(), val.value.size());
    ConcordAssert(nodes_.find(serializedLeafKey) == nodes_.end());
    nodes_[serializedLeafKey] = serializedLeafNode;
    // std::cout << "DEBUG: " << serializedLeafKey << ":" << serializedLeafNode << "\n";
  }
  void put(const InternalNodeKey& key, const BatchedInternalNode& val) {
    std::string serializedInternalNodeKey = concord::kvbc::v2MerkleTree::detail::serialize(key);
    std::string serializedBatchedInternalNode = concord::kvbc::v2MerkleTree::detail::serialize(val);

    ConcordAssert(nodes_.find(serializedInternalNodeKey) == nodes_.end());
    nodes_[serializedInternalNodeKey] = serializedBatchedInternalNode;
    // std::cout << "DEBUG: " << serializedInternalNodeKey << ":" << serializedBatchedInternalNode << "\n";

    if (latest_version_[key.customPrefix().value()] < key.version()) {
      latest_version_[key.customPrefix().value()] = key.version();
    }
  }

  bool del(const LeafKey& key) {
    std::string serializedLeafKey = concord::kvbc::v2MerkleTree::detail::serialize(key);
    return nodes_.erase(serializedLeafKey) == 1;
  }

  bool del(const InternalNodeKey& key) {
    std::string serializedInternalNodeKey = concord::kvbc::v2MerkleTree::detail::serialize(key);
    return nodes_.erase(serializedInternalNodeKey) == 1;
  }

  BatchedInternalNode get_latest_root(std::string custom_prefix = "") const override {
    if (latest_version_.find(custom_prefix) == latest_version_.end()) {
      return BatchedInternalNode();
    }
    auto root_key = InternalNodeKey::root(custom_prefix, latest_version_.at(custom_prefix));
    auto ser_root_key = concord::kvbc::v2MerkleTree::detail::serialize(root_key);
    ConcordAssert(nodes_.find(ser_root_key) != nodes_.end());
    auto retVal = nodes_.at(ser_root_key);
    return concord::kvbc::v2MerkleTree::detail::deserialize<BatchedInternalNode>(
        concordUtils::Sliver{std::move(retVal)});
  }

  BatchedInternalNode get_internal(const InternalNodeKey& key) const override {
    auto ser_key = concord::kvbc::v2MerkleTree::detail::serialize(key);
    ConcordAssert(nodes_.find(ser_key) != nodes_.end());
    auto retVal = nodes_.at(ser_key);
    return concord::kvbc::v2MerkleTree::detail::deserialize<BatchedInternalNode>(
        concordUtils::Sliver{std::move(retVal)});
  }

 private:
  map<std::string, Version> latest_version_;
  map<std::string, std::string> nodes_;
};
