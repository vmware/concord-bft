// Concord
//
// Copyright (c) 2023 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#include "sparse_merkle/proof_path_processor.h"
#include "sparse_merkle/internal_node.h"
#include "sparse_merkle/keys.h"

namespace concord {
namespace kvbc {
namespace sparse_merkle {
namespace proof_path_processor {

using Sliver = concordUtils::Sliver;

bool verifyProofPath(Sliver key, Sliver value, const std::vector<Hash>& proofPath, const Hash& rootHash) {
  Hasher hasher{};
  auto keyHash = hasher.hash(key.data(), key.length());
  auto valueHash = hasher.hash(value.data(), value.length());

  enum class Direction : std::uint8_t { Left, Right };
  const auto BatchedInternalNodeLevelsIterated = BatchedInternalNode::MAX_HEIGHT - 1;

  std::vector<Direction> pathOrdering;
  for (int nibble_index = 0; nibble_index < Hash::MAX_NIBBLES; nibble_index++) {
    auto current_nibble = keyHash.getNibble(nibble_index).data();
    auto node_index = BatchedInternalNode::nibbleToIndex(current_nibble);
    std::vector<Direction> subPath;
    subPath.reserve(BatchedInternalNodeLevelsIterated);
    for (int i = 0; i < BatchedInternalNodeLevelsIterated; i++) {
      if (BatchedInternalNode::isLeftChild(node_index)) {
        subPath.push_back(Direction::Left);
      } else {
        subPath.push_back(Direction::Right);
      }
      auto current_index_opt = BatchedInternalNode::parentIndex(node_index);
      if (current_index_opt.has_value()) {
        node_index = current_index_opt.value();
      } else {
        break;
      }
    }
    pathOrdering.insert(pathOrdering.end(), subPath.rbegin(), subPath.rend());
  }

  ConcordAssert(pathOrdering.size() >= proofPath.size());

  size_t index = proofPath.size() - 1;
  Hash next = valueHash;
  for (auto peerHash = proofPath.rbegin(); peerHash != proofPath.rend(); peerHash++) {
    if (pathOrdering[index] == Direction::Right) {
      next = hasher.parent(proofPath[index], next);
    } else {
      next = hasher.parent(next, proofPath[index]);
    }
    index--;
  }

  return next == rootHash;
}

std::vector<Hash> getProofPath(Sliver key, std::shared_ptr<IDBReader> db, const std::string& custom_prefix) {
  std::vector<Hash> retVal;
  Hasher hasher;
  Hash valueHash{};
  Hash globalLeaveHash{};
  auto key_hash = hasher.hash(key.data(), key.length());

  enum class Nodetype : std::uint8_t { None, InternalChild, LeafChild };
  const auto BatchedInternalNodeLevelsIterated = BatchedInternalNode::MAX_HEIGHT - 1;

  Nodetype nextNodetype = Nodetype::InternalChild;
  Version internalChildVersion{};
  Version leafChildVersion{};
  Hash leafHash{};

  for (int nibble_index = 0; nibble_index < Hash::MAX_NIBBLES; nibble_index++) {
    BatchedInternalNode node;
    if (nibble_index == 0) {
      node = db->get_latest_root(custom_prefix);
    } else {
      if (nextNodetype == Nodetype::InternalChild) {
        NibblePath np;
        for (int i = 0; i < nibble_index; i++) {
          np.append(key_hash.getNibble(i).data());
        }
        auto ik = InternalNodeKey{custom_prefix, internalChildVersion, np};
        node = db->get_internal(ik);
      } else {
        break;
      }
    }

    auto current_index = node.nibbleToIndex(key_hash.getNibble(nibble_index));
    auto child = node.children()[current_index];
    nextNodetype = Nodetype::None;
    bool firstInternalNodeFount = false;
    std::vector<Hash> hashesCollectedFromInternalNode;
    hashesCollectedFromInternalNode.reserve(BatchedInternalNodeLevelsIterated);
    for (int i = 0; i < BatchedInternalNodeLevelsIterated; i++) {
      if (nextNodetype == Nodetype::None && child.has_value() && std::get_if<InternalChild>(&child.value())) {
        nextNodetype = Nodetype::InternalChild;
      } else if (nextNodetype == Nodetype::None && child.has_value() && std::get_if<LeafChild>(&child.value())) {
        nextNodetype = Nodetype::LeafChild;
        auto v = std::get_if<LeafChild>(&child.value());
        leafHash = v->key.hash();
        leafChildVersion = v->key.version();
      }
      if (child.has_value()) {
        auto val = child.value();
        if (auto v = std::get_if<InternalChild>(&val)) {
          if (!firstInternalNodeFount) {
            firstInternalNodeFount = true;
            internalChildVersion = v->version;
          }
          hashesCollectedFromInternalNode.push_back(node.getHash(node.calcPeerIndex(current_index)));
        } else if (auto v = std::get_if<LeafChild>(&val)) {
          hashesCollectedFromInternalNode.push_back(node.getHash(node.calcPeerIndex(current_index)));
        }
      }
      auto current_index_opt = node.parentIndex(current_index);
      if (current_index_opt.has_value()) {
        current_index = current_index_opt.value();
        child = node.children()[current_index];
      }
    }
    retVal.insert(retVal.end(), hashesCollectedFromInternalNode.rbegin(), hashesCollectedFromInternalNode.rend());
  }
  return retVal;
}

}  // namespace proof_path_processor
}  // namespace sparse_merkle
}  // namespace kvbc
}  // namespace concord
