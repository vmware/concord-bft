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
#include "sparse_merkle/walker.h"
#include "assertUtils.hpp"

using namespace concordUtils;
using namespace concord::diagnostics;

namespace concord::kvbc::sparse_merkle::detail {

void Walker::appendEmptyNodes(const Hash& key, size_t nodes_to_create) {
  stack_.push(current_node_);
  for (size_t i = 0; i < nodes_to_create - 1 && nodes_to_create > 0; i++) {
    Nibble next_nibble = key.getNibble(depth());
    nibble_path_.append(next_nibble);
    stack_.emplace(BatchedInternalNode());
  }
  Nibble next_nibble = key.getNibble(depth());
  nibble_path_.append(next_nibble);
  current_node_ = BatchedInternalNode();
  stale_version_ = current_node_.version();
}

void Walker::descend(const Hash& key, Version next_version) {
  TimeRecorder scoped_timer(*histograms.walker_descend);
  stack_.push(current_node_);
  Nibble next_nibble = key.getNibble(depth());
  nibble_path_.append(next_nibble);
  InternalNodeKey next_internal_key{next_version, nibble_path_};
  current_node_ = cache_.getInternalNode(next_internal_key);
  stale_version_ = current_node_.version();
}

void Walker::ascendToRoot() { ascendToRoot(std::nullopt); }

void Walker::ascendToRoot(const std::optional<LeafKey>& key) {
  cache_.putStale(key);
  while (!atRoot()) {
    ascend();
  }
  // Manage root node updates
  markCurrentNodeStale();
  cacheCurrentNode();
}

void Walker::ascend() {
  ConcordAssert(!stack_.empty());
  TimeRecorder scoped_timer(*histograms.walker_ascend);

  markCurrentNodeStale();
  cacheCurrentNode();

  auto [child_key, hash] = pop();

  InternalChild update{hash, version()};
  current_node_.linkChild(child_key, update);
}

std::optional<Nibble> Walker::removeCurrentNode() {
  markCurrentNodeStale();
  cache_.remove(nibble_path_);
  if (!atRoot()) {
    return pop().first;
  }
  return std::nullopt;
}

void Walker::insertEmptyRootAtCurrentVersion() {
  auto children = BatchedInternalNode::Children{};
  children[0] = InternalChild{PLACEHOLDER_HASH, cache_.version()};
  cache_.put(NibblePath{}, BatchedInternalNode{children});
}

std::pair<Nibble, Hash> Walker::pop() {
  Hash hash = current_node_.hash();
  Nibble child_key = nibble_path_.popBack();
  current_node_ = stack_.top();
  stack_.pop();

  // Capture the current version of the node before we update it. This will
  // allow us to mark it stale on the next ascent.
  stale_version_ = current_node_.version();
  return std::make_pair(child_key, hash);
}

void Walker::markCurrentNodeStale() {
  // Don't mark the initial root stale, and don't mark an already cached node
  // stale.
  if (stale_version_ != Version(0) && stale_version_ != version()) {
    cache_.putStale(InternalNodeKey(stale_version_, nibble_path_));
  }
}

void Walker::cacheCurrentNode() { cache_.put(nibble_path_, current_node_); }

}  // namespace concord::kvbc::sparse_merkle::detail
