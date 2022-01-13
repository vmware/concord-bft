// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include <iostream>
#include <queue>
#include <algorithm>
#include <stack>

#include "RangeValidationTree.hpp"

using namespace std;
using namespace concord::kvbc;
using namespace concord::serialize;

namespace bftEngine::bcst::impl {

using HashVal = RangeValidationTree::HashVal;
using HashVal_t = CryptoPP::Integer;
using RVTNode = RangeValidationTree::RVTNode;
using RVBNode = RangeValidationTree::RVBNode;
using HashMaxValues = RangeValidationTree::HashMaxValues;

/////////////////////////////////////////// HashVal Operations ////////////////////////////////////////////////

HashVal_t HashMaxValues::calcNodeHashMaxValue() {
  std::string maxHashVal("0x");
  std::string maxHashValSuffix(HashVal::kNodeHashSizeBytes, 'F');
  maxHashVal += maxHashValSuffix;
  HashVal_t v(reinterpret_cast<unsigned char*>(const_cast<char*>(maxHashVal.c_str())), maxHashVal.size());
  return v;
}

HashVal_t HashMaxValues::calcNodeHashModulo() {
  std::string nodeHashModulo("0x1");
  std::string nodeHashModuloSuffix(HashVal::kNodeHashSizeBytes, '0');
  nodeHashModulo += nodeHashModuloSuffix;
  HashVal_t m(reinterpret_cast<unsigned char*>(const_cast<char*>(nodeHashModulo.c_str())), nodeHashModulo.size());
  return m;
}

const HashVal_t HashMaxValues::kNodeHashMaxValue_ = calcNodeHashMaxValue();
const HashVal_t HashMaxValues::kNodeHashModulo_ = calcNodeHashModulo();

HashVal::HashVal(shared_ptr<char[]> val) {
  hash_val_ = HashVal_t(reinterpret_cast<unsigned char*>(val.get()), kNodeHashSizeBytes);
}

HashVal::HashVal(char* val, size_t size) { hash_val_ = HashVal_t(reinterpret_cast<unsigned char*>(val), size); }

HashVal::HashVal(HashVal_t* val) { hash_val_ = *val; }

HashVal& HashVal::operator=(const HashVal& other) {
  hash_val_ = other.hash_val_;
  return *this;
}

HashVal& HashVal::operator+=(HashVal& other) {
  hash_val_ = ((hash_val_ + other.hash_val_) % HashMaxValues::kNodeHashModulo_);
  return *this;
}

HashVal& HashVal::operator-=(HashVal& other) {
  hash_val_ = ((hash_val_ - other.hash_val_) % HashMaxValues::kNodeHashModulo_);
  return *this;
}

// Used only to print
std::string HashVal::valToString() const noexcept {
  std::ostringstream oss;
  oss << std::hex << hash_val_;
  return oss.str();
}

std::string HashVal::getDecodedHashVal() const noexcept {
  // Encode to string does not work
  vector<char> enc_input(hash_val_.MinEncodedSize());
  hash_val_.Encode(reinterpret_cast<CryptoPP::byte*>(enc_input.data()), enc_input.size());
  ostringstream oss_en;
  for (auto& c : enc_input) {
    oss_en << c;
  }
  return oss_en.str();
}
//////////////////////////////// End of HashVal operations ///////////////////////////////////////

std::string RangeValidationTree::NodeId::getPrettyVal() const noexcept {
  std::ostringstream os;
  os << " [" << level << "," << rvb_index << "]";
  return os.str();
}

shared_ptr<char[]> RVBNode::computeNodeHash(NodeId& node_id, const STDigest& digest) {
  ConcordAssertGT(node_id.getVal(), 0);
  DigestContext c;
  auto nid = std::to_string(node_id.getVal());
  c.update(nid.c_str(), nid.size());
  c.update(digest.get(), BLOCK_DIGEST_SIZE);
  // TODO
  // Use default_delete in case memleak is reported by ASAN
  std::shared_ptr<char[]> outDigest(new char[BLOCK_DIGEST_SIZE]);
  c.writeDigest(outDigest.get());
  return outDigest;
}

/////////////////////////////// Start of RVT node methods /////////////////////////////////////

RVTNode::RVTNode(uint32_t RVT_K, shared_ptr<RVBNode>& node)
    : RVBNode(kDefaultRVTLeafLevel, node->id.rvb_index, getZeroedDigest()) {
  n_child++;
  min_child_id = NodeId(kDefaultRVBLeafLevel, node->id.rvb_index).getVal();
  max_child_id = NodeId(kDefaultRVBLeafLevel, node->id.rvb_index + RVT_K - 1).getVal();
}

RVTNode::RVTNode(uint32_t RVT_K, std::shared_ptr<RVTNode>& node)
    : RVBNode(node->id.level + 1, node->id.rvb_index, getZeroedDigest()) {
  n_child++;
  auto level = node->id.level;
  min_child_id = NodeId(level, node->id.rvb_index).getVal();
  max_child_id = NodeId(level,
                        node->id.rvb_index + RangeValidationTree::pow_int(RVT_K, level + 1) -
                            RangeValidationTree::pow_int(RVT_K, level))
                     .getVal();
  ConcordAssert(n_child <= RVT_K);
}

RVTNode::RVTNode(SerializedRVTNode& node, char* hash_val, size_t hash_size) : RVBNode(node.id, hash_val, hash_size) {
  n_child = node.n_child;
  min_child_id = node.min_child_id;
  max_child_id = node.max_child_id;
  parent_id = node.parent_id;
}

uint64_t RVTNode::getNextSiblingId(uint32_t RVT_K) noexcept {
  auto level = id.level;
  auto rvb_index = id.rvb_index;
  // Formula to find id of next sibling
  return NodeId(level, (rvb_index + RangeValidationTree::pow_int(RVT_K, level))).getVal();
}

std::ostringstream RVTNode::serialize() {
  std::ostringstream os;
  Serializable::serialize(os, n_child);
  Serializable::serialize(os, min_child_id);
  Serializable::serialize(os, max_child_id);
  Serializable::serialize(os, parent_id);
  Serializable::serialize(os, hash_val.getSize());
  auto val = hash_val.getDecodedHashVal();
  Serializable::serialize(os, val.data(), hash_val.getSize());
  return os;
}

shared_ptr<RVTNode> RVTNode::create(std::istringstream& is) {
  SerializedRVTNode snode;
  Serializable::deserialize(is, snode.id);
  Serializable::deserialize(is, snode.n_child);
  Serializable::deserialize(is, snode.min_child_id);
  Serializable::deserialize(is, snode.max_child_id);
  Serializable::deserialize(is, snode.parent_id);
  Serializable::deserialize(is, snode.hash_size);
  std::unique_ptr<char[]> ptr = std::make_unique<char[]>(snode.hash_size);
  Serializable::deserialize(is, ptr.get(), snode.hash_size);
  return std::make_shared<RVTNode>(snode, ptr.get(), snode.hash_size);
}
////////////////////////////// End of RVT node methods /////////////////////////////////////////

RangeValidationTree::RangeValidationTree(const logging::Logger& logger,
                                         uint32_t RVT_K,
                                         uint32_t fetch_range_size,
                                         size_t hash_size)
    : rightmostRVTNode_(NodeId::kMaxLevels, nullptr),
      leftmostRVTNode_(NodeId::kMaxLevels, nullptr),
      logger_(logger),
      RVT_K(RVT_K),
      fetch_range_size_(fetch_range_size),
      hash_size_(hash_size) {}

bool RangeValidationTree::valid(const uint64_t rvb_id, const STDigest& digest) {
  if (rvb_id == 0 or digest.isZero() or fetch_range_size_ == 0 or rvb_id % fetch_range_size_ != 0) {
    LOG_ERROR(logger_, "invalid input data" << KVLOG(rvb_id, digest, fetch_range_size_));
    return false;
  }
  return true;
}

uint64_t RangeValidationTree::pow_int(uint64_t base, uint64_t exp) noexcept {
  ConcordAssertOR(base != 0, exp != 0);  // both zero are undefined
  uint64_t res{base};
  if (base == 0) return 1;
  if (exp == 0) return 1;
  for (size_t i{1}; i < exp; ++i) {
    res *= base;
  }
  return res;
}

void RangeValidationTree::addRVBNode(shared_ptr<RVBNode>& rvb_node) {
  auto node = openForInsertion(RVTNode::kDefaultRVTLeafLevel);
  if (!node) {
    // adding first rvb node but it might not be first child (reason: pruning)
    node = make_shared<RVTNode>(RVT_K, rvb_node);
    rightmostRVTNode_[RVTNode::kDefaultRVTLeafLevel] = node;
    if (!leftmostRVTNode_[RVTNode::kDefaultRVTLeafLevel]) {
      leftmostRVTNode_[RVTNode::kDefaultRVTLeafLevel] = node;
    }
    ConcordAssert(id_to_node_map_.insert({node->id.getVal(), node}).second == true);
    if (!root_) {
      setNewRoot(node);
      return;
    }
    addInternalNode(node);
  } else {
    ++node->n_child;
    ConcordAssertLE(node->n_child, RVT_K);
    ConcordAssertLE(node->min_child_id + node->n_child - 1, node->max_child_id);
    node->addRVBNodeHash(rvb_node);
    addHashValToInternalNodes(node, rvb_node);
  }
}

void RangeValidationTree::removeRVBNode(shared_ptr<RVBNode>& rvb_node) {
  auto node = openForRemoval(RVTNode::kDefaultRVTLeafLevel);
  ConcordAssert(node != nullptr);

  if (node->n_child == 1) {
    // no more RVB childs, erase the node
    auto id = node->id.getVal();
    auto node_to_remove_iter = id_to_node_map_.find(id);
    ConcordAssert(node_to_remove_iter != id_to_node_map_.end());
    if ((node == leftmostRVTNode_[RVTNode::kDefaultRVTLeafLevel]) &&
        (node == rightmostRVTNode_[RVTNode::kDefaultRVTLeafLevel])) {
      leftmostRVTNode_[RVTNode::kDefaultRVTLeafLevel] = nullptr;
      rightmostRVTNode_[RVTNode::kDefaultRVTLeafLevel] = nullptr;
    } else {
      if (node == leftmostRVTNode_[RVTNode::kDefaultRVTLeafLevel]) {
        auto id = NodeId(RVTNode::kDefaultRVTLeafLevel, rvb_node->id.rvb_index + 1).getVal();
        auto iter = id_to_node_map_.find(id);
        leftmostRVTNode_[RVTNode::kDefaultRVTLeafLevel] = (iter == id_to_node_map_.end()) ? nullptr : iter->second;
      }
      if (node == rightmostRVTNode_[RVTNode::kDefaultRVTLeafLevel]) {
        rightmostRVTNode_[RVTNode::kDefaultRVTLeafLevel] = nullptr;
      }
    }
    id_to_node_map_.erase(node_to_remove_iter);
    // TODO
    // ConcordAssertEQ(node->min_child_id, node->max_child_id);
  }

  --node->n_child;
  if (node->n_child > 0) {
    ++node->min_child_id;
  }
  ConcordAssertLE(node->min_child_id + node->n_child - 1, node->max_child_id);
  removeHashValFromInternalNodes(node, rvb_node);
}

void RangeValidationTree::addHashValToInternalNodes(shared_ptr<RVTNode>& node, shared_ptr<RVBNode>& rvb_node) {
  if (!root_ || !node || !rvb_node) {
    return;
  }
  while (node != root_) {
    auto itr = id_to_node_map_.find(node->parent_id);
    ConcordAssert(itr != id_to_node_map_.end());
    auto parent_node = itr->second;
    parent_node->addRVTNodeHash(node);
    node = parent_node;
  }
}

void RangeValidationTree::removeHashValFromInternalNodes(shared_ptr<RVTNode>& removed_rvt_node,
                                                         shared_ptr<RVBNode>& rvb_node) {
  ConcordAssert(root_ != nullptr);
  ConcordAssert(removed_rvt_node != nullptr);
  ConcordAssert(rvb_node != nullptr);

  // Loop 1
  // Trim the tree from removed_rvt_node to root
  shared_ptr<RVTNode> cur_node = removed_rvt_node;
  while (cur_node != root_) {
    auto parent_node = getParentNode(cur_node);

    // When removed_rvt_node is removed, we need to update its parent
    if (cur_node->n_child == 0) {
      --parent_node->n_child;
      if (parent_node->n_child > 0) {
        auto level = cur_node->id.level;
        auto id = NodeId(level, cur_node->id.rvb_index + RangeValidationTree::pow_int(RVT_K, level)).getVal();
        ConcordAssert(id_to_node_map_.find(id) != id_to_node_map_.end());
        parent_node->min_child_id = id;
      }
      if (cur_node != removed_rvt_node) {
        id_to_node_map_.erase(cur_node->id.getVal());
        if ((leftmostRVTNode_[cur_node->id.level] == cur_node) && (rightmostRVTNode_[cur_node->id.level] == cur_node)) {
          rightmostRVTNode_[cur_node->id.level] = nullptr;
          leftmostRVTNode_[cur_node->id.level] = nullptr;
        } else if (leftmostRVTNode_[cur_node->id.level] == cur_node) {
          leftmostRVTNode_[cur_node->id.level] = getRVTNodeOfRightSibling(cur_node);
        }
      }
    }  // Trim the tree from removed_rvt_node to root
    parent_node->removeRVTNodeHash(cur_node);
    if (cur_node == removed_rvt_node) {
      cur_node->removeRVBNodeHash(rvb_node);
    }
    parent_node->addRVTNodeHash(cur_node);
    cur_node = parent_node;
  }

  // Loop 2
  // Shrink the tree from root to bottom (level 1) . In theory, tree can end empty after this loop
  while ((cur_node == root_) && (cur_node->n_child == 1) && (cur_node->id.level > 1)) {
    id_to_node_map_.erase(cur_node->id.getVal());
    ConcordAssertEQ(rightmostRVTNode_[cur_node->id.level - 1], leftmostRVTNode_[cur_node->id.level - 1])
        rightmostRVTNode_[cur_node->id.level] = nullptr;
    leftmostRVTNode_[cur_node->id.level] = nullptr;
    cur_node = rightmostRVTNode_[cur_node->id.level - 1];
    setNewRoot(cur_node);
  }
}

// level 0: RVB node
// level 1 and above: RVT node
// first RVT may end up adding few more RVTs in top
//
// RVT             1,1             1,4
//              /   |   \          /
//             /    |    \        /
// RVB        0,1  0,2   0,3     0,4

void RangeValidationTree::addInternalNode(shared_ptr<RVTNode>& node) {
  ConcordAssert(root_ != node);
  // bool found_existing_parent{false};
  std::shared_ptr<RVTNode> parent_node;
  // uint64_t rvb_index = node->id.rvb_index;
  while (node->id.level != root_->id.level) {
    if (node->parent_id == 0) {
      parent_node = openForInsertion(node->id.level + 1);
      if (parent_node) {
        // Add the node to parent_node which still has more space for childs
        // found_existing_parent = true;
        node->parent_id = parent_node->id.getVal();
        parent_node->n_child++;
        ConcordAssert(parent_node->n_child <= RVT_K);
      } else {
        // construct new internal RVT node
        ConcordAssert(node->isMinChild(RVT_K) == true);
        parent_node = make_shared<RVTNode>(RVT_K, node);
        ConcordAssert(id_to_node_map_.insert({parent_node->id.getVal(), parent_node}).second == true);
        rightmostRVTNode_[parent_node->id.level] = parent_node;
        node->parent_id = parent_node->id.getVal();
      }
    } else {
      auto iter = id_to_node_map_.find(node->parent_id);
      ConcordAssert(iter != id_to_node_map_.end());
      parent_node = iter->second;
    }
    parent_node->addRVTNodeHash(node);
    node = parent_node;
  }

  // no need to create new root as we have reached to root while updating hash
  if (node->id.getVal() == root_->id.getVal()) {
    return;
  }

  // create new root
  auto new_root = make_shared<RVTNode>(RVT_K, root_);
  ConcordAssert(id_to_node_map_.insert({new_root->id.getVal(), new_root}).second == true);
  node->parent_id = new_root->id.getVal();  // special case
  // TODO
  // how to recover from crash at this point?
  new_root->addRVTNodeHash(root_);
  new_root->addRVTNodeHash(node);
  new_root->n_child++;  // special case
  ConcordAssert(new_root->n_child <= RVT_K);
  setNewRoot(new_root);
}

void RangeValidationTree::printToLog(bool only_node_id) const noexcept {
  if (!root_ or totalNodes() == 0) {
    LOG_INFO(logger_, "Empty RVT");
    return;
  }
  std::ostringstream oss;
  oss << " #Levels=" << root_->id.level;
  oss << " #Nodes=" << totalNodes();
  oss << " Structure: ";
  queue<shared_ptr<RVTNode>> q;
  q.push(root_);
  while (q.size()) {
    auto& node = q.front();
    q.pop();
    oss << node->id.getPrettyVal() << " ";
    if (not only_node_id) {
      oss << " = " << node->hash_val.valToString() << " ";
    }
    if (node->id.level == 1) continue;

    uint16_t count = 0;
    uint64_t id = node->min_child_id;
    // intentional redundant check
    // child count is required as right side substree may not be complete tree
    while (count++ < node->n_child) {
      auto iter = id_to_node_map_.find(id);
      ConcordAssert(iter != id_to_node_map_.end());
      auto& child_node = iter->second;
      q.push(child_node);
      id = child_node->getNextSiblingId(RVT_K);
    }
  }
  LOG_INFO(logger_, oss.str());
}

/////////////////////////////////// start of API //////////////////////////////////////////////

void RangeValidationTree::addNode(const RVBId rvb_id, const STDigest& digest) {
  if (not valid(rvb_id, digest)) {
    throw std::invalid_argument("invalid input data");
  }
  auto rvb_index = rvb_id / fetch_range_size_;
  auto node = make_shared<RVBNode>(rvb_index, digest);
  if (max_rvb_index_ > 0) {
    ConcordAssertEQ(max_rvb_index_ + 1, rvb_index);
  }
  addRVBNode(node);
  max_rvb_index_ = rvb_index;
  if (min_rvb_index_ == 0) {
    min_rvb_index_ = rvb_index;
  }
  // LOG_TRACE(logger_, KVLOG(rvb_id, max_rvb_index_));
}

void RangeValidationTree::removeNode(const RVBId rvb_id, const STDigest& digest) {
  ConcordAssert(root_ != nullptr);
  if (not valid(rvb_id, digest)) {
    throw std::invalid_argument("invalid input data");
  }
  auto rvb_index = rvb_id / fetch_range_size_;
  auto node = make_shared<RVBNode>(rvb_index, digest);
  ConcordAssertEQ(min_rvb_index_, rvb_index);
  removeRVBNode(node);
  if (!root_) {
    min_rvb_index_ = 0;
    max_rvb_index_ = 0;
  } else {
    ++min_rvb_index_;
  }
  // LOG_TRACE(logger_, KVLOG(rvb_id, min_rvb_index_));
}

std::ostringstream RangeValidationTree::getSerializedRvbData() const {
  LOG_TRACE(logger_, "");
  std::ostringstream os;
  if (!root_) {
    return os;
  }
  Serializable::serialize(os, magic_num_);
  Serializable::serialize(os, version_num_);
  Serializable::serialize(os, RVT_K);
  Serializable::serialize(os, fetch_range_size_);
  Serializable::serialize(os, hash_size_);
  Serializable::serialize(os, root_->id.getVal());

  Serializable::serialize(os, totalNodes());
  for (auto& itr : id_to_node_map_) {
    Serializable::serialize(os, itr.first);
    auto serialized_node = itr.second->serialize();
    Serializable::serialize(os, serialized_node.str().data(), serialized_node.str().size());
  }

  uint64_t null_node_id = 0;
  auto max_levels = root_->id.level;
  for (uint64_t i = 0; i <= max_levels; i++) {
    auto node = rightmostRVTNode_[i];
    if (!node) {
      Serializable::serialize(os, null_node_id);
    } else {
      Serializable::serialize(os, node->id.getVal());
    }
  }
  for (uint64_t i = 0; i <= max_levels; i++) {
    auto node = leftmostRVTNode_[i];
    if (!node) {
      Serializable::serialize(os, null_node_id);
    } else {
      Serializable::serialize(os, node->id.getVal());
    }
  }
  LOG_TRACE(logger_, KVLOG(os.str().size()));
  LOG_TRACE(logger_,
            "Serialized tree with " << totalNodes() << " nodes having root hash as " << root_->hash_val.valToString());
  return os;
}

bool RangeValidationTree::setSerializedRvbData(std::istringstream& is) {
  LOG_TRACE(logger_, is.str().size());
  ConcordAssertGT(is.str().size(), 0);
  id_to_node_map_.clear();
  rightmostRVTNode_.clear();
  leftmostRVTNode_.clear();
  root_ = nullptr;
  max_rvb_index_ = 0;
  min_rvb_index_ = 0;
  RVTMetadata data;

  Serializable::deserialize(is, data.magic_num);
  ConcordAssertEQ(data.magic_num, magic_num_);
  Serializable::deserialize(is, data.version_num);
  ConcordAssertEQ(data.version_num, version_num_);
  Serializable::deserialize(is, data.RVT_K);
  // TODO remove unnecessary asserts
  ConcordAssertEQ(data.RVT_K, RVT_K);
  Serializable::deserialize(is, data.fetch_range_size);
  ConcordAssertEQ(data.fetch_range_size, fetch_range_size_);
  Serializable::deserialize(is, data.hash_size);
  ConcordAssert(data.hash_size == hash_size_);
  Serializable::deserialize(is, data.root_node_id);

  // populate id_to_node_map_
  Serializable::deserialize(is, data.total_nodes);
  id_to_node_map_.reserve(data.total_nodes);
  uint64_t min_rvb_index{std::numeric_limits<uint64_t>::max()}, max_rvb_index{0};
  for (uint64_t i = 0; i < data.total_nodes; i++) {
    auto node = RVTNode::create(is);
    uint64_t id = node->id.getVal();
    id_to_node_map_.emplace(id, node);

    if (node->id.level == 1) {
      // level 0 child IDs can be treated as rvb_indexes
      if (node->min_child_id < min_rvb_index) {
        min_rvb_index = node->min_child_id;
      }
      if ((node->min_child_id + node->n_child) > max_rvb_index) {
        max_rvb_index = (node->min_child_id + node->n_child);
      }
    }
  }
  if (data.total_nodes > 0) {
    max_rvb_index_ = max_rvb_index;
    min_rvb_index_ = min_rvb_index;
  }

  setNewRoot(id_to_node_map_[data.root_node_id]);

  // populate rightmostRVTNode_
  uint64_t node_id;
  uint64_t null_node_id = 0;
  auto max_levels = root_->id.level;
  for (size_t i = 0; i <= max_levels; i++) {
    Serializable::deserialize(is, node_id);
    if (node_id == null_node_id) {
      rightmostRVTNode_.push_back(nullptr);
    } else {
      rightmostRVTNode_.push_back(id_to_node_map_[node_id]);
    }
  }

  // populate leftmostRVTNode_
  for (size_t i = 0; i <= max_levels; i++) {
    Serializable::deserialize(is, node_id);
    if (node_id == null_node_id) {
      leftmostRVTNode_.push_back(nullptr);
    } else {
      leftmostRVTNode_.push_back(id_to_node_map_[node_id]);
    }
  }

  LOG_TRACE(logger_,
            "Created tree with " << totalNodes() << " nodes having root hash as " << root_->hash_val.valToString());
  is.peek();
  return is.eof();
}

std::vector<RVBGroupId> RangeValidationTree::getRvbGroupIds(RVBId start_block_id, RVBId end_block_id) const {
  LOG_TRACE(logger_, KVLOG(start_block_id, end_block_id));
  ConcordAssertGT(start_block_id, 0);
  ConcordAssertGT(end_block_id, 0);
  ConcordAssertLE(start_block_id, end_block_id);
  ConcordAssert((start_block_id % fetch_range_size_) == 0);
  ConcordAssert((end_block_id % fetch_range_size_) == 0);
  std::vector<RVBGroupId> rvb_group_ids;

  RVBIndex parent_rvb_index;
  for (uint64_t rvb_id{start_block_id}; rvb_id <= end_block_id; rvb_id += fetch_range_size_) {
    RVBIndex rvb_index = rvb_id / fetch_range_size_;
    RVBIndex rvb_group_index = rvb_index / RVT_K;
    if (rvb_index <= RVT_K) {
      parent_rvb_index = 1;
    } else if ((rvb_index > RVT_K) and (rvb_index % RVT_K) == 0) {
      parent_rvb_index = (--rvb_group_index * RVT_K) + 1;
    } else {
      parent_rvb_index = (rvb_group_index * RVT_K) + 1;
    }
    RVBGroupId rvb_group_id = NodeId(RVTNode::kDefaultRVTLeafLevel, parent_rvb_index).getVal();
    if (id_to_node_map_.find(rvb_group_id) == id_to_node_map_.end()) {
      LOG_WARN(logger_, KVLOG(rvb_group_id, parent_rvb_index, start_block_id, end_block_id));
      return rvb_group_ids;
    }
    if (rvb_group_ids.empty() || (rvb_group_ids.back() != rvb_group_id)) {
      rvb_group_ids.push_back(rvb_group_id);
    }
  }
  ConcordAssert(!rvb_group_ids.empty());
  return rvb_group_ids;
}

std::vector<RVBId> RangeValidationTree::getRvbIds(RVBGroupId rvb_group_id) const {
  LOG_TRACE(logger_, KVLOG(rvb_group_id));
  ConcordAssertGT(rvb_group_id, 0);
  std::vector<RVBId> rvb_ids;

  if (not valid(rvb_group_id)) {
    LOG_ERROR(logger_, KVLOG(rvb_group_id));
    return rvb_ids;
  }
  const auto iter = id_to_node_map_.find(rvb_group_id);
  if (iter == id_to_node_map_.end()) {
    LOG_WARN(logger_, KVLOG(rvb_group_id));
    return rvb_ids;
  }

  auto min_child_id = iter->second->min_child_id;
  for (size_t rvb_index{min_child_id}; rvb_index < min_child_id + iter->second->n_child; ++rvb_index) {
    rvb_ids.push_back(rvb_index * fetch_range_size_);
  }
  return rvb_ids;
}

// validation can happen on partial RVGgroup as ST cycle might not fetch 256K blocks
// Example:
// FR = 256 rvbid = 256/1, 512/2, ...
// RVT_K = 1024 * 256 = single RVGGroupId represents 256K blocks
// received only 128K blocks from network

std::string RangeValidationTree::getDirectParentHashVal(RVBId rvb_id) const {
  LOG_TRACE(logger_, KVLOG(rvb_id));
  ConcordAssert(rvb_id % fetch_range_size_ == 0);
  RVBIndex rvb_index = rvb_id / fetch_range_size_;
  RVBIndex parent_rvb_index;
  if (rvb_index <= RVT_K) {
    parent_rvb_index = 1;
  } else if (rvb_index % RVT_K == 0) {
    parent_rvb_index = rvb_index - RVT_K + 1;
  } else {
    parent_rvb_index = (rvb_index - (rvb_index % RVT_K)) + 1;
  }
  std::string hash_val;
  RVBGroupId parent_node_id = NodeId(RVTNode::kDefaultRVTLeafLevel, parent_rvb_index).getVal();
  auto itr = id_to_node_map_.find(parent_node_id);
  if (itr == id_to_node_map_.end()) {
    LOG_WARN(logger_, KVLOG(parent_node_id, rvb_index, rvb_id));
    return hash_val;
  }
  hash_val = itr->second->hash_val.valToString();
  LOG_TRACE(logger_, KVLOG(rvb_id, parent_node_id, hash_val));
  return hash_val;
}

RVBId RangeValidationTree::getMinRvbId() const { return (min_rvb_index_ * fetch_range_size_); }

RVBId RangeValidationTree::getMaxRvbId() const {
  if (max_rvb_index_ == 0) {
    ConcordAssertEQ(totalNodes(), 0);
    ConcordAssertEQ(root_, nullptr);
    return 0;
  }
  return max_rvb_index_ * fetch_range_size_;
}

void RangeValidationTree::setNewRoot(shared_ptr<RVTNode> new_root) {
  if (!new_root) {
    // setting root to null
    ConcordAssert(root_->id.level == 1);
    ConcordAssert(root_ != nullptr);
    root_ = nullptr;
    // do we need to initialize vector to nullptr explicitly?
    // TODO - optimize later
    for (uint8_t level = 0; level < NodeId::kMaxLevels; level++) {
      rightmostRVTNode_[level] = nullptr;
      leftmostRVTNode_[level] = nullptr;
    }
    return;
  }
  if (root_) {
    // replacing roots
    int new_root_level = static_cast<int>(new_root->id.level);
    int old_root_level = static_cast<int>(root_->id.level);
    ConcordAssert(new_root_level != 0);
    ConcordAssert(std::abs(new_root_level - old_root_level) == 1);
    if (new_root_level > old_root_level) {
      // replacing the root - tree grows 1 level
      root_->parent_id = new_root->id.getVal();
    } else if (new_root_level < old_root_level) {
      // replacing the root - tree shrinks 1 level
      rightmostRVTNode_[root_->id.level] = nullptr;
      leftmostRVTNode_[root_->id.level] = nullptr;
    }
  }
  root_ = new_root;
  root_->parent_id = 0;
  rightmostRVTNode_[new_root->id.level] = new_root;
  leftmostRVTNode_[new_root->id.level] = new_root;
}

////////////////////////////////// Helper functions //////////////////////////////////////////
inline std::shared_ptr<RVTNode> RangeValidationTree::openForInsertion(uint64_t level) const {
  if (rightmostRVTNode_[level] == nullptr) {
    return nullptr;
  }
  auto& node = rightmostRVTNode_[level];
  uint64_t min_child_actual_rvb_index = node->min_child_id & NodeId::kRvbIndexMask;
  uint64_t max_child_possible_rvb_index = node->max_child_id & NodeId::kRvbIndexMask;
  uint64_t max_child_actual_rvb_index =
      min_child_actual_rvb_index + RangeValidationTree::pow_int(RVT_K, node->id.level - 1) * (node->n_child - 1);
  return (max_child_actual_rvb_index < max_child_possible_rvb_index) ? node : nullptr;
}

inline std::shared_ptr<RVTNode> RangeValidationTree::openForRemoval(uint64_t level) const {
  return leftmostRVTNode_[level];
}

shared_ptr<RVTNode> RangeValidationTree::getRVTNodeOfLeftSibling(shared_ptr<RVTNode>& node) const {
  if (leftmostRVTNode_[node->id.level] == node) return nullptr;
  auto id = NodeId(node->id.level, node->id.rvb_index - RangeValidationTree::pow_int(RVT_K, node->id.level)).getVal();
  auto iter = id_to_node_map_.find(id);
  return (iter == id_to_node_map_.end()) ? nullptr : iter->second;
}

shared_ptr<RVTNode> RangeValidationTree::getRVTNodeOfRightSibling(shared_ptr<RVTNode>& node) const {
  if (rightmostRVTNode_[node->id.level] == node) return nullptr;
  auto id = NodeId(node->id.level, node->id.rvb_index + RangeValidationTree::pow_int(RVT_K, node->id.level)).getVal();
  auto iter = id_to_node_map_.find(id);
  return (iter == id_to_node_map_.end()) ? nullptr : iter->second;
}

shared_ptr<RVTNode> RangeValidationTree::getParentNode(std::shared_ptr<RVTNode>& node) const noexcept {
  auto itr = id_to_node_map_.find(node->parent_id);
  ConcordAssert(itr != id_to_node_map_.end());
  return itr->second;
}
//////////////////////////////////// End of API ////////////////////////////////////////////////
}  // namespace bftEngine::bcst::impl
