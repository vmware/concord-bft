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

#include "RangeValidationTree.hpp"

using namespace std;
using namespace concord::kvbc;
using namespace concord::serialize;

namespace bftEngine::bcst::impl {

/////////////////////////////////////////// HashVal ////////////////////////////////////////////////
RangeValidationTree::HashMaxValues::HashMaxValues() {
  std::string maxHashVal("0x");
  std::string maxHashValSuffix(HashVal::kNodeHashSizeBytes, 'F');
  maxHashVal += maxHashValSuffix;
  kNodeHashMaxValue_ =
      HashVal_t(reinterpret_cast<unsigned char*>(const_cast<char*>(maxHashVal.c_str())), maxHashVal.size());

  std::string nodeHashModulo("0x1");
  std::string nodeHashModuloSuffix(HashVal::kNodeHashSizeBytes, '0');
  nodeHashModulo += nodeHashModuloSuffix;
  kNodeHashModulo_ =
      HashVal_t(reinterpret_cast<unsigned char*>(const_cast<char*>(nodeHashModulo.c_str())), nodeHashModulo.size());
}

RangeValidationTree::HashVal::HashVal(shared_ptr<char[]> val) {
  hash_max_val_ = HashMaxValuesSingleton::getInstance();
  hash_val_ = HashVal_t(reinterpret_cast<unsigned char*>(val.get()), kNodeHashSizeBytes);
}

RangeValidationTree::HashVal::HashVal(char* val, size_t size) {
  hash_max_val_ = HashMaxValuesSingleton::getInstance();
  hash_val_ = HashVal_t(reinterpret_cast<unsigned char*>(val), size);
}

RangeValidationTree::HashVal::HashVal(HashVal_t* val) {
  hash_max_val_ = HashMaxValuesSingleton::getInstance();
  hash_val_ = *val;
}
RangeValidationTree::HashVal& RangeValidationTree::HashVal::operator=(const HashVal& other) {
  hash_val_ = other.hash_val_;
  hash_max_val_ = HashMaxValuesSingleton::getInstance();
  return *this;
}
bool RangeValidationTree::HashVal::operator==(HashVal& other) const { return hash_val_ == other.hash_val_; }
RangeValidationTree::HashVal& RangeValidationTree::HashVal::operator+=(HashVal& other) {
  hash_val_ = ((hash_val_ + other.hash_val_) % hash_max_val_.kNodeHashModulo_);
  return *this;
}
RangeValidationTree::HashVal& RangeValidationTree::HashVal::operator-=(HashVal& other) {
  hash_val_ = ((hash_val_ - other.hash_val_) % hash_max_val_.kNodeHashModulo_);
  return *this;
}

// Used only to print
std::string RangeValidationTree::HashVal::valToString() {
  std::ostringstream oss;
  oss << std::hex << hash_val_;
  return oss.str();
}

std::string RangeValidationTree::HashVal::getDecodedHashVal() const {
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

shared_ptr<char[]> RangeValidationTree::RVBNode::computeNodeHash(NodeId& node_id, const STDigest& digest) {
  ConcordAssertGT(node_id.getVal(), 0);
  DigestContext c;
  auto nid = std::to_string(node_id.getVal());
  c.update(nid.c_str(), nid.size());
  c.update(digest.get(), BLOCK_DIGEST_SIZE);
  // TODO Use default_delete in case memleak is reported by ASAN
  std::shared_ptr<char[]> outDigest(new char[BLOCK_DIGEST_SIZE]);
  c.writeDigest(outDigest.get());
  return outDigest;
}

/////////////////////////////// Start of RVT node methods /////////////////////////////////////

RangeValidationTree::RVTNode::RVTNode(uint32_t RVT_K, shared_ptr<RVBNode>& node)
    : RangeValidationTree::RVBNode(kDefaultRVTLeafLevel, node->id.rvb_index, getZeroedDigest()) {
  n_child++;
  // Would be used during pruning
  min_child_id = RangeValidationTree::NodeId(kDefaultRVBLeafLevel, node->id.rvb_index).getVal();
  max_child_id = RangeValidationTree::NodeId(kDefaultRVBLeafLevel, node->id.rvb_index + RVT_K - 1).getVal();
}

RangeValidationTree::RVTNode::RVTNode(uint32_t RVT_K, std::shared_ptr<RVTNode>& node)
    : RangeValidationTree::RVBNode(node->id.level + 1, node->id.rvb_index, getZeroedDigest()) {
  n_child++;
  auto level = node->id.level;
  min_child_id = RangeValidationTree::NodeId(level, node->id.rvb_index).getVal();
  max_child_id =
      RangeValidationTree::NodeId(level, node->id.rvb_index + pow(RVT_K, level + 1) - pow(RVT_K, level)).getVal();
  ConcordAssert(n_child <= RVT_K);
}

RangeValidationTree::RVTNode::RVTNode(SerializedRVTNode& node, char* hash_val, size_t hash_size)
    : RangeValidationTree::RVBNode(node.id, hash_val, hash_size) {
  n_child = node.n_child;
  min_child_id = node.min_child_id;
  max_child_id = node.max_child_id;
  parent_id = node.parent_id;
}

std::ostringstream RangeValidationTree::RVTNode::serialize() {
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

shared_ptr<RangeValidationTree::RVTNode> RangeValidationTree::RVTNode::deserialize(std::istringstream& is) {
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
    : openRVTNodeForInsertion_(NodeId::kRVTMaxLevels, nullptr),
      openRVTNodeForRemoval_(NodeId::kRVTMaxLevels, nullptr),
      logger_(logger),
      RVT_K(RVT_K),
      fetch_range_size_(fetch_range_size),
      hash_size_(hash_size) {
#ifdef USE_LOG4CPP
  log4cplus::LogLevel logLevel = log4cplus::TRACE_LOG_LEVEL;
#else
  logging::LogLevel logLevel = logging::LogLevel::trace;
#endif
  logging::Logger::getInstance("concord.bft.st.rvt").setLogLevel(logLevel);
}

bool RangeValidationTree::valid(const uint64_t rvb_id, const STDigest& digest) {
  if (rvb_id == 0 or digest.isZero() or fetch_range_size_ == 0 or rvb_id % fetch_range_size_ != 0) {
    LOG_ERROR(logger_, "invalid input data" << KVLOG(rvb_id, digest, fetch_range_size_));
    return false;
  }
  return true;
}

void RangeValidationTree::addRVBNode(shared_ptr<RVBNode>& rvb_node) {
  auto node = openRVTNodeForInsertion_[RVTNode::kDefaultRVTLeafLevel];
  if (!node) {
    // adding first rvb node but it might not be first child (reason: pruning)
    node = make_shared<RVTNode>(RVT_K, rvb_node);
    openRVTNodeForInsertion_[RVTNode::kDefaultRVTLeafLevel] = node;
    id_to_node_map_.emplace(node->id.getVal(), node);
    if (!root_) {
      root_ = node;
      return;
    }
    addInternalNode(node, rvb_node->id.rvb_index);
  } else {
    if (rvb_node->isLastChild(RVT_K)) {
      openRVTNodeForInsertion_[RVTNode::kDefaultRVTLeafLevel] = nullptr;
    }
    ConcordAssertLE(++node->n_child, RVT_K);
    node->addRVBNodeHash(rvb_node);
    addHashValToInternalNodes(node, rvb_node);
  }
}

void RangeValidationTree::removeRVBNode(shared_ptr<RVBNode>& rvb_node) {
  auto node = openRVTNodeForRemoval_[RVTNode::kDefaultRVTLeafLevel];
  if (!node) {
    ConcordAssert(rvb_node->isFirstChild(RVT_K) == true);
    node = make_shared<RVTNode>(RVT_K, rvb_node);
    auto id = NodeId(RVTNode::kDefaultRVTLeafLevel, rvb_node->id.rvb_index).getVal();
    auto itr = id_to_node_map_.find(id);
    ConcordAssert(itr != id_to_node_map_.end());
    node = itr->second;
  }
  if ((--(node->n_child) == 0) or (rvb_node->isLastChild(RVT_K))) {
    ConcordAssertGE(node->n_child, 0);
    openRVTNodeForRemoval_[RVTNode::kDefaultRVTLeafLevel] = nullptr;
    auto id = node->id.getVal();
    auto itr = id_to_node_map_.find(id);
    ConcordAssert(itr != id_to_node_map_.end());
    id_to_node_map_.erase(itr);
    // TODO
    // ConcordAssertEQ(node->min_child_id, node->max_child_id);
  } else {
    openRVTNodeForRemoval_[RVTNode::kDefaultRVTLeafLevel] = node;
    ConcordAssertLE(++node->min_child_id, node->max_child_id);
  }
  node->removeRVBNodeHash(rvb_node);
  removeHashValFromInternalNodes(node, rvb_node);
}

void RangeValidationTree::addHashValToInternalNodes(shared_ptr<RVTNode>& node, shared_ptr<RVBNode>& rvb_node) {
  if (!root_ || !node || !rvb_node) {
    return;
  }
  while (node != root_) {
    auto itr = id_to_node_map_.find(node->parent_id);
    ConcordAssert(itr != id_to_node_map_.end());
    auto& parent_node = itr->second;
    // Formula to decide on when to close any open parent node at any given level.
    if (rvb_node->id.rvb_index == pow(RVT_K, parent_node->id.level) + parent_node->id.rvb_index - 1) {
      openRVTNodeForInsertion_[parent_node->id.level] = nullptr;
    }
    parent_node->addRVTNodeHash(node);
    node = parent_node;
  }
}

void RangeValidationTree::removeHashValFromInternalNodes(shared_ptr<RVTNode>& node, shared_ptr<RVBNode>& rvb_node) {
  if (!root_ || !node || !rvb_node) {
    return;
  }
  while (node != root_) {
    auto itr = id_to_node_map_.find(node->parent_id);
    ConcordAssert(itr != id_to_node_map_.end());
    auto& parent_node = itr->second;
    if (node->n_child == 0) {
      parent_node->n_child--;
    }
    if (parent_node->n_child == 0 or parent_node->max_child_id == node->id.getVal()) {
      openRVTNodeForRemoval_[parent_node->id.level] = nullptr;
      id_to_node_map_.erase(parent_node->id.getVal());
    } else {
      if (parent_node->min_child_id == node->id.getVal()) {
        openRVTNodeForRemoval_[parent_node->id.level] = parent_node;
      }
      parent_node->removeRVTNodeHash(node);
    }
    node = parent_node;
  }
  if (node == root_ and id_to_node_map_.size() == 0) {
    root_ = nullptr;
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

void RangeValidationTree::addInternalNode(shared_ptr<RVTNode>& node, uint64_t rvb_index) {
  ConcordAssert(root_ != node);
  while (node->id.level != root_->id.level) {
    shared_ptr<RVTNode>& pnode = openRVTNodeForInsertion_[node->id.level + 1];
    if (pnode) {
      node->parent_id = pnode->id.getVal();
      // child gets set to new value
      auto max_possible_child_rvb_index = pow(RVT_K, node->id.level);
      if (rvb_index % static_cast<uint64_t>(max_possible_child_rvb_index) == 1) {
        pnode->n_child++;
        ConcordAssert(pnode->n_child <= RVT_K);
      }
      if (rvb_index == pow(RVT_K, pnode->id.level) + pnode->id.rvb_index - 1) {
        openRVTNodeForInsertion_[pnode->id.level] = nullptr;
      }
    } else {
      // construct new internal RVT node
      ConcordAssert(node->isFirstChild(RVT_K) == true);
      pnode = make_shared<RVTNode>(RVT_K, node);
      ConcordAssert(id_to_node_map_.insert({pnode->id.getVal(), pnode}).second == true);
      openRVTNodeForInsertion_[pnode->id.level] = pnode;
      node->parent_id = pnode->id.getVal();
    }
    pnode->addRVTNodeHash(node);
    node = pnode;
  }

  // no need to create new root as we have reached to root while updating hash
  if (node->id.getVal() == root_->id.getVal()) {
    return;
  }

  // create new root
  auto new_root = make_shared<RVTNode>(RVT_K, root_);
  ConcordAssert(id_to_node_map_.insert({new_root->id.getVal(), new_root}).second == true);
  openRVTNodeForInsertion_[new_root->id.level] = new_root;
  root_->parent_id = new_root->id.getVal();
  node->parent_id = new_root->id.getVal();  // special case
  // TODO: how to recover from crash at this point?
  new_root->addRVTNodeHash(root_);
  new_root->addRVTNodeHash(node);
  new_root->n_child++;  // special case
  ConcordAssert(new_root->n_child <= RVT_K);
  root_ = new_root;
}

void RangeValidationTree::printToLog(bool only_node_id) const noexcept {
  if (!root_ or id_to_node_map_.size() == 0) {
    LOG_INFO(logger_, "Empty RVT");
    return;
  }
  std::ostringstream oss;
  oss << " #Levels=" << root_->id.level;
  oss << " #Nodes=" << id_to_node_map_.size();
  oss << " Structure: ";
  queue<shared_ptr<RVTNode>> q;
  q.push(root_);
  while (q.size()) {
    auto& node = q.front();
    q.pop();
    auto level = node->id.level;
    auto rvb_index = node->id.rvb_index;

    auto getPrettyNode = [&]() {
      std::ostringstream os;
      os << " [" << level << "," << rvb_index << "]";
      return os;
    };
    if (only_node_id) {
      oss << getPrettyNode().str() << " ";
    } else {
      oss << getPrettyNode().str() << " = " << node->hash_val.valToString() << " ";
    }

    if (level == 1) continue;

    uint16_t count = 0;
    uint64_t id = node->min_child_id;
    // intentional redundant check using n_child & map.find
    // child count is required as right side substree may not be complete tree
    while (count++ < node->n_child) {
      auto iter = id_to_node_map_.find(id);
      ConcordAssert(iter != id_to_node_map_.end());
      auto& child_node = iter->second;
      q.push(child_node);
      auto level = child_node->id.level;
      // Formula to find id of next sibling
      id = NodeId(level, (child_node->id.rvb_index + pow(RVT_K, level))).getVal();
    }
  }
  LOG_INFO(logger_, oss.str());
}

/////////////////////////////////// start of API //////////////////////////////////////////////

void RangeValidationTree::addNode(const RVBId rvb_id, const STDigest& digest) {
  // LOG_TRACE(logger_, KVLOG(rvb_id, digest.toString()));
  if (not valid(rvb_id, digest)) {
    throw std::invalid_argument("invalid input data");
  }
  auto rvb_index = rvb_id / fetch_range_size_;
  auto node = make_shared<RVBNode>(rvb_index, digest);
  // ConcordAssertOR(
  //     (last_added_node_id_.rvb_index == rvb_index),
  //     (last_added_node_id_.rvb_index + 1 == rvb_index));
  if (last_added_node_id_.rvb_index > 0) {
    ConcordAssertEQ(last_added_node_id_.rvb_index + 1, rvb_index);
  }
  addRVBNode(node);
  last_added_node_id_ = node->id;
  // LOG_TRACE(logger_, KVLOG(rvb_id, last_added_node_id_.getVal()));
}

void RangeValidationTree::removeNode(const RVBId rvb_id, const STDigest& digest) {
  // LOG_TRACE(logger_, KVLOG(rvb_id, digest.toString()));
  if (not valid(rvb_id, digest)) {
    throw std::invalid_argument("invalid input data");
  }
  auto rvb_index = rvb_id / fetch_range_size_;
  auto node = make_shared<RVBNode>(rvb_index, digest);
  // ConcordAssertOR(
  //     (last_removed_node_id_.rvb_index == rvb_index),
  //     (last_removed_node_id_.rvb_index + 1 == rvb_index));
  ConcordAssertEQ(last_removed_node_id_.rvb_index + 1, rvb_index);
  removeRVBNode(node);
  last_removed_node_id_ = node->id;
  // LOG_TRACE(logger_, KVLOG(rvb_id, last_removed_node_id_.getVal()));
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

  Serializable::serialize(os, id_to_node_map_.size());
  for (auto& itr : id_to_node_map_) {
    Serializable::serialize(os, itr.first);
    auto serialized_node = itr.second->serialize();
    Serializable::serialize(os, serialized_node.str().data(), serialized_node.str().size());
  }

  uint64_t null_node_id = 0;
  auto max_levels = root_->id.level;
  for (uint64_t i = 0; i <= max_levels; i++) {
    auto node = openRVTNodeForInsertion_[i];
    if (!node) {
      Serializable::serialize(os, null_node_id);
    } else {
      Serializable::serialize(os, node->id.getVal());
    }
  }
  for (uint64_t i = 0; i <= max_levels; i++) {
    auto node = openRVTNodeForRemoval_[i];
    if (!node) {
      Serializable::serialize(os, null_node_id);
    } else {
      Serializable::serialize(os, node->id.getVal());
    }
  }
  LOG_TRACE(logger_, KVLOG(os.str().size()));
  LOG_TRACE(logger_,
            "Serialized tree with " << id_to_node_map_.size() << " nodes having root as " << root_->id.getVal());
  return os;
}

bool RangeValidationTree::setSerializedRvbData(std::istringstream& is) {
  LOG_TRACE(logger_, is.str().size());
  ConcordAssertGT(is.str().size(), 0);
  id_to_node_map_.clear();
  openRVTNodeForInsertion_.clear();
  openRVTNodeForRemoval_.clear();
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
    auto node = RVTNode::deserialize(is);
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
    last_added_node_id_ = NodeId(0, max_rvb_index);
    last_removed_node_id_ = NodeId(0, min_rvb_index - 1);
  }

  root_ = id_to_node_map_[data.root_node_id];

  // populate openRVTNodeForInsertion_
  uint64_t node_id;
  uint64_t null_node_id = 0;
  auto max_levels = root_->id.level;
  for (size_t i = 0; i <= max_levels; i++) {
    Serializable::deserialize(is, node_id);
    if (node_id == null_node_id) {
      openRVTNodeForInsertion_.push_back(nullptr);
    } else {
      openRVTNodeForInsertion_.push_back(id_to_node_map_[node_id]);
    }
  }

  // populate openRVTNodeForRemoval_
  for (size_t i = 0; i <= max_levels; i++) {
    Serializable::deserialize(is, node_id);
    if (node_id == null_node_id) {
      openRVTNodeForRemoval_.push_back(nullptr);
    } else {
      openRVTNodeForRemoval_.push_back(id_to_node_map_[node_id]);
    }
  }

  LOG_TRACE(logger_, "Created tree with " << id_to_node_map_.size() << " nodes having root as " << root_->id.getVal());
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

  for (size_t rvb_index{iter->second->min_child_id}; rvb_index < iter->second->min_child_id + iter->second->n_child;
       ++rvb_index) {
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
  // TODO should valToString be used instead?
  return hash_val;
}

// TODO Should last pruned node id be serialized?
RVBId RangeValidationTree::getMinRvbId() const {
  if (last_removed_node_id_.getVal() == 0) {
    ConcordAssertEQ(id_to_node_map_.size(), 0);
    ConcordAssertEQ(root_, nullptr);
    return 0;
  }
  return last_removed_node_id_.rvb_index * fetch_range_size_;
}

// TODO Should last added node id be serialized?
RVBId RangeValidationTree::getMaxRvbId() const {
  if (last_added_node_id_.getVal() == 0) {
    ConcordAssertEQ(id_to_node_map_.size(), 0);
    ConcordAssertEQ(root_, nullptr);
    return 0;
  }
  return last_added_node_id_.rvb_index * fetch_range_size_;
}

//////////////////////////////////// End of API ////////////////////////////////////////////////

}  // namespace bftEngine::bcst::impl
