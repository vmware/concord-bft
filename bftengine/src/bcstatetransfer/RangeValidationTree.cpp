// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include <queue>
#include <algorithm>

#include "RangeValidationTree.hpp"

using namespace std;
using namespace concord::kvbc;
using namespace concord::serialize;

namespace bftEngine::bcst::impl {

using NodeVal = RangeValidationTree::NodeVal;
using NodeVal_t = CryptoPP::Integer;
using RVTNode = RangeValidationTree::RVTNode;
using RVBNode = RangeValidationTree::RVBNode;
using NodeInfo = RangeValidationTree::NodeInfo;

/////////////////////////////////////////// NodeVal Operations ////////////////////////////////////////////////

NodeVal_t NodeVal::calcMaxValue(size_t val_size) {
  val_size = (1ULL << (val_size * 8ULL)) - 1ULL;
  return NodeVal_t(val_size);
}

NodeVal_t NodeVal::calcModulo(size_t val_size) {
  val_size = 1ULL << (val_size * 8ULL);
  return NodeVal_t(val_size);
}

NodeVal_t NodeVal::kNodeValueMax_ = 0;
NodeVal_t NodeVal::kNodeValueModulo_ = 0;

NodeVal::NodeVal(const shared_ptr<char[]>&& val, size_t size) {
  val_ = NodeVal_t(reinterpret_cast<unsigned char*>(val.get()), size) % kNodeValueModulo_;
}

NodeVal::NodeVal(const char* val, size_t size) {
  val_ = NodeVal_t(reinterpret_cast<const unsigned char*>(val), size) % kNodeValueModulo_;
}

NodeVal::NodeVal(const NodeVal_t* val) { val_ = (*val) % kNodeValueModulo_; }

NodeVal& NodeVal::operator+=(const NodeVal& other) {
  val_ = (val_ + other.val_) % kNodeValueModulo_;
  return *this;
}

NodeVal& NodeVal::operator-=(const NodeVal& other) {
  val_ = ((val_ - other.val_) % kNodeValueModulo_);
  return *this;
}

// Used only to print
std::string NodeVal::toString() const noexcept {
  std::ostringstream oss;
  oss << std::dec << val_;
  // oss << std::hex << val_;
  return oss.str();
}

std::string NodeVal::getDecoded() const noexcept {
  // Encode to string does not work
  vector<char> enc_input(val_.MinEncodedSize());
  val_.Encode(reinterpret_cast<CryptoPP::byte*>(enc_input.data()), enc_input.size());
  ostringstream oss_en;
  for (auto& c : enc_input) {
    oss_en << c;
  }
  return oss_en.str();
}
//////////////////////////////// End of NodeVal operations ///////////////////////////////////////

std::string NodeInfo::toString() const noexcept {
  std::ostringstream os;
  os << " [" << level << "," << rvb_index << "]";
  return os.str();
}

uint32_t RVBNode::RVT_K{};

const shared_ptr<char[]> RVBNode::computeNodeInitialValue(NodeInfo& node_info, const STDigest& digest) {
  ConcordAssertGT(node_info.id, 0);
  DigestContext c;

  c.update(reinterpret_cast<const char*>(&node_info.id), sizeof(node_info.id));
  c.update(digest.get(), sizeof(STDigest));
  // TODO - Use default_delete in case memleak is reported by ASAN
  std::shared_ptr<char[]> outDigest(new char[sizeof(STDigest)]);
  c.writeDigest(outDigest.get());
  return outDigest;
}

void RangeValidationTree::RVTMetadata::staticAssert() noexcept {
  static_assert(sizeof(RVTMetadata::magic_num) == sizeof(RangeValidationTree::magic_num_));
  static_assert(sizeof(RVTMetadata::version_num) == sizeof(RangeValidationTree::version_num_));
  static_assert(sizeof(RVTMetadata::RVT_K) == sizeof(RangeValidationTree::RVT_K));
  static_assert(sizeof(RVTMetadata::fetch_range_size) == sizeof(RangeValidationTree::fetch_range_size_));
  static_assert(sizeof(RVTMetadata::value_size) == sizeof(RangeValidationTree::value_size_));
}

void RangeValidationTree::SerializedRVTNode::staticAssert() noexcept {
  static_assert(sizeof(SerializedRVTNode::id) == sizeof(NodeInfo::id));
  static_assert(sizeof(SerializedRVTNode::value_size) == sizeof(RangeValidationTree::value_size_));
  static_assert(sizeof(SerializedRVTNode::n_child) == sizeof(RVTNode::n_child));
  static_assert(sizeof(SerializedRVTNode::min_child_id) == sizeof(RVTNode::min_child_id));
  static_assert(sizeof(SerializedRVTNode::max_child_id) == sizeof(RVTNode::max_child_id));
  static_assert(sizeof(SerializedRVTNode::parent_id) == sizeof(RVTNode::parent_id));
}

void RVBNode::logInfoVal(std::string prefix) {
  // uncomment to debug
  ostringstream oss;
  oss << prefix << info_.toString() << " " << nvalue_.toString();
  //LOG_ERROR(GL, oss.str());
}

RVBNode::RVBNode(uint64_t rvb_index, const STDigest& digest)
    : info_(kDefaultRVBLeafLevel, rvb_index), nvalue_(computeNodeInitialValue(info_, digest), sizeof(digest)) {
  // leave for debug
  logInfoVal("construct: ");
}

RVBNode::RVBNode(uint8_t level, uint64_t rvb_index)
    : info_(level, rvb_index), nvalue_(computeNodeInitialValue(info_, STDigest{}), sizeof(STDigest)) {
  // leave for debug
  logInfoVal("construct: ");
}

RVBNode::RVBNode(uint64_t node_id, char* val, size_t size) : info_(node_id), nvalue_(val, size) {
  // leave for debug
  logInfoVal("construct: ");
}

RVTNode::RVTNode(shared_ptr<RVBNode>& node)
    : RVBNode(kDefaultRVTLeafLevel, node->info_.rvb_index),
      min_child_id{node->info_.rvb_index},
      max_child_id{node->info_.rvb_index + RVT_K - 1} {
  n_child++;
}

RVTNode::RVTNode(std::shared_ptr<RVTNode>& node) : RVBNode(node->info_.level + 1, node->info_.rvb_index) {
  n_child++;
  auto level = node->info_.level;
  min_child_id = NodeInfo(level, node->info_.rvb_index).id;
  uint64_t rvb_index = node->info_.rvb_index + RangeValidationTree::pow_int(RVT_K, level + 1) -
                       RangeValidationTree::pow_int(RVT_K, level);
  max_child_id = NodeInfo(level, rvb_index).id;
  ConcordAssert(n_child <= RVT_K);
}

RVTNode::RVTNode(SerializedRVTNode& node, char* val, size_t value_size)
    : RVBNode(node.id, val, value_size),
      n_child{node.n_child},
      min_child_id{node.min_child_id},
      max_child_id{node.max_child_id},
      parent_id{node.parent_id} {}

void RVTNode::addValue(const NodeVal& nvalue) {
  logInfoVal("[before add: ");  // leave for debug
  this->nvalue_ += nvalue;
  // leave for debugstart
  //LOG_ERROR(GL, "Adding value " << nvalue.toString());
  logInfoVal("after add]");
}

void RVTNode::substructValue(const NodeVal& nvalue) {
  logInfoVal();  // leave for debug
  this->nvalue_ -= nvalue;
  // leave for debug
  //LOG_ERROR(GL, "Substructing value " << nvalue.toString());
  logInfoVal();
}

uint64_t RVTNode::getRightSiblingId() const noexcept {
  auto level = info_.level;
  auto rvb_index = info_.rvb_index;
  // Formula to find id of next sibling
  return NodeInfo(level, (rvb_index + RangeValidationTree::pow_int(RVT_K, level))).id;
}

std::ostringstream RVTNode::serialize() const {
  std::ostringstream os;
  Serializable::serialize(os, n_child);
  Serializable::serialize(os, min_child_id);
  Serializable::serialize(os, max_child_id);
  Serializable::serialize(os, parent_id);
  Serializable::serialize(os, nvalue_.getSize());
  auto decoded_val = nvalue_.getDecoded();
  Serializable::serialize(os, decoded_val.data(), nvalue_.getSize());
  return os;
}

shared_ptr<RVTNode> RVTNode::createFromSerialized(std::istringstream& is) {
  SerializedRVTNode snode;
  Serializable::deserialize(is, snode.id);
  Serializable::deserialize(is, snode.n_child);
  Serializable::deserialize(is, snode.min_child_id);
  Serializable::deserialize(is, snode.max_child_id);
  Serializable::deserialize(is, snode.parent_id);
  Serializable::deserialize(is, snode.value_size);
  std::unique_ptr<char[]> ptr = std::make_unique<char[]>(snode.value_size);
  Serializable::deserialize(is, ptr.get(), snode.value_size);
  return std::make_shared<RVTNode>(snode, ptr.get(), snode.value_size);
}

RangeValidationTree::RangeValidationTree(const logging::Logger& logger,
                                         uint32_t RVT_K,
                                         uint32_t fetch_range_size,
                                         size_t value_size)
    : rightmostRVTNode_{},
      leftmostRVTNode_{},
      logger_(logger),
      RVT_K(RVT_K),
      fetch_range_size_(fetch_range_size),
      // value_size_(value_size) {
      value_size_(1) {  // temporary
  NodeVal::kNodeValueMax_ = NodeVal::calcMaxValue(value_size_);
  NodeVal::kNodeValueModulo_ = NodeVal::calcModulo(value_size_);
  ConcordAssertLE(RVT_K, static_cast<uint64_t>(1ULL << (sizeof(RVTNode::n_child) * 8ULL)));
  RVTMetadata::staticAssert();
  SerializedRVTNode::staticAssert();
  RVBNode::RVT_K = RVT_K;
}

uint64_t RangeValidationTree::pow_int(uint64_t base, uint64_t exp) noexcept {
  ConcordAssertOR(base != 0, exp != 0);  // both zero are undefined
  uint64_t res{base};
  if (base == 0) {
    return 1;
  }
  if (exp == 0) {
    return 1;
  }
  for (size_t i{1}; i < exp; ++i) {
    res *= base;
  }
  return res;
}

bool RangeValidationTree::validate() const noexcept {
  if (not root_) {
    return true;
  }
  auto validateValues = [&](vector<RangeValidationTree::NodeVal> child_values, string parent_val) -> bool {
    auto first_val = child_values[0].getDecoded();
    NodeVal child_values_sum(first_val.c_str(), first_val.size());
    LOG_INFO(logger_, "input parent value " << parent_val);
    LOG_INFO(logger_, "1st child value " << child_values_sum.toString());
    for (size_t i = 1; i < child_values.size(); i++) {
      child_values_sum += child_values[i];
      LOG_INFO(logger_, "added child value " << child_values[i].toString());
      LOG_INFO(logger_, KVLOG(i, child_values_sum.toString()));
    }
    LOG_INFO(logger_, "calculatd parent value " << child_values_sum.toString());
    return (parent_val == child_values_sum.toString());
  };

  // step 1: validate value of parent is matching with sum of all childs
  queue<shared_ptr<RVTNode>> q;
  q.push(root_);
  string parent_val = root_->nvalue_.toString();

  while (q.size()) {
    auto total_nodes = q.size();
    vector<RangeValidationTree::NodeVal> child_values;
    for (size_t cnt = 0; cnt < total_nodes; cnt++) {
      auto& node = q.front();
      q.pop();

      if (node->info_.level == 1) {
        continue;
      }

      uint64_t id = node->min_child_id;
      for (uint16_t count = 0; count < node->n_child; count++) {
        auto iter = id_to_node_.find(id);
        ConcordAssert(iter != id_to_node_.end());
        auto& child_node = iter->second;
        child_values.push_back(child_node->nvalue_);
        q.push(child_node);
        id = child_node->getRightSiblingId();
      }
    }
    if (child_values.empty()) {
      continue;
    }
    if (not validateValues(child_values, parent_val)) {
      LOG_ERROR(logger_, "validation failed");
      return false;
    }
  }
  LOG_INFO(
      logger_,
      "validation succeeded for " << root_->info_.toString() << " " << root_->nvalue_.toString() << " " << parent_val);

  return true;
}

void RangeValidationTree::printToLog(bool only_node_id) const noexcept {
  if (!root_ or totalNodes() == 0) {
    LOG_INFO(logger_, "Empty RVT");
    return;
  }
  if (totalNodes() > kMaxNodesToPrint) {
    LOG_WARN(logger_, "Huge tree so would not log");
    return;
  }
  std::ostringstream oss;
  oss << " #Levels=" << root_->info_.level;
  oss << " #Nodes=" << totalNodes();
  oss << " Structure: ";
  queue<shared_ptr<RVTNode>> q;
  q.push(root_);
  while (q.size()) {
    auto& node = q.front();
    q.pop();
    oss << node->info_.toString() << " ";
    if (not only_node_id) {
      oss << " = " << node->nvalue_.toString() << " ";
    }
    if (node->info_.level == 1) {
      continue;
    }

    uint16_t count = 0;
    uint64_t id = node->min_child_id;
    while (count++ < node->n_child) {
      auto iter = id_to_node_.find(id);
      ConcordAssert(iter != id_to_node_.end());
      auto& child_node = iter->second;
      q.push(child_node);
      id = child_node->getRightSiblingId();
    }
  }
  LOG_INFO(logger_, oss.str());
}

bool RangeValidationTree::isValidRvbId(const RVBId& block_id) const noexcept {
  return ((block_id != 0) && (fetch_range_size_ != 0) && (block_id % fetch_range_size_ == 0));
}

bool RangeValidationTree::validateRvbId(const uint64_t rvb_id, const STDigest& digest) const {
  if ((!isValidRvbId(rvb_id)) || digest.isZero()) {
    LOG_ERROR(logger_, "invalid input data" << KVLOG(rvb_id, digest, fetch_range_size_));
    return false;
  }
  return true;
}

bool RangeValidationTree::validateRVBGroupId(const RVBGroupId rvb_group_id) const {
  NodeInfo nid(rvb_group_id);
  if (rvb_group_id == 0) {
    return false;
  }
  return ((nid.level == RVTNode::kDefaultRVTLeafLevel) && ((nid.rvb_index % RVT_K) == 1));
}

shared_ptr<RVTNode> RangeValidationTree::getRVTNodeOfLeftSibling(shared_ptr<RVTNode>& node) const {
  if (leftmostRVTNode_[node->info_.level] == node) {
    return nullptr;
  }
  auto id =
      NodeInfo(node->info_.level, node->info_.rvb_index - RangeValidationTree::pow_int(RVT_K, node->info_.level)).id;
  auto iter = id_to_node_.find(id);
  return (iter == id_to_node_.end()) ? nullptr : iter->second;
}

shared_ptr<RVTNode> RangeValidationTree::getRVTNodeOfRightSibling(shared_ptr<RVTNode>& node) const {
  if (rightmostRVTNode_[node->info_.level] == node) {
    return nullptr;
  }
  auto id =
      NodeInfo(node->info_.level, node->info_.rvb_index + RangeValidationTree::pow_int(RVT_K, node->info_.level)).id;
  auto iter = id_to_node_.find(id);
  return (iter == id_to_node_.end()) ? nullptr : iter->second;
}

shared_ptr<RVTNode> RangeValidationTree::getParentNode(std::shared_ptr<RVTNode>& node) const noexcept {
  auto itr = id_to_node_.find(node->parent_id);
  ConcordAssert(itr != id_to_node_.end());
  return itr->second;
}

void RangeValidationTree::addRVBNode(shared_ptr<RVBNode>& rvb_node) {
  auto parent_node = openForInsertion(RVTNode::kDefaultRVTLeafLevel);
  if (!parent_node) {
    // adding first rvb parent_node but it might not be first child (reason: pruning)
    parent_node = make_shared<RVTNode>(rvb_node);
    rightmostRVTNode_[RVTNode::kDefaultRVTLeafLevel] = parent_node;
    if (!leftmostRVTNode_[RVTNode::kDefaultRVTLeafLevel]) {
      leftmostRVTNode_[RVTNode::kDefaultRVTLeafLevel] = parent_node;
    }
    ConcordAssert(id_to_node_.insert({parent_node->info_.id, parent_node}).second == true);
    if (!root_) {
      setNewRoot(parent_node);
    }
    addInternalNode(parent_node);
  } else {
    ++parent_node->n_child;
    ConcordAssertLE(parent_node->n_child, RVT_K);
    ConcordAssertLE(parent_node->min_child_id + parent_node->n_child - 1, parent_node->max_child_id);
    // parent_node->addValue(rvb_node->nvalue_);
  }
  addValueToInternalNodes(parent_node, rvb_node);
}

void RangeValidationTree::removeRVBNode(shared_ptr<RVBNode>& rvb_node) {
  auto node = openForRemoval(RVTNode::kDefaultRVTLeafLevel);
  ConcordAssert(node != nullptr);

  if (node->n_child == 1) {
    // no more RVB childs, erase the node
    auto id = node->info_.id;
    auto node_to_remove_iter = id_to_node_.find(id);
    ConcordAssert(node_to_remove_iter != id_to_node_.end());
    if ((node == leftmostRVTNode_[RVTNode::kDefaultRVTLeafLevel]) &&
        (node == rightmostRVTNode_[RVTNode::kDefaultRVTLeafLevel])) {
      leftmostRVTNode_[RVTNode::kDefaultRVTLeafLevel] = nullptr;
      rightmostRVTNode_[RVTNode::kDefaultRVTLeafLevel] = nullptr;
    } else {
      if (node == leftmostRVTNode_[RVTNode::kDefaultRVTLeafLevel]) {
        auto id = NodeInfo(RVTNode::kDefaultRVTLeafLevel, rvb_node->info_.rvb_index + 1).id;
        auto iter = id_to_node_.find(id);
        leftmostRVTNode_[RVTNode::kDefaultRVTLeafLevel] = (iter == id_to_node_.end()) ? nullptr : iter->second;
      }
      if (node == rightmostRVTNode_[RVTNode::kDefaultRVTLeafLevel]) {
        rightmostRVTNode_[RVTNode::kDefaultRVTLeafLevel] = nullptr;
      }
    }
    id_to_node_.erase(node_to_remove_iter);
  }

  --node->n_child;
  if (node->n_child > 0) {
    ++node->min_child_id;
  }
  ConcordAssertLE(node->min_child_id + node->n_child - 1, node->max_child_id);
  removeAndUpdateInternalNodes(node, rvb_node);
}

void RangeValidationTree::addValueToInternalNodes(std::shared_ptr<RVTNode>& direct_parent,
                                                  std::shared_ptr<RVBNode>& rvb_node) {
  ConcordAssert(direct_parent != nullptr);
  ConcordAssert(rvb_node != nullptr);

  auto current_node = direct_parent;
  do {
    current_node->addValue(rvb_node->nvalue_);
    if (current_node->parent_id != 0) {
      ConcordAssert(root_ != current_node);
      auto itr = id_to_node_.find(current_node->parent_id);
      ConcordAssert(itr != id_to_node_.end());
      current_node = itr->second;
    } else {
      ConcordAssert(root_ == current_node);
    }
  } while (current_node != root_);
  if (current_node != direct_parent) {
    current_node->addValue(rvb_node->nvalue_);
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
  // ConcordAssert(root_ != node);
  std::shared_ptr<RVTNode> parent_node;
  while (node->info_.level != root_->info_.level) {
    if (node->parent_id == 0) {
      parent_node = openForInsertion(node->info_.level + 1);
      if (parent_node) {
        // Add the node to parent_node which still has more space for childs
        node->parent_id = parent_node->info_.id;
        parent_node->n_child++;
        ConcordAssert(parent_node->n_child <= RVT_K);
      } else {
        // construct new internal RVT node
        ConcordAssert(node->isMinChild() == true);
        parent_node = make_shared<RVTNode>(node);
        ConcordAssert(id_to_node_.insert({parent_node->info_.id, parent_node}).second == true);
        rightmostRVTNode_[parent_node->info_.level] = parent_node;
        node->parent_id = parent_node->info_.id;
      }
    } else {
      auto iter = id_to_node_.find(node->parent_id);
      ConcordAssert(iter != id_to_node_.end());
      parent_node = iter->second;
    }
    // parent_node->addValue(node->nvalue_);
    node = parent_node;
  }

  // no need to create new root as we have reached to root while updating value
  if (node->info_.id == root_->info_.id) {
    return;
  }

  // create new root
  auto new_root = make_shared<RVTNode>(root_);
  ConcordAssert(id_to_node_.insert({new_root->info_.id, new_root}).second == true);
  node->parent_id = new_root->info_.id;
  new_root->n_child++;
  new_root->addValue(root_->nvalue_);
  new_root->addValue(node->nvalue_);
  ConcordAssert(new_root->n_child <= RVT_K);
  setNewRoot(new_root);
}

void RangeValidationTree::removeAndUpdateInternalNodes(shared_ptr<RVTNode>& rvt_node,
                                                       shared_ptr<RVBNode>& rvb_node) {
  ConcordAssert(root_ != nullptr);
  ConcordAssert(rvt_node != nullptr);
  ConcordAssert(rvb_node != nullptr);

  // Loop 1
  // Trim the tree from rvt_node to root
  shared_ptr<RVTNode> cur_node = rvt_node;
  while (cur_node != root_) {
    auto parent_node = getParentNode(cur_node);

    // When rvt_node is removed, we need to update its parent
    if (cur_node->n_child == 0) {
      --parent_node->n_child;
      if (parent_node->n_child > 0) {
        auto level = cur_node->info_.level;
        auto id = NodeInfo(level, cur_node->info_.rvb_index + RangeValidationTree::pow_int(RVT_K, level)).id;
        ConcordAssert(id_to_node_.find(id) != id_to_node_.end());
        parent_node->min_child_id = id;
      }
      if (cur_node != rvt_node) {
        id_to_node_.erase(cur_node->info_.id);
        if ((leftmostRVTNode_[cur_node->info_.level] == cur_node) &&
            (rightmostRVTNode_[cur_node->info_.level] == cur_node)) {
          rightmostRVTNode_[cur_node->info_.level] = nullptr;
          leftmostRVTNode_[cur_node->info_.level] = nullptr;
        } else if (leftmostRVTNode_[cur_node->info_.level] == cur_node) {
          leftmostRVTNode_[cur_node->info_.level] = getRVTNodeOfRightSibling(cur_node);
        }
      }
    }  // Trim the tree from rvt_node to root
    // parent_node->substructValue(cur_node->nvalue_);
    // if (cur_node == rvt_node) {
    //   cur_node->substructValue(rvb_node->nvalue_);
    // }
    // parent_node->addValue(cur_node->nvalue_);
    cur_node->substructValue(rvb_node->nvalue_);
    cur_node = parent_node;
  }

  // Loop 2
  // Shrink the tree from root to bottom (level 1) . In theory, tree can end empty after this loop
  while ((cur_node == root_) && (cur_node->n_child == 1) && (cur_node->info_.level > 1)) {
    id_to_node_.erase(cur_node->info_.id);
    ConcordAssertEQ(rightmostRVTNode_[cur_node->info_.level - 1], leftmostRVTNode_[cur_node->info_.level - 1])
        rightmostRVTNode_[cur_node->info_.level] = nullptr;
    leftmostRVTNode_[cur_node->info_.level] = nullptr;
    cur_node = rightmostRVTNode_[cur_node->info_.level - 1];
    setNewRoot(cur_node);
  }
}

void RangeValidationTree::setNewRoot(shared_ptr<RVTNode> new_root) {
  if (!new_root) {
    // setting root to null
    ConcordAssert(root_->info_.level == 1);
    ConcordAssert(root_ != nullptr);
    reset();
    return;
  }
  if (root_) {
    // replacing roots
    int new_root_level = static_cast<int>(new_root->info_.level);
    int old_root_level = static_cast<int>(root_->info_.level);
    ConcordAssert(new_root_level != 0);
    ConcordAssert(std::abs(new_root_level - old_root_level) == 1);
    if (new_root_level > old_root_level) {
      // replacing the root - tree grows 1 level
      root_->parent_id = new_root->info_.id;
    } else if (new_root_level < old_root_level) {
      // replacing the root - tree shrinks 1 level
      rightmostRVTNode_[root_->info_.level] = nullptr;
      leftmostRVTNode_[root_->info_.level] = nullptr;
    }
  }
  root_ = new_root;
  root_->parent_id = 0;
  rightmostRVTNode_[new_root->info_.level] = new_root;
  leftmostRVTNode_[new_root->info_.level] = new_root;
}

inline std::shared_ptr<RVTNode> RangeValidationTree::openForInsertion(uint64_t level) const {
  if (rightmostRVTNode_[level] == nullptr) {
    return nullptr;
  }
  auto& node = rightmostRVTNode_[level];
  uint64_t min_child_actual_rvb_index = node->min_child_id & NodeInfo::kRvbIndexMask;
  uint64_t max_child_possible_rvb_index = node->max_child_id & NodeInfo::kRvbIndexMask;
  uint64_t max_child_actual_rvb_index =
      min_child_actual_rvb_index + RangeValidationTree::pow_int(RVT_K, node->info_.level - 1) * (node->n_child - 1);
  return (max_child_actual_rvb_index < max_child_possible_rvb_index) ? node : nullptr;
}

inline std::shared_ptr<RVTNode> RangeValidationTree::openForRemoval(uint64_t level) const {
  return leftmostRVTNode_[level];
}

void RangeValidationTree::reset() noexcept {
  // clear() reduced size to 0 and crashed while setting new root using operator []
  for (uint8_t level = 0; level < NodeInfo::kMaxLevels; level++) {
    rightmostRVTNode_[level] = nullptr;
    leftmostRVTNode_[level] = nullptr;
  }
  id_to_node_.clear();
  root_ = nullptr;
  max_rvb_index_ = 0;
  min_rvb_index_ = 0;
}

/////////////////////////////////// start of API //////////////////////////////////////////////

void RangeValidationTree::addNode(const RVBId rvb_id, const STDigest& digest) {
  if (not validateRvbId(rvb_id, digest)) {
    throw std::invalid_argument("invalid input data");
  }
  auto rvb_index = rvb_id / fetch_range_size_;
  //LOG_ERROR(GL, "===start");
  auto node = make_shared<RVBNode>(rvb_index, digest);
  if (max_rvb_index_ > 0) {
    ConcordAssertEQ(max_rvb_index_ + 1, rvb_index);
  }
  addRVBNode(node);
  //LOG_ERROR(GL, "===end");
  max_rvb_index_ = rvb_index;
  if (min_rvb_index_ == 0) {
    min_rvb_index_ = rvb_index;
  }
  LOG_TRACE(logger_, KVLOG(rvb_id, max_rvb_index_));
}

void RangeValidationTree::removeNode(const RVBId rvb_id, const STDigest& digest) {
  ConcordAssert(root_ != nullptr);
  if (not validateRvbId(rvb_id, digest)) {
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
  LOG_TRACE(logger_, KVLOG(rvb_id, min_rvb_index_));
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
  Serializable::serialize(os, value_size_);
  Serializable::serialize(os, root_->info_.id);

  Serializable::serialize(os, totalNodes());
  for (auto& itr : id_to_node_) {
    Serializable::serialize(os, itr.first);
    auto serialized_node = itr.second->serialize();
    Serializable::serialize(os, serialized_node.str().data(), serialized_node.str().size());
  }

  uint64_t null_node_id = 0;
  auto max_levels = root_->info_.level;
  for (uint64_t i = 0; i <= max_levels; i++) {
    auto node = rightmostRVTNode_[i];
    if (!node) {
      Serializable::serialize(os, null_node_id);
    } else {
      Serializable::serialize(os, node->info_.id);
    }
  }
  for (uint64_t i = 0; i <= max_levels; i++) {
    auto node = leftmostRVTNode_[i];
    if (!node) {
      Serializable::serialize(os, null_node_id);
    } else {
      Serializable::serialize(os, node->info_.id);
    }
  }
  LOG_TRACE(logger_, KVLOG(os.str().size()));
  LOG_TRACE(logger_, "Nodes:" << totalNodes() << " root value:" << root_->nvalue_.toString());
  return os;
}

bool RangeValidationTree::setSerializedRvbData(std::istringstream& is) {
  if (!(is.str().size())) {
    LOG_ERROR(logger_, "invalid input");
    return false;
  }

  reset();
  RVTMetadata data;
  Serializable::deserialize(is, data.magic_num);
  Serializable::deserialize(is, data.version_num);
  Serializable::deserialize(is, data.RVT_K);
  Serializable::deserialize(is, data.fetch_range_size);
  Serializable::deserialize(is, data.value_size);
  Serializable::deserialize(is, data.root_node_id);
  if ((data.magic_num != magic_num_) || (data.version_num != version_num_) || (data.RVT_K != RVT_K) ||
      (data.fetch_range_size != fetch_range_size_) || (data.value_size != value_size_)) {
    LOG_ERROR(logger_, "Failed to deserialize metadata");
    reset();
    return false;
  }

  // populate id_to_node_ map
  Serializable::deserialize(is, data.total_nodes);
  id_to_node_.reserve(data.total_nodes);
  uint64_t min_rvb_index{std::numeric_limits<uint64_t>::max()}, max_rvb_index{0};
  for (uint64_t i = 0; i < data.total_nodes; i++) {
    auto node = RVTNode::createFromSerialized(is);
    id_to_node_.emplace(node->info_.id, node);

    if (node->info_.level == 1) {
      // level 0 child IDs can be treated as rvb_indexes
      if (node->min_child_id < min_rvb_index) {
        min_rvb_index = node->min_child_id;
      }
      if ((node->min_child_id + node->n_child - 1) > max_rvb_index) {
        max_rvb_index = (node->min_child_id + node->n_child - 1);
      }
    }
  }
  if (data.total_nodes > 0) {
    ConcordAssertNE(max_rvb_index, 0);
    ConcordAssertNE(min_rvb_index, std::numeric_limits<uint64_t>::max());
    max_rvb_index_ = max_rvb_index;
    min_rvb_index_ = min_rvb_index;
  }

  setNewRoot(id_to_node_[data.root_node_id]);

  // populate rightmostRVTNode_
  uint64_t node_id;
  uint64_t null_node_id = 0;
  auto max_levels = root_->info_.level;
  for (size_t i = 0; i <= max_levels; i++) {
    Serializable::deserialize(is, node_id);
    if (node_id == null_node_id) {
      rightmostRVTNode_[i] = nullptr;
    } else {
      rightmostRVTNode_[i] = id_to_node_[node_id];
    }
  }

  // populate leftmostRVTNode_
  for (size_t i = 0; i <= max_levels; i++) {
    Serializable::deserialize(is, node_id);
    if (node_id == null_node_id) {
      leftmostRVTNode_[i] = nullptr;
    } else {
      leftmostRVTNode_[i] = id_to_node_[node_id];
    }
  }

  is.peek();
  if (not is.eof()) {
    LOG_ERROR(logger_, "Still some data left to read from stream");
    reset();
    return false;
  }

  LOG_TRACE(logger_, "Nodes:" << totalNodes() << " root value:" << root_->nvalue_.toString());
  return true;
}

std::vector<RVBGroupId> RangeValidationTree::getRvbGroupIds(RVBId start_block_id, RVBId end_block_id) const {
  LOG_TRACE(logger_, KVLOG(start_block_id, end_block_id));
  std::vector<RVBGroupId> rvb_group_ids;
  if ((start_block_id == 0) || (end_block_id == 0) || (start_block_id > end_block_id) ||
      (!isValidRvbId(start_block_id)) || (!isValidRvbId(end_block_id))) {
    LOG_ERROR(logger_, "invalid input data" << KVLOG(start_block_id, end_block_id));
    return rvb_group_ids;
  }

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
    RVBGroupId rvb_group_id = NodeInfo(RVTNode::kDefaultRVTLeafLevel, parent_rvb_index).id;
    if (id_to_node_.find(rvb_group_id) == id_to_node_.end()) {
      LOG_WARN(logger_, KVLOG(rvb_group_id, parent_rvb_index, start_block_id, end_block_id));
      return rvb_group_ids;
    }
    if (rvb_group_ids.empty() || (rvb_group_ids.back() != rvb_group_id)) {
      rvb_group_ids.push_back(rvb_group_id);
    }
  }
  LOG_TRACE(logger_, KVLOG(rvb_group_ids.size()));
  return rvb_group_ids;
}

std::vector<RVBId> RangeValidationTree::getRvbIds(RVBGroupId rvb_group_id) const {
  LOG_TRACE(logger_, KVLOG(rvb_group_id));
  std::vector<RVBId> rvb_ids;

  if (!validateRVBGroupId(rvb_group_id)) {
    LOG_ERROR(logger_, KVLOG(rvb_group_id));
    return rvb_ids;
  }
  const auto iter = id_to_node_.find(rvb_group_id);
  if (iter == id_to_node_.end()) {
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

std::string RangeValidationTree::getDirectParentValueStr(RVBId rvb_id) const {
  std::string val;
  if (!isValidRvbId(rvb_id)) {
    return val;
  }
  LOG_TRACE(logger_, KVLOG(rvb_id));
  RVBIndex rvb_index = rvb_id / fetch_range_size_;
  RVBIndex parent_rvb_index;
  if (rvb_index <= RVT_K) {
    parent_rvb_index = 1;
  } else if (rvb_index % RVT_K == 0) {
    parent_rvb_index = rvb_index - RVT_K + 1;
  } else {
    parent_rvb_index = (rvb_index - (rvb_index % RVT_K)) + 1;
  }
  RVBGroupId parent_node_id = NodeInfo(RVTNode::kDefaultRVTLeafLevel, parent_rvb_index).id;
  auto itr = id_to_node_.find(parent_node_id);
  if (itr == id_to_node_.end()) {
    LOG_WARN(logger_, KVLOG(parent_node_id, rvb_index, rvb_id));
    return val;
  }
  val = itr->second->nvalue_.toString();
  LOG_TRACE(logger_, KVLOG(rvb_id, parent_node_id, val));
  return val;
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

//////////////////////////////////// End of API ////////////////////////////////////////////////

}  // namespace bftEngine::bcst::impl
