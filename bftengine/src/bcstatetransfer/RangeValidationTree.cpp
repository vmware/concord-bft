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
#include "STDigest.hpp"

using namespace std;
using namespace concord::serialize;

namespace bftEngine::bcst::impl {

using NodeVal = RangeValidationTree::NodeVal;
using NodeVal_t = CryptoPP::Integer;
using RVTNode = RangeValidationTree::RVTNode;
using RVBNode = RangeValidationTree::RVBNode;
using NodeInfo = RangeValidationTree::NodeInfo;
using RVTNodePtr = RangeValidationTree::RVTNodePtr;

/////////////////////////////////////////// NodeVal Operations ////////////////////////////////////////////////

NodeVal_t NodeVal::calcMaxValue(size_t val_size) {
  NodeVal_t v = NodeVal_t(1);
  v = (v << (val_size * 8ULL)) - NodeVal_t(1);
  return v;
}

NodeVal_t NodeVal::calcModulo(size_t val_size) {
  NodeVal_t v = NodeVal_t(1);
  v = v << (val_size * 8ULL);
  return v;
}

NodeVal_t NodeVal::kNodeValueMax_ = 0;
NodeVal_t NodeVal::kNodeValueModulo_ = 0;

NodeVal::NodeVal(const shared_ptr<char[]>&& val, size_t size) {
  val_ = NodeVal_t(reinterpret_cast<unsigned char*>(val.get()), size) % kNodeValueModulo_;
}

NodeVal::NodeVal(const char* val_ptr, size_t size) {
  val_ = NodeVal_t(reinterpret_cast<const unsigned char*>(val_ptr), size) % kNodeValueModulo_;
}

NodeVal::NodeVal(const NodeVal_t* val) { val_ = (*val) % kNodeValueModulo_; }

NodeVal::NodeVal(const NodeVal_t& val) { val_ = (val) % kNodeValueModulo_; }

NodeVal::NodeVal(const NodeVal_t&& val) { val_ = (val) % kNodeValueModulo_; }

NodeVal::NodeVal() : val_{(int64_t)0} {}

NodeVal& NodeVal::operator+=(const NodeVal& other) {
  val_ = (val_ + other.val_) % kNodeValueModulo_;
  return *this;
}

NodeVal& NodeVal::operator-=(const NodeVal& other) {
  val_ = ((val_ - other.val_) % kNodeValueModulo_);
  return *this;
}

bool NodeVal::operator!=(const NodeVal& other) { return (val_ != other.val_); }

bool NodeVal::operator==(const NodeVal& other) { return (val_ == other.val_); }

// Used only to print
std::string NodeVal::toString() const noexcept {
  std::ostringstream oss;
  // oss << std::dec << val_;
  oss << std::hex << val_;
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

// TODO - for now, the outDigest is of a fixed size. We can match the final size to RangeValidationTree::value_size
// This requires us to write our own DigestContext
const shared_ptr<char[]> RVBNode::computeNodeInitialValue(NodeInfo& node_info, const char* data, size_t data_size) {
  ConcordAssertGT(node_info.id, 0);
  DigestContext c;

  c.update(reinterpret_cast<const char*>(&node_info.id), sizeof(node_info.id));
  c.update(data, data_size);
  // TODO - Use default_delete in case memleak is reported by ASAN
  static std::shared_ptr<char[]> outDigest(new char[NodeVal::kDigestContextOutputSize]);
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
  static_assert(sizeof(SerializedRVTNode::n_child) == sizeof(RVTNode::n_child));
  static_assert(sizeof(SerializedRVTNode::min_child_id) == sizeof(RVTNode::min_child_id));
  static_assert(sizeof(SerializedRVTNode::max_child_id) == sizeof(RVTNode::max_child_id));
  static_assert(sizeof(SerializedRVTNode::parent_id) == sizeof(RVTNode::parent_id));
}

void RVBNode::logInfoVal(const std::string& prefix) {
  // uncomment to debug
  // ostringstream oss;
  // oss << prefix << info_.toString() << " " << current_value_.toString();
  // LOG_ERROR(GL, oss.str());
}

RVBNode::RVBNode(uint64_t rvb_index, const char* data, size_t data_size)
    : info_(kDefaultRVBLeafLevel, rvb_index),
      current_value_(computeNodeInitialValue(info_, data, data_size), NodeVal::kDigestContextOutputSize) {
  // logInfoVal("construct: ");
}

RVBNode::RVBNode(uint8_t level, uint64_t rvb_index)
    : info_(level, rvb_index),
      current_value_(
          computeNodeInitialValue(info_, NodeVal::initialValueZeroData.data(), NodeVal::kDigestContextOutputSize),
          NodeVal::kDigestContextOutputSize) {}

RVBNode::RVBNode(uint64_t node_id, char* val_ptr, size_t size) : info_(node_id), current_value_(val_ptr, size) {}

RVTNode::RVTNode(const shared_ptr<RVBNode>& node)
    : RVBNode(kDefaultRVTLeafLevel, node->info_.rvb_index),
      min_child_id{node->info_.rvb_index},
      max_child_id{node->info_.rvb_index + RVT_K - 1},
      initial_value_(current_value_) {
  n_child++;
}

RVTNode::RVTNode(const RVTNodePtr& node)
    : RVBNode(node->info_.level + 1, node->info_.rvb_index), initial_value_(current_value_) {
  n_child++;
  auto level = node->info_.level;
  min_child_id = node->info_.id;
  uint64_t rvb_index = node->info_.rvb_index + RangeValidationTree::pow_uint(RVT_K, level + 1) -
                       RangeValidationTree::pow_uint(RVT_K, level);
  max_child_id = NodeInfo(level, rvb_index).id;
  ConcordAssert(n_child <= RVT_K);
  // logInfoVal("construct: ");
}

RVTNode::RVTNode(SerializedRVTNode& node, char* cur_val_ptr, size_t cur_value_size)
    : RVBNode(node.id, cur_val_ptr, cur_value_size),
      n_child{node.n_child},
      min_child_id{node.min_child_id},
      max_child_id{node.max_child_id},
      parent_id{node.parent_id},
      initial_value_(
          computeNodeInitialValue(info_, NodeVal::initialValueZeroData.data(), NodeVal::kDigestContextOutputSize),
          NodeVal::kDigestContextOutputSize) {}

void RVTNode::addValue(const NodeVal& nvalue) { this->current_value_ += nvalue; }

void RVTNode::substractValue(const NodeVal& nvalue) { this->current_value_ -= nvalue; }

std::ostringstream RVTNode::serialize() const {
  std::ostringstream os;
  Serializable::serialize(os, n_child);
  Serializable::serialize(os, min_child_id);
  Serializable::serialize(os, max_child_id);
  Serializable::serialize(os, parent_id);

  Serializable::serialize(os, current_value_.getSize());
  auto decoded_val = current_value_.getDecoded();
  Serializable::serialize(os, decoded_val.data(), current_value_.getSize());

  // We do not serialize initial_value_ since it can be easily re-calculated
  return os;
}

RVTNodePtr RVTNode::createFromSerialized(std::istringstream& is) {
  SerializedRVTNode snode;
  Serializable::deserialize(is, snode.id);
  Serializable::deserialize(is, snode.n_child);
  Serializable::deserialize(is, snode.min_child_id);
  Serializable::deserialize(is, snode.max_child_id);
  Serializable::deserialize(is, snode.parent_id);

  Serializable::deserialize(is, snode.current_value_encoded_size);
  std::unique_ptr<char[]> ptr_cur = std::make_unique<char[]>(snode.current_value_encoded_size);
  Serializable::deserialize(is, ptr_cur.get(), snode.current_value_encoded_size);
  return std::make_shared<RVTNode>(snode, ptr_cur.get(), snode.current_value_encoded_size);
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
      value_size_(value_size) {
  NodeVal::kNodeValueMax_ = NodeVal::calcMaxValue(value_size_);
  NodeVal::kNodeValueModulo_ = NodeVal::calcModulo(value_size_);
  ConcordAssertEQ(NodeVal::kNodeValueMax_ + NodeVal_t(1), NodeVal::kNodeValueModulo_);
  ConcordAssert(NodeVal::kNodeValueMax_ != NodeVal_t(static_cast<signed long>(0)));
  ConcordAssert(NodeVal::kNodeValueModulo_ != NodeVal_t(static_cast<signed long>(0)));
  ConcordAssertLE(RVT_K, static_cast<uint64_t>(1ULL << (sizeof(RVTNode::n_child) * 8ULL)));
  RVTMetadata::staticAssert();
  SerializedRVTNode::staticAssert();
  RVBNode::RVT_K = RVT_K;
}

uint64_t RangeValidationTree::pow_uint(uint64_t base, uint64_t exp) noexcept {
  ConcordAssertOR(base != 0, exp != 0);  // both zero are undefined
  uint64_t res{base};
  if (base == 0) {
    return 0;
  }
  if (exp == 0) {
    return 1;
  }
  for (size_t i{1}; i < exp; ++i) {
    res *= base;
  }
  return res;
}

bool RangeValidationTree::validateTreeStructure() const noexcept { return true; }

bool RangeValidationTree::validateTreeValues() const noexcept {
  if (root_ == nullptr && empty()) {
    return true;
  }

  // Currently we do not get any level 0 data from outside, so we cannot validate level 1
  // TODO - add support for level 1 validation
  if (totalLevels() <= RVTNode::kDefaultRVTLeafLevel) {
    return true;
  }

  // Lets start from level 2
  size_t current_level{RVTNode::kDefaultRVTLeafLevel + 1};
  auto current_node = leftmostRVTNode_[current_level];
  ConcordAssert(current_node != nullptr);

  do {
    NodeVal sum_of_childs{};
    auto child_node = getLeftMostChildNode(current_node);
    ConcordAssert(child_node != nullptr);

    for (uint16_t i = 0; i < current_node->n_child; ++i) {
      sum_of_childs += child_node->current_value_;
      child_node = getRVTNodeOfRightSibling(child_node);
      if (i < current_node->n_child - 1) {
        ConcordAssert(child_node != nullptr);
      }
    }

    // Forumula:
    // Every node has an initial value and current value.
    // A correct value for a node is such that:
    // ((current value - initial value) % mod) == ((sum of current values of all childs) % mod)

    NodeVal added_to_current{};
    added_to_current += current_node->current_value_;
    added_to_current -= current_node->initial_value_;
    if (added_to_current != sum_of_childs) {
      LOG_ERROR(logger_,
                "Value validation failed: info=" << current_node->info_.toString()
                                                 << " ,curent value=" << current_node->current_value_.toString()
                                                 << " ,initial value=" << current_node->initial_value_.toString()
                                                 << " ,sum of childs=" << sum_of_childs.toString()
                                                 << " ,added_to_current=" << added_to_current.toString());
      // 1st error found - exit
      return false;
    }
    auto next_sibling = getRVTNodeOfRightSibling(current_node);
    if (next_sibling == nullptr) {
      // go 1 level up and leftmost
      current_node = leftmostRVTNode_[++current_level];
    } else {
      current_node = next_sibling;
    }
  } while (current_node && current_node->parent_id != 0);
  return true;
}

bool RangeValidationTree::validate() const noexcept {
  if (validateTreeStructure()) {
    return validateTreeValues();
  }
  return false;
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
  queue<RVTNodePtr> q;
  q.push(root_);
  while (q.size()) {
    auto& node = q.front();
    q.pop();
    oss << node->info_.toString() << " ";
    if (not only_node_id) {
      oss << " = " << node->current_value_.toString() << " ";
    }
    if (node->info_.level == 1) {
      continue;
    }

    uint16_t count = 0;
    auto child_node = getLeftMostChildNode(node);
    ConcordAssert(child_node != nullptr);
    while (count++ < node->n_child) {
      q.push(child_node);
      child_node = getRVTNodeOfRightSibling(child_node);
      if (count < node->n_child) {
        ConcordAssert(child_node != nullptr);
      }
    }
  }
  LOG_INFO(logger_, oss.str());
}

bool RangeValidationTree::isValidRvbId(const RVBId& block_id) const noexcept {
  return ((block_id != 0) && (fetch_range_size_ != 0) && (block_id % fetch_range_size_ == 0));
}

bool RangeValidationTree::validateRVBGroupId(const RVBGroupId rvb_group_id) const {
  NodeInfo nid(rvb_group_id);
  if (rvb_group_id == 0) {
    return false;
  }
  return ((nid.level == RVTNode::kDefaultRVTLeafLevel) && ((nid.rvb_index % RVT_K) == 1));
}

RVTNodePtr RangeValidationTree::getRVTNodeOfLeftSibling(const RVTNodePtr& node) const {
  if (leftmostRVTNode_[node->info_.level] == node) {
    return nullptr;
  }
  auto level = node->info_.level;
  auto rvb_index = node->info_.rvb_index;
  auto id = NodeInfo(level, rvb_index - RangeValidationTree::pow_uint(RVT_K, level)).id;
  auto iter = id_to_node_.find(id);
  return (iter == id_to_node_.end()) ? nullptr : iter->second;
}

RVTNodePtr RangeValidationTree::getRVTNodeOfRightSibling(const RVTNodePtr& node) const {
  if (rightmostRVTNode_[node->info_.level] == node) {
    return nullptr;
  }
  auto level = node->info_.level;
  auto rvb_index = node->info_.rvb_index;
  auto id = NodeInfo(level, rvb_index + RangeValidationTree::pow_uint(RVT_K, level)).id;
  auto iter = id_to_node_.find(id);
  return (iter == id_to_node_.end()) ? nullptr : iter->second;
}

RVTNodePtr RangeValidationTree::getParentNode(const RVTNodePtr& node) const noexcept {
  auto itr = id_to_node_.find(node->parent_id);
  return (itr == id_to_node_.end()) ? nullptr : itr->second;
}

RVTNodePtr RangeValidationTree::getLeftMostChildNode(const RVTNodePtr& node) const noexcept {
  auto itr = id_to_node_.find(node->min_child_id);
  return (itr == id_to_node_.end() ? nullptr : itr->second);
}

void RangeValidationTree::addRVBNode(const shared_ptr<RVBNode>& rvb_node) {
  auto parent_node = openForInsertion(RVTNode::kDefaultRVTLeafLevel);
  if (!parent_node) {
    // adding first rvb parent_node but it might not be first child (reason: pruning)
    parent_node = make_shared<RVTNode>(rvb_node);
    rightmostRVTNode_[RVTNode::kDefaultRVTLeafLevel] = parent_node;
    if (!leftmostRVTNode_[RVTNode::kDefaultRVTLeafLevel]) {
      leftmostRVTNode_[RVTNode::kDefaultRVTLeafLevel] = parent_node;
    }
    ConcordAssert(id_to_node_.insert({parent_node->info_.id, parent_node}).second == true);
    if (!root_) {  // TODO - is this needed???
      setNewRoot(parent_node);
    }
    addInternalNode(parent_node);
  } else {
    ++parent_node->n_child;
    ConcordAssertLE(parent_node->n_child, RVT_K);
    ConcordAssertLE(parent_node->min_child_id + parent_node->n_child - 1, parent_node->max_child_id);
  }
  addValueToInternalNodes(parent_node, rvb_node->current_value_);
}

void RangeValidationTree::removeRVBNode(const shared_ptr<RVBNode>& rvb_node) {
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
    auto val_negative = node->initial_value_;
    val_negative.val_.SetNegative();
    addValueToInternalNodes(getParentNode(node), val_negative);
  }

  --node->n_child;
  if (node->n_child > 0) {
    ++node->min_child_id;
  }
  ConcordAssertLE(node->min_child_id + node->n_child - 1, node->max_child_id);
  removeAndUpdateInternalNodes(node, rvb_node->current_value_);
}

void RangeValidationTree::addValueToInternalNodes(const RVTNodePtr& bottom_node, const NodeVal& value) {
  if (bottom_node == nullptr) return;

  auto current_node = bottom_node;
  do {
    current_node->addValue(value);
    if (current_node->parent_id != 0) {
      ConcordAssert(root_ != current_node);
      current_node = getParentNode(current_node);
      ConcordAssert(current_node != nullptr);
    } else {
      ConcordAssert(root_ == current_node);
    }
  } while (current_node != root_);
  if (current_node != bottom_node) {
    current_node->addValue(value);
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

void RangeValidationTree::addInternalNode(const RVTNodePtr& node_to_add) {
  RVTNodePtr parent_node, bottom_node;
  auto current_node = node_to_add;
  NodeVal val_to_add = current_node->initial_value_;

  while (current_node->info_.level != root_->info_.level) {
    if (current_node->parent_id == 0) {
      parent_node = openForInsertion(current_node->info_.level + 1);
      if (parent_node) {
        // Add the current_node to parent_node which still has more space for childs
        current_node->parent_id = parent_node->info_.id;
        parent_node->n_child++;
        ConcordAssert(parent_node->n_child <= RVT_K);
        parent_node->addValue(val_to_add);
      } else {
        // construct new internal RVT current_node
        ConcordAssert(current_node->isMinChild() == true);
        parent_node = make_shared<RVTNode>(current_node);
        parent_node->addValue(val_to_add);
        val_to_add += parent_node->initial_value_;
        ConcordAssert(id_to_node_.insert({parent_node->info_.id, parent_node}).second == true);
        rightmostRVTNode_[parent_node->info_.level] = parent_node;
        current_node->parent_id = parent_node->info_.id;
      }
    } else {
      parent_node = getParentNode(current_node);
      ConcordAssert(parent_node != nullptr);
      parent_node->addValue(val_to_add);
    }
    current_node = parent_node;
  }

  // no need to create new root as we have reached to root while updating value
  if (current_node->info_.id == root_->info_.id) {
    return;
  }

  // create new root
  auto new_root = make_shared<RVTNode>(root_);
  ConcordAssert(id_to_node_.insert({new_root->info_.id, new_root}).second == true);
  current_node->parent_id = new_root->info_.id;
  new_root->n_child++;
  new_root->addValue(root_->current_value_);
  new_root->addValue(current_node->current_value_);
  ConcordAssert(new_root->n_child <= RVT_K);
  setNewRoot(new_root);
}

void RangeValidationTree::removeAndUpdateInternalNodes(const RVTNodePtr& rvt_node, const NodeVal& value) {
  ConcordAssert(rvt_node != nullptr);

  // Loop 1
  // Trim the tree from rvt_node to root
  RVTNodePtr cur_node = rvt_node;
  while (cur_node != root_) {
    auto parent_node = getParentNode(cur_node);

    // When rvt_node is removed, we need to update its parent
    if (cur_node->n_child == 0) {
      --parent_node->n_child;
      if (parent_node->n_child > 0) {
        auto level = cur_node->info_.level;
        auto id = NodeInfo(level, cur_node->info_.rvb_index + RangeValidationTree::pow_uint(RVT_K, level)).id;
        ConcordAssert(id_to_node_.find(id) != id_to_node_.end());
        parent_node->min_child_id = id;
      }
      if (cur_node != rvt_node) {
        id_to_node_.erase(cur_node->info_.id);
        auto val_negative = cur_node->initial_value_;
        val_negative.val_.SetNegative();
        addValueToInternalNodes(getParentNode(cur_node), val_negative);
        if ((leftmostRVTNode_[cur_node->info_.level] == cur_node) &&
            (rightmostRVTNode_[cur_node->info_.level] == cur_node)) {
          rightmostRVTNode_[cur_node->info_.level] = nullptr;
          leftmostRVTNode_[cur_node->info_.level] = nullptr;
        } else if (leftmostRVTNode_[cur_node->info_.level] == cur_node) {
          leftmostRVTNode_[cur_node->info_.level] = getRVTNodeOfRightSibling(cur_node);
        }
      }
    }
    cur_node->substractValue(value);
    cur_node = parent_node;
  }
  if (cur_node == root_) {
    root_->substractValue(value);
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

void RangeValidationTree::setNewRoot(const RVTNodePtr& new_root) {
  if (!new_root) {
    ConcordAssert(root_->info_.level == 1);
    ConcordAssert(root_ != nullptr);
    clear();
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

inline RVTNodePtr RangeValidationTree::openForInsertion(uint64_t level) const {
  if (rightmostRVTNode_[level] == nullptr) {
    return nullptr;
  }
  auto& node = rightmostRVTNode_[level];
  uint64_t min_child_actual_rvb_index = node->min_child_id & NodeInfo::kRvbIndexMask;
  uint64_t max_child_possible_rvb_index = node->max_child_id & NodeInfo::kRvbIndexMask;
  uint64_t max_child_actual_rvb_index =
      min_child_actual_rvb_index + RangeValidationTree::pow_uint(RVT_K, node->info_.level - 1) * (node->n_child - 1);
  return (max_child_actual_rvb_index < max_child_possible_rvb_index) ? node : nullptr;
}

inline RVTNodePtr RangeValidationTree::openForRemoval(uint64_t level) const { return leftmostRVTNode_[level]; }

void RangeValidationTree::clear() noexcept {
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

void RangeValidationTree::addNode(const RVBId rvb_id, const char* data, size_t data_size) {
  if (!isValidRvbId(rvb_id)) {
    LOG_ERROR(logger_, "invalid input data" << KVLOG(rvb_id, data_size, fetch_range_size_));
    ConcordAssert(false);
  }
  auto rvb_index = rvb_id / fetch_range_size_;
  auto node = make_shared<RVBNode>(rvb_index, data, data_size);
  if (max_rvb_index_ > 0) {
    ConcordAssertEQ(max_rvb_index_ + 1, rvb_index);
  }
  addRVBNode(node);
  max_rvb_index_ = rvb_index;
  if (min_rvb_index_ == 0) {
    min_rvb_index_ = rvb_index;
  }
  LOG_TRACE(logger_, KVLOG(rvb_id, max_rvb_index_));
}

void RangeValidationTree::removeNode(const RVBId rvb_id, const char* data, size_t data_size) {
  ConcordAssert(root_ != nullptr);
  if (!isValidRvbId(rvb_id)) {
    LOG_ERROR(logger_, "invalid input data" << KVLOG(rvb_id, data_size, fetch_range_size_));
    ConcordAssert(false);
  }
  auto rvb_index = rvb_id / fetch_range_size_;
  auto node = make_shared<RVBNode>(rvb_index, data, data_size);
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
  LOG_TRACE(logger_, "Nodes:" << totalNodes() << " root value:" << root_->current_value_.toString());
  return os;
}

bool RangeValidationTree::setSerializedRvbData(std::istringstream& is) {
  if (!(is.str().size())) {
    LOG_ERROR(logger_, "invalid input");
    return false;
  }

  clear();
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
    clear();
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
    clear();
    return false;
  }

  LOG_TRACE(logger_, "Nodes:" << totalNodes() << " root value:" << root_->current_value_.toString());
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
    } else if ((rvb_index % RVT_K) == 0) {
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
  val = itr->second->current_value_.toString();
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
