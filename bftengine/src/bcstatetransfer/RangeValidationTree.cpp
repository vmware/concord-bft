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

#ifdef DO_DEBUG
#define DEBUG_PRINT(x, y) LOG_INFO(x, y)
#define logInfoVal(x) logInfoVal(x)
#else
#define DEBUG_PRINT(x, y)
#define logInfoVal(x)
#endif

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
  os << " [" << level() << "," << rvb_index() << "]";
  return os.str();
}

// TODO - for now, the outDigest is of a fixed size. We can match the final size to RangeValidationTree::value_size
// This requires us to write our own DigestContext
const shared_ptr<char[]> RVBNode::computeNodeInitialValue(NodeInfo& node_info, const char* data, size_t data_size) {
  ConcordAssertGT(node_info.id(), 0);
  DigestContext c;

  c.update(reinterpret_cast<const char*>(&node_info.id_data_), sizeof(node_info.id_data_));
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
  static_assert(sizeof(SerializedRVTNode::id) == sizeof(NodeInfo::id_data_));
  static_assert(sizeof(SerializedRVTNode::parent_id) == sizeof(RVTNode::parent_id_));
  static_assert(sizeof(SerializedRVTNode::last_insertion_index) == sizeof(RVTNode::last_insertion_index_));
  static_assert(sizeof(SerializedRVTNode::child_ids) == sizeof(RVTNode::child_ids_));
}

#ifdef DO_DEBUG
void RVBNode::logInfoVal(const std::string& prefix) {
#ifdef DEBUG
  return;
#endif
  ostringstream oss;
  oss << prefix << info_.toString() << " " << current_value_.toString();
  // Keep for debug, do not remove please!
  DEBUG_PRINT(GL, oss.str());
}
#endif

RVBNode::RVBNode(uint64_t rvb_index, const char* data, size_t data_size)
    : info_(kDefaultRVBLeafLevel, rvb_index),
      current_value_(computeNodeInitialValue(info_, data, data_size), NodeVal::kDigestContextOutputSize) {
  // logInfoVal("construct: ");
}

RVBNode::RVBNode(uint8_t level, uint64_t rvb_index)
    : info_(level, rvb_index),
      current_value_(
          computeNodeInitialValue(info_, NodeVal::initialValueZeroData.data(), NodeVal::kDigestContextOutputSize),
          NodeVal::kDigestContextOutputSize) {
  // logInfoVal("construct: ");
}

RVBNode::RVBNode(uint64_t node_id, char* val_ptr, size_t size) : info_(node_id), current_value_(val_ptr, size) {
  // logInfoVal("construct: ");
}

// Level 1 RVT Node ctor
RVTNode::RVTNode(const shared_ptr<RVBNode>& child_node)
    : RVBNode(kDefaultRVTLeafLevel, child_node->info_.rvb_index()),
      parent_id_{},
      last_insertion_index_{},
      initial_value_(current_value_) {
  // in some cases RVB node might be inserted in as a middle child.
  // The parent inherits the min RVB index of the 1st child inserted, and stays with it.
  // In that sense it has an "unalleged rvb index".
  // We need to find the child's location and update last_insertion_index_.
  // For example: child is inserted with an ID [0,4] - level, rvb_index and RVT_K is 12. That means there are left only
  // 9 insertions. For higher levels, the calculation is more complex.
  auto mod = child_node->info_.rvb_index() % (RangeValidationTree::RVT_K);
  if (mod == 0) {
    last_insertion_index_ = RangeValidationTree::RVT_K - 1;
  } else {
    last_insertion_index_ += (mod - 1);
  }

  // TODO - add extra validatations here and inside child_ids child id and correct index
  pushChildId(child_node->info_.id());
}

// level 2 and up nodes
RVTNode::RVTNode(const RVTNodePtr& child_node)
    : RVBNode(child_node->info_.level() + 1, child_node->info_.id()),
      parent_id_{},
      last_insertion_index_{},
      initial_value_(current_value_) {
  // In some cases RVB node might be inserted in as a middle child (see more details in above ctor)
  // The next calculation was verified with many examples.
  // Here is one:
  // RVT_K=12 and child is on level 3 with  rvb_index 3000 (first insertion to tree)
  // We know that current parent has the potential next child span:
  // 13*12^2+1, 14*12^+1, ... [until 12th child] 23*12^2+1
  // or: 1729	1873	2017	2161	2305	2449	2593	2737	2881	3025	3169	3313
  // We would like to find in which index child starts and update number of left insertions.
  // 3000 enters on the 8th index. since there is push inside which ncrements the last_insertion_index_, we don't
  // increment it here.
  auto pow_child = pow_uint(RangeValidationTree::RVT_K, child_node->info_.level());
  auto pow = pow_child * RangeValidationTree::RVT_K;
  auto min_span_rvb_index = (child_node->info_.rvb_index() / pow) * pow + 1;
  auto index_inside_span = (child_node->info_.rvb_index() - min_span_rvb_index) / pow_child;
  last_insertion_index_ = index_inside_span;
  pushChildId(child_node->info_.id());
}

RVTNode::RVTNode(SerializedRVTNode& node, char* cur_val_ptr, size_t cur_value_size)
    : RVBNode(node.id, cur_val_ptr, cur_value_size),
      parent_id_(node.parent_id),
      child_ids_(std::move(node.child_ids)),
      last_insertion_index_{node.last_insertion_index},
      initial_value_(
          computeNodeInitialValue(info_, NodeVal::initialValueZeroData.data(), NodeVal::kDigestContextOutputSize),
          NodeVal::kDigestContextOutputSize) {}

void RVTNode::addValue(const NodeVal& nvalue) {
  // Keep for debug, do not remove please!
  logInfoVal("Before add:");
  DEBUG_PRINT(GL, "Adding " << nvalue.toString());

  this->current_value_ += nvalue;

  // Keep for debug, do not remove please!
  logInfoVal("After:");
}

void RVTNode::substractValue(const NodeVal& nvalue) {
  // Keep for debug, do not remove please!
  logInfoVal("Before sub:");
  DEBUG_PRINT(GL, "Substracting " << nvalue.toString());

  this->current_value_ -= nvalue;

  logInfoVal("After:");
}

std::ostringstream RVTNode::serialize() const {
  std::ostringstream os;
  Serializable::serialize(os, info_.id());
  Serializable::serialize(os, parent_id_);
  Serializable::serialize(os, last_insertion_index_);
  Serializable::serialize(os, child_ids_);
  // Serializable::serialize(os, child_ids_.size());
  // for (const auto& cid : child_ids_) {
  //   Serializable::serialize(os, cid);
  // }

  Serializable::serialize(os, current_value_.getSize());
  auto decoded_val = current_value_.getDecoded();
  Serializable::serialize(os, decoded_val.data(), current_value_.getSize());

  // We do not serialize initial_value_ since it can be easily re-calculated
  return os;
}

RVTNodePtr RVTNode::createFromSerialized(std::istringstream& is) {
  SerializedRVTNode snode;
  // size_t nchilds {};
  // uint64_t cid;

  Serializable::deserialize(is, snode.id);
  Serializable::deserialize(is, snode.parent_id);
  Serializable::deserialize(is, snode.last_insertion_index);
  // Serializable::deserialize(is, nchilds);
  Serializable::deserialize(is, snode.child_ids);
  // for (size_t i{}; i < nchilds; ++i) {
  //   Serializable::deserialize(is, cid);
  //   snode.child_ids.push_back(cid);
  // }

  Serializable::deserialize(is, snode.current_value_encoded_size);
  std::unique_ptr<char[]> ptr_cur = std::make_unique<char[]>(snode.current_value_encoded_size);
  Serializable::deserialize(is, ptr_cur.get(), snode.current_value_encoded_size);
  return std::make_shared<RVTNode>(snode, ptr_cur.get(), snode.current_value_encoded_size);
}

void RVTNode::pushChildId(uint64_t id) {
  if (!child_ids_.empty()) {
    ConcordAssert(id > child_ids_.back());
  }
  ConcordAssertLT(last_insertion_index_, RVT_K);
  child_ids_.push_back(id);
  ++last_insertion_index_;
}

void RVTNode::popChildId(uint64_t id) {
  ConcordAssert(!child_ids_.empty());
  ConcordAssertEQ(child_ids_.front(), id);
  child_ids_.pop_front();
}

uint32_t RangeValidationTree::RVT_K{};

RangeValidationTree::RangeValidationTree(const logging::Logger& logger,
                                         uint32_t _RVT_K,
                                         uint32_t fetch_range_size,
                                         size_t value_size)
    : rightmostRVTNode_{},
      leftmostRVTNode_{},
      logger_(logger),
      // RVT_K(_RVT_K),
      fetch_range_size_(fetch_range_size),
      value_size_(value_size) {
  NodeVal::kNodeValueMax_ = NodeVal::calcMaxValue(value_size_);
  NodeVal::kNodeValueModulo_ = NodeVal::calcModulo(value_size_);
  ConcordAssertEQ(NodeVal::kNodeValueMax_ + NodeVal_t(1), NodeVal::kNodeValueModulo_);
  ConcordAssert(NodeVal::kNodeValueMax_ != NodeVal_t(static_cast<signed long>(0)));
  ConcordAssert(NodeVal::kNodeValueModulo_ != NodeVal_t(static_cast<signed long>(0)));
  // ConcordAssertLE(_RVT_K, static_cast<uint64_t>(1ULL << (sizeof(RVTNode::n_child) * 8ULL)));
  RVTMetadata::staticAssert();
  SerializedRVTNode::staticAssert();
  RangeValidationTree::RVT_K = _RVT_K;
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

bool RangeValidationTree::validateTreeValues() const noexcept {
  if (root_ == nullptr && empty()) {
    return true;
  }

  // Currently we do not get any level 0 data from outside, so we cannot validate level 1
  // TODO - add support for level 1 validation
  if (totalLevels() <= RVTNode::kDefaultRVTLeafLevel) {
    return true;
  }

  // Lets scan all non level 1 nodes in the map
  auto iter = id_to_node_.begin();
  ConcordAssert(iter->second != nullptr);
  std::set<uint64_t> validated_ids;
  size_t skipped{};

  do {
    NodeVal sum_of_childs{};
    auto current_node = iter->second;

    if (iter == id_to_node_.end()) {
      break;
    }
    if (current_node->info_.level() <= 1) {
      ++iter;
      validated_ids.insert(current_node->info_.id());
      ++skipped;
      continue;
    }

    ConcordAssert(current_node->hasChilds());
    for (const auto& cid : current_node->child_ids_) {
      auto iter1 = id_to_node_.find(cid);
      ConcordAssert(iter1 != id_to_node_.end());
      sum_of_childs += iter1->second->current_value_;
    }

    // Formula:
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
    validated_ids.insert(current_node->info_.id());
    ++iter;
  } while (iter != id_to_node_.end());

  // final check: see that we've validated all nodes in tree:
  ConcordAssertEQ(validated_ids.size(), id_to_node_.size());

  std::set<uint64_t> id_to_node_keys;
  std::transform(id_to_node_.begin(),
                 id_to_node_.end(),
                 std::inserter(id_to_node_keys, id_to_node_keys.begin()),
                 [](const std::unordered_map<uint64_t, RVTNodePtr>::value_type& pair) { return pair.first; });
  ConcordAssertEQ(id_to_node_keys, validated_ids);

  return true;
}

bool RangeValidationTree::validateTreeStructure() const noexcept { return true; }

bool RangeValidationTree::validate() const noexcept {
  if (validateTreeStructure()) {
    return validateTreeValues();
  }
  return false;
}

void RangeValidationTree::printToLog(LogPrintVerbosity verbosity) const noexcept {
  if (!root_ or totalNodes() == 0) {
    LOG_INFO(logger_, "Empty RVT");
    return;
  }
  if (totalNodes() > kMaxNodesToPrint) {
    LOG_WARN(logger_, "Huge tree so would not log");
    return;
  }
  std::ostringstream oss;
  oss << " #Levels=" << root_->info_.level();
  oss << " ,#Nodes=" << totalNodes();
  oss << " ,max_rvb_index_=" << max_rvb_index_;
  oss << " ,min_rvb_index_=" << min_rvb_index_;
  oss << " ,RVT_K=" << RVT_K;
  oss << " ,FRS=" << fetch_range_size_;
  oss << " ,value_size=" << value_size_;

  oss << " Structure:";
  queue<RVTNodePtr> q;
  q.push(root_);
  while (q.size()) {
    auto& node = q.front();
    q.pop();
    oss << "|" << node->info_.toString() << " ";

    if (verbosity == LogPrintVerbosity::DETAILED) {
      oss << " level=" << node->info_.level() << " rvb_index=" << node->info_.rvb_index()
          << " last_insertion_index_=" << node->last_insertion_index_
          << " current_value_=" << node->current_value_.toString()
          << " min_cid=" << NodeInfo(node->minChildId()).toString()
          << " max_cid=" << NodeInfo(node->maxChildId()).toString();

      // Keep for debugging
      // ConcordAssert(node->hasChilds());
      // oss << " child_ids:";
      // for (const auto& cid : node->child_ids_) {
      //   oss << NodeInfo(cid).toString();
      // }
    }
    if (node->info_.level() == 1) {
      continue;
    }

    auto child_node = getRVTNodeByType(node, NodeType::LEFTMOST_LEVEL_DOWN_NODE);
    ConcordAssert(child_node != nullptr);
    for (const auto& cid : node->child_ids_) {
      auto iter = id_to_node_.find(cid);
      ConcordAssert(iter != id_to_node_.end());
      q.push(iter->second);
    }
  }
  LOG_INFO(logger_, oss.str());
}

bool RangeValidationTree::isValidRvbId(const RVBId& block_id) const noexcept {
  return ((block_id != 0) && (fetch_range_size_ != 0) && (block_id % fetch_range_size_ == 0));
}

bool RangeValidationTree::validateRVBGroupId(const RVBGroupId rvb_group_id) const {
  NodeInfo node_info(rvb_group_id);
  if (rvb_group_id == 0) {
    return false;
  }
  return ((node_info.level() == RVTNode::kDefaultRVTLeafLevel) && ((node_info.rvb_index() % RVT_K) == 1));
}

RVTNodePtr RangeValidationTree::getRVTNodeByType(const RVTNodePtr& node, NodeType type) const {
  std::deque<uint64_t>::iterator iter_target;
  RVTNodePtr parent;
  uint64_t node_id{};

  if (type == NodeType::LEFTMOST_LEVEL_DOWN_NODE) {
    if (node->info_.level() == 1) {
      return nullptr;
    }
    return leftmostRVTNode_[node->info_.level()];
  } else if (type == NodeType::RIGHTMOST_LEVEL_DOWN_NODE) {
    if (node->info_.level() == 1) {
      return nullptr;
    }
    return rightmostRVTNode_[node->info_.level()];
  } else if (type == NodeType::LEFTMOST_SAME_LEVEL_NODE) {
    auto leftmost_sib = leftmostRVTNode_[node->info_.level()];
    if (leftmostRVTNode_[node->info_.level()] == node) {
      return nullptr;
    }
    return leftmost_sib;
  } else if (type == NodeType::RIGHTMOST_SAME_LEVEL_NODE) {
    auto rightmost_sib = rightmostRVTNode_[node->info_.level()];
    if (rightmostRVTNode_[node->info_.level()] == node) {
      return nullptr;
    }
    return rightmost_sib;
  } else if ((type == NodeType::LEFTMOST_CHILD) || (type == NodeType::RIGHTMOST_CHILD)) {
    if (node->hasNoChilds()) {
      return nullptr;
    }
    if (type == NodeType::LEFTMOST_CHILD) {
      iter_target = node->child_ids_.begin();
      if ((*iter_target) == node->info_.id()) {
        return nullptr;
      }
    } else {  // rightmost child
      iter_target = node->child_ids_.end();
      if ((*iter_target) == node->info_.id()) {
        return nullptr;
      }
      --iter_target;
    }
    node_id = *iter_target;
  } else {
    // case: PARENT, RIGHT_SIBLING, LEFT_SIBLING

    if (node == root_) {
      // no parents and not siblings for a root
      return nullptr;
    }

    auto iter = id_to_node_.find(node->parent_id_);
    ConcordAssert(iter != id_to_node_.end());
    if (type == NodeType::PARENT) {
      return iter->second;
    }
    parent = iter->second;

    const auto& cids = parent->child_ids_;
    ConcordAssert(!cids.empty());
    if (cids.size() == 1) {
      return nullptr;
    }

    // find myself in parent's container
    auto iter_target = std::find(cids.begin(), cids.end(), node->info_.id());
    ConcordAssert(iter_target != cids.end());
    if (type == NodeType::LEFT_SIBLING) {
      if ((iter_target) == cids.begin()) {
        return nullptr;
      }
      --iter_target;
    } else if (type == NodeType::RIGHT_SIBLING) {
      ++iter_target;
      if (iter_target == cids.end()) {
        return nullptr;
      }
    } else {
      LOG_FATAL(logger_, "Unhandled NodeType case!");
    }
    node_id = *iter_target;
  }
  ConcordAssertNE(node_id, 0) return id_to_node_.find(node_id)->second;
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
    ConcordAssert(id_to_node_.insert({parent_node->info_.id(), parent_node}).second == true);
    // Keep for debug, do not remove please!
    DEBUG_PRINT(logger_, "Added node" << parent_node->info_.toString() << " id " << parent_node->info_.id());
    if (!root_) {  // TODO - is this needed???
      setNewRoot(parent_node);
    }
    addInternalNode(parent_node);
  } else {
    parent_node->pushChildId(rvb_node->info_.id());
    ConcordAssertLE(parent_node->numChilds(), RVT_K);
    // ConcordAssertLE(parent_node->minChildId + parent_node->n_child - 1, parent_node->maxChildId);
  }
  addValueToInternalNodes(parent_node, rvb_node->current_value_);
}

void RangeValidationTree::removeRVBNode(const shared_ptr<RVBNode>& rvb_node) {
  auto node = openForRemoval(RVTNode::kDefaultRVTLeafLevel);
  ConcordAssert(node != nullptr);

  if (node->numChilds() == 1) {
    // no more RVB childs, erase the node
    auto id = node->info_.id();
    auto node_to_remove_iter = id_to_node_.find(id);
    ConcordAssert(node_to_remove_iter != id_to_node_.end());
    if ((node == leftmostRVTNode_[RVTNode::kDefaultRVTLeafLevel]) &&
        (node == rightmostRVTNode_[RVTNode::kDefaultRVTLeafLevel])) {
      leftmostRVTNode_[RVTNode::kDefaultRVTLeafLevel] = nullptr;
      rightmostRVTNode_[RVTNode::kDefaultRVTLeafLevel] = nullptr;
    } else {
      if (node == leftmostRVTNode_[RVTNode::kDefaultRVTLeafLevel]) {
        auto id = NodeInfo(RVTNode::kDefaultRVTLeafLevel, rvb_node->info_.rvb_index() + 1).id();
        auto iter = id_to_node_.find(id);
        leftmostRVTNode_[RVTNode::kDefaultRVTLeafLevel] = (iter == id_to_node_.end()) ? nullptr : iter->second;
      }
      if (node == rightmostRVTNode_[RVTNode::kDefaultRVTLeafLevel]) {
        rightmostRVTNode_[RVTNode::kDefaultRVTLeafLevel] = nullptr;
      }
    }
    node_ids_to_erase_.insert(id);
    // id_to_node_.erase(node_to_remove_iter);
    auto val_negative = node->initial_value_;
    val_negative.val_.SetNegative();
    addValueToInternalNodes(getRVTNodeByType(node, NodeType::PARENT), val_negative);
  }

  // --node->n_child;
  // //--node->n_max_child;
  // if (node->n_child > 0) {
  //   ++node->minChildId;
  // }
  // ConcordAssertLE(node->minChildId + node->n_child - 1, node->maxChildId);
  node->popChildId(rvb_node->info_.id());
  removeAndUpdateInternalNodes(node, rvb_node->current_value_);
}

void RangeValidationTree::addValueToInternalNodes(const RVTNodePtr& bottom_node, const NodeVal& value) {
  if (bottom_node == nullptr) return;

  auto current_node = bottom_node;
  do {
    current_node->addValue(value);
    if (current_node->parent_id_ != 0) {
      ConcordAssert(root_ != current_node);
      current_node = getRVTNodeByType(current_node, NodeType::PARENT);
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

  while (current_node->info_.level() != root_->info_.level()) {
    if (current_node->parent_id_ == 0) {
      parent_node = openForInsertion(current_node->info_.level() + 1);
      if (parent_node) {
        // Add the current_node to parent_node which still has more space for childs
        current_node->parent_id_ = parent_node->info_.id();
        // parent_node->n_child++;
        parent_node->pushChildId(current_node->info_.id());
        ConcordAssert(parent_node->numChilds() <= RVT_K);
        parent_node->addValue(val_to_add);
      } else {
        // construct new internal RVT current_node
        // ConcordAssert(current_node->isMinPossibleChild() == true); // ??
        parent_node = make_shared<RVTNode>(current_node);
        parent_node->addValue(val_to_add);
        val_to_add += parent_node->initial_value_;
        ConcordAssert(id_to_node_.insert({parent_node->info_.id(), parent_node}).second == true);
        // Keep for debug, do not remove please!
        DEBUG_PRINT(logger_, "Added node" << parent_node->info_.toString() << " id " << parent_node->info_.id());
        rightmostRVTNode_[parent_node->info_.level()] = parent_node;
        current_node->parent_id_ = parent_node->info_.id();
      }
    } else {
      parent_node = getRVTNodeByType(current_node, NodeType::PARENT);
      ConcordAssert(parent_node != nullptr);
      parent_node->addValue(val_to_add);
    }
    current_node = parent_node;
  }

  // no need to create new root as we have reached to root while updating value
  if (current_node->info_.id() == root_->info_.id()) {
    return;
  }

  // create new root
  auto new_root = make_shared<RVTNode>(root_);
  ConcordAssert(id_to_node_.insert({new_root->info_.id(), new_root}).second == true);
  // Keep for debug, do not remove please!
  DEBUG_PRINT(logger_, "Added node" << new_root->info_.toString() << " id " << new_root->info_.id());
  current_node->parent_id_ = new_root->info_.id();
  new_root->pushChildId(current_node->info_.id());
  new_root->addValue(root_->current_value_);
  new_root->addValue(current_node->current_value_);
  ConcordAssert(new_root->numChilds() <= RVT_K);
  setNewRoot(new_root);
}

void RangeValidationTree::removeAndUpdateInternalNodes(const RVTNodePtr& rvt_node, const NodeVal& value) {
  ConcordAssert(rvt_node != nullptr);

  // Loop 1
  // Trim the tree from rvt_node to root
  RVTNodePtr cur_node = rvt_node;
  while (cur_node != root_) {
    auto parent_node = getRVTNodeByType(cur_node, NodeType::PARENT);

    // When rvt_node is removed, we need to update its parent
    if (cur_node->hasNoChilds()) {
      auto right_sib = getRVTNodeByType(cur_node, NodeType::RIGHT_SIBLING);
      parent_node->popChildId(cur_node->info_.id());
      if (cur_node != rvt_node) {
        node_ids_to_erase_.insert(cur_node->info_.id());
        // id_to_node_.erase(cur_node->info_.id());
        auto val_negative = cur_node->initial_value_;
        val_negative.val_.SetNegative();
        addValueToInternalNodes(parent_node, val_negative);
        if ((leftmostRVTNode_[cur_node->info_.level()] == cur_node) &&
            (rightmostRVTNode_[cur_node->info_.level()] == cur_node)) {
          rightmostRVTNode_[cur_node->info_.level()] = nullptr;
          leftmostRVTNode_[cur_node->info_.level()] = nullptr;
        } else if (leftmostRVTNode_[cur_node->info_.level()] == cur_node) {
          leftmostRVTNode_[cur_node->info_.level()] = right_sib;
        }
      }
    }
    cur_node->substractValue(value);
    cur_node = parent_node;
  }

  if (cur_node == root_) {
    root_->substractValue(value);

    if (root_->hasNoChilds()) {  // next 3 lines not to be merged
      setNewRoot(nullptr);
      return;
    }
  }

  // Loop 2
  // Shrink the tree from root to bottom (level 1) . In theory, tree can end empty after this loop
  while ((cur_node == root_) && (cur_node->numChilds() == 1) && (cur_node->info_.level() > 1)) {
    node_ids_to_erase_.insert(cur_node->info_.id());
    // id_to_node_.erase(cur_node->info_.id());
    // TODO - validate when we are done, not here
    // ConcordAssertEQ(rightmostRVTNode_[cur_node->info_.level() - 1], leftmostRVTNode_[cur_node->info_.level() - 1]);
    rightmostRVTNode_[cur_node->info_.level()] = nullptr;
    leftmostRVTNode_[cur_node->info_.level()] = nullptr;
    cur_node = rightmostRVTNode_[cur_node->info_.level() - 1];
    setNewRoot(cur_node);
  }
}

void RangeValidationTree::setNewRoot(const RVTNodePtr& new_root) {
  if (!new_root) {
    ConcordAssert(root_->info_.level() == 1);
    ConcordAssert(root_ != nullptr);
    clear();
    return;
  }
  if (root_) {
    // replacing roots
    int new_root_level = static_cast<int>(new_root->info_.level());
    int old_root_level = static_cast<int>(root_->info_.level());
    ConcordAssert(new_root_level != 0);
    ConcordAssert(std::abs(new_root_level - old_root_level) == 1);
    if (new_root_level > old_root_level) {
      // replacing the root - tree grows 1 level
      root_->parent_id_ = new_root->info_.id();
    } else if (new_root_level < old_root_level) {
      // replacing the root - tree shrinks 1 level
      rightmostRVTNode_[root_->info_.level()] = nullptr;
      leftmostRVTNode_[root_->info_.level()] = nullptr;
    }
  }
  root_ = new_root;
  root_->parent_id_ = 0;
  rightmostRVTNode_[new_root->info_.level()] = new_root;
  leftmostRVTNode_[new_root->info_.level()] = new_root;
}

inline RVTNodePtr RangeValidationTree::openForInsertion(uint64_t level) const {
  if (!rightmostRVTNode_[level]) {
    return nullptr;
  }
  auto rnode = rightmostRVTNode_[level];
  return rnode->openForInsertion() ? rnode : nullptr;
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
  for (auto id : node_ids_to_erase_) {
    // Keep for debug, do not remove please!
    DEBUG_PRINT(logger_, "Removed node id " << id);
    id_to_node_.erase(id);
  }
  node_ids_to_erase_.clear();
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

  ConcordAssert(validate());
  RVTMetadata data{magic_num_, version_num_, RVT_K, fetch_range_size_, value_size_, root_->info_.id(), totalNodes()};
  Serializable::serialize(os, reinterpret_cast<char*>(&data), sizeof(data));

  // Serializable::serialize(os, totalNodes());
  for (auto& itr : id_to_node_) {
    auto serialized_node = itr.second->serialize();
    Serializable::serialize(os, serialized_node.str().data(), serialized_node.str().size());
  }

  uint64_t null_node_id = 0;
  auto max_levels = root_->info_.level();
  for (uint64_t i = 0; i <= max_levels; i++) {
    auto node = rightmostRVTNode_[i];
    if (!node) {
      Serializable::serialize(os, null_node_id);
    } else {
      Serializable::serialize(os, node->info_.id());
    }
  }
  for (uint64_t i = 0; i <= max_levels; i++) {
    auto node = leftmostRVTNode_[i];
    if (!node) {
      Serializable::serialize(os, null_node_id);
    } else {
      Serializable::serialize(os, node->info_.id());
    }
  }
  // temp
  LOG_INFO(logger_, "xxx");
  printToLog(LogPrintVerbosity::DETAILED);
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
  RVTMetadata data{};
  Serializable::deserialize(is, reinterpret_cast<char*>(&data), sizeof(data));

  if ((data.magic_num != magic_num_) || (data.version_num != version_num_) || (data.RVT_K != RVT_K) ||
      (data.fetch_range_size != fetch_range_size_) || (data.value_size != value_size_)) {
    LOG_ERROR(logger_, "Failed to deserialize metadata");
    clear();
    return false;
  }

  // populate id_to_node_ map
  // Serializable::deserialize(is, data.total_nodes);
  id_to_node_.reserve(data.total_nodes);
  uint64_t min_rvb_index{std::numeric_limits<uint64_t>::max()}, max_rvb_index{0};
  for (uint64_t i = 0; i < data.total_nodes; i++) {
    auto node = RVTNode::createFromSerialized(is);
    id_to_node_.emplace(node->info_.id(), node);

    if (node->info_.level() == 1) {
      // level 0 child IDs can be treated as rvb_indexes
      if (node->minChildId() < min_rvb_index) {
        min_rvb_index = node->minChildId();
      }
      if ((node->minChildId() + node->numChilds() - 1) > max_rvb_index) {
        max_rvb_index = (node->minChildId() + node->numChilds() - 1);
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
  auto max_levels = root_->info_.level();
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
      parent_rvb_index = ((--rvb_group_index) * RVT_K) + 1;
    } else {
      parent_rvb_index = (rvb_group_index * RVT_K) + 1;
    }
    RVBGroupId rvb_group_id = NodeInfo(RVTNode::kDefaultRVTLeafLevel, parent_rvb_index).id();
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

  auto minChildId = iter->second->minChildId();
  for (size_t rvb_index{minChildId}; rvb_index < minChildId + iter->second->numChilds(); ++rvb_index) {
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
  RVBGroupId parent_node_id = NodeInfo(RVTNode::kDefaultRVTLeafLevel, parent_rvb_index).id();
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
