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
#include "Digest.hpp"
#include "type_traits"
#include "throughput.hpp"

using concord::util::digest::DigestUtil;

using namespace std;
using namespace concord::serialize;

namespace bftEngine::bcst::impl {

using NodeVal = RangeValidationTree::NodeVal;
using NodeVal_t = CryptoPP::Integer;
using RVTNode = RangeValidationTree::RVTNode;
using RVBNode = RangeValidationTree::RVBNode;
using NodeInfo = RangeValidationTree::NodeInfo;
using RVTNodePtr = RangeValidationTree::RVTNodePtr;

// uncomment to add debug prints
// #define RANGE_VALIDATION_TREE_DO_DEBUG

#ifdef RANGE_VALIDATION_TREE_DO_DEBUG
#undef DEBUG_PRINT
#define DEBUG_PRINT(x, y) LOG_INFO(x, y)
#define logInfoVal(x) logInfoVal(x)
#else
#define DEBUG_PRINT(x, y)
#define logInfoVal(x)
#endif

/////////////////////////////////////////// NodeVal ////////////////////////////////////////////////

NodeVal_t NodeVal::calcModulo(size_t val_size) {
  NodeVal_t v = NodeVal_t(1);
  v = v << (val_size * 8ULL);
  return v;
}

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
  vector<char> output(val_.MinEncodedSize());
  val_.Encode(reinterpret_cast<CryptoPP::byte*>(output.data()), output.size());
  ostringstream oss;
  for (auto& c : output) {
    oss << c;
  }
  return oss.str();
}

//////////////////////////////// NodeInfo  ///////////////////////////////////
uint64_t NodeInfo::id(uint8_t level, uint64_t rvb_index) {
  IdData data(level, rvb_index);
  return data.id;
}

std::string NodeInfo::toString() const noexcept {
  std::ostringstream os;
  os << " [" << level() << "," << rvb_index() << "]";
  return os.str();
}

uint64_t NodeInfo::parentRvbIndexFromChild(uint64_t child_rvb_index, uint8_t child_level) {
  // formula: int((child_rvb_index-1)/(RVT_K^parent_level))*(RVT_K^parent_level)+1
  auto span_parent = pow_uint(RangeValidationTree::RVT_K, child_level + 1);
  return ((child_rvb_index - 1) / span_parent) * span_parent + 1;
}

uint64_t NodeInfo::minPossibleSiblingRvbIndex(uint64_t child_rvb_index, uint8_t child_level) {
  return parentRvbIndexFromChild(child_rvb_index, child_level);
}

uint64_t NodeInfo::maxPossibleSiblingRvbIndex(uint64_t child_rvb_index, uint8_t child_level) {
  // formula: parentRvbIndexFromChild(child_rvb_index, child_level) + (RVT_K-1)*(RVT_K^child_level)
  return minPossibleSiblingRvbIndex(child_rvb_index, child_level) + (RVT_K - 1) * pow_uint(RVT_K, child_level);
}

uint64_t NodeInfo::nextRvbIndex(uint64_t rvb_index, uint8_t level) {
  // formula: rvb_index + (RVT_K ^ level)
  const auto pow = pow_uint(RVT_K, level);
  ConcordAssertEQ((rvb_index - 1) % pow, 0);
  return rvb_index + pow;
}

uint64_t NodeInfo::prevRvbIndex(uint64_t rvb_index, uint8_t level) {
  // formula: rvb_index - (RVT_K ^ level)
  auto pow = pow_uint(RVT_K, level);
  ConcordAssertEQ((rvb_index - 1) % pow, 0);
  ConcordAssertGT(rvb_index, pow);
  return rvb_index - pow;
}

// TODO - for now, the outDigest is of a fixed size. We can match the final size to RangeValidationTree::value_size
// This requires us to write our own DigestContext
const shared_ptr<char[]> RVBNode::computeNodeInitialValue(NodeInfo& node_info, const char* data, size_t data_size) {
  ConcordAssertGT(node_info.id(), 0);
  DigestUtil::Context c;

  c.update(reinterpret_cast<const char*>(&node_info.id_data_), sizeof(node_info.id_data_));
  c.update(data, data_size);
  // TODO - Use default_delete in case memleak is reported by ASAN
  static std::shared_ptr<char[]> out_digest_buff(new char[NodeVal::kDigestContextOutputSize]);
  c.writeDigest(out_digest_buff.get());
  return out_digest_buff;
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
  static_assert(sizeof(SerializedRVTNode::last_insertion_index) == sizeof(RVTNode::insertion_counter_));
  static_assert(std::is_same<decltype(SerializedRVTNode::child_ids), std::deque<uint64_t>>::value);
  static_assert(std::is_same<decltype(RVTNode::child_ids_), std::deque<uint64_t>>::value);
}

#ifdef __RVT_DO_DEBUG
void RVBNode::logInfoVal(const std::string& prefix) {
  ostringstream oss;
  oss << prefix << info_.toString() << " (" << info_.id() << ") " << current_value_.toString();
  // Keep for debug, do not remove please!
  DEBUG_PRINT(GL, oss.str());
}
#endif

// level 0
RVBNode::RVBNode(uint64_t rvb_index, const char* data, size_t data_size)
    : info_(kDefaultRVBLeafLevel, rvb_index),
      current_value_(computeNodeInitialValue(info_, data, data_size), NodeVal::kDigestContextOutputSize) {
  ConcordAssertEQ(info_.level(), kDefaultRVBLeafLevel);
  logInfoVal("construct: ");  // leave for debugging
}

// level 1 and up
RVBNode::RVBNode(uint8_t level, uint64_t rvb_index)
    : info_(level, NodeInfo::minPossibleSiblingRvbIndex(rvb_index, level - 1)),
      current_value_(
          computeNodeInitialValue(info_, NodeVal::initialValueZeroData.data(), NodeVal::kDigestContextOutputSize),
          NodeVal::kDigestContextOutputSize) {
  ConcordAssertGE(level, kDefaultRVTLeafLevel);
  logInfoVal("construct: ");  // leave for debugging
}

// serialized in memory nodes
RVBNode::RVBNode(uint64_t node_id, char* val_ptr, size_t size) : info_(node_id), current_value_(val_ptr, size) {
  ConcordAssertGE(info_.level(), kDefaultRVTLeafLevel);
  logInfoVal("construct: ");  // leave for debugging
}

// Level 1 RVT Node ctor
RVTNode::RVTNode(const shared_ptr<RVBNode>& child_node)
    : RVBNode(kDefaultRVTLeafLevel, child_node->info_.rvb_index()),
      parent_id_{},
      insertion_counter_{kInsertionCounterNotInitialized_},
      initial_value_(current_value_) {}

// level 2 and up nodes
RVTNode::RVTNode(uint8_t level, uint64_t rvb_index)
    : RVBNode(level + 1, rvb_index),
      parent_id_{},
      // node is initially closed, until 1st push. Then the counter is initialized, depends on child rvb_index
      insertion_counter_{kInsertionCounterNotInitialized_},
      initial_value_(current_value_) {
  ConcordAssertLE(info_.level(), NodeInfo::kMaxLevels);
}

// Created from serialized data
RVTNode::RVTNode(SerializedRVTNode& node, char* cur_val_ptr, size_t cur_value_size)
    : RVBNode(node.id, cur_val_ptr, cur_value_size),
      parent_id_(node.parent_id),
      child_ids_(std::move(node.child_ids)),
      insertion_counter_{node.last_insertion_index},
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

  // leave for debugging
  logInfoVal("After:");
}

std::ostringstream RVTNode::serialize() const {
  std::ostringstream os;
  Serializable::serialize(os, info_.id());
  Serializable::serialize(os, parent_id_);
  Serializable::serialize(os, insertion_counter_);
  Serializable::serialize(os, child_ids_);
  Serializable::serialize(os, current_value_.getSize());
  auto decoded_val = current_value_.getDecoded();
  Serializable::serialize(os, decoded_val.data(), current_value_.getSize());
  // We do not serialize initial_value_ since it can be easily re-calculated
  return os;
}

RVTNodePtr RVTNode::createFromSerialized(std::istringstream& is) {
  SerializedRVTNode snode;
  Serializable::deserialize(is, snode.id);
  Serializable::deserialize(is, snode.parent_id);
  Serializable::deserialize(is, snode.last_insertion_index);
  // Serializable::deserialize(is, nchilds);
  Serializable::deserialize(is, snode.child_ids);
  Serializable::deserialize(is, snode.current_value_encoded_size);
  std::unique_ptr<char[]> ptr_cur = std::make_unique<char[]>(snode.current_value_encoded_size);
  Serializable::deserialize(is, ptr_cur.get(), snode.current_value_encoded_size);
  return std::make_shared<RVTNode>(snode, ptr_cur.get(), snode.current_value_encoded_size);
}

// In some cases RVB node might be inserted in as a middle child (see more details in above ctor)
// The next calculation was verified with many examples.
// Here is one:
// RVT_K=12 and level 0 child is inserted on level 3 with an rvb_index 3000 (first insertion to tree)
// We know that current parent has the potential next child span:
// 13*12^2+1, 14*12^+1, ... [until 12th child] 23*12^2+1
// or: 1729	1873	2017	2161	2305	2449	2593	2737	2881	3025	3169	3313
// We would like to find in which index child starts and update number of left insertions.
// 3000 enters on the 8th index. since there is push inside which increments the insertion_counter_, we don't
// increment it here.
void RVTNode::pushChildId(uint64_t id) {
  if (!child_ids_.empty()) {
    ConcordAssert(id > child_ids_.back());
  }
  if (insertion_counter_ == kInsertionCounterNotInitialized_) {
    // The 1st push determine how many insertions left: initialize insertion_counter_
    auto min_sib_rvb_index = NodeInfo::minPossibleSiblingRvbIndex(NodeInfo::rvb_index(id), NodeInfo::level(id));
    const auto pow_child = pow_uint(RangeValidationTree::RVT_K, NodeInfo::level(id));
    insertion_counter_ = (NodeInfo::rvb_index(id) - min_sib_rvb_index) / pow_child;
  }
  ConcordAssertLT(insertion_counter_, RVT_K);
  child_ids_.push_back(id);
  ++insertion_counter_;
}

void RVTNode::popChildId(uint64_t id) {
  ConcordAssert(!child_ids_.empty());
  ConcordAssertEQ(child_ids_.front(), id);
  child_ids_.pop_front();
}

uint32_t RangeValidationTree::RVT_K{};

RangeValidationTree::RangeValidationTree(const logging::Logger& logger,
                                         uint32_t RVT_K,
                                         uint32_t fetch_range_size,
                                         size_t value_size)
    : rightmost_rvt_node_{},
      leftmost_rvt_node_{},
      logger_(logger),
      fetch_range_size_(fetch_range_size),
      value_size_(value_size),
      metrics_component_{
          concordMetrics::Component("range_validation_tree", std::make_shared<concordMetrics::Aggregator>())},
      metrics_{metrics_component_.RegisterGauge("rvt_size_in_bytes", 0),
               metrics_component_.RegisterGauge("total_rvt_nodes", 0),
               metrics_component_.RegisterGauge("total_rvt_levels", 0),
               metrics_component_.RegisterGauge("rvt_min_rvb_id", 0),
               metrics_component_.RegisterGauge("rvt_max_rvb_id", 0),
               metrics_component_.RegisterGauge("serialized_rvt_size", 0),
               metrics_component_.RegisterCounter("rvt_validation_failures")} {
  LOG_INFO(logger_, KVLOG(RVT_K, fetch_range_size_, value_size));
  NodeVal::kNodeValueModulo_ = NodeVal::calcModulo(value_size_);
  ConcordAssert(NodeVal::kNodeValueModulo_ != NodeVal_t(static_cast<signed long>(0)));
  RVTMetadata::staticAssert();
  SerializedRVTNode::staticAssert();
  RangeValidationTree::RVT_K = RVT_K;
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

bool RangeValidationTree::validateTreeStructure() const noexcept {
  if (root_ and empty()) {
    LOG_ERROR(logger_, "Found stale root with empty tree");
    return false;
  }
  if (!root_ and !empty()) {
    LOG_ERROR(logger_, "Found null root with stale map");
    return false;
  }
  if (root_ == nullptr) {
    return true;
  }
  if (totalLevels() > NodeInfo::kMaxLevels) {
    LOG_ERROR(logger_, "Found corrupt root node");
    return false;
  }
  // Root at level more than 1 should always have more than 1 child.
  if (root_->info_.level() > kDefaultRVTLeafLevel && root_->numChildren() == 1) {
    LOG_ERROR(logger_, "Root having single child. Issue with tree rotation during remove operations?");
    return false;
  }
  // Leftmost & righmost nodes at levels lesser than root should never be empty.
  for (size_t i = kDefaultRVTLeafLevel; i < totalLevels(); i++) {
    if (leftmost_rvt_node_[i] == nullptr || rightmost_rvt_node_[i] == nullptr) {
      LOG_ERROR(logger_, "Could be issue with leftmost, rightmost calculation");
      return false;
    } else {
      if (id_to_node_.find(leftmost_rvt_node_[i]->info_.id()) == id_to_node_.end()) {
        LOG_ERROR(logger_, "Could be issue with leftmost, rightmost calculation");
        return false;
      }
      if (leftmost_rvt_node_[i]->info_.level() != i) {
        LOG_ERROR(logger_, "Level mismatch");
        return false;
      }
    }
    if (leftmost_rvt_node_[i]->info_.id() > rightmost_rvt_node_[i]->info_.id()) {
      LOG_ERROR(logger_, "Validate min, max rvb id calcuation");
      return false;
    }
  }
  // Negative test to confirm that levels above root are set to nullptr
  for (size_t i = totalLevels() + 1; i < NodeInfo::kMaxLevels; i++) {
    if (leftmost_rvt_node_[i] != nullptr || rightmost_rvt_node_[i] != nullptr) {
      LOG_ERROR(logger_, "Could be issue with cleanup during remove operation.");
      return false;
    }
  }
  // Travel to root from all nodes present at level 1.
  // Count number of nodes visited. They should never be more than total levels in tree.
  if (totalLevels() > kDefaultRVTLeafLevel) {
    auto current_node = leftmost_rvt_node_[kDefaultRVTLeafLevel];
    do {
      if (current_node->info_.rvb_index() > max_rvb_index_) {
        LOG_ERROR(logger_, "Max rvb index found corrupted");
        return false;
      }
      RVTNodePtr copy_of_current_node = current_node;
      RVTNodePtr parent_node;
      for (size_t i = kDefaultRVTLeafLevel; i <= totalLevels(); ++i) {
        parent_node = getRVTNodeByType(current_node, NodeType::PARENT);
        // nodes in path before reaching root should not be null
        if (parent_node == nullptr && i != totalLevels()) {
          LOG_ERROR(logger_, "Could be issue with setting up parent_id");
          return false;
        }
        // parent of root node should be null
        // parent id of root node should be 0
        if ((i == totalLevels()) && (parent_node || current_node->parent_id_)) {
          LOG_ERROR(logger_, "Could be issue with tree rotation during remove operations");
          return false;
        }
        if (parent_node) {
          // rvb_index of child should be within range covered by parent
          auto rvb_index = current_node->info_.rvb_index();
          auto level = current_node->info_.level();
          if (rvb_index > current_node->info_.maxPossibleSiblingRvbIndex(rvb_index, level)) {
            LOG_ERROR(logger_, "Could be issue with creating new root node");
            return false;
          }
          if (rvb_index < current_node->info_.minPossibleSiblingRvbIndex(rvb_index, level)) {
            LOG_ERROR(logger_, "Could be issue with creating new root node");
            return false;
          }
          // next node is correct
          auto right_sibling = getRVTNodeByType(current_node, NodeType::RIGHT_SIBLING);
          if (right_sibling && right_sibling->info_.rvb_index() != current_node->info_.nextRvbIndex(rvb_index, level)) {
            LOG_ERROR(logger_, "Found next node incorrect");
            return false;
          }
        }
        current_node = parent_node;
      }
      current_node = getRVTNodeByType(copy_of_current_node, NodeType::RIGHT_SIBLING);
    } while (current_node);
  }

  return true;
}

bool RangeValidationTree::validateTreeValues() const noexcept {
  if (root_ == nullptr && empty()) {
    return true;
  }

  // Currently we do not get any level 0 data from outside, so we cannot validate level 1
  // TODO - add support for level 1 validation
  if (totalLevels() <= kDefaultRVTLeafLevel) {
    return true;
  }

  // Lets scan all non level 1 nodes in the map
  auto iter = id_to_node_.begin();
  ConcordAssert(iter->second != nullptr);
  std::set<uint64_t> validated_ids;

  do {
    NodeVal sum_of_childs{};

    if (iter == id_to_node_.end()) {
      break;
    }

    auto current_node = iter->second;

    if (current_node->info_.level() <= 1) {
      ++iter;
      validated_ids.insert(current_node->info_.id());
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

  // final check: see that we've validated all nodes in tree
  ConcordAssertEQ(validated_ids.size(), id_to_node_.size());

  std::set<uint64_t> id_to_node_keys;
  std::transform(id_to_node_.begin(),
                 id_to_node_.end(),
                 std::inserter(id_to_node_keys, id_to_node_keys.begin()),
                 [](const std::unordered_map<uint64_t, RVTNodePtr>::value_type& pair) { return pair.first; });
  ConcordAssertEQ(id_to_node_keys, validated_ids);

  return true;
}

bool RangeValidationTree::validate() const noexcept {
  auto ret = validateTreeStructure();
  if (!ret) {
    metrics_.rvt_validation_failures_++;
    return false;
  }
  ret = validateTreeValues();
  if (!ret) {
    metrics_.rvt_validation_failures_++;
  }
  return true;
}

void RangeValidationTree::printToLog(LogPrintVerbosity verbosity, string&& user_label) const noexcept {
  if (!root_ or totalNodes() == 0) {
    LOG_INFO(logger_, "Empty RVT");
    return;
  }
  std::ostringstream oss;
  if (!user_label.empty()) {
    oss << "Label=" << user_label << ", ";
  }
  oss << "#Levels=" << root_->info_.level() << ", #Nodes=" << totalNodes() << ", min_rvb_index_=" << min_rvb_index_
      << ", max_rvb_index_=" << max_rvb_index_ << ", RVT_K=" << RVT_K << ", FRS=" << fetch_range_size_
      << ", value_size=" << value_size_;

  // For large trees, print only basic info without structure
  if ((totalNodes() > kMaxNodesToPrintStructure) || (verbosity == LogPrintVerbosity::SUMMARY)) {
    LOG_INFO(logger_, oss.str());
    return;
  }

  oss << " ,Structure:";
  queue<RVTNodePtr> q;
  q.push(root_);
  while (q.size()) {
    auto& node = q.front();
    oss << node->info_.toString() << " ";

    oss << ", level=" << node->info_.level() << ", rvb_index=" << node->info_.rvb_index()
        << ", insertion_counter_=" << node->insertion_counter_ << ", current_value_=" << node->current_value_.toString()
        << ", min_cid=" << NodeInfo(node->minChildId()).toString()
        << ", max_cid=" << NodeInfo(node->maxChildId()).toString();

    // Keep for debugging
    // ConcordAssert(node->hasChilds());
    // oss << " child_ids:";
    // for (const auto& cid : node->child_ids_) {
    //   oss << NodeInfo(cid).toString();
    // }
    oss << "|";
    if (node->info_.level() == 1) {
      q.pop();
      continue;
    }

    auto child_node = getRVTNodeByType(node, NodeType::LEFTMOST_LEVEL_DOWN_NODE);
    ConcordAssert(child_node != nullptr);
    for (const auto& cid : node->child_ids_) {
      auto iter = id_to_node_.find(cid);
      ConcordAssert(iter != id_to_node_.end());
      q.push(iter->second);
    }
    q.pop();
  }
  LOG_INFO(logger_, oss.str());
}

bool RangeValidationTree::isValidRvbId(RVBId block_id) const noexcept {
  return ((block_id != 0) && (fetch_range_size_ != 0) && (block_id % fetch_range_size_ == 0));
}

bool RangeValidationTree::validateRVBGroupId(RVBGroupId rvb_group_id) const {
  NodeInfo node_info(rvb_group_id);
  if (rvb_group_id == 0) {
    return false;
  }
  return ((node_info.level() == kDefaultRVTLeafLevel) && ((node_info.rvb_index() % RVT_K) == 1));
}

RVTNodePtr RangeValidationTree::getRVTNodeByType(const RVTNodePtr& node, NodeType type) const {
  std::deque<uint64_t>::iterator iter_target;
  RVTNodePtr parent;
  uint64_t node_id{};

  if (type == NodeType::LEFTMOST_LEVEL_DOWN_NODE) {
    if (node->info_.level() == 1) {
      return nullptr;
    }
    return leftmost_rvt_node_[node->info_.level()];
  } else if (type == NodeType::RIGHTMOST_LEVEL_DOWN_NODE) {
    if (node->info_.level() == 1) {
      return nullptr;
    }
    return rightmost_rvt_node_[node->info_.level()];
  } else if (type == NodeType::LEFTMOST_SAME_LEVEL_NODE) {
    auto leftmost_sib = leftmost_rvt_node_[node->info_.level()];
    if (leftmost_rvt_node_[node->info_.level()] == node) {
      return nullptr;
    }
    return leftmost_sib;
  } else if (type == NodeType::RIGHTMOST_SAME_LEVEL_NODE) {
    auto rightmost_sib = rightmost_rvt_node_[node->info_.level()];
    if (rightmost_rvt_node_[node->info_.level()] == node) {
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
  auto parent_node = openForInsertion(kDefaultRVTLeafLevel);
  if (!parent_node) {
    // adding first rvb parent_node but it might not be first child (reason: pruning)
    parent_node = make_shared<RVTNode>(rvb_node->info_.level(), rvb_node->info_.rvb_index());
    parent_node->pushChildId(rvb_node->info_.id());
    updateOpenRvtNodeArrays(ArrUpdateType::ADD_NODE_TO_RIGHT, parent_node);
    ConcordAssert(id_to_node_.insert({parent_node->info_.id(), parent_node}).second == true);
    // Keep for debug
    DEBUG_PRINT(logger_, "Added node" << parent_node->info_.toString() << " id " << parent_node->info_.id());
    if (!root_) {  // TODO - is this needed???
      setNewRoot(parent_node);
    }
    addInternalNode(parent_node);
  } else {
    parent_node->pushChildId(rvb_node->info_.id());
    ConcordAssertLE(parent_node->numChildren(), RVT_K);
  }
  addValueToInternalNodes(parent_node, rvb_node->current_value_);
}

void RangeValidationTree::removeRVBNode(const shared_ptr<RVBNode>& rvb_node) {
  auto node = openForRemoval(kDefaultRVTLeafLevel);
  ConcordAssert(node != nullptr);

  if (node->numChildren() == 1) {
    // no more RVB childs, erase the node
    auto id = node->info_.id();
    auto node_to_remove_iter = id_to_node_.find(id);
    ConcordAssert(node_to_remove_iter != id_to_node_.end());
    updateOpenRvtNodeArrays(ArrUpdateType::CHECK_REMOVE_NODE, node);
    node_ids_to_erase_.insert(id);
    auto val_negative = node->initial_value_;
    val_negative.val_.SetNegative();
    addValueToInternalNodes(getRVTNodeByType(node, NodeType::PARENT), val_negative);
  }
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
  ConcordAssertEQ(node_to_add->parent_id_, 0);
  RVTNodePtr parent_node, bottom_node;
  auto current_node = node_to_add;
  NodeVal val_to_add = current_node->initial_value_;

  while (current_node->info_.level() != root_->info_.level()) {
    if (current_node->parent_id_ == 0) {
      parent_node = openForInsertion(current_node->info_.level() + 1);
      if (parent_node) {
        // Add the current_node to parent_node which still has more space for childs
        current_node->parent_id_ = parent_node->info_.id();
        parent_node->pushChildId(current_node->info_.id());
        ConcordAssert(parent_node->numChildren() <= RVT_K);
        parent_node->addValue(val_to_add);
      } else {
        // construct new internal RVT current_node
        parent_node = make_shared<RVTNode>(current_node->info_.level(), current_node->info_.rvb_index());
        parent_node->pushChildId(current_node->info_.id());
        parent_node->addValue(val_to_add);
        val_to_add += parent_node->initial_value_;
        ConcordAssert(id_to_node_.insert({parent_node->info_.id(), parent_node}).second == true);
        // Keep for debug
        DEBUG_PRINT(logger_, "Added node" << parent_node->info_.toString() << " id " << parent_node->info_.id());
        updateOpenRvtNodeArrays(ArrUpdateType::ADD_NODE_TO_RIGHT, parent_node);
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

  /* If we are here, we might need to create multiple levels, dependent on belonging of current_node to an RVB group.
    Tree is aligned to rvb index 1.
    There might be a tree like this (RVT_K=4)
    L3        1
    L2   33			    49
    L1   45			    49
    L0   46	47	48	49

    In the above tree L0 is in storage. (L1,45) is parent of 46 to 48. (L1,49) is parent of 49. 45 and 49 can't share
    the same parent, according to the formula presented in parentRvbIndexFromChild. We continue creating parents at
    level 2, which in this special example do not share same parent as well. (L3,1) span  already cover (L2,33) and
    (L2,49),
    therefore it becomes the new root.
  */
  uint64_t current_root_parent_rvb_index{}, current_root_sibling_rvb_index{};
  do {
    // 1) We have the root, and a new sibling (current_node) on its side. We need to find a common parent (new_root)
    current_root_parent_rvb_index = NodeInfo::parentRvbIndexFromChild(root_->info_.rvb_index(), root_->info_.level());
    current_root_sibling_rvb_index =
        NodeInfo::parentRvbIndexFromChild(current_node->info_.rvb_index(), current_node->info_.level());

    // 2 Create new root 1 level above, according to root_ info, then attach root to the new root and set it as new
    // root.
    auto new_root = make_shared<RVTNode>(root_->info_.level(), root_->info_.rvb_index());
    new_root->pushChildId(root_->info_.id());
    new_root->addValue(root_->current_value_);
    ConcordAssert(id_to_node_.insert({new_root->info_.id(), new_root}).second == true);
    // Keep for debug
    DEBUG_PRINT(logger_, "Added node" << new_root->info_.toString() << " id " << new_root->info_.id());
    setNewRoot(new_root);
    ConcordAssert(new_root->numChildren() <= RVT_K);

    // 3) now we need to connect the right sibling , current_node. It might share a common parent, or we must create a
    // new parent to it. This will trigger the need to create a new root. As we go up the chance that we won't find
    // a common parent decrease in ratio of RVT_K
    if (current_root_parent_rvb_index != current_root_sibling_rvb_index) {
      // create a new sibling to the new  from current_node and call addInternalNode to add it
      auto new_root_sib = make_shared<RVTNode>(current_node->info_.level(), current_node->info_.rvb_index());
      ConcordAssert(id_to_node_.insert({new_root_sib->info_.id(), new_root_sib}).second == true);
      current_node->parent_id_ = new_root_sib->info_.id();
      new_root_sib->pushChildId(current_node->info_.id());
      new_root_sib->addValue(current_node->current_value_);
      updateOpenRvtNodeArrays(ArrUpdateType::ADD_NODE_TO_RIGHT, new_root_sib);
      ConcordAssert(new_root_sib->numChildren() <= RVT_K);
      current_node = new_root_sib;
    } else {
      // common parent found
      current_node->parent_id_ = new_root->info_.id();
      new_root->pushChildId(current_node->info_.id());
      new_root->addValue(current_node->current_value_);
      ConcordAssert(new_root->numChildren() <= RVT_K);
      break;
    }
  } while (current_root_parent_rvb_index != current_root_sibling_rvb_index);
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
      parent_node->popChildId(cur_node->info_.id());
      if (cur_node != rvt_node) {
        node_ids_to_erase_.insert(cur_node->info_.id());
        auto val_negative = cur_node->initial_value_;
        val_negative.val_.SetNegative();
        updateOpenRvtNodeArrays(ArrUpdateType::CHECK_REMOVE_NODE, cur_node);
        addValueToInternalNodes(parent_node, val_negative);
      }
    }
    cur_node->substractValue(value);
    cur_node = parent_node;
  }

  ConcordAssertEQ(cur_node, root_);
  root_->substractValue(value);

  if (root_->hasNoChilds()) {
    setNewRoot(nullptr);
    return;
  }

  // Loop 2
  // Shrink the tree from root to bottom (level 1) . In theory, tree can end empty after this loop
  while ((cur_node == root_) && (cur_node->numChildren() == 1) && (cur_node->info_.level() > 1)) {
    node_ids_to_erase_.insert(cur_node->info_.id());
    ConcordAssertEQ(rightmost_rvt_node_[cur_node->info_.level() - 1], leftmost_rvt_node_[cur_node->info_.level() - 1]);
    updateOpenRvtNodeArrays(ArrUpdateType::CHECK_REMOVE_NODE, cur_node);
    cur_node = rightmost_rvt_node_[cur_node->info_.level() - 1];
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
      updateOpenRvtNodeArrays(ArrUpdateType::CHECK_REMOVE_NODE, root_);
    }
  }
  root_ = new_root;
  root_->parent_id_ = 0;
  DEBUG_PRINT(logger_, "Set new root" << root_->info_.toString() << " id " << root_->info_.id());
  updateOpenRvtNodeArrays(ArrUpdateType::ADD_NODE_TO_RIGHT, new_root);
}

RVTNodePtr RangeValidationTree::openForInsertion(uint64_t level) const {
  if (!rightmost_rvt_node_[level]) {
    return nullptr;
  }
  auto rnode = rightmost_rvt_node_[level];
  return rnode->openForInsertion() ? rnode : nullptr;
}

RVTNodePtr RangeValidationTree::openForRemoval(uint64_t level) const { return leftmost_rvt_node_[level]; }

uint64_t RangeValidationTree::rvbIdToIndex(RVBId rvb_id) const {
  ConcordAssert(isValidRvbId(rvb_id));
  return rvb_id / fetch_range_size_;
}

uint64_t RangeValidationTree::rvbIndexToId(uint64_t rvb_index) const { return rvb_index * fetch_range_size_; }

void RangeValidationTree::updateOpenRvtNodeArrays(ArrUpdateType update_type, const RVTNodePtr& node) {
  auto lvl = node->info_.level();
  auto& leftmost_node = leftmost_rvt_node_[lvl];
  auto& rightmost_node = rightmost_rvt_node_[lvl];

  if (leftmost_node != rightmost_node) {
    // If both are equal then both must be the same node, or nullptr
    // Else, they are not equal - check that non of them is nullptr
    ConcordAssertAND(leftmost_node != nullptr, rightmost_node != nullptr);
  }
  switch (update_type) {
    case (ArrUpdateType::ADD_NODE_TO_RIGHT): {
      rightmost_node = node;
      if (!leftmost_node) {
        leftmost_node = node;
      }
    } break;
    case (ArrUpdateType::CHECK_REMOVE_NODE): {
      // In this type of tree, nodes cannot be removed from the right
      ConcordAssert(!((node == rightmost_node) && (node != leftmost_node)));

      if (node == leftmost_node) {
        auto next_rvb_index = NodeInfo::nextRvbIndex(node->info_.rvb_index(), lvl);
        auto id = NodeInfo::id(lvl, next_rvb_index);
        auto iter = id_to_node_.find(id);
        leftmost_node = (iter == id_to_node_.end()) ? nullptr : iter->second;
      }
      if (node == rightmost_node) {
        // node removals cannot happen just in the right, the come from the left, if this is the right-most
        // node - then all the level is clear of nodes.
        rightmost_node = nullptr;
      }
    } break;
    default:
      LOG_FATAL(logger_, "Unsupported update type!");
  }
}

void RangeValidationTree::clear() noexcept {
  LOG_TRACE(logger_, "");
  // clear() reduced size to 0 and crashed while setting new root using operator []
  for (uint8_t level = 0; level < NodeInfo::kMaxLevels; level++) {
    rightmost_rvt_node_[level] = nullptr;
    leftmost_rvt_node_[level] = nullptr;
  }
  id_to_node_.clear();
  root_ = nullptr;
  max_rvb_index_ = 0;
  min_rvb_index_ = 0;
  node_ids_to_erase_.clear();
}

/////////////////////////////////// start of API //////////////////////////////////////////////

void RangeValidationTree::addRightNode(const RVBId rvb_id, const char* data, size_t data_size) {
  if (!isValidRvbId(rvb_id)) {
    LOG_FATAL(logger_, "invalid input data" << KVLOG(rvb_id, data_size, fetch_range_size_));
  }
  auto rvb_index = rvbIdToIndex(rvb_id);
  auto node = make_shared<RVBNode>(rvb_index, data, data_size);
  if (max_rvb_index_ > 0) {
    ConcordAssertEQ(max_rvb_index_ + 1, rvb_index);
  }
  addRVBNode(node);
  max_rvb_index_ = rvb_index;
  if (min_rvb_index_ == 0) {
    min_rvb_index_ = rvb_index;
  }
  metrics_.rvt_size_in_bytes_.Get().Set(id_to_node_.size() * (sizeof(uint64_t) + sizeof(RVTNode)));
  metrics_.rvt_min_rvb_id_.Get().Set(getMinRvbId());
  metrics_.rvt_max_rvb_id_.Get().Set(getMaxRvbId());
  metrics_.total_rvt_nodes_.Get().Set(totalNodes());
  metrics_.total_rvt_levels_.Get().Set(totalLevels());
  LOG_TRACE(logger_, KVLOG(min_rvb_index_, max_rvb_index_, rvb_id));
}

void RangeValidationTree::removeLeftNode(const RVBId rvb_id, const char* data, size_t data_size) {
  ConcordAssert(root_ != nullptr);
  if (!isValidRvbId(rvb_id)) {
    LOG_FATAL(logger_, "invalid input data" << KVLOG(rvb_id, data_size, fetch_range_size_));
  }
  auto rvb_index = rvbIdToIndex(rvb_id);
  auto node = make_shared<RVBNode>(rvb_index, data, data_size);
  ConcordAssertEQ(min_rvb_index_, rvb_index);
  removeRVBNode(node);
  for (auto id : node_ids_to_erase_) {
    // Keep for debug
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
  metrics_.rvt_size_in_bytes_.Get().Set(id_to_node_.size() * (sizeof(uint64_t) + sizeof(RVTNode)));
  metrics_.rvt_min_rvb_id_.Get().Set(getMinRvbId());
  metrics_.rvt_max_rvb_id_.Get().Set(getMaxRvbId());
  metrics_.total_rvt_nodes_.Get().Set(totalNodes());
  metrics_.total_rvt_levels_.Get().Set(totalLevels());
  LOG_TRACE(logger_, KVLOG(min_rvb_index_, max_rvb_index_, rvb_id));
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

  for (auto& itr : id_to_node_) {
    auto serialized_node = itr.second->serialize();
    Serializable::serialize(os, serialized_node.str().data(), serialized_node.str().size());
  }

  uint64_t null_node_id = 0;
  auto max_levels = root_->info_.level();
  for (uint64_t i = 0; i <= max_levels; i++) {
    auto node = rightmost_rvt_node_[i];
    if (!node) {
      Serializable::serialize(os, null_node_id);
    } else {
      Serializable::serialize(os, node->info_.id());
    }
  }
  for (uint64_t i = 0; i <= max_levels; i++) {
    auto node = leftmost_rvt_node_[i];
    if (!node) {
      Serializable::serialize(os, null_node_id);
    } else {
      Serializable::serialize(os, node->info_.id());
    }
  }

  LOG_TRACE(logger_, KVLOG(os.str().size()));
  metrics_.serialized_rvt_size_.Get().Set(os.str().size());
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
    LOG_WARN(logger_,
             "Failed to deserialize metadata" << KVLOG(data.magic_num,
                                                       magic_num_,
                                                       data.version_num,
                                                       version_num_,
                                                       data.RVT_K,
                                                       RVT_K,
                                                       data.fetch_range_size,
                                                       fetch_range_size_,
                                                       data.value_size,
                                                       value_size_));
    clear();
    return false;
  }

  // populate id_to_node_ map
  uint64_t min_rvb_index{std::numeric_limits<uint64_t>::max()}, max_rvb_index{0};
  for (uint64_t i = 0; i < data.total_nodes; i++) {
    auto node = RVTNode::createFromSerialized(is);
    id_to_node_.emplace(node->info_.id(), node);

    if (node->info_.level() == 1) {
      // level 0 child IDs can be treated as rvb_indexes
      if (node->minChildId() < min_rvb_index) {
        min_rvb_index = node->minChildId();
      }
      if (node->maxChildId() > max_rvb_index) {
        max_rvb_index = node->maxChildId();
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

  // populate rightmost_rvt_node_
  uint64_t node_id;
  uint64_t null_node_id = 0;
  auto max_levels = root_->info_.level();
  for (size_t i = 0; i <= max_levels; i++) {
    Serializable::deserialize(is, node_id);
    if (node_id == null_node_id) {
      rightmost_rvt_node_[i] = nullptr;
    } else {
      rightmost_rvt_node_[i] = id_to_node_[node_id];
    }
  }

  // populate leftmost_rvt_node_
  for (size_t i = 0; i <= max_levels; i++) {
    Serializable::deserialize(is, node_id);
    if (node_id == null_node_id) {
      leftmost_rvt_node_[i] = nullptr;
    } else {
      leftmost_rvt_node_[i] = id_to_node_[node_id];
    }
  }

  is.peek();
  if (not is.eof()) {
    LOG_ERROR(logger_, "Still some data left to read from stream");
    clear();
    return false;
  }

  metrics_.rvt_size_in_bytes_.Get().Set(id_to_node_.size() * (sizeof(uint64_t) + sizeof(RVTNode)));
  metrics_.total_rvt_nodes_.Get().Set(totalNodes());
  metrics_.total_rvt_levels_.Get().Set(totalLevels());
  metrics_.rvt_min_rvb_id_.Get().Set(getMinRvbId());
  metrics_.rvt_max_rvb_id_.Get().Set(getMaxRvbId());
  metrics_.serialized_rvt_size_.Get().Set(is.str().size());
  LOG_TRACE(logger_, "Nodes:" << totalNodes() << " root value:" << root_->current_value_.toString());
  return true;
}

// TODO - move to a common file, this function is duplicated
template <typename T>
static inline std::string vecToStr(const std::vector<T>& vec) {
  std::stringstream ss;
  for (size_t i{0}; i < vec.size(); ++i) {
    if (i != 0) ss << ",";
    ss << vec[i];
  }
  return ss.str();
}

std::vector<RVBGroupId> RangeValidationTree::getRvbGroupIds(RVBId start_rvb_id,
                                                            RVBId end_rvb_id,
                                                            bool must_be_in_tree) const {
  LOG_TRACE(logger_, KVLOG(start_rvb_id, end_rvb_id));
  ConcordAssertAND(isValidRvbId(start_rvb_id), isValidRvbId(end_rvb_id));
  std::vector<RVBGroupId> rvb_group_ids;

  if ((start_rvb_id == 0) || (end_rvb_id == 0) || (start_rvb_id > end_rvb_id) || (!isValidRvbId(start_rvb_id)) ||
      (!isValidRvbId(end_rvb_id))) {
    LOG_ERROR(logger_, "invalid input data" << KVLOG(start_rvb_id, end_rvb_id));
    return rvb_group_ids;
  }

  uint64_t end_rvb_index = rvbIdToIndex(end_rvb_id);
  for (uint64_t current_rvb_index{rvbIdToIndex(start_rvb_id)}; current_rvb_index <= end_rvb_index;) {
    auto parent_rvb_index = NodeInfo::parentRvbIndexFromChild(current_rvb_index, kDefaultRVBLeafLevel);
    RVBGroupId rvb_group_id = NodeInfo::id(kDefaultRVTLeafLevel, parent_rvb_index);
    if (rvb_group_ids.empty() || (rvb_group_ids.back() != rvb_group_id)) {
      if (must_be_in_tree && (id_to_node_.find(rvb_group_id) == id_to_node_.end())) {
        LOG_ERROR(logger_,
                  "Can't find current_rvb_index=" << current_rvb_index << " in id_to_node_!"
                                                  << KVLOG(rvb_group_id, start_rvb_id, end_rvb_id, parent_rvb_index));
        return std::vector<RVBGroupId>();
      }
      rvb_group_ids.push_back(rvb_group_id);
    }
    current_rvb_index = NodeInfo::maxPossibleSiblingRvbIndex(current_rvb_index, kDefaultRVBLeafLevel) + 1;
  }

  LOG_TRACE(logger_, "rvb_group_ids:" << vecToStr(rvb_group_ids));
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

  auto min_child_id = iter->second->minChildId();
  for (size_t rvb_index{min_child_id}; rvb_index <= iter->second->maxChildId(); ++rvb_index) {
    rvb_ids.push_back(rvbIndexToId(rvb_index));
  }
  return rvb_ids;
}

// validation can happen on partial RVGgroup as ST cycle might not fetch 256K blocks
// Example:
// FR = 256 rvbid = 256/1, 512/2, ...
// RVT_K = 1024 * 256 = single RVGGroupId represents 256K blocks
// received only 128K blocks from network
std::string RangeValidationTree::getDirectParentValueStr(RVBId rvb_id) const {
  LOG_TRACE(logger_, KVLOG(rvb_id));
  std::string val;

  if (!isValidRvbId(rvb_id)) {
    return val;
  }

  RVBIndex rvb_index = rvbIdToIndex(rvb_id);
  RVBIndex parent_rvb_index;
  if (rvb_index <= RVT_K) {
    parent_rvb_index = 1;
  } else if (rvb_index % RVT_K == 0) {
    parent_rvb_index = rvb_index - RVT_K + 1;
  } else {
    parent_rvb_index = (rvb_index - (rvb_index % RVT_K)) + 1;
  }
  RVBGroupId parent_node_id = NodeInfo::id(kDefaultRVTLeafLevel, parent_rvb_index);
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
  return rvbIndexToId(max_rvb_index_);
}

//////////////////////////////////// End of API ////////////////////////////////////////////////

}  // namespace bftEngine::bcst::impl
