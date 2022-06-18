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

#pragma once

#include <iostream>
#include <vector>
#include <memory>
#include <unordered_map>
#include <string>
#include <cmath>
#include <limits>
#include <unordered_set>

#include <cryptopp/integer.h>

#include "Digest.hpp"
#include "Serializable.h"
#include "Logger.hpp"
#include "Metrics.hpp"

namespace bftEngine::bcst::impl {

using concordMetrics::GaugeHandle;
using concordMetrics::Aggregator;
using concordMetrics::CounterHandle;

using RVBGroupId = uint64_t;
using RVBId = uint64_t;
using RVBIndex = uint64_t;

// A Range Validation Tree (RVT)
//
// RVT is used to confirm that blocks collected from any single source (part of consensus network) during State Transfer
// are valid and true. This is done by validating digest of certain blocks at frequent interval (RVB) against the tree.
// RVT would be updated during checkpointing and also in the context of pruning. New nodes would be added to RVT during
// checkpointing where as existing nodes would be deleted when pruning on old blocks begins.
// Comment: this data structure is easy to be used to validate any type of data, not only block digests.
//
// Insertion and removal of nodes:
// * Insertion: nodes are inserted to the right, with the next expected RVB ID by calling addRightNode.
// * Removal: nodes are removed from the left, with the next expected RVB ID by calling removeLeftnode.
//
// Terms used throughout code -
// 1. RVT_K = Maximum number of children any node can have
// 2. Fetch range = Not all blocks will be validated. Only max block from given fetch range will be validated.
// 3. RVB Id = Any block id in multiple of fetch range size
// 4. RVB Index = RVBId / fetch range size (RVB Index must divide to a positive integer to be valid)
// 5. RVB GroupIndex (RVBGroupId) = Minimum of RVBIndex of all childrens.
//
// Things to remember -
// 1. Tree does not store RVB nodes.
// 2. Only blocks at specific interval are validated to improve replica recovery time.
// 3. Each node in tree is represented having type as NodeInfo.
// 4. NodeVal is stored in form of CryptoPP::Integer.
//
// Implementation notes -
// 1. APIs do not throw exception
// 2. Thread safety is delegated to caller (e.g. RVBManager)
// 3. Bad inputs values are asserted
//

class RangeValidationTree {
  // The next friend declerations are used strictly for testing
  friend class BcStTestDelegator;

  using NodeVal_t = CryptoPP::Integer;

 public:
  /////////////////////////// API /////////////////////////////////////
  RangeValidationTree(const logging::Logger& logger, uint32_t RVT_K, uint32_t fetch_range_size, size_t value_size = 32);
  ~RangeValidationTree() = default;
  RangeValidationTree(const RangeValidationTree&) = delete;
  RangeValidationTree& operator=(RangeValidationTree&) = delete;

  // Add a node to the right of the tree. Expected id: (getMaxRvbId() + 1) * fetch_range_size
  // If RVB is invalid, we terminate (internal bug).
  void addRightNode(const RVBId id, const char* data, size_t data_size);

  // Remove a node from the left of the tree. Expected id: (getMinRvbId() - 1) * fetch_range_size
  // If RVB is invalid, we terminate (internal bug).
  void removeLeftNode(const RVBId id, const char* data, size_t data_size);

  // Return complete tree along with metadata in serialized format
  // In case of failure can assert
  std::ostringstream getSerializedRvbData() const;

  // Initialize metadata & build tree by deserializing input stream
  // If function fails, tree reset to null and returns false.
  // In case of failure can assert
  bool setSerializedRvbData(std::istringstream& iss);

  // Returns RVB group ids for the range [start_block_id, end_block_id] in ascending order.
  // In case of failure, returns an empty vector.
  // The blocks start/end should be RVB Block Ids.
  // if must_be_in_tree is true, every RVBGroupId returned in the vector is expected to be part of the tree, else all
  // ids are not checked against the tree.
  std::vector<RVBGroupId> getRvbGroupIds(RVBId start_rvb_id, RVBId end_rvb_id, bool must_be_in_tree) const;

  // Returns all actual childs in ascending order. In case of failure, returns empty vector.
  std::vector<RVBId> getRvbIds(RVBGroupId id) const;

  // Returns value of direct parent of RVB. In case of failure, returns empty string with size zero.
  std::string getDirectParentValueStr(RVBId rvb_id) const;

  // Return the min RVB ID in the tree. Return 0 if tree is empty.
  RVBId getMinRvbId() const;

  // Return the max RVB ID in the tree. Return 0 if tree is empty.
  RVBId getMaxRvbId() const;

  // Returns true if tree is empty.
  bool empty() const { return (id_to_node_.size() == 0) ? true : false; }

  // Returns current state of the tree. In practice: a hexadecimal format string represting the root current value.
  const std::string getRootCurrentValueStr() const { return root_ ? root_->current_value_.toString() : ""; }

  // Clear the tree
  void clear() noexcept;

  enum class LogPrintVerbosity { DETAILED, SUMMARY };

  // Prints current tree information to log. Basic information is always printed. If verbosity is DETAILED and there
  // less than kMaxNodesToPrintStructure+1 nodes in the tree, structure is printed as well.
  // The label is an optional string, to mark the caller.
  void printToLog(LogPrintVerbosity verbosity, std::string&& user_label = "") const noexcept;

  // Validate structure and values inside tree. In case of failure can assert.
  bool validate() const noexcept;

  size_t totalNodes() const { return id_to_node_.size(); }
  size_t totalLevels() const { return root_ ? root_->info_.level() : 0; }

  // Metrics
  void UpdateAggregator() { metrics_component_.UpdateAggregator(); }
  concordMetrics::Component& getMetricComponent() { return metrics_component_; }
  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) {
    metrics_component_.SetAggregator(aggregator);
  }

 public:
  struct NodeVal {
    static NodeVal_t kNodeValueModulo_;
    static NodeVal_t calcModulo(size_t val_size);

    NodeVal(const std::shared_ptr<char[]>&& val, size_t size);
    NodeVal(const char* val_ptr, size_t size);
    explicit NodeVal(const NodeVal_t* val);
    explicit NodeVal(const NodeVal_t& val);
    explicit NodeVal(const NodeVal_t&& val);
    NodeVal();

    NodeVal& operator+=(const NodeVal& other);
    NodeVal& operator-=(const NodeVal& other);
    bool operator!=(const NodeVal& other);
    bool operator==(const NodeVal& other);

    const NodeVal_t& getVal() const { return val_; }
    std::string toString() const noexcept;
    std::string getDecoded() const noexcept;
    size_t getSize() const { return val_.MinEncodedSize(); }

    static constexpr size_t kDigestContextOutputSize = DIGEST_SIZE;
    static constexpr std::array<char, kDigestContextOutputSize> initialValueZeroData{};

    NodeVal_t val_;
  };

  struct NodeInfo {
    explicit NodeInfo(uint64_t node_id) : id_data_(node_id) {}
    NodeInfo(uint8_t l, uint64_t index) : id_data_(l, index) {}
    NodeInfo() = delete;

    bool operator<(const NodeInfo& other) const noexcept {
      return ((level() <= other.level()) || (rvb_index() < other.rvb_index())) ? true : false;
    }
    bool operator!=(const NodeInfo& other) const noexcept {
      return ((level() != other.level()) || (rvb_index() != other.rvb_index())) ? true : false;
    }
    std::string toString() const noexcept;

    static constexpr size_t kNIDBitsPerLevel = 8;
    static constexpr size_t kNIDBitsPerRVBIndex = ((sizeof(uint64_t) * 8) - kNIDBitsPerLevel);  // 56
    static constexpr size_t kMaxLevels = ((0x1 << kNIDBitsPerLevel) - 1);                       // 0xFF
    static constexpr uint64_t kLevelMask = (kMaxLevels) << kNIDBitsPerRVBIndex;                 // 0xFF000000 00000000
    static constexpr uint64_t kRvbIndexMask =
        std::numeric_limits<uint64_t>::max() & (~kLevelMask);  // 0x00FFFFFF FFFFFFFF

    union IdData {
      friend NodeInfo;

     public:
      IdData() = default;
      IdData(uint64_t node_id) : id(node_id){};
      IdData(uint8_t l, uint64_t index) {
        bits.rvb_index = index;
        bits.level = l;
      }

     private:
      struct IdBits {
        uint64_t rvb_index : kNIDBitsPerRVBIndex;
        uint64_t level : kNIDBitsPerLevel;
      } bits;
      const uint64_t id{};
    } id_data_;

   public:
    uint64_t rvb_index() const { return id_data_.bits.rvb_index; };
    uint64_t level() const { return id_data_.bits.level; };
    uint64_t id() const { return id_data_.id; };

    static uint64_t rvb_index(uint64_t id) { return IdData(id).bits.rvb_index; }
    static uint8_t level(uint64_t id) { return IdData(id).bits.level; }
    static uint64_t id(uint8_t level, uint64_t rvb_index);

    // The min/max possible rvt_index of a parent childs, given one of the potential childs rvb index and level (parents
    // level is level+1)
    static inline uint64_t parentRvbIndexFromChild(uint64_t child_rvb_index, uint8_t child_level);

    // For a given child rvb index and child level, calculate the min rvb index of all potential siblings
    // All siblings must have the same parents. Not all siblings must currently be childs of that parent.
    static inline uint64_t minPossibleSiblingRvbIndex(uint64_t child_rvb_index, uint8_t child_level);
    static inline uint64_t maxPossibleSiblingRvbIndex(uint64_t child_rvb_index, uint8_t child_level);

    // for a give RVB index, return the next/prev one depends on level
    // rvb_index must be valid and fullfill : (rvb_index-1) % (RVT_K^level) == 0
    static inline uint64_t nextRvbIndex(uint64_t rvb_index, uint8_t level);
    // caller should not call with 1st rvb_index
    static inline uint64_t prevRvbIndex(uint64_t rvb_index, uint8_t level);
  };  // NodeInfo

  // Each node, even 0 level nodes, holds a current value and an initial value.
  //
  // The node is constructed with an initial value:
  // 1) Initial value of a level 0 nodes is a function of its ID and an external (user input) message of size N.
  // 2) Initial value of any other value is a function of its ID and a zero message of size N.
  //
  // Initially, both current value and initial values are equal.
  // The tree is maintained in such way that the current value (CV) and the (IV) should have the next equation always
  // true for any level I>0 node in the tree: CV = IV +  Sum(IV of all nodes connected directly or indirectly to that
  // node in lower levels) and also CV - IV = Sum(CV of all direct connected children)
  struct RVBNode {
    RVBNode(uint64_t rvb_index, const char* data, size_t data_size);
    RVBNode(uint8_t level, uint64_t rvb_index);
    RVBNode(uint64_t node_id, char* val, size_t size);
    void logInfoVal(const std::string& prefix = "");
    const std::shared_ptr<char[]> computeNodeInitialValue(NodeInfo& node_id, const char* data, size_t data_size);

    NodeInfo info_;
    NodeVal current_value_;
  };

 public:
#pragma pack(push, 1)
  struct RVTMetadata {
    uint64_t magic_num;
    uint8_t version_num;
    uint32_t RVT_K;
    uint32_t fetch_range_size;
    size_t value_size;
    uint64_t root_node_id;
    uint64_t total_nodes;

    static void staticAssert() noexcept;
  };
#pragma pack(pop)

  struct SerializedRVTNode {
    uint64_t id;
    uint64_t parent_id;
    size_t last_insertion_index;
    std::deque<uint64_t> child_ids;
    size_t current_value_encoded_size;

    static void staticAssert() noexcept;
  };

  struct RVTNode;
  using RVBNodePtr = std::shared_ptr<RVBNode>;
  using RVTNodePtr = std::shared_ptr<RVTNode>;

  struct RVTNode : public RVBNode {
    explicit RVTNode(const RVBNodePtr& child_node);
    RVTNode(uint8_t level, uint64_t rvb_index);
    RVTNode(SerializedRVTNode& node, char* cur_val_ptr, size_t cur_value_size);
    static RVTNodePtr createFromSerialized(std::istringstream& is);

    void addValue(const NodeVal& nvalue);
    void substractValue(const NodeVal& nvalue);
    std::ostringstream serialize() const;

    uint64_t parent_id_;
    std::deque<uint64_t> child_ids_;
    static constexpr size_t kInsertionCounterNotInitialized_ = std::numeric_limits<size_t>::max();
    size_t insertion_counter_;     // When reaches RVT_K, node is considered closed.
    const NodeVal initial_value_;  // We need to keep this value to validate node's current value

    size_t insertionsLeft() const { return RangeValidationTree::RVT_K - insertion_counter_; }
    size_t numChildren() const { return child_ids_.size(); }
    size_t hasNoChilds() const { return child_ids_.empty(); }
    size_t hasChilds() const { return !child_ids_.empty(); }
    const uint64_t minChildId() const { return child_ids_.front(); }
    const uint64_t maxChildId() const { return child_ids_.back(); }
    bool openForInsertion() const { return insertionsLeft() > 0; }
    bool openForRemoval() const { return numChildren() > 0; }
    void pushChildId(uint64_t id);
    void popChildId(uint64_t id);
  };

  // validation functions
 protected:
  static uint64_t pow_uint(uint64_t base, uint64_t exp) noexcept;
  bool isValidRvbId(RVBId block_id) const noexcept;
  bool validateRVBGroupId(RVBGroupId rvb_group_id) const;
  bool validateTreeStructure() const noexcept;
  bool validateTreeValues() const noexcept;

  // Helper functions
  enum class NodeType {
    PARENT,
    RIGHT_SIBLING,  // Sibling: must have common parent
    LEFT_SIBLING,
    LEFTMOST_SAME_LEVEL_NODE,  // Same level node: we not necessarily share the same parent
    RIGHTMOST_SAME_LEVEL_NODE,
    LEFTMOST_CHILD,  // Child: Must be one of my childs
    RIGHTMOST_CHILD,
    LEFTMOST_LEVEL_DOWN_NODE,  // Level down node: not has to be my child. If my level is X, this nodes level is x-1.
    RIGHTMOST_LEVEL_DOWN_NODE
  };
  // will never return the input node. If couldn't find such node, returns nullptr
  RVTNodePtr getRVTNodeByType(const RVTNodePtr& node, NodeType type) const;

  // tree internal manipulation functions
  void addRVBNode(const RVBNodePtr& node);
  void addInternalNode(const RVTNodePtr& node);
  void removeRVBNode(const RVBNodePtr& node);
  void addValueToInternalNodes(const RVTNodePtr& bottom_node, const NodeVal& value);
  void removeAndUpdateInternalNodes(const RVTNodePtr& rvt_node, const NodeVal& value);
  void setNewRoot(const RVTNodePtr& new_root);
  inline RVTNodePtr openForInsertion(uint64_t level) const;
  inline RVTNodePtr openForRemoval(uint64_t level) const;
  inline uint64_t rvbIdToIndex(RVBId rvb_id) const;  // rvb_id must be valid
  inline uint64_t rvbIndexToId(RVBId rvb_id) const;  // rvb_id must be valid

  enum class ArrUpdateType { ADD_NODE_TO_RIGHT, CHECK_REMOVE_NODE };
  void updateOpenRvtNodeArrays(ArrUpdateType update_type, const RVTNodePtr& node);

 protected:
  // vector index represents level in tree
  // level 0 represents RVB node so it would always hold 0x0
  std::array<RVTNodePtr, NodeInfo::kMaxLevels> rightmost_rvt_node_;
  std::array<RVTNodePtr, NodeInfo::kMaxLevels> leftmost_rvt_node_;
  std::map<uint64_t, RVTNodePtr> id_to_node_;
  RVTNodePtr root_{nullptr};
  uint64_t min_rvb_index_{};  // RVB index is (RVB ID / fetch range size). This is the minimal index in the tree.
  uint64_t max_rvb_index_{};  // RVB index is (RVB ID / fetch range size). This is the maximal index in the tree.
  std::unordered_set<uint64_t> node_ids_to_erase_;
  const logging::Logger& logger_;

  // constants
  static uint32_t RVT_K;
  const uint32_t fetch_range_size_{};
  const size_t value_size_{};
  static constexpr size_t kMaxNodesToPrintStructure{100};
  static constexpr uint8_t CHECKPOINT_PERSISTENCY_VERSION{1};
  static constexpr uint8_t version_num_{CHECKPOINT_PERSISTENCY_VERSION};
  static constexpr uint64_t magic_num_{0x1122334455667788};
  static constexpr uint8_t kDefaultRVBLeafLevel = 0;
  static constexpr uint8_t kDefaultRVTLeafLevel = 1;

 protected:
  concordMetrics::Component metrics_component_;
  struct Metrics {
    GaugeHandle rvt_size_in_bytes_;
    GaugeHandle total_rvt_nodes_;
    GaugeHandle total_rvt_levels_;
    GaugeHandle rvt_min_rvb_id_;
    GaugeHandle rvt_max_rvb_id_;
    GaugeHandle serialized_rvt_size_;
    CounterHandle rvt_validation_failures_;
  };
  mutable Metrics metrics_;
};

using LogPrintVerbosity = RangeValidationTree::LogPrintVerbosity;

}  // namespace bftEngine::bcst::impl
