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
#include <mutex>
#include <limits>

#include <cryptopp/integer.h>

#include "Serializable.h"
#include "STDigest.hpp"
#include "Logger.hpp"
#include "kv_types.hpp"

namespace bftEngine::bcst::impl {

using RVBGroupId = uint64_t;
using RVBId = uint64_t;
using RVBIndex = uint64_t;

// A Range Validation Tree (RVT)
//
// RVT is used to confirm that blocks collected from any single source (part of consensus network) during State Transfer
// are valid and true. This is done by validating digest of certain blocks at frequent interval (RVB) against the tree.
// RVT would be updated during checkpointing and also in the context of pruning. New nodes would be added to RVT during
// checkpointing where as existing nodes would be deleted when pruning on old blocks begins.
//
//
// Terms used throughout code -
// 1. RVT_K = Maximum number of children any node can have
// 2. Fetch range = Not all blocks will be validated. Only max block from given fetch range will be validated.
// 3. RVB Id = Any block id in multiple of fetch range size
// 4. RVB Index = RVBId / fetch range size
// 5. RVB GroupIndex = Minimum of RVBIndex of all childrens
//
// Things to remember -
// 1. Tree does not store RVB nodes.
// 2. Only blocks at specific interval are validated to improve replica recovery time.
// 3. Each node in tree is represented as NodeInfo having type.
// 4. NodeVal is stored in form of CryptoPP::Integer.

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

  // These API deals with range validation block ids
  void addNode(const RVBId id, const STDigest& digest);
  void removeNode(const RVBId id, const STDigest& digest);
  // Return complete tree along with metadata in serialized format
  std::ostringstream getSerializedRvbData() const;
  // Initialize metadata & build tree by deserializing input stream
  // If function fails, tree reset to null and returns false.
  bool setSerializedRvbData(std::istringstream& iss);
  // Returns RVB group ids in ascending order. In case of error, returns empty vector.
  std::vector<RVBGroupId> getRvbGroupIds(RVBId start_block_id, RVBId end_block_id) const;
  // Returns min actual and max actual RVB ids in ascending order. In case of error, returns empty vector.
  std::vector<RVBId> getRvbIds(RVBGroupId id) const;
  // Returns value of direct parent on RVB. In case of error, returns empty string with size zero.
  std::string getDirectParentValueStr(RVBId rvb_id) const;
  // Return the min RVB ID in the tree. Return 0 if tree is empty.
  RVBId getMinRvbId() const;
  // Return the max RVB ID in the tree. Return 0 if tree is empty.
  RVBId getMaxRvbId() const;
  bool empty() const { return (id_to_node_.size() == 0) ? true : false; }
  const std::string getRootCurrentValueStr() const { return root_ ? root_->current_value_.toString() : ""; }
  void printToLog(bool only_node_id) const noexcept;
  bool validate() const noexcept;

 public:
  struct NodeVal {
    static NodeVal_t kNodeValueMax_;
    static NodeVal_t kNodeValueModulo_;
    static NodeVal_t calcMaxValue(size_t val_size);
    static NodeVal_t calcModulo(size_t val_size);

    NodeVal(const shared_ptr<char[]>&& val, size_t size);
    NodeVal(const char* val_ptr, size_t size);
    NodeVal(const NodeVal_t* val);
    NodeVal(const NodeVal_t& val);
    NodeVal(const NodeVal_t&& val);
    NodeVal();

    NodeVal& operator+=(const NodeVal& other);
    NodeVal& operator-=(const NodeVal& other);
    bool operator!=(const NodeVal& other);
    bool operator==(const NodeVal& other);
    bool operator==(NodeVal& other) const { return val_ == other.val_; }

    const NodeVal_t& getMaxVal() const { return kNodeValueMax_; }
    const NodeVal_t& getVal() const { return val_; }
    std::string toString() const noexcept;
    std::string getDecoded() const noexcept;
    size_t getSize() const { return val_.MinEncodedSize(); }

    NodeVal_t val_;
  };

  struct NodeInfo {
    NodeInfo(uint64_t node_id)
        : level(node_id >> kNIDBitsPerRVBIndex),
          rvb_index(node_id & kRvbIndexMask),
          id((static_cast<uint64_t>(level) << kNIDBitsPerRVBIndex) | rvb_index) {}
    NodeInfo(uint8_t l, uint64_t index)
        : level(l),
          rvb_index(index & kRvbIndexMask),
          id((static_cast<uint64_t>(level) << kNIDBitsPerRVBIndex) | rvb_index) {}
    NodeInfo() = delete;

    bool operator<(NodeInfo& other) const noexcept {
      return ((level <= other.level) || (rvb_index < other.rvb_index)) ? true : false;
    }
    bool operator!=(NodeInfo& other) const noexcept {
      return ((level != other.level) || (rvb_index != other.rvb_index)) ? true : false;
    }
    std::string toString() const noexcept;

    static constexpr size_t kNIDBitsPerLevel = 8;
    static constexpr size_t kNIDBitsPerRVBIndex = ((sizeof(uint64_t) * 8) - kNIDBitsPerLevel);
    static constexpr size_t kMaxLevels = ((0x1 << kNIDBitsPerLevel) - 1);
    static constexpr uint64_t kRvbIdMask = (kMaxLevels) << kNIDBitsPerRVBIndex;
    static constexpr uint64_t kRvbIndexMask = std::numeric_limits<uint64_t>::max() & (~kRvbIdMask);

    // node location in the tree
    const uint64_t level : kNIDBitsPerLevel;
    const uint64_t rvb_index : kNIDBitsPerRVBIndex;

    // node ID
    const uint64_t id;
  };

  struct RVBNode {
    RVBNode(uint64_t rvb_index, const STDigest& digest);
    RVBNode(uint8_t level, uint64_t rvb_index);
    RVBNode(uint64_t node_id, char* val, size_t size);

    bool isMinChild() { return info_.rvb_index % RVT_K == 1; }
    bool isMaxChild() { return info_.rvb_index % RVT_K == 0; }
    void logInfoVal(std::string prefix = "");
    const shared_ptr<char[]> computeNodeInitialValue(NodeInfo& node_id, const STDigest& digest);

    static constexpr uint8_t kDefaultRVBLeafLevel{0};
    NodeInfo info_;
    NodeVal current_value_;
    static uint32_t RVT_K;
  };

 public:
#pragma pack(push, 1)
  struct RVTMetadata {
    uint64_t magic_num{0};
    uint8_t version_num{0};
    uint32_t RVT_K{0};
    uint32_t fetch_range_size{0};
    size_t value_size{0};
    uint64_t total_nodes{0};
    uint64_t root_node_id{0};

    static void staticAssert() noexcept;
  };
#pragma pack(pop)

  struct SerializedRVTNode {
    uint64_t id{0};
    size_t current_value_size{0};
    size_t initial_value_size{0};
    uint16_t n_child{0};
    uint64_t min_child_id{0};
    uint64_t max_child_id{0};
    uint64_t parent_id{0};

    static void staticAssert() noexcept;
  };

  struct RVTNode;
  using RVBNodePtr = std::shared_ptr<RVBNode>;
  using RVTNodePtr = std::shared_ptr<RVTNode>;
  struct RVTNode : public RVBNode {
    RVTNode(const RVBNodePtr& node);
    RVTNode(const RVTNodePtr& node);
    RVTNode(
        SerializedRVTNode& node, char* cur_val_ptr, size_t cur_value_size, char* init_val_ptr, size_t init_value_size);
    static RVTNodePtr createFromSerialized(std::istringstream& is);

    void addValue(const NodeVal& nvalue);
    void substructValue(const NodeVal& nvalue);
    std::ostringstream serialize() const;

    static constexpr uint8_t kDefaultRVTLeafLevel = 1;
    uint16_t n_child{0};
    uint64_t min_child_id{0};      // Minimal actual child id
    uint64_t max_child_id{0};      // Maximal possible child id. The max actual is min_child_id + n_child.
    uint64_t parent_id{0};         // for root - will be 0
    const NodeVal initial_value_;  // We need to keep this value to validate node's current value
  };

 public:
  // validation functions
  static uint64_t pow_int(uint64_t base, uint64_t exp) noexcept;
  size_t totalNodes() const { return id_to_node_.size(); }
  size_t totalLevels() const { return root_ ? root_->info_.level : 0; }

 protected:
  bool isValidRvbId(const RVBId& block_id) const noexcept;
  bool validateRvbId(const RVBId id, const STDigest& digest) const;
  bool validateRVBGroupId(const RVBGroupId rvb_group_id) const;
  bool validateTreeStructure() const noexcept;
  bool validateTreeValues() const noexcept;

  // Helper functions
  RVTNodePtr getRVTNodeOfLeftSibling(RVTNodePtr& node) const;
  RVTNodePtr getRVTNodeOfRightSibling(RVTNodePtr& node) const;
  RVTNodePtr getParentNode(const RVTNodePtr& node) const noexcept;

  // tree internal manipulation functions
  void addRVBNode(const RVBNodePtr& node);
  void addInternalNode(const RVTNodePtr& node);
  void removeRVBNode(const RVBNodePtr& node);
  void addValueToInternalNodes(const RVTNodePtr& bottom_node, const NodeVal& value);
  void removeAndUpdateInternalNodes(const RVTNodePtr& rvt_node, const NodeVal& value);
  void setNewRoot(const RVTNodePtr& new_root);
  RVTNodePtr openForInsertion(uint64_t level) const;
  RVTNodePtr openForRemoval(uint64_t level) const;
  void reset() noexcept;

 protected:
  static constexpr size_t kMaxNodesToPrint{10000};
  // vector index represents level in tree
  // level 0 represents RVB node so it would always hold 0x0
  std::array<RVTNodePtr, NodeInfo::kMaxLevels> rightmostRVTNode_;
  std::array<RVTNodePtr, NodeInfo::kMaxLevels> leftmostRVTNode_;
  std::unordered_map<uint64_t, RVTNodePtr> id_to_node_;
  RVTNodePtr root_{nullptr};
  uint64_t max_rvb_index_{0};  // RVB index is (RVB ID / fetch range size). This is the maximal index in the tree.
  uint64_t min_rvb_index_{0};  // RVB index is (RVB ID / fetch range size). This is the minimal index in the tree.

  const logging::Logger& logger_;
  const uint32_t RVT_K{0};
  const uint32_t fetch_range_size_{0};
  const size_t value_size_{0};
  static constexpr uint8_t CHECKPOINT_PERSISTENCY_VERSION{1};
  static constexpr uint8_t version_num_{CHECKPOINT_PERSISTENCY_VERSION};
  static constexpr uint64_t magic_num_{0x1122334455667788};
};

}  // namespace bftEngine::bcst::impl
