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

// TODO - add few lines on what is an RVT

// A Range Validation Tree
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
// 4. HashVal is stored in form of CryptoPP::Integer.

class RangeValidationTree {
  // The next friend declerations are used strictly for testing
  friend class BcStTestDelegator;

  using HashVal_t = CryptoPP::Integer;

 public:
  /////////////////////////// API /////////////////////////////////////
  RangeValidationTree(const logging::Logger& logger,
                      uint32_t RVT_K,
                      uint32_t fetch_range_size,
                      size_t hash_size = HashVal::kNodeHashSizeBytes);
  ~RangeValidationTree() = default;
  RangeValidationTree(const RangeValidationTree&) = delete;
  RangeValidationTree& operator=(RangeValidationTree&) = delete;

  // These API deals with range validation block ids and not indexes
  void addNode(const RVBId id, const STDigest& digest);
  void removeNode(const RVBId id, const STDigest& digest);
  // Return complete tree along with metadata in serialized format
  std::ostringstream getSerializedRvbData() const;
  // Initialize metadata & build tree by deserializing input stream
  // If function fails, tree reset to null and returns false.
  bool setSerializedRvbData(std::istringstream& iss);
  std::vector<RVBGroupId> getRvbGroupIds(RVBId start_block_id, RVBId end_block_id) const;
  std::vector<RVBId> getRvbIds(RVBGroupId id) const;
  std::string getDirectParentHashVal(RVBId rvb_id) const;
  bool empty() const { return (id_to_node_.size() == 0) ? true : false; }
  // Return the min RVB ID in the tree. Return 0 if tree is empty.
  RVBId getMinRvbId() const;
  // Return the max RVB ID in the tree. Return 0 if tree is empty.
  RVBId getMaxRvbId() const;

 public:
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

  struct HashMaxValues {
    static const HashVal_t kNodeHashMaxValue_;
    static const HashVal_t kNodeHashModulo_;
    static HashVal_t calcNodeHashMaxValue();
    static HashVal_t calcNodeHashModulo();
    HashMaxValues() = delete;
  };

  struct HashVal {
    HashVal(const shared_ptr<char[]>&& val);
    HashVal(const char* val, size_t size);
    HashVal(const HashVal_t* val);
    HashVal& operator=(const HashVal& other);
    HashVal& operator+=(const HashVal& other);
    HashVal& operator-=(const HashVal& other);
    bool operator==(HashVal& other) const { return hash_val_ == other.hash_val_; }
    const HashVal_t& getMaxVal() const { return HashMaxValues::kNodeHashMaxValue_; }
    const HashVal_t& getVal() const { return hash_val_; }
    std::string valToString() const noexcept;
    std::string getDecodedHashVal() const noexcept;
    size_t getSize() const { return hash_val_.MinEncodedSize(); }

    static constexpr uint8_t kNodeHashSizeBytes = 32;
    static_assert(kNodeHashSizeBytes == BLOCK_DIGEST_SIZE);
    HashVal_t hash_val_;
  };

  struct RVBNode {
    RVBNode(uint64_t rvb_index, const STDigest& digest)
        : id(kDefaultRVBLeafLevel, rvb_index), hash_val(computeNodeHash(id, digest)) {}
    RVBNode(uint8_t level, uint64_t rvb_index) : id(level, rvb_index), hash_val(computeNodeHash(id, STDigest{})) {}
    RVBNode(uint64_t node_id, char* hash_ptr, size_t hash_size) : id(node_id), hash_val(HashVal(hash_ptr, hash_size)) {}

    bool isMinChild() { return id.rvb_index % RVT_K == 1; }
    bool isMaxChild() { return id.rvb_index % RVT_K == 0; }
    const shared_ptr<char[]> computeNodeHash(NodeInfo& node_id, const STDigest& digest);

    static constexpr uint8_t kDefaultRVBLeafLevel{0};
    NodeInfo id;
    HashVal hash_val;
    static uint32_t RVT_K;
  };

 public:
#pragma pack(push, 1)
  struct RVTMetadata {
    uint64_t magic_num{0};
    uint8_t version_num{0};
    uint32_t RVT_K{0};
    uint32_t fetch_range_size{0};
    size_t hash_size{0};
    uint64_t total_nodes{0};
    uint64_t root_node_id{0};
    // TODO num of nodes at each level
  };
#pragma pack(pop)

  struct SerializedRVTNode {
    uint64_t id{0};
    size_t hash_size{0};
    uint16_t n_child{0};
    uint64_t min_child_id{0};
    uint64_t max_child_id{0};
    uint64_t parent_id{0};
  };

  struct RVTNode : public RVBNode {
    RVTNode(std::shared_ptr<RVBNode>& node);
    RVTNode(std::shared_ptr<RVTNode>& node);
    RVTNode(SerializedRVTNode& node, char* hash_val, size_t hash_size);
    static shared_ptr<RVTNode> createFromSerialized(std::istringstream& is);

    // TODO known bug; fix it; verify with GTest
    void addHashVal(const HashVal& hash_val) { this->hash_val += hash_val; }
    void removeHashVal(const HashVal& hash_val) { this->hash_val -= hash_val; }
    std::ostringstream serialize() const;
    uint64_t getRightSiblingId() const noexcept;

    static constexpr uint8_t kDefaultRVTLeafLevel = 1;
    uint16_t n_child{0};
    uint64_t min_child_id{0};  // Minimal actual child id
    uint64_t max_child_id{0};  // Maximal possible child id. The max actual is min_child_id + n_child.
    uint64_t parent_id{0};     // for root - will be 0
  };

  ///////////////////////// Debug / Validate ////////////////////////////////////////////
  size_t totalNodes() const { return id_to_node_.size(); }
  size_t totalLevels() const { return root_ ? root_->id.level : 0; }
  // TODO Should getDecodedHashVal() be used instead?
  const std::string getRootHashVal() const { return root_ ? root_->hash_val.valToString() : ""; }
  void printToLog(bool only_node_id) const noexcept;
  bool validate() const noexcept;
  static uint64_t pow_int(uint64_t base, uint64_t exp) noexcept;

 protected:
  // tree internal manipulation
  void addRVBNode(std::shared_ptr<RVBNode>& node);
  void addInternalNode(std::shared_ptr<RVTNode>& node);
  void removeRVBNode(std::shared_ptr<RVBNode>& node);
  void addHashValToInternalNodes(std::shared_ptr<RVTNode>& node, std::shared_ptr<RVBNode>& rvb_node);
  void removeAndUpdateInternalNodes(std::shared_ptr<RVTNode>& node, std::shared_ptr<RVBNode>& rvb_node);
  void setNewRoot(shared_ptr<RVTNode> new_root);
  std::shared_ptr<RVTNode> openForInsertion(uint64_t level) const;
  std::shared_ptr<RVTNode> openForRemoval(uint64_t level) const;

  // Helper functions
  shared_ptr<RVTNode> getRVTNodeOfLeftSibling(shared_ptr<RVTNode>& node) const;
  shared_ptr<RVTNode> getRVTNodeOfRightSibling(shared_ptr<RVTNode>& node) const;
  shared_ptr<RVTNode> getParentNode(std::shared_ptr<RVTNode>& node) const noexcept;
  bool validateRvbId(const RVBId id, const STDigest& digest) const;
  bool validateRVBGroupId(const RVBGroupId rvb_group_id) const;

 protected:
  // vector index represents level in tree
  // level 0 represents RVB node so it would always hold 0x0
  std::vector<std::shared_ptr<RVTNode>> rightmostRVTNode_;
  std::vector<std::shared_ptr<RVTNode>> leftmostRVTNode_;
  std::unordered_map<uint64_t, std::shared_ptr<RVTNode>> id_to_node_;
  std::shared_ptr<RVTNode> root_{nullptr};
  uint64_t max_rvb_index_{0};  // RVB index is (RVB ID / fetch range size). This is the maximal index in the tree.
  uint64_t min_rvb_index_{0};  // RVB index is (RVB ID / fetch range size). This is the minimal index in the tree.
  const logging::Logger& logger_;
  const uint32_t RVT_K{0};
  const uint32_t fetch_range_size_{0};
  const size_t hash_size_{0};
  static constexpr uint8_t CHECKPOINT_PERSISTENCY_VERSION{1};
  static constexpr uint8_t version_num_{CHECKPOINT_PERSISTENCY_VERSION};
  static constexpr uint64_t magic_num_{0x1122334455667788};
};

}  // namespace bftEngine::bcst::impl
