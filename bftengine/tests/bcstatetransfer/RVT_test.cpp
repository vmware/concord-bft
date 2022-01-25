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
#include <string>
#include <random>
#include <vector>
#include <string>
#include <cmath>

#include <cryptopp/integer.h>
#include "gtest/gtest.h"

#include "RangeValidationTree.hpp"
#include "Logger.hpp"
#include "kv_types.hpp"

using namespace std;
using namespace CryptoPP;
using namespace concord::kvbc;

namespace bftEngine::bcst::impl {

using NodeInfo = RangeValidationTree::NodeInfo;
using RVTNode = RangeValidationTree::RVTNode;
#define ASSERT_NFF ASSERT_NO_FATAL_FAILURE

class DataGenerator {
 public:
  DataGenerator() = default;
  DataGenerator(const size_t hash_size) : hash_size_(hash_size) {}
  static std::string randomString(size_t length);
  static uint32_t randomNum(uint32_t min = 3, uint32_t max = UINT32_MAX / 1000, uint32_t in_multiple_of = 0);

  struct InputValues {
    InputValues(const string& left_child, const string& right_child, const string& parent)
        : a(left_child.c_str()), b(right_child.c_str()), c(parent.c_str()) {}
    Integer a;
    Integer b;
    Integer c;
  };

  const size_t hash_size_{32};
  InputValues values_{randomString(hash_size_), randomString(hash_size_), randomString(hash_size_)};
};

std::string DataGenerator::randomString(size_t length) {
  static auto& chrs = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  std::mt19937 rg{std::random_device{}()};
  // String literals are null terminated and index starts from 0
  std::uniform_int_distribution<std::string::size_type> pick(1, sizeof(chrs) - 2);
  std::string s;
  s.reserve(length);
  do {
    while (length--) s += chrs[pick(rg)];
  } while (s.empty());
  return s;
}

uint32_t DataGenerator::randomNum(uint32_t min, uint32_t max, uint32_t in_multiple_of) {
  std::mt19937 rg{std::random_device{}()};
  std::uniform_int_distribution<std::string::size_type> pick(min, max);
  uint32_t num;
  do {
    num = pick(rg);
    if (in_multiple_of) {
      num -= (num % in_multiple_of);
    }
  } while (num == 0);
  return num;
}

struct RVTConfig {
  RVTConfig() = default;
  RVTConfig(const uint32_t n_childs, const uint32_t fetch_range_size, const size_t hash_size)
      : RVT_K(n_childs), fetch_range_size_(fetch_range_size), hash_size_(hash_size) {}
  const uint32_t RVT_K{3};              // max number of child any node can have
  const uint32_t fetch_range_size_{4};  // interval at which block will be validated
  const size_t hash_size_{32};          // in bytes
};

class BcStTestDelegator {
 public:
  BcStTestDelegator(const RVTConfig& config, const logging::Logger& logger) : config_(config), logger_(logger) {
    rvt_ = std::make_unique<RangeValidationTree>(logger_, config_.RVT_K, config_.fetch_range_size_, config_.hash_size_);
  };
  BcStTestDelegator() = delete;
  bool validateRVBGroupId(const RVBGroupId rvb_group_id) const { return rvt_->validateRVBGroupId(rvb_group_id); }
  void validate() const {
    bool result;
    ASSERT_NFF(result = rvt_->validate());
    ASSERT_EQ(result, true);
  }
  size_t totalNodes() const { return rvt_->totalNodes(); }
  size_t totalLevels() const { return rvt_->totalLevels(); }
  bool empty() const { return rvt_->empty(); }
  void clear() const { ASSERT_NFF(return rvt_->clear()); }
  std::ostringstream getSerializedRvbData() const { return rvt_->getSerializedRvbData(); }
  bool setSerializedRvbData(std::istringstream& iss) const { return rvt_->setSerializedRvbData(iss); }
  std::string getRootCurrentValueStr() const { return rvt_->getRootCurrentValueStr(); }
  RVBId getMaxRvbId() const { return rvt_->getMaxRvbId(); }
  RVBId getMinRvbId() const { return rvt_->getMinRvbId(); }
  std::vector<RVBGroupId> getRvbGroupIds(RVBId start_block_id, RVBId end_block_id, bool flag) const {
    return rvt_->getRvbGroupIds(start_block_id, end_block_id, flag);
  }
  std::string getDirectParentValueStr(RVBId rvb_id) const { return rvt_->getDirectParentValueStr(rvb_id); }
  std::vector<RVBId> getRvbIds(RVBGroupId id) const { return rvt_->getRvbIds(id); }
  void printToLog() const { return rvt_->printToLog(LogPrintVerbosity::SUMMARY); }

  void addNode(const RVBId start_id, const RVBId end_id);
  void removeNode(const RVBId start_id, const RVBId end_id);

 protected:
  std::unique_ptr<RangeValidationTree> rvt_;
  const RVTConfig config_;
  const logging::Logger logger_;
};

void BcStTestDelegator::addNode(const RVBId start_id, const RVBId end_id) {
  for (auto i = start_id; i <= end_id; i = i + config_.fetch_range_size_) {
    const auto str = std::to_string(i);
    ASSERT_NFF(rvt_->addNode(i, str.data(), str.size()));
    ASSERT_NFF(validate());
  }
}

void BcStTestDelegator::removeNode(const RVBId start_id, const RVBId end_id) {
  for (auto i = start_id; i <= end_id; i = i + config_.fetch_range_size_) {
    const auto str = std::to_string(i);
    ASSERT_NFF(rvt_->removeNode(i, str.data(), str.size()));
    ASSERT_NFF(validate());
  }
}

class RVTTest : public ::testing::Test {
 public:
  void SetUp() override{};
  void TearDown() override{};
  void init(const RVTConfig& config);
  RVTConfig getRandomConfig() const;

  std::unique_ptr<BcStTestDelegator> rvt_delegator_;
  std::unique_ptr<DataGenerator> data_generator_;
  RVTConfig rvt_config_{3, 4, 32};
  logging::Logger logger_{logging::getLogger("concord.bft.st.rvb")};
};

void RVTTest::init(const RVTConfig& config) {
  data_generator_ = std::make_unique<DataGenerator>(config.hash_size_);
  rvt_delegator_ = std::make_unique<BcStTestDelegator>(config, logger_);
  logging::Logger::getInstance("concord.bft.st.rvb").setLogLevel(log4cplus::INFO_LOG_LEVEL);
}

RVTConfig RVTTest::getRandomConfig() const {
  auto RVT_K = DataGenerator::randomNum(3, 2048);
  auto fetch_range_size = DataGenerator::randomNum(3, 10);
  auto hash_size = DataGenerator::randomNum(1, 64, 8);
  LOG_INFO(logger_, KVLOG(RVT_K, fetch_range_size, hash_size));
  return RVTConfig(RVT_K, fetch_range_size, hash_size);
}

// Validate add, sub ops on Integer data type
TEST_F(RVTTest, cryptoPPIntegerAdditionSubtraction) {
  init(RVTConfig{});
  auto a = data_generator_->values_.a;
  auto b = data_generator_->values_.b;
  auto c = a + b;
  ASSERT_EQ(a + b, c);
  ASSERT_EQ(c - b, a);
}

// Validate cumulative, associdate properties on Integer data type
TEST_F(RVTTest, cryptoPPIntegerCumulativeAssociativeProperty) {
  init(getRandomConfig());
  auto a = data_generator_->values_.a;
  auto b = data_generator_->values_.b;
  auto c = data_generator_->values_.c;
  ASSERT_EQ(a + b + c, c + a + b);
  ASSERT_EQ(a + (b + c), (c + a) + b);
  if ((a == (-b)) || (c == b) || (b == 0)) {
    return;
  }
  ASSERT_NE(a - b, b - a);
  ASSERT_NE(a + (b + c), (c + a) - b);
}

TEST_F(RVTTest, cryptoPPIntegerModOps) {
  init(getRandomConfig());
  auto a = data_generator_->values_.a;
  auto b = data_generator_->values_.b;
  auto c = data_generator_->values_.c;
  if (c == 0) {
    // random value 0 - return
    return;
  }
  Integer mod_res = (a + b) % c;
  Integer div_res = (a + b) / c;
  ASSERT_EQ(c * div_res + mod_res, a + b);
}

TEST_F(RVTTest, encodeDecodeCrytoPPIntegerWithString) {
  std::string input = DataGenerator::randomString(DataGenerator::randomNum(1, 64, 8));
  Integer obj(reinterpret_cast<unsigned char*>(input.data()), input.size());
  size_t len = obj.MinEncodedSize();
  ASSERT_EQ(len, input.size());
  string output;
  output.resize(len);
  obj.Encode(reinterpret_cast<CryptoPP::byte*>(output.data()), output.size(), Integer::UNSIGNED);
  ASSERT_EQ(output, input);
}

TEST_F(RVTTest, encodeDecodeCrytoPPIntegerWithStream) {
  std::string input = DataGenerator::randomString(DataGenerator::randomNum(1, 64, 8));
  Integer obj(reinterpret_cast<unsigned char*>(input.data()), input.size());
  ASSERT_EQ(obj.MinEncodedSize(), input.size());
  vector<char> output(obj.MinEncodedSize());
  obj.Encode(reinterpret_cast<CryptoPP::byte*>(output.data()), output.size());
  ostringstream oss;
  for (auto& c : output) {
    oss << c;
  }
  ASSERT_EQ(oss.str(), input);
}

TEST_F(RVTTest, StartIntheMiddleInsertionsOnly) {
  const uint32_t RVT_K = 12;
  const uint32_t fetch_range_size = 5;
  auto config = RVTConfig(RVT_K, fetch_range_size, 32);
  init(config);
  // These are block Ids, the matching RVB index is 1,10,600,30
  std::vector<RVBId> start_rvb_ids{5, 50, 3000, 150};

  for (const auto& start_rvb_id : start_rvb_ids) {
    RVBId i = 0;
    RVBId j = 1000;
    ASSERT_NFF(rvt_delegator_->addNode(start_rvb_id + i * fetch_range_size, start_rvb_id + j * fetch_range_size));
    ASSERT_NFF(rvt_delegator_->clear());
  }
}

TEST_F(RVTTest, constructSingleNodeHavingFirstChild) {
  auto config = getRandomConfig();
  init(config);
  ASSERT_NFF(rvt_delegator_->addNode(config.fetch_range_size_, config.fetch_range_size_));
  ASSERT_EQ(rvt_delegator_->empty(), false);
  ASSERT_EQ(rvt_delegator_->totalLevels(), 1);
  ASSERT_EQ(rvt_delegator_->totalNodes(), 1);
}

TEST_F(RVTTest, constructTreesWithSingleMiddleNode) {
  const size_t fetch_range_size = 4;
  const uint32_t RVT_K = 4;
  auto config = RVTConfig(RVT_K, fetch_range_size, 32);
  init(config);
  for (size_t i = 100; i < 1000; ++i) {
    for (size_t j{i}; j < (i + RVT_K * 2 + 1); ++j) {
      rvt_delegator_->addNode(j * fetch_range_size, j * fetch_range_size);
    }
    for (size_t j{i}; j < (i + RVT_K * 2 + 1); ++j) {
      rvt_delegator_->removeNode(j * fetch_range_size, j * fetch_range_size);
      rvt_delegator_->empty();
    }
  }
}

TEST_F(RVTTest, constructSingleNodeWithOnlyLastChild) {
  auto config = getRandomConfig();
  init(config);
  auto fetch_range_size = config.fetch_range_size_;
  ASSERT_NFF(rvt_delegator_->addNode(fetch_range_size * config.RVT_K, fetch_range_size * config.RVT_K));
  ASSERT_EQ(rvt_delegator_->totalLevels(), 1);
  ASSERT_EQ(rvt_delegator_->totalNodes(), 1);
}

TEST_F(RVTTest, treeHavingOnlyTwoNodes) {
  auto config = getRandomConfig();
  init(config);
  ASSERT_NFF(rvt_delegator_->addNode(config.fetch_range_size_,
                                     (config.fetch_range_size_ * config.RVT_K) + config.fetch_range_size_));
  ConcordAssertEQ(rvt_delegator_->totalLevels(), 2);
  ConcordAssertEQ(rvt_delegator_->totalNodes(), 3);
}

TEST_F(RVTTest, constructAndRemoveSingleNode) {
  auto config = getRandomConfig();
  init(config);
  ASSERT_NFF(rvt_delegator_->addNode(config.fetch_range_size_,
                                     (config.fetch_range_size_ * config.RVT_K) + config.fetch_range_size_));
  ASSERT_NFF(rvt_delegator_->removeNode(config.fetch_range_size_,
                                        (config.fetch_range_size_ * config.RVT_K) + config.fetch_range_size_));
  ASSERT_EQ(rvt_delegator_->totalNodes(), 0);
  ASSERT_EQ(rvt_delegator_->empty(), true);
}

class RVTTestserializeDeserializeTreeFixture : public RVTTest,
                                               public testing::WithParamInterface<std::pair<uint32_t, uint32_t>> {};
TEST_P(RVTTestserializeDeserializeTreeFixture, serializeDeserializeTree) {
  auto inputs = GetParam();
  const uint32_t RVT_K = inputs.first;
  const uint32_t fetch_range_size = inputs.second;
  size_t random_num_of_nodes_to_add = DataGenerator::randomNum(1, 10 * RVT_K);
  auto config = RVTConfig(RVT_K, fetch_range_size, 32);
  init(config);
  std::cout << KVLOG(random_num_of_nodes_to_add) << std::endl;
  ASSERT_NFF(rvt_delegator_->addNode(fetch_range_size, fetch_range_size * random_num_of_nodes_to_add));

  // TODO - move this into ctor on product in RVTNode
  auto root_hash = rvt_delegator_->getRootCurrentValueStr();
  auto total_levels = rvt_delegator_->totalLevels();
  auto total_nodes = rvt_delegator_->totalNodes();

  std::ostringstream oss;
  ASSERT_NFF(oss = rvt_delegator_->getSerializedRvbData());
  std::istringstream iss(oss.str());
  rvt_delegator_->setSerializedRvbData(iss);

  ASSERT_EQ(root_hash, rvt_delegator_->getRootCurrentValueStr());
  ASSERT_EQ(total_nodes, rvt_delegator_->totalNodes());
  ASSERT_EQ(total_levels, rvt_delegator_->totalLevels());
}
INSTANTIATE_TEST_CASE_P(
    serializeDeserializeTree,
    RVTTestserializeDeserializeTreeFixture,
    ::testing::Values(std::make_pair(DataGenerator::randomNum(3, 10), DataGenerator::randomNum(4, 20)),
                      std::make_pair(DataGenerator::randomNum(3, 10), DataGenerator::randomNum(4, 20)),
                      std::make_pair(DataGenerator::randomNum(3, 10), DataGenerator::randomNum(4, 20))), );

class RVTTestTreeLevelsByFormulaFixture : public RVTTest,
                                          public testing::WithParamInterface<std::pair<uint32_t, uint32_t>> {};
TEST_P(RVTTestTreeLevelsByFormulaFixture, validateTreeLevelsByFormula) {
  auto inputs = GetParam();
  const uint32_t RVT_K = inputs.first;
  const uint32_t fetch_range_size = inputs.second;
  const uint32_t n_nodes = fetch_range_size * RVT_K * DataGenerator::randomNum(1, 20);
  auto config = RVTConfig(RVT_K, fetch_range_size, 32);
  init(config);

  cout << "creating n_nodes:" << n_nodes << std::endl;
  ASSERT_NFF(rvt_delegator_->addNode(fetch_range_size, fetch_range_size * n_nodes));

  // Old formula
  // auto min_rvb = 1;
  // auto n_rvb_groups = (((n_nodes / fetch_range_size) - min_rvb) / (RVT_K)) + 1;
  // cout << "n_rvb_grousp:" << n_rvb_groups << std::endl;
  // auto max_level_by_formula = ceil(log(n_rvb_groups) / log(RVT_K)) + 1;

  auto max_level_from_tree = rvt_delegator_->totalLevels();
  // TODO - Fix below or below formula and change below assert to match exact level
  // New formula
  auto max_level_by_formula = ceil(log(rvt_delegator_->totalNodes()) / log(RVT_K));
  ASSERT_TRUE((max_level_by_formula == max_level_from_tree) || (max_level_by_formula + 1 == max_level_from_tree));
  // TODO Find formula to validate total nodes
}
INSTANTIATE_TEST_CASE_P(validateTreeLevelsByFormula,
                        RVTTestTreeLevelsByFormulaFixture,
                        ::testing::Values(std::make_pair(DataGenerator::randomNum(128, 256),
                                                         DataGenerator::randomNum(4, 20))), );

// TODO Need to be improved to have random RVT_K and validation logic
TEST_F(RVTTest, validate_RvbIds_GroupIds_DirectParentVal_APIs) {
  const uint32_t RVT_K = 4;
  const uint32_t fetch_range_size = 5;
  auto config = RVTConfig(RVT_K, fetch_range_size, 32);
  init(config);
  // TODO genesis block to have 0 digest?
  ASSERT_NFF(rvt_delegator_->addNode(fetch_range_size, fetch_range_size * RVT_K * 2 + fetch_range_size));

  // Both blocks fall under same parent rvb-group-id
  std::vector<RVBGroupId> rvb_group_ids;
  rvb_group_ids = rvt_delegator_->getRvbGroupIds(5, 5, true);
  ASSERT_EQ(rvb_group_ids.size(), 1);
  auto hash_val_1 = rvt_delegator_->getDirectParentValueStr(DataGenerator::randomNum(5, 10, 5));
  auto hash_val_2 = rvt_delegator_->getDirectParentValueStr(DataGenerator::randomNum(15, 20, 5));
  ASSERT_EQ(hash_val_1, hash_val_2);

  // Blocks span across multiple rvb-group-ids
  rvb_group_ids = rvt_delegator_->getRvbGroupIds(5, 45, true);
  ASSERT_EQ(rvb_group_ids.size(), 3);
  ASSERT_NE(rvt_delegator_->getDirectParentValueStr(DataGenerator::randomNum(5, 20, 5)),
            rvt_delegator_->getDirectParentValueStr(DataGenerator::randomNum(25, 40, 5)));
  ASSERT_NE(rvt_delegator_->getDirectParentValueStr(DataGenerator::randomNum(5, 20, 5)),
            rvt_delegator_->getDirectParentValueStr(45));
  ASSERT_NE(rvt_delegator_->getDirectParentValueStr(DataGenerator::randomNum(25, 40, 5)),
            rvt_delegator_->getDirectParentValueStr(45));

  // TODO - Call getRvbGroupIds() with blocks spanning multiple rvb-group-ids.
  // Mention starting blocks such that first few group-ids are missing (pruning).
  // API should still continue & return next rvg-group-id

  std::vector<BlockId> rvb_block_ids = rvt_delegator_->getRvbIds(rvb_group_ids[0]);
  if (not rvt_delegator_->validateRVBGroupId(rvb_group_ids[0])) {
    ASSERT_EQ(rvb_block_ids.size(), 0);
  } else {
    ASSERT_EQ(rvb_block_ids.size(), 4);
  }
}

class RVTTestconstructTreeStartingSomewhereMiddleFixture
    : public RVTTest,
      public testing::WithParamInterface<std::pair<uint32_t, uint32_t>> {};
TEST_P(RVTTestconstructTreeStartingSomewhereMiddleFixture, constructTreeStartingSomewhereMiddle) {
  auto inputs = GetParam();
  const uint32_t RVT_K = inputs.first;
  const uint32_t fetch_range_size = inputs.second;
  auto config = RVTConfig(RVT_K, fetch_range_size, 32);
  init(config);

  auto getRvbIdRange = [&](uint32_t start_from, uint32_t n_rvb_groups) {
    auto start_rvb_id = fetch_range_size * start_from;
    auto end_rvb_id = fetch_range_size * RVT_K * n_rvb_groups;
    return make_pair(start_rvb_id, end_rvb_id);
  };

  auto addRemoveNodes = [&](RVBId start_rvb_id, RVBId end_rvb_id) {
    ASSERT_EQ(rvt_delegator_->getMaxRvbId(), 0);
    ASSERT_EQ(rvt_delegator_->getMinRvbId(), 0);
    ASSERT_NFF(rvt_delegator_->addNode(start_rvb_id, end_rvb_id));
    ASSERT_NFF(rvt_delegator_->removeNode(start_rvb_id, end_rvb_id));
    ASSERT_TRUE(rvt_delegator_->empty());
    rvt_delegator_->clear();
  };

  RVBId start_rvb_id;
  RVBId end_rvb_id;
  uint64_t n_rvb_groups;

  std::tie(start_rvb_id, end_rvb_id) = getRvbIdRange(DataGenerator::randomNum(2, RVT_K), 2);
  addRemoveNodes(start_rvb_id, end_rvb_id);

  n_rvb_groups = RVT_K * RVT_K * fetch_range_size;
  std::tie(start_rvb_id, end_rvb_id) = getRvbIdRange(DataGenerator::randomNum(RVT_K + 1, RVT_K * 2), n_rvb_groups);
  addRemoveNodes(start_rvb_id, end_rvb_id);
  n_rvb_groups = RVT_K * RVT_K * fetch_range_size;
  std::tie(start_rvb_id, end_rvb_id) = getRvbIdRange(
      DataGenerator::randomNum(RVT_K * fetch_range_size + 1, RVT_K * RVT_K * fetch_range_size), n_rvb_groups);
  addRemoveNodes(start_rvb_id, end_rvb_id);
}

INSTANTIATE_TEST_CASE_P(constructTreeStartingSomewhereMiddle,
                        RVTTestconstructTreeStartingSomewhereMiddleFixture,
                        ::testing::Values(std::make_pair(DataGenerator::randomNum(3, 10),
                                                         DataGenerator::randomNum(4, 20))), );

class RVTTestaddRemoveNodesRandomlyInTreeFixture : public RVTTest,
                                                   public testing::WithParamInterface<std::pair<uint32_t, uint32_t>> {};
TEST_P(RVTTestaddRemoveNodesRandomlyInTreeFixture, addRemoveNodesRandomlyInTree) {
  auto inputs = GetParam();
  const uint32_t RVT_K = inputs.first;
  const uint32_t fetch_range_size = inputs.second;
  const uint32_t n_nodes = fetch_range_size * DataGenerator::randomNum(10, 100);
  std::cout << KVLOG(n_nodes) << std::endl;
  auto config = RVTConfig(RVT_K, fetch_range_size, 32);
  init(config);
  // add, remove nodes randomly.
  for (uint32_t i = fetch_range_size; i <= n_nodes; i = i + fetch_range_size) {
    ASSERT_NFF(rvt_delegator_->addNode(rvt_delegator_->getMaxRvbId() + fetch_range_size,
                                       rvt_delegator_->getMaxRvbId() + fetch_range_size));
    auto num = DataGenerator::randomNum(1, 2);
    // TODO Use ASSERT_NFF in below code block
    ((num % 2) || rvt_delegator_->empty())
        ? rvt_delegator_->addNode(rvt_delegator_->getMaxRvbId() + fetch_range_size,
                                  rvt_delegator_->getMaxRvbId() + fetch_range_size)
        : rvt_delegator_->removeNode(rvt_delegator_->getMinRvbId(), rvt_delegator_->getMinRvbId());
  }
}
INSTANTIATE_TEST_CASE_P(
    addRemoveNodesRandomlyInTree,
    RVTTestaddRemoveNodesRandomlyInTreeFixture,
    ::testing::Values(std::make_pair(DataGenerator::randomNum(3, 10), DataGenerator::randomNum(4, 20)),
                      std::make_pair(DataGenerator::randomNum(3, 10), DataGenerator::randomNum(4, 20)),
                      std::make_pair(DataGenerator::randomNum(3, 10), DataGenerator::randomNum(4, 20))), );

class RVTTestFixture
    : public RVTTest,
      public testing::WithParamInterface<tuple<uint64_t, uint64_t, tuple<vector<size_t>, vector<size_t>>>> {};

TEST_P(RVTTestFixture, addRemoveWithRootValidation) {
  uint64_t test_progress{0};
  const uint64_t fetch_range_size = 4;
  const uint64_t RVT_K = get<0>(GetParam());
  const auto rvt_value_size = get<1>(GetParam());
  size_t add_i{1}, rem_i{1};
  auto scenario = get<2>(GetParam());
  auto add_nodes_itertion_size = get<0>(scenario);
  auto remove_nodes_itertion_size = get<1>(scenario);

  std::cout << "Params:" << KVLOG(RVT_K, rvt_value_size) << endl;
  auto config = RVTConfig(RVT_K, fetch_range_size, rvt_value_size);
  init(config);

  ASSERT_EQ(add_nodes_itertion_size.size(), remove_nodes_itertion_size.size());
  for (size_t i{}; i < add_nodes_itertion_size.size(); ++i) {
    for (; add_i < add_nodes_itertion_size[i]; ++add_i) {
      ASSERT_NFF(rvt_delegator_->addNode(add_i * fetch_range_size, add_i * fetch_range_size));
      ++test_progress;
      if (test_progress % 100000 == 0) {
        std::cout << "Iteration # " << test_progress << std::endl;
      }
    }
    for (; rem_i < remove_nodes_itertion_size[i]; ++rem_i) {
      ASSERT_NFF(rvt_delegator_->removeNode(rem_i * fetch_range_size, rem_i * fetch_range_size));
      ++test_progress;
      if (test_progress % 100000 == 0) {
        std::cout << "Iteration # " << test_progress << std::endl;
      }
    }
  }
  ASSERT_TRUE(rvt_delegator_->empty());
}

std::vector<uint64_t> RVT_Ks = {4, 1024};
std::vector<uint64_t> value_sizes = {1, 32};
vector<std::tuple<std::vector<size_t>, std::vector<size_t>>> scenarios = {
    {{1000, 500, 500, 400}, {1000, 300, 600, 500}}, {{1024 * 5}, {1024 * 5}}};

INSTANTIATE_TEST_CASE_P(RVTTest,
                        RVTTestFixture,
                        ::testing::Combine(::testing::ValuesIn(RVT_Ks),
                                           ::testing::ValuesIn(value_sizes),
                                           ::testing::ValuesIn(scenarios)), );

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
}  // namespace bftEngine::bcst::impl
