// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "pruning_reserved_pages_client.hpp"

#include "kv_types.hpp"
#include "ReservedPagesClient.hpp"
#include "ReservedPagesMock.hpp"

#include "gtest/gtest.h"

#include <chrono>
#include <memory>
#include <stdexcept>

namespace {

using concord::kvbc::BlockId;
using concord::kvbc::INITIAL_GENESIS_BLOCK_ID;
using namespace concord::kvbc::pruning;
using namespace std::chrono_literals;

class pruning_reserved_pages_client_test : public ::testing::Test {
 protected:
  bftEngine::test::ReservedPagesMock<ReservedPagesClient> res_pages_mock_;
  std::unique_ptr<ReservedPagesClient> client_;
  const std::chrono::seconds tick_period_{1s};
  const std::uint64_t batch_blocks_num_{10};
  const BlockId last_agreed_prunable_block_id_{100};

  void SetUp() override {
    bftEngine::ReservedPagesClientBase::setReservedPages(&res_pages_mock_);
    client_ = std::make_unique<ReservedPagesClient>();
  }
};

TEST_F(pruning_reserved_pages_client_test, create_valid_agreement) {
  const auto agreement = createAgreement(tick_period_, batch_blocks_num_, last_agreed_prunable_block_id_);
  ASSERT_EQ(agreement.tick_period_seconds, tick_period_.count());
  ASSERT_EQ(agreement.batch_blocks_num, batch_blocks_num_);
  ASSERT_EQ(agreement.last_agreed_prunable_block_id, last_agreed_prunable_block_id_);
}

TEST_F(pruning_reserved_pages_client_test, create_agreement_with_invalid_last_agreed) {
  ASSERT_THROW(
      createAgreement(tick_period_, batch_blocks_num_, INITIAL_GENESIS_BLOCK_ID > 0 ? INITIAL_GENESIS_BLOCK_ID - 1 : 0),
      std::invalid_argument);
}

TEST_F(pruning_reserved_pages_client_test, save_agreement) {
  const auto agreement = createAgreement(tick_period_, batch_blocks_num_, last_agreed_prunable_block_id_);
  client_->saveAgreement(agreement);

  // Verify the agreement has been cached properly.
  const auto cached_agreement = client_->latestAgreement();
  ASSERT_TRUE(cached_agreement.has_value());
  ASSERT_EQ(*cached_agreement, agreement);

  // Create a new client and verify it can load the saved agreement.
  client_ = std::make_unique<ReservedPagesClient>();
  const auto loaded_agreement = client_->latestAgreement();
  ASSERT_TRUE(loaded_agreement.has_value());
  ASSERT_EQ(*loaded_agreement, agreement);
}

TEST_F(pruning_reserved_pages_client_test, save_latest_batch) {
  const auto to = BlockId{42};
  client_->saveLatestBatch(to);

  // Verify the batch has been cached properly.
  const auto cached_to = client_->latestBatchBlockIdTo();
  ASSERT_TRUE(cached_to.has_value());
  ASSERT_EQ(*cached_to, to);

  // Create a new client and verify it can load the saved batch.
  client_ = std::make_unique<ReservedPagesClient>();
  const auto loaded_to = client_->latestBatchBlockIdTo();
  ASSERT_TRUE(loaded_to.has_value());
  ASSERT_EQ(*loaded_to, to);
}

TEST_F(pruning_reserved_pages_client_test, empty_client_has_no_data) {
  ASSERT_FALSE(client_->latestAgreement().has_value());
  ASSERT_FALSE(client_->latestBatchBlockIdTo().has_value());
}

TEST_F(pruning_reserved_pages_client_test, update_non_existent_agreement) {
  ASSERT_DEATH(client_->updateExistingAgreement(tick_period_, batch_blocks_num_), "");
  ASSERT_DEATH(client_->updateExistingAgreement(batch_blocks_num_), "");
  ASSERT_DEATH(client_->updateExistingAgreement(tick_period_), "");
}

TEST_F(pruning_reserved_pages_client_test, update_tick_period_and_batch_blocks_num_on_existing_agreement) {
  const auto original_agreement = createAgreement(tick_period_, batch_blocks_num_, last_agreed_prunable_block_id_);
  client_->saveAgreement(original_agreement);

  // Update.
  client_->updateExistingAgreement(tick_period_ + 1s, batch_blocks_num_ * 2);

  // Verify it has been updated in the existing client.
  {
    const auto updated_agreement = client_->latestAgreement();
    ASSERT_TRUE(updated_agreement.has_value());
    ASSERT_EQ(*updated_agreement,
              createAgreement(tick_period_ + 1s, batch_blocks_num_ * 2, last_agreed_prunable_block_id_));
  }

  {
    // Create a new client and verify it sees the updates too.
    client_ = std::make_unique<ReservedPagesClient>();
    const auto loaded_agreement = client_->latestAgreement();
    ASSERT_TRUE(loaded_agreement.has_value());
    ASSERT_EQ(*loaded_agreement,
              createAgreement(tick_period_ + 1s, batch_blocks_num_ * 2, last_agreed_prunable_block_id_));
  }
}

TEST_F(pruning_reserved_pages_client_test, update_tick_period_on_existing_agreement) {
  const auto original_agreement = createAgreement(tick_period_, batch_blocks_num_, last_agreed_prunable_block_id_);
  client_->saveAgreement(original_agreement);

  // Update.
  client_->updateExistingAgreement(tick_period_ + 1s);

  // Verify it has been updated in the existing client.
  {
    const auto updated_agreement = client_->latestAgreement();
    ASSERT_TRUE(updated_agreement.has_value());
    ASSERT_EQ(*updated_agreement,
              createAgreement(tick_period_ + 1s, batch_blocks_num_, last_agreed_prunable_block_id_));
  }

  {
    // Create a new client and verify it sees the updates too.
    client_ = std::make_unique<ReservedPagesClient>();
    const auto loaded_agreement = client_->latestAgreement();
    ASSERT_TRUE(loaded_agreement.has_value());
    ASSERT_EQ(*loaded_agreement, createAgreement(tick_period_ + 1s, batch_blocks_num_, last_agreed_prunable_block_id_));
  }
}

TEST_F(pruning_reserved_pages_client_test, update_batch_blocks_num_on_existing_agreement) {
  const auto original_agreement = createAgreement(tick_period_, batch_blocks_num_, last_agreed_prunable_block_id_);
  client_->saveAgreement(original_agreement);

  // Update.
  client_->updateExistingAgreement(batch_blocks_num_ * 2);

  // Verify it has been updated in the existing client.
  {
    const auto updated_agreement = client_->latestAgreement();
    ASSERT_TRUE(updated_agreement.has_value());
    ASSERT_EQ(*updated_agreement, createAgreement(tick_period_, batch_blocks_num_ * 2, last_agreed_prunable_block_id_));
  }

  {
    // Create a new client and verify it sees the updates too.
    client_ = std::make_unique<ReservedPagesClient>();
    const auto loaded_agreement = client_->latestAgreement();
    ASSERT_TRUE(loaded_agreement.has_value());
    ASSERT_EQ(*loaded_agreement, createAgreement(tick_period_, batch_blocks_num_ * 2, last_agreed_prunable_block_id_));
  }
}

}  // namespace
