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

#pragma once

#include "ReservedPagesClient.hpp"

#include "kv_types.hpp"

#include "pruning_msgs.cmf.hpp"

#include <cstdint>
#include <chrono>

namespace concord::kvbc::pruning {

// Creates a pruning agreement.
// `tick_period` must be at least 1 second.
// `batch_blocks_num` must be non-zero.
// `last_agreed_prunable_block_id` must be at least INITIAL_GENESIS_BLOCK_ID.
Agreement createAgreement(const std::chrono::seconds& tick_period,
                          std::uint64_t batch_blocks_num,
                          BlockId last_agreed_prunable_block_id);

// Provides persistence of pruning agreement and progress information in reserved pages.
//
// Pseudo code for how it can be used:
//
// * On PruneRequest:
//   client.saveAgreement(agreement);
//   if (genesis <= agreement.last_agreed_prunable_block_id) ticks_gen.start();
//
// * On Tick:
//   auto agreement = client.lastAgreement();
//   if (agreement) {
//     auto until = std::min(genesis + agreement->batch_blocks_num, agreement->last_agreed_prunable_block_id + 1);
//     if (until >= genesis) {
//       client.saveLatestBatch(until - 1);
//       blockchain.deleteBlocksUntil(until);
//     }
//     if (genesis >= agreement->last_agreed_prunable_block_id) ticks_gen.stop();
//   }
//
// * On Startup and State Transfer:
//   auto agreement = client.lastAgreement();
//   const auto to = client.latestBatchBlockIdTo();
//   if (to && *to + 1 > genesis) blockchain.deleteUntil(*to + 1);
//   if (genesis < agreement->last_agreed_prunable_block_id) ticks_gen.start();
//   else ticks_gen.stop();
class ReservedPagesClient {
 private:
  static constexpr auto kPruningResPageCount = 2;
  using ClientType = bftEngine::ResPagesClient<ReservedPagesClient, kPruningResPageCount>;

 public:
  // Returns the number of reserved pages for this client.
  static std::uint32_t numberOfReservedPagesForClient() { return ClientType::numberOfReservedPagesForClient(); }

 public:
  // Loads data from reserved pages on construction.
  ReservedPagesClient();

 public:
  // Saves the given agreement to reserved pages.
  void saveAgreement(const Agreement& agreement);

  // Updates an existing agreement's `tick_period` and `batch_blocks_num` in reserved pages.
  // Precondition: an agreement must already exist. If not, behaviour is undefined.
  void updateExistingAgreement(const std::chrono::seconds& tick_period, std::uint64_t batch_blocks_num);

  // Updates an existing agreement's `tick_period` in reserved pages.
  // Precondition: an agreement must already exist. If not, behaviour is undefined.
  void updateExistingAgreement(const std::chrono::seconds& tick_period);

  // Updates an existing agreement's `batch_blocks_num` in reserved pages.
  // Precondition: an agreement must already exist. If not, behaviour is undefined.
  void updateExistingAgreement(std::uint64_t batch_blocks_num);

  // Saves the `to` block ID of the latest batch. A batch is a range [genesis, to].
  void saveLatestBatch(BlockId to);

 public:
  // Returns the latest agreement or std::nullopt if no agreement has been reached yet.
  const std::optional<Agreement>& latestAgreement() const { return latest_agreement_; }

  // Returns the `to` block ID of the latest batch or std::nullopt if no batch pruning has started yet.
  const std::optional<BlockId>& latestBatchBlockIdTo() const { return latest_batch_block_id_to_; }

 private:
  static constexpr std::uint32_t kLatestAgreementPageId{0};
  static constexpr std::uint32_t kLatestBatchBlockIdToPageId{1};

 private:
  std::optional<Agreement> latest_agreement_;
  std::optional<BlockId> latest_batch_block_id_to_;
  ClientType client_;
};

}  // namespace concord::kvbc::pruning
