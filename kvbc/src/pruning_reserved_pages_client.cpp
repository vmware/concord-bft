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

#include "assertUtils.hpp"

#include <limits>
#include <stdexcept>
#include <vector>

namespace concord::kvbc::pruning {

static char* data(std::vector<std::uint8_t>& v) { return reinterpret_cast<char*>(v.data()); }
static const char* cdata(const std::vector<std::uint8_t>& v) { return reinterpret_cast<const char*>(v.data()); }

static std::uint32_t toSecondsCount(const std::chrono::seconds& s) {
  if (s.count() <= 0 || s.count() > std::numeric_limits<std::uint32_t>::max()) {
    throw std::invalid_argument{"Invalid tick period (seconds) value"};
  }
  return static_cast<std::uint32_t>(s.count());
}

Agreement createAgreement(const std::chrono::seconds& tick_period,
                          std::uint64_t batch_blocks_num,
                          BlockId last_agreed_prunable_block_id) {
  if (batch_blocks_num == 0 || last_agreed_prunable_block_id < INITIAL_GENESIS_BLOCK_ID) {
    throw std::invalid_argument{"Invalid pruning agreement paramteres"};
  }
  return Agreement{toSecondsCount(tick_period), batch_blocks_num, last_agreed_prunable_block_id};
}

ReservedPagesClient::ReservedPagesClient() {
  // Note: Since we don't override any virtual methods from the base class, we can call them in the constructor. If that
  // changes in the future, we would need to introduce an init() method that does the current constructor's work.
  auto in = std::vector<std::uint8_t>(sizeOfReservedPage());
  if (loadReservedPage(kLatestAgreementPageId, in.size(), data(in))) {
    latest_agreement_.emplace();
    deserialize(in, *latest_agreement_);
  }

  if (loadReservedPage(kLatestBatchBlockIdToPageId, in.size(), data(in))) {
    auto batch = Batch{};
    deserialize(in, batch);
    latest_batch_block_id_to_ = batch.latest_batch_block_id_to;
  }
}

void ReservedPagesClient::saveAgreement(const Agreement& agreement) {
  auto out = std::vector<std::uint8_t>{};
  serialize(out, agreement);
  saveReservedPage(kLatestAgreementPageId, out.size(), cdata(out));
  // Currently, the assignment operator cannot throw as it is an std::optional data structure with integer types only.
  // Change behaviour here if assignment can throw, because if it does, data would be persisted to reserved pages, while
  // data in memory would remain intact or in an undefined state.
  latest_agreement_ = agreement;
}

void ReservedPagesClient::updateExistingAgreement(const std::chrono::seconds& tick_period,
                                                  std::uint64_t batch_blocks_num) {
  ConcordAssert(latest_agreement_.has_value());
  saveAgreement(createAgreement(tick_period, batch_blocks_num, latest_agreement_->last_agreed_prunable_block_id));
}

void ReservedPagesClient::updateExistingAgreement(const std::chrono::seconds& tick_period) {
  ConcordAssert(latest_agreement_.has_value());
  saveAgreement(createAgreement(
      tick_period, latest_agreement_->batch_blocks_num, latest_agreement_->last_agreed_prunable_block_id));
}

void ReservedPagesClient::updateExistingAgreement(std::uint64_t batch_blocks_num) {
  ConcordAssert(latest_agreement_.has_value());
  saveAgreement(createAgreement(std::chrono::seconds{latest_agreement_->tick_period_seconds},
                                batch_blocks_num,
                                latest_agreement_->last_agreed_prunable_block_id));
}

void ReservedPagesClient::saveLatestBatch(BlockId to) {
  auto out = std::vector<std::uint8_t>{};
  serialize(out, Batch{to});
  saveReservedPage(kLatestBatchBlockIdToPageId, out.size(), cdata(out));
  latest_batch_block_id_to_ = to;
}

}  // namespace concord::kvbc::pruning
