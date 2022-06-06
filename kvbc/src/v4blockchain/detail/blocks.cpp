// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "v4blockchain/detail/blocks.h"

namespace concord::kvbc::v4blockchain::detail {

void Block::addUpdates(const concord::kvbc::categorization::Updates& category_updates) {
  ConcordAssert(isValid_);
  static thread_local std::vector<uint8_t> updates_buffer;
  concord::kvbc::categorization::serialize(updates_buffer, category_updates.categoryUpdates());
  buffer_.resize(HEADER_SIZE + updates_buffer.size());
  auto dist = std::distance(buffer_.begin(), buffer_.begin() + HEADER_SIZE);
  std::copy(updates_buffer.begin(), updates_buffer.end(), buffer_.begin() + dist);
  updates_buffer.clear();
}

categorization::Updates Block::getUpdates() const {
  ConcordAssert(isValid_);
  ConcordAssert(buffer_.size() > HEADER_SIZE);
  concord::kvbc::categorization::CategoryInput category_updates;
  const uint8_t* start = buffer_.data() + HEADER_SIZE;
  const uint8_t* end = buffer_.data() + buffer_.size();
  concord::kvbc::categorization::deserialize(start, end, category_updates);
  return concord::kvbc::categorization::Updates(std::move(category_updates));
}

}  // namespace concord::kvbc::v4blockchain::detail
