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
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "assertUtils.hpp"
#include "categorized_kvbc_msgs.cmf.hpp"
#include "rocksdb/native_client.h"

#include <cstddef>
#include <cstdint>

namespace concord::benchmark {

// A batch of sub-batches, allowing for multiple threads calling RocksDB MultiGet() at the same time.
template <typename Buffer>
class MultiGetBatch {
 public:
  MultiGetBatch(std::uint64_t max_size, std::uint32_t num_sub_batches)
      : num_sub_batches_{num_sub_batches}, sub_batch_size_{max_size / num_sub_batches} {
    ConcordAssertGE(max_size, num_sub_batches_);
    ConcordAssertEQ(0, max_size % num_sub_batches_);
    sub_batches_ = std::vector<std::vector<Buffer>>(num_sub_batches);
    value_slices_ = std::vector<std::vector<::rocksdb::PinnableSlice>>(num_sub_batches_);
    statuses_ = std::vector<std::vector<::rocksdb::Status>>(num_sub_batches);
  }

  void push_back(const concord::kvbc::categorization::VersionedKey& key) {
    keys_.push_back(key);
    push_back(concord::kvbc::categorization::detail::serializeThreadLocal(key));
  }

  void clear() {
    current_sub_batch_idx_ = 0;
    clearVectors(sub_batches_);
    clearVectors(value_slices_);
    clearVectors(statuses_);
    keys_.clear();
  }

  const concord::kvbc::categorization::VersionedKey& operator[](std::size_t idx) const noexcept { return keys_[idx]; }

  const std::vector<Buffer>& serializedKeys(std::size_t sub_batch_idx) const noexcept {
    return sub_batches_[sub_batch_idx];
  }
  std::vector<::rocksdb::PinnableSlice>& valueSlices(std::size_t sub_batch_idx) noexcept {
    return value_slices_[sub_batch_idx];
  }
  std::vector<::rocksdb::Status>& statuses(std::size_t sub_batch_idx) noexcept { return statuses_[sub_batch_idx]; }

  std::uint32_t numSubBatches() const noexcept { return num_sub_batches_; }
  bool empty() const noexcept { return keys_.empty(); }
  std::size_t size() const noexcept { return keys_.size(); }

 private:
  template <typename T>
  static void clearVectors(std::vector<T>& vec) {
    for (auto& v : vec) {
      v.clear();
    }
  }

  void push_back(const Buffer& b) {
    auto& current_sub_batch = sub_batches_[current_sub_batch_idx_];
    if (current_sub_batch.size() == sub_batch_size_) {
      ++current_sub_batch_idx_;
    }
    sub_batches_[current_sub_batch_idx_].push_back(b);
  }

 private:
  const std::uint32_t num_sub_batches_{1};
  const std::size_t sub_batch_size_{1};
  std::size_t current_sub_batch_idx_{0};
  std::vector<std::vector<Buffer>> sub_batches_;
  std::vector<std::vector<::rocksdb::PinnableSlice>> value_slices_;
  std::vector<std::vector<::rocksdb::Status>> statuses_;
  std::vector<concord::kvbc::categorization::VersionedKey> keys_;
};

}  // namespace concord::benchmark
