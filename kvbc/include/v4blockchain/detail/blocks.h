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

#pragma once

#include <string_view>
#include <vector>

// #include "blocks.h"
#include "kv_types.hpp"
#include "Digest.hpp"
#include "bcstatetransfer/SimpleBCStateTransfer.hpp"
#include "categorized_kvbc_msgs.cmf.hpp"
#include "assertUtils.hpp"
#include "blockchain_misc.hpp"
#include "categorization/updates.h"
#include "v4blockchain/detail/detail.h"

namespace concord::kvbc::v4blockchain::detail {
/*
Block is the unit that composes the block chain.
It recursively links the state by including the digest of the previous block.
In this implementation, the block will contains all data i.e. the set of key-values.
its layout is as follows
|VERSION|DIGEST|KEY-VALUES|
it uses a buffer, and fast access due to the known locations.
It is stored as is in DB and is a passable unit for the use of state-transfer.
*/
class Block {
 public:
  static constexpr block_version BLOCK_VERSION = block_version::V1;
  static constexpr uint64_t HEADER_SIZE = BLOCK_VERSION_SIZE + sizeof(concord::util::digest::BlockDigest);
  // Pre-reserve buffer size
  Block(uint64_t reserve_size) : buffer_(HEADER_SIZE, 0) {
    buffer_.reserve(reserve_size);
    addVersion();
  }
  Block() : Block(HEADER_SIZE) {}

  Block(const std::string& buffer) : buffer_(buffer.size()) {
    std::copy(buffer.cbegin(), buffer.cend(), buffer_.begin());
  }

  Block(const std::string_view& buffer) : buffer_(buffer.size()) {
    std::copy(buffer.cbegin(), buffer.cend(), buffer_.begin());
  }

  Block(const Block&) = default;
  Block(Block&&) = default;
  Block& operator=(const Block&) & = default;
  Block& operator=(Block&&) & = default;

  const block_version& getVersion() const {
    ConcordAssert(isValid_);
    ConcordAssert(buffer_.size() >= sizeof(version_type));
    return *reinterpret_cast<const block_version*>(buffer_.data());
  }

  const concord::util::digest::BlockDigest& parentDigest() const {
    ConcordAssert(isValid_);
    ConcordAssert(buffer_.size() >= HEADER_SIZE);
    return *reinterpret_cast<const concord::util::digest::BlockDigest*>(buffer_.data() + sizeof(version_type));
  }

  void addDigest(const concord::util::digest::BlockDigest& digest) {
    ConcordAssert(isValid_);
    ConcordAssert(buffer_.size() >= HEADER_SIZE);
    std::copy(digest.cbegin(), digest.cend(), buffer_.data() + sizeof(version_type));
  }

  concord::util::digest::BlockDigest calculateDigest(concord::kvbc::BlockId id) const {
    ConcordAssert(isValid_);
    return calculateDigest(id, reinterpret_cast<const char*>(buffer_.data()), buffer_.size());
  }

  void addUpdates(const concord::kvbc::categorization::Updates& category_updates);

  concord::kvbc::categorization::Updates getUpdates() const;

  const std::vector<uint8_t>& getBuffer() const {
    ConcordAssert(isValid_);
    return buffer_;
  }

  size_t size() const {
    ConcordAssert(isValid_);
    return buffer_.size();
  }

  std::vector<uint8_t>&& moveBuffer() {
    ConcordAssert(isValid_);
    isValid_ = false;
    return std::move(buffer_);
  }

  static concord::util::digest::BlockDigest calculateDigest(concord::kvbc::BlockId id,
                                                            const char* buffer,
                                                            uint64_t size) {
    return bftEngine::bcst::computeBlockDigest(id, buffer, size);
  }

 private:
  void addVersion() {
    ConcordAssert(isValid_);
    ConcordAssert(buffer_.size() >= sizeof(block_version));
    *((block_version*)buffer_.data()) = BLOCK_VERSION;
  }

  std::vector<uint8_t> buffer_;
  bool isValid_{true};
};

}  // namespace concord::kvbc::v4blockchain::detail
