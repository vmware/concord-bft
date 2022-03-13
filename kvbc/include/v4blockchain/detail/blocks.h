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

// #include "blocks.h"
#include "kv_types.hpp"
#include "Digest.hpp"
#include "bcstatetransfer/SimpleBCStateTransfer.hpp"
#include <vector>
#include "categorized_kvbc_msgs.cmf.hpp"
#include "assertUtils.hpp"
#include "categorization/updates.h"

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
  using version = std::array<char, 2>;
  static constexpr version BLOCK_VERSION = {0xf, 0x1};
  static constexpr uint64_t HEADER_SIZE = sizeof(version) + sizeof(concord::util::digest::BlockDigest);
  // Pre-reserve buffer size
  Block(uint64_t reserve_size) : buffer_(HEADER_SIZE, 0) {
    buffer_.reserve(reserve_size);
    addVersion();
  }
  Block() : Block(HEADER_SIZE) {}

  Block(const Block&) = default;
  Block(Block&&) = default;
  Block& operator=(const Block&) & = default;
  Block& operator=(Block&&) & = default;

  const version& getVersion() const {
    ConcordAssert(buffer_.size() >= sizeof(version));
    return *reinterpret_cast<const version*>(buffer_.data());
  }

  const concord::util::digest::BlockDigest& parentDigest() const {
    ConcordAssert(buffer_.size() >= HEADER_SIZE);
    return *reinterpret_cast<const concord::util::digest::BlockDigest*>(buffer_.data() + sizeof(version));
  }

  void addDigest(const concord::util::digest::BlockDigest& digest) {
    ConcordAssert(buffer_.size() >= HEADER_SIZE);
    std::copy(digest.cbegin(), digest.cend(), buffer_.data() + sizeof(BLOCK_VERSION));
  }

  concord::util::digest::BlockDigest calculateDigest(concord::kvbc::BlockId id) const {
    return bftEngine::bcst::computeBlockDigest(id, reinterpret_cast<const char*>(buffer_.data()), buffer_.size());
  }

  void addUpdates(const concord::kvbc::categorization::Updates& category_updates);

  concord::kvbc::categorization::Updates getUpdates() const;

  const std::vector<uint8_t>& getBuffer() const { return buffer_; }

 private:
  void addVersion() {
    ConcordAssert(buffer_.size() >= sizeof(version));
    std::copy(BLOCK_VERSION.cbegin(), BLOCK_VERSION.cend(), buffer_.data());
  }

  std::vector<uint8_t> buffer_;
};

}  // namespace concord::kvbc::v4blockchain::detail