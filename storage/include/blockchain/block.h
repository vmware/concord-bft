// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
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

#include "db_types.h"
#include "kv_types.hpp"
#include "sliver.hpp"
#include "hash_defs.h"

#include "bcstatetransfer/SimpleBCStateTransfer.hpp"

#include <cstdint>
#include <utility>

namespace concord {
namespace storage {
namespace blockchain {
namespace block {

inline constexpr auto BLOCK_DIGEST_SIZE = bftEngine::SimpleBlockchainStateTransfer::BLOCK_DIGEST_SIZE;

inline namespace v1DirectKeyValue {
// Creates a block. The passed parentDigest buffer must be of size BLOCK_DIGEST_SIZE bytes.
concordUtils::Sliver create(const concordUtils::SetOfKeyValuePairs &updates,
                            concordUtils::SetOfKeyValuePairs &outUpdatesInNewBlock,
                            const void *parentDigest);

// Returns the block data in the form of a set of key/value pairs.
concordUtils::SetOfKeyValuePairs getData(const concordUtils::Sliver &block);

// Returns the parent digest of size BLOCK_DIGEST_SIZE bytes.
const void *getParentDigest(const concordUtils::Sliver &block);

// Block structure is an implementation detail. External users should not rely on it.
namespace detail {

struct Header {
  std::uint32_t numberOfElements;
  std::uint32_t parentDigestLength;
  std::uint8_t parentDigest[BLOCK_DIGEST_SIZE];
};

// Entry structures are coming immediately after the header.
struct Entry {
  std::uint32_t keyOffset;
  std::uint32_t keySize;
  std::uint32_t valOffset;
  std::uint32_t valSize;
};

}  // namespace detail

}  // namespace v1DirectKeyValue

}  // namespace block
}  // namespace blockchain
}  // namespace storage
}  // namespace concord
