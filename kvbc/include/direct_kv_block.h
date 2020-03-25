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

#include "bcstatetransfer/SimpleBCStateTransfer.hpp"
#include "kv_types.hpp"
#include "sliver.hpp"

#include <cstddef>
#include <cstdint>
#include <utility>

namespace concord {
namespace kvbc {
inline namespace v1DirectKeyValue {
namespace block {
inline constexpr auto BLOCK_DIGEST_SIZE = bftEngine::SimpleBlockchainStateTransfer::BLOCK_DIGEST_SIZE;

// Creates a block with the user data appended at the end of the returned Sliver. The passed parentDigest buffer must be
// of size BLOCK_DIGEST_SIZE bytes.
concordUtils::Sliver create(const concord::kvbc::SetOfKeyValuePairs &updates,
                            concord::kvbc::SetOfKeyValuePairs &outUpdatesInNewBlock,
                            const void *parentDigest,
                            const void *userData,
                            std::size_t userDataSize);

// Creates a block. The passed parentDigest buffer must be of size BLOCK_DIGEST_SIZE bytes.
concordUtils::Sliver create(const concord::kvbc::SetOfKeyValuePairs &updates,
                            concord::kvbc::SetOfKeyValuePairs &outUpdatesInNewBlock,
                            const void *parentDigest);

// Returns the block data in the form of a set of key/value pairs.
concord::kvbc::SetOfKeyValuePairs getData(const concordUtils::Sliver &block);

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

}  // namespace block

}  // namespace v1DirectKeyValue
}  // namespace kvbc
}  // namespace concord
