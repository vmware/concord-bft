// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
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

#include <cstdint>

namespace concord {
namespace storage {
namespace blockchain {

inline namespace v1DirectKeyValue {
// DB key types are an implementation detail. External users should not rely on it.
namespace detail {

enum class EDBKeyType : std::uint8_t {
  E_DB_KEY_TYPE_FIRST = 1,
  E_DB_KEY_TYPE_BLOCK = E_DB_KEY_TYPE_FIRST,
  E_DB_KEY_TYPE_KEY,
  E_DB_KEY_TYPE_BFT_METADATA_KEY,
  E_DB_KEY_TYPE_BFT_ST_KEY,
  E_DB_KEY_TYPE_BFT_ST_PENDING_PAGE_KEY,
  E_DB_KEY_TYPE_BFT_ST_RESERVED_PAGE_STATIC_KEY,
  E_DB_KEY_TYPE_BFT_ST_RESERVED_PAGE_DYNAMIC_KEY,
  E_DB_KEY_TYPE_BFT_ST_CHECKPOINT_DESCRIPTOR_KEY,
  E_DB_KEY_TYPE_LAST
};

}  // namespace detail
}  // namespace v1DirectKeyValue

namespace v2MerkleTree {

// DB key types are an implementation detail. External users should not rely on it.
namespace detail {

// Top-level DB key types used when saving the blockchain in the form of a merkle tree.
// Key types might have subtypes so that the top-level enum is not quickly exhausted and keys are structured in
// a clearer way. A note is that there is an overhead of 1 byte in the key length when using subtypes.
enum class EDBKeyType : std::uint8_t {
  Block,
  Key,
  BFT,
};

// Key subtypes. Internal and Stale are used internally by the merkle tree implementation. The Leaf type is the one
// containing actual application data.
enum class EKeyType : std::uint8_t {
  Internal,
  Stale,
  Leaf,
};

// BFT subtypes.
enum class EBFTType : std::uint8_t {
  Metadata,
  ST,
  STPendingPage,
  STReservedPageStatic,
  STReserverdPageDynamic,
  STCheckpointDescriptor,
};

}  // namespace detail

}  // namespace v2MerkleTree

typedef std::uint64_t BlockId;
typedef std::uint32_t ObjectId;

}  // namespace blockchain
}  // namespace storage
}  // namespace concord
