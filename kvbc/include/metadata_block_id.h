// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of sub-components with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE file.

#pragma once

#include "assertUtils.hpp"
#include "kvbc_adapter/replica_adapter.hpp"
#include "endianness.hpp"
#include "kv_types.hpp"
#include "PersistentStorage.hpp"

#include <memory>
#include <optional>

namespace concord::kvbc {

// Persist the last KVBC block ID in big-endian in metadata's user data field.
template <bool in_transaction>
void persistLastBlockIdInMetadata(const adapter::ReplicaBlockchain &blockchain,
                                  const std::shared_ptr<bftEngine::impl::PersistentStorage> &metadata) {
  const auto user_data = concordUtils::toBigEndianArrayBuffer(blockchain.getLastBlockId());
  if constexpr (in_transaction) {
    metadata->setUserDataInTransaction(user_data.data(), user_data.size());
  } else {
    metadata->setUserDataAtomically(user_data.data(), user_data.size());
  }
}

template <bool in_transaction>
void persistLastBlockIdInMetadata(const BlockId bid,
                                  const std::shared_ptr<bftEngine::impl::PersistentStorage> &metadata) {
  const auto user_data = concordUtils::toBigEndianArrayBuffer(bid);
  if constexpr (in_transaction) {
    metadata->setUserDataInTransaction(user_data.data(), user_data.size());
  } else {
    metadata->setUserDataAtomically(user_data.data(), user_data.size());
  }
}

inline std::optional<BlockId> getLastBlockIdFromMetadata(
    const std::shared_ptr<bftEngine::impl::PersistentStorage> &metadata) {
  const auto ud = metadata->getUserData();
  if (!ud) {
    return std::nullopt;
  }
  ConcordAssertEQ(sizeof(BlockId), ud->size());
  return concordUtils::fromBigEndianBuffer<BlockId>(ud->data());
}

}  // namespace concord::kvbc
