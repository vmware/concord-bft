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

#include "categorization/kv_blockchain.h"
#include "endianness.hpp"
#include "PersistentStorage.hpp"

#include <memory>

namespace concord::kvbc {

// Persist the last KVBC block ID in big-endian in metadata's user data field.
inline void persistLastBlockIdInMetadata(const categorization::KeyValueBlockchain &blockchain,
                                         const std::shared_ptr<bftEngine::impl::PersistentStorage> &metadata) {
  const auto userData = concordUtils::toBigEndianArrayBuffer(blockchain.getLastReachableBlockId());
  metadata->setUserData(userData.data(), userData.size());
}

}  // namespace concord::kvbc
