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
//
// Update added to spsc subscription queue for consumption by subscribers.
// Note: BlockUpdate is NOT stored on the blockchain. Consequently it does NOT update the blockchain state.

#ifndef CONCORD_KVBC_BLOCK_UPDATE_H_
#define CONCORD_KVBC_BLOCK_UPDATE_H_

#include <optional>

#include "categorization/updates.h"
#include "kv_types.hpp"

namespace concord::kvbc {

struct BlockUpdate {
  kvbc::BlockId block_id;
  std::string correlation_id;
  kvbc::categorization::ImmutableInput immutable_kv_pairs;
  std::optional<std::string> parent_span;
};

}  // namespace concord::kvbc

#endif  // CONCORD_KVBC_BLOCK_UPDATE_H_
