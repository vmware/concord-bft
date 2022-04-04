// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "assertUtils.hpp"
#include "kvbc_adapter/state_snapshot_adapter.hpp"

namespace concord::kvbc::statesnapshot {

void KVBCStateSnapshot::computeAndPersistPublicStateHash(BlockId checkpoint_block_id,
                                                         const Converter& value_converter) {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    std::get<CatBCimpl>(kvbc_)->computeAndPersistPublicStateHash(checkpoint_block_id, value_converter);
    return;
  }
  ConcordAssert(false);
}

std::optional<PublicStateKeys> KVBCStateSnapshot::getPublicStateKeys() const {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    return std::get<CatBCimpl>(kvbc_)->getPublicStateKeys();
  }
  ConcordAssert(false);
}

void KVBCStateSnapshot::iteratePublicStateKeyValues(const std::function<void(std::string&&, std::string&&)>& f) const {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    std::get<CatBCimpl>(kvbc_)->iteratePublicStateKeyValues(f);
    return;
  }
  ConcordAssert(false);
}

bool KVBCStateSnapshot::iteratePublicStateKeyValues(const std::function<void(std::string&&, std::string&&)>& f,
                                                    const std::string& after_key) const {
  if (std::holds_alternative<CatBCimpl>(kvbc_)) {
    return std::get<CatBCimpl>(kvbc_)->iteratePublicStateKeyValues(f, after_key);
  }
  ConcordAssert(false);
}
}  // End of namespace concord::kvbc::statesnapshot
