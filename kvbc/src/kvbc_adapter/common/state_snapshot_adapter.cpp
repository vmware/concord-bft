// Concord
//
// Copyright (c) 2022-2023 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <variant>
#include "categorization/details.h"
#include "categorization/db_categories.h"
#include "kvbc_key_types.hpp"
#include "kvbc_adapter/common/state_snapshot_adapter.hpp"

namespace concord::kvbc::adapter::common::statesnapshot {
////////////////////////////IKVBCStateSnapshot////////////////////////////////////////////////////////////////////////
void KVBCStateSnapshot::computeAndPersistPublicStateHash(BlockId checkpoint_block_id,
                                                         const Converter& value_converter) {
  auto hash = concord::kvbc::categorization::detail::hash(std::string{});
  iteratePublicStateKeyValues([&](std::string&& key, std::string&& value) {
    value = value_converter(std::move(value));
    auto hasher = concord::kvbc::categorization::Hasher{};
    hasher.init();
    hasher.update(hash.data(), hash.size());
    const auto key_hash = concord::kvbc::categorization::detail::hash(key);
    hasher.update(key_hash.data(), key_hash.size());
    hasher.update(value.data(), value.size());
    hash = hasher.finish();
  });
  native_client_->put(concord::kvbc::bcutil::BlockChainUtils::publicStateHashKey(),
                      concord::kvbc::categorization::detail::serialize(
                          concord::kvbc::categorization::StateHash{checkpoint_block_id, hash}));
}

std::optional<concord::kvbc::categorization::PublicStateKeys> KVBCStateSnapshot::getPublicStateKeys() const {
  const auto opt_val = reader_->getLatest(concord::kvbc::categorization::kConcordInternalCategoryId,
                                          concord::kvbc::keyTypes::state_public_key_set);
  if (!opt_val) {
    return std::nullopt;
  }
  auto public_state = concord::kvbc::categorization::PublicStateKeys{};
  const auto val = std::get_if<concord::kvbc::categorization::VersionedValue>(&opt_val.value());
  ConcordAssertNE(val, nullptr);
  concord::kvbc::categorization::detail::deserialize(val->data, public_state);
  return std::make_optional(std::move(public_state));
}

void KVBCStateSnapshot::iteratePublicStateKeyValues(const std::function<void(std::string&&, std::string&&)>& f) const {
  const auto ret = iteratePublicStateKeyValuesImpl(f, std::nullopt);
  ConcordAssert(ret);
}

bool KVBCStateSnapshot::iteratePublicStateKeyValues(const std::function<void(std::string&&, std::string&&)>& f,
                                                    const std::string& after_key) const {
  return iteratePublicStateKeyValuesImpl(f, after_key);
  ;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

bool KVBCStateSnapshot::iteratePublicStateKeyValuesImpl(const std::function<void(std::string&&, std::string&&)>& f,
                                                        const std::optional<std::string>& after_key) const {
  const auto public_state = getPublicStateKeys();
  if (!public_state) {
    return true;
  }

  auto idx = 0ull;
  if (after_key) {
    auto it = std::lower_bound(public_state->keys.cbegin(), public_state->keys.cend(), *after_key);
    if (it == public_state->keys.cend() || *it != *after_key) {
      return false;
    }
    // Start from the key after `after_key`.
    idx = std::distance(public_state->keys.cbegin(), it) + 1;
  }

  const auto batch_size = bftEngine::ReplicaConfig::instance().stateIterationMultiGetBatchSize;
  auto keys_batch = std::vector<std::string>{};
  keys_batch.reserve(batch_size);
  auto opt_values = std::vector<std::optional<concord::kvbc::categorization::Value>>{};
  opt_values.reserve(batch_size);
  while (idx < public_state->keys.size()) {
    keys_batch.clear();
    opt_values.clear();
    while (keys_batch.size() < batch_size) {
      if (idx == public_state->keys.size()) {
        break;
      }
      keys_batch.push_back(public_state->keys[idx]);
      ++idx;
    }
    reader_->multiGetLatest(concord::kvbc::categorization::kExecutionProvableCategory, keys_batch, opt_values);
    ConcordAssertEQ(keys_batch.size(), opt_values.size());
    for (auto i = 0ull; i < keys_batch.size(); ++i) {
      auto& opt_value = opt_values[i];
      ConcordAssert(opt_value.has_value());
      auto value = std::get_if<concord::kvbc::categorization::MerkleValue>(&opt_value.value());
      ConcordAssertNE(value, nullptr);
      f(std::move(keys_batch[i]), std::move(value->data));
    }
  }
  return true;
}

}  // namespace concord::kvbc::adapter::common::statesnapshot
