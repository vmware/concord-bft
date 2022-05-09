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

#include "kvbc_adapter/replica_adapter.hpp"
#include "kvbc_adapter/categorization/blocks_deleter_adapter.hpp"
#include "kvbc_adapter/categorization/kv_blockchain_adapter.hpp"
#include "kvbc_adapter/categorization/app_state_adapter.hpp"
#include "kvbc_adapter/categorization/state_snapshot_adapter.hpp"
#include "kvbc_adapter/v4blockchain/blocks_deleter_adapter.hpp"
#include "kvbc_adapter/v4blockchain/blocks_adder_adapter.hpp"
#include "kvbc_adapter/v4blockchain/blocks_reader_adapter.hpp"

namespace concord::kvbc::adapter {
ReplicaBlockchain::~ReplicaBlockchain() {
  deleter_ = nullptr;
  reader_ = nullptr;
  adder_ = nullptr;
  app_state_ = nullptr;
  state_snapshot_ = nullptr;
  db_chkpt_ = nullptr;
}

void ReplicaBlockchain::switch_to_rawptr() {
  deleter_ = up_deleter_.get();
  reader_ = up_reader_.get();
  adder_ = up_adder_.get();
  app_state_ = up_app_state_.get();
  state_snapshot_ = up_state_snapshot_.get();
  db_chkpt_ = up_db_chkpt_.get();
}

ReplicaBlockchain::ReplicaBlockchain(
    const std::shared_ptr<concord::storage::rocksdb::NativeClient> &native_client,
    bool link_st_chain,
    const std::optional<std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE>> &category_types,
    const std::optional<aux::AdapterAuxTypes> &aux_types)
    : logger_(logging::getLogger("skvbc.replica.adapter")) {
  if (bftEngine::ReplicaConfig::instance().kvBlockchainVersion == BLOCKCHAIN_VERSION::CATEGORIZED_BLOCKCHAIN) {
    LOG_INFO(CAT_BLOCK_LOG, "Instantiating categorized type blockchain");
    kvbc_ = std::make_shared<concord::kvbc::categorization::KeyValueBlockchain>(
        native_client, link_st_chain, category_types);
    if (aux_types.has_value()) {
      kvbc_->setAggregator(aux_types->aggregator_);
    }
    up_deleter_ = std::make_unique<concord::kvbc::adapter::categorization::BlocksDeleterAdapter>(kvbc_, aux_types);
    up_reader_ = std::make_unique<concord::kvbc::adapter::categorization::KeyValueBlockchain>(kvbc_);
    up_adder_ = std::make_unique<concord::kvbc::adapter::categorization::KeyValueBlockchain>(kvbc_);
    up_app_state_ = std::make_unique<concord::kvbc::adapter::categorization::AppStateAdapter>(kvbc_);
    up_state_snapshot_ =
        std::make_unique<concord::kvbc::adapter::categorization::statesnapshot::KVBCStateSnapshot>(kvbc_);
    up_db_chkpt_ = std::make_unique<concord::kvbc::adapter::categorization::statesnapshot::KVBCStateSnapshot>(kvbc_);
  } else if (bftEngine::ReplicaConfig::instance().kvBlockchainVersion == BLOCKCHAIN_VERSION::NATURAL_BLOCKCHAIN) {
    LOG_INFO(V4_BLOCK_LOG, "Instantiating v4 type blockchain");
    v4_kvbc_ =
        std::make_shared<concord::kvbc::v4blockchain::KeyValueBlockchain>(native_client, link_st_chain, category_types);
    up_deleter_ = std::make_unique<concord::kvbc::adapter::v4blockchain::BlocksDeleterAdapter>(v4_kvbc_, aux_types);
    up_reader_ = std::make_unique<concord::kvbc::adapter::v4blockchain::BlocksReaderAdapter>(v4_kvbc_);
    up_adder_ = std::make_unique<concord::kvbc::adapter::v4blockchain::BlocksAdderAdapter>(v4_kvbc_);
  }

  switch_to_rawptr();
  ConcordAssertNE(deleter_, nullptr);
  ConcordAssertNE(reader_, nullptr);
  ConcordAssertNE(adder_, nullptr);
  ConcordAssertNE(app_state_, nullptr);
  ConcordAssertNE(state_snapshot_, nullptr);
  ConcordAssertNE(db_chkpt_, nullptr);
}

std::optional<PublicStateKeys> ReplicaBlockchain::getPublicStateKeys() const {
    const auto opt_val = IReader->getLatest(kConcordInternalCategoryId, keyTypes::state_public_key_set);
    if (!opt_val) {
      return std::nullopt;
    }
    auto public_state = PublicStateKeys{};
    const auto val = std::get_if<VersionedValue>(&opt_val.value());
    ConcordAssertNE(val, nullptr);
    detail::deserialize(val->data, public_state);
    return std::make_optional(std::move(public_state));
  }

  void ReplicaBlockchain::iteratePublicStateKeyValues(const std::function<void(std::string &&, std::string &&)> &f) const{
    iteratePublicStateKeyValuesImpl(f);
  }

  bool ReplicaBlockchain::iteratePublicStateKeyValues(const std::function<void(std::string &&, std::string &&)> &f,
                                   const std::string &after_key) const override final {
    return iteratePublicStateKeyValuesImpl(f, after_key);
  }


  bool ReplicaBlockchain::iteratePublicStateKeyValuesImpl(const std::function<void(std::string&&, std::string&&)>& f,
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
  auto opt_values = std::vector<std::optional<Value>>{};
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
    IReader->multiGetLatest(kExecutionProvableCategory, keys_batch, opt_values);
    ConcordAssertEQ(keys_batch.size(), opt_values.size());
    for (auto i = 0ull; i < keys_batch.size(); ++i) {
      auto& opt_value = opt_values[i];
      ConcordAssert(opt_value.has_value());
      auto value = std::get_if<MerkleValue>(&opt_value.value());
      ConcordAssertNE(value, nullptr);
      f(std::move(keys_batch[i]), std::move(value->data));
    }
  }
  return true;
}

void KeyValueBlockchain::computeAndPersistPublicStateHash(BlockId checkpoint_block_id,
                                                          const Converter& value_converter  = [](std::string &&s) -> std::string { return std::move(s); }) {
  auto hash = kInitialHash;
  iteratePublicStateKeyValues([&](std::string&& key, std::string&& value) {
    value = value_converter(std::move(value));
    auto hasher = Hasher{};
    hasher.init();
    hasher.update(hash.data(), hash.size());
    const auto key_hash = detail::hash(key);
    hasher.update(key_hash.data(), key_hash.size());
    hasher.update(value.data(), value.size());
    hash = hasher.finish();
  });
  native_client_->put(concord::kvbc::bcutil::BlockChainUtils::publicStateHashKey(),
                      detail::serialize(StateHash{checkpoint_block_id, hash}));
}


}  // namespace concord::kvbc::adapter
