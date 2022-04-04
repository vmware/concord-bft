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

#pragma once

#include <string>
#include <type_traits>

#include "state_snapshot_interface.hpp"
#include "categorization/kv_blockchain.h"
#include "rocksdb/native_client.h"
#include "blockchain_type.hpp"
#include "ReplicaConfig.hpp"

using concord::storage::rocksdb::NativeClient;
using concord::kvbc::IKVBCStateSnapshot;
using concord::kvbc::KVBlockChain;
using concord::kvbc::categorization::Converter;

namespace concord::kvbc::statesnapshot {
class KVBCStateSnapshot : public IKVBCStateSnapshot {
 public:
  template <typename OPTIONS,
            typename = std::enable_if_t<std::is_same_v<OPTIONS, NativeClient::DefaultOptions> ||
                                        std::is_same_v<OPTIONS, NativeClient::ExistingOptions> ||
                                        std::is_same_v<OPTIONS, NativeClient::UserOptions>>>
  [[maybe_unused]] explicit KVBCStateSnapshot(const std::string& db_path, bool read_only, const OPTIONS& options) {
    const auto link_st_chain = false;
    // The logic of choosing type of block chain will happen here.
    if (bftEngine::ReplicaConfig::instance().kvBlockchainVersion == BLOCKCHAIN_VERSION::CATEGORIZED_BLOCKCHAIN) {
      auto kv = std::make_shared<CatBCimpl::element_type>(NativeClient::newClient(db_path, read_only, options),
                                                          link_st_chain);
      kvbc_.emplace<CatBCimpl>(kv);
    }
  }

  explicit KVBCStateSnapshot(
      const std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client,
      const std::optional<std::map<std::string, categorization::CATEGORY_TYPE>>& category_types = std::nullopt) {
    const auto link_st_chain = false;
    if (bftEngine::ReplicaConfig::instance().kvBlockchainVersion == BLOCKCHAIN_VERSION::CATEGORIZED_BLOCKCHAIN) {
      auto kv = std::make_shared<CatBCimpl::element_type>(native_client, link_st_chain, category_types);
      kvbc_.emplace<CatBCimpl>(kv);
    }
  }

  virtual void computeAndPersistPublicStateHash(
      BlockId checkpoint_block_id,
      const Converter& value_converter = [](std::string&& s) -> std::string { return std::move(s); }) override;

  virtual std::optional<PublicStateKeys> getPublicStateKeys() const override;

  virtual void iteratePublicStateKeyValues(const std::function<void(std::string&&, std::string&&)>& f) const override;

  virtual bool iteratePublicStateKeyValues(const std::function<void(std::string&&, std::string&&)>& f,
                                           const std::string& after_key) const override;

  virtual ~KVBCStateSnapshot() {}

 private:
  KVBlockChain kvbc_;
};
}  // end of namespace concord::kvbc::statesnapshot
