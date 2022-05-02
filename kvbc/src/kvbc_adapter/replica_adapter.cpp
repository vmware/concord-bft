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

}  // namespace concord::kvbc::adapter
