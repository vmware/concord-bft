// Concord
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <memory>

#include "blockchain_misc.hpp"
#include "kvbc_adapter/replica_adapter.hpp"
#include "categorization/db_categories.h"
#include "categorization/column_families.h"
#include "v4blockchain/detail/column_families.h"

template <concord::kvbc::BLOCKCHAIN_VERSION V>
class BlockchainAdapter {
 public:
  BlockchainAdapter(const std::shared_ptr<concord::storage::rocksdb::NativeClient> &native_client,
                    bool link_st_chain,
                    const std::optional<concord::kvbc::adapter::aux::AdapterAuxTypes> &aux_types = std::nullopt) {
    bftEngine::ReplicaConfig::instance().kvBlockchainVersion = V;
    replica_blockchain_ = std::make_shared<concord::kvbc::adapter::ReplicaBlockchain>(
        native_client, link_st_chain, getCategoryMap(), aux_types);
  }

  std::shared_ptr<concord::kvbc::adapter::ReplicaBlockchain> &getAdapter() { return replica_blockchain_; }

 private:
  const std::optional<std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE>> getCategoryMap() {
    return std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE>{
        {concord::kvbc::categorization::kExecutionProvableCategory,
         concord::kvbc::categorization::CATEGORY_TYPE::block_merkle},
        {concord::kvbc::categorization::kExecutionPrivateCategory,
         concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv},
        {concord::kvbc::categorization::kExecutionEventsCategory,
         concord::kvbc::categorization::CATEGORY_TYPE::immutable},
        {concord::kvbc::categorization::kExecutionEventGroupDataCategory,
         concord::kvbc::categorization::CATEGORY_TYPE::immutable},
        {concord::kvbc::categorization::kExecutionEventGroupTagCategory,
         concord::kvbc::categorization::CATEGORY_TYPE::immutable},
        {concord::kvbc::categorization::kExecutionEventGroupLatestCategory,
         concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv},
        {concord::kvbc::categorization::kConcordInternalCategoryId,
         concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv},
        {concord::kvbc::categorization::kConcordReconfigurationCategoryId,
         concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv},
        {concord::kvbc::categorization::kRequestsRecord, concord::kvbc::categorization::CATEGORY_TYPE::immutable}};
  }

 private:
  std::shared_ptr<concord::kvbc::adapter::ReplicaBlockchain> replica_blockchain_;
};
