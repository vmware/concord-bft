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
#include <vector>
#include <memory>

#include "Logger.hpp"
#include "blockchain_misc.hpp"
#include "kv_types.hpp"
#include "rocksdb/native_client.h"
#include "categorization/base_types.h"
#include "categorization/updates.h"
#include "categorized_kvbc_msgs.cmf.hpp"
#include "ReplicaConfig.hpp"
#include "db_interfaces.h"
#include "categorization/kv_blockchain.h"
#include "ReplicaResources.h"
#include "replica_adapter_auxilliary_types.hpp"

using concord::storage::rocksdb::NativeClient;

namespace concord::kvbc::adapter {

class KeyValueBlockchain : public IReader, public IBlockAdder {
 public:
  virtual ~KeyValueBlockchain() { kvbc_ = nullptr; }
  explicit KeyValueBlockchain(std::shared_ptr<concord::kvbc::categorization::KeyValueBlockchain> &kvbc);

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // IReader
  std::optional<categorization::Value> get(const std::string &category_id,
                                           const std::string &key,
                                           BlockId block_id) const override final {
    return kvbc_->get(category_id, key, block_id);
  }

  std::optional<categorization::Value> getLatest(const std::string &category_id,
                                                 const std::string &key) const override final {
    return kvbc_->getLatest(category_id, key);
  }

  void multiGet(const std::string &category_id,
                const std::vector<std::string> &keys,
                const std::vector<BlockId> &versions,
                std::vector<std::optional<categorization::Value>> &values) const override final {
    return kvbc_->multiGet(category_id, keys, versions, values);
  }

  void multiGetLatest(const std::string &category_id,
                      const std::vector<std::string> &keys,
                      std::vector<std::optional<categorization::Value>> &values) const override final {
    return kvbc_->multiGetLatest(category_id, keys, values);
  }

  std::optional<categorization::TaggedVersion> getLatestVersion(const std::string &category_id,
                                                                const std::string &key) const override final {
    return kvbc_->getLatestVersion(category_id, key);
  }

  void multiGetLatestVersion(const std::string &category_id,
                             const std::vector<std::string> &keys,
                             std::vector<std::optional<categorization::TaggedVersion>> &versions) const override final {
    return kvbc_->multiGetLatestVersion(category_id, keys, versions);
  }

  std::optional<categorization::Updates> getBlockUpdates(BlockId block_id) const override final {
    return kvbc_->getBlockUpdates(block_id);
  }

  // Get the current genesis block ID in the system.
  BlockId getGenesisBlockId() const override final { return kvbc_->getGenesisBlockId(); }

  // Get the last block ID in the system.
  BlockId getLastBlockId() const override final { return kvbc_->getLastReachableBlockId(); }
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // IBlockAdder
  BlockId add(categorization::Updates &&updates) override final { return kvbc_->addBlock(std::move(updates)); }
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

 private:
  concord::kvbc::categorization::KeyValueBlockchain *kvbc_{nullptr};
};

}  // namespace concord::kvbc::adapter
