// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
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

#include "assertUtils.hpp"
#include "db_interfaces.h"
#include "replica_adapter.hpp"

#include <memory>

namespace concord::kvbc::adapter {

// A utility that adapts a KeyValueBlockchain instance to an IReader.
class IdempotentReader : public IReader {
 public:
  // Constructs a IdempotentReader from a non-null KeyValueBlockchain pointer.
  // Precondition: kvbc != nullptr
  IdempotentReader(const std::shared_ptr<const ReplicaBlockchain> &kvbc) : kvbc_{kvbc} {
    ConcordAssertNE(kvbc, nullptr);
  }

  virtual ~IdempotentReader() = default;

 public:
  std::optional<categorization::Value> get(const std::string &category_id,
                                           const std::string &key,
                                           BlockId block_id) const override {
    return kvbc_->get(category_id, key, block_id);
  }

  std::optional<categorization::Value> getLatest(const std::string &category_id,
                                                 const std::string &key) const override {
    return kvbc_->getLatest(category_id, key);
  }

  void multiGet(const std::string &category_id,
                const std::vector<std::string> &keys,
                const std::vector<BlockId> &versions,
                std::vector<std::optional<categorization::Value>> &values) const override {
    kvbc_->multiGet(category_id, keys, versions, values);
  }

  void multiGetLatest(const std::string &category_id,
                      const std::vector<std::string> &keys,
                      std::vector<std::optional<categorization::Value>> &values) const override {
    kvbc_->multiGetLatest(category_id, keys, values);
  }

  std::optional<categorization::TaggedVersion> getLatestVersion(const std::string &category_id,
                                                                const std::string &key) const override {
    return kvbc_->getLatestVersion(category_id, key);
  }

  void multiGetLatestVersion(const std::string &category_id,
                             const std::vector<std::string> &keys,
                             std::vector<std::optional<categorization::TaggedVersion>> &versions) const override {
    kvbc_->multiGetLatestVersion(category_id, keys, versions);
  }

  std::optional<categorization::Updates> getBlockUpdates(BlockId block_id) const override {
    return kvbc_->getBlockUpdates(block_id);
  }

  BlockId getGenesisBlockId() const override { return kvbc_->getGenesisBlockId(); }

  BlockId getLastBlockId() const override { return kvbc_->getLastBlockId(); }

 private:
  const std::shared_ptr<const ReplicaBlockchain> kvbc_;
};

}  // namespace concord::kvbc::adapter
