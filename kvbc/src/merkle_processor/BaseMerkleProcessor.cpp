// Concord
//
// Copyright (c) 2023 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#include "merkle_processor/BaseMerkleProcessor.h"

namespace concord {
namespace kvbc {
namespace sparse_merkle {

bool BaseMerkleProcessor::needProcessing(char type) const {
  if (type == kKvbKeyEthBalance || type == kKvbKeyEthCode || type == kKvbKeyEthStorage || type == kKvbKeyEthNonce) {
    return true;
  }
  return false;
}

IMerkleProcessor::address BaseMerkleProcessor::getAddress(const std::string& key) const {
  ConcordAssert(key.size() > IMerkleProcessor::address_size);  // Include 1 byte indicating key type
  return address(&key[1], IMerkleProcessor::address_size);
}

void BaseMerkleProcessor::ProcessUpdates(const categorization::Updates& updates) {
  BeginVersionUpdateBatch();
  for (const auto& k : updates.categoryUpdates().kv) {
    if (const categorization::BlockMerkleInput* pval = std::get_if<categorization::BlockMerkleInput>(&k.second)) {
      for (const auto& v : pval->kv) {
        if (needProcessing(v.first[0])) {
          auto hasher = Hasher{};
          const auto value_hash = hasher.digest(v.second.data(), v.second.size());
          LOG_INFO(V4_BLOCK_LOG, "MerkleProcessor: categorization::BlockMerkleInput");
          UpdateAccountTree(getAddress(v.first), v.first, value_hash);
        }
      }
    } else if (const categorization::VersionedInput* pval = std::get_if<categorization::VersionedInput>(&k.second)) {
      for (const auto& v : pval->kv) {
        if (needProcessing(v.first[0])) {
          auto hasher = Hasher{};
          const auto value_hash = hasher.digest(v.second.data.data(), v.second.data.size());
          LOG_INFO(V4_BLOCK_LOG, "MerkleProcessor: categorization::VersionedInput");
          UpdateAccountTree(getAddress(v.first), v.first, value_hash);
        }
      }
    } else if (const categorization::ImmutableInput* pval = std::get_if<categorization::ImmutableInput>(&k.second)) {
      for (const auto& v : pval->kv) {
        if (needProcessing(v.first[0])) {
          auto hasher = Hasher{};
          const auto value_hash = hasher.digest(v.second.data.data(), v.second.data.size());
          LOG_INFO(V4_BLOCK_LOG, "MerkleProcessor: categorization::ImmutableInput");
          UpdateAccountTree(getAddress(v.first), v.first, value_hash);
        }
      }
    }
  }
  CommitVersionUpdateBatch();
}

}  // namespace sparse_merkle
}  // namespace kvbc
}  // namespace concord