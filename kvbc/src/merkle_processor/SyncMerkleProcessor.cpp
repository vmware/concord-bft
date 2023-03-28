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

#include "merkle_processor/SyncMerkleProcessor.h"
#include "kvbc_app_filter/kvbc_key_types.h"
#include "util/hex_tools.hpp"

namespace concord {
namespace kvbc {
namespace sparse_merkle {

using concordUtils::bufferToHex;

void SyncMerkleProcessor::Init(uint numVersionsStored) {}

void SyncMerkleProcessor::BeginVersionUpdateBatch() {}

void SyncMerkleProcessor::UpdateAccountTree(const address& addr, const std::string& key, const value& data) {
  LOG_INFO(V4_BLOCK_LOG,
           "MerkleProcessor: UpdateAccountTree(addr=" << bufferToHex(addr.data(), sizeof(addr))
                                                    << ", key=" << bufferToHex(key.c_str(), key.size()));
}

void SyncMerkleProcessor::CommitVersionUpdateBatch() {
  LOG_INFO(V4_BLOCK_LOG, "MerkleProcessor: CommitVersionUpdateBatch");
}

std::vector<std::string> SyncMerkleProcessor::GetAccountMerkleRootPath(const address& addr) { return {}; }

std::vector<std::string> SyncMerkleProcessor::GetAccountStorageKeyMerklePath(const address& addr,
                                                                           const std::string& key) {
  return {};
}

std::string SyncMerkleProcessor::GetProof(const std::string& key) { return {}; }

bool SyncMerkleProcessor::VerifyMerkleTreePath(std::string root_hash, std::string key, std::vector<std::string> path) {
  return true;
}

void SyncMerkleProcessor::WaitForScheduledTasks() {}

}  // namespace sparse_merkle
}  // namespace kvbc
}  // namespace concord
