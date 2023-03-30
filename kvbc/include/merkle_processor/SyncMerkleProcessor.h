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

#pragma once

#include "BaseMerkleProcessor.h"

namespace concord {
namespace kvbc {
namespace sparse_merkle {

using concordUtils::bufferToHex;

class SyncMerkleProcessor : public BaseMerkleProcessor {
 public:
  virtual ~SyncMerkleProcessor() = default;

  virtual void Init(uint numVersionsStored) override;

  virtual void BeginVersionUpdateBatch() override;
  virtual void UpdateAccountTree(const address& addr, const std::string& key, const value& data) override;

  virtual void CommitVersionUpdateBatch() override;

  virtual std::vector<std::string> GetAccountMerkleRootPath(const address& addr) override;
  virtual std::vector<std::string> GetAccountStorageKeyMerklePath(const address& addr, const std::string& key) override;
  virtual std::vector<concord::Byte> GetProof(const std::string& key) override;
  virtual bool VerifyMerkleTreePath(std::string root_hash, std::string key, std::vector<std::string> path) override;
  virtual void WaitForScheduledTasks() override;
};

}  // namespace sparse_merkle
}  // namespace kvbc
}  // namespace concord
