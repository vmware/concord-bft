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

//#include <stdint.h>
#include <string>
#include <vector>
#include "categorization/updates.h"
#include "proof_processor/IProofProcessor.h"
#include "util/sliver.hpp"

namespace concord {
namespace kvbc {
namespace sparse_merkle {

// This is an interface used for managing a set of Merle trees' storage.
class IMerkleProcessor : public IProofProcessor {
 public:
  using address = concordUtils::Sliver;
  static const unsigned int address_size = 20;
  using value = std::array<unsigned char, 32>;

  virtual ~IMerkleProcessor() = default;

  virtual void Init(uint numVersionsStored) override = 0;  // from IProofProcessor

  virtual void BeginVersionUpdateBatch() = 0;
  virtual void UpdateAccountTree(const address& addr, const std::string& key, const value& data) = 0;
  virtual void CommitVersionUpdateBatch() = 0;

  virtual void ProcessUpdates(const categorization::Updates& updates) override = 0;  // from IProofProcessor

  virtual std::vector<std::string> GetAccountMerkleRootPath(const address& addr) = 0;
  virtual std::vector<std::string> GetAccountStorageKeyMerklePath(const address& addr, const std::string& key) = 0;
  virtual std::string GetProof(const std::string& key) override = 0;  // from IProofProcessor
  virtual bool VerifyMerkleTreePath(std::string root_hash, std::string key, std::vector<std::string> path) = 0;
  virtual void WaitForScheduledTasks() override = 0;  // from IProofProcessor
};

}  // namespace sparse_merkle
}  // namespace kvbc
}  // namespace concord
