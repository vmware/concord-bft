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

#include "IProofProcessor.h"
#include "kvbc_app_filter/kvbc_key_types.h"

namespace concord {
namespace kvbc {

class EmptyProofProcessor : public IProofProcessor {
 public:
  virtual ~EmptyProofProcessor() = default;

  virtual void Init(uint numVersionsStored) override {}

  virtual void ProcessUpdates(const categorization::Updates& updates) override {}

  virtual std::vector<concord::Byte> GetProof(const std::string& key) override { return {}; }
  virtual void WaitForScheduledTasks() override {}
};

}  // namespace kvbc
}  // namespace concord
