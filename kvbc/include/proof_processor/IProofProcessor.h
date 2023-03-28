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
#include "util/sliver.hpp"

namespace concord {
namespace kvbc {

// This is an interface of a general proof builder.
class IProofProcessor {
 public:
  virtual ~IProofProcessor() = default;

  // Initialize with proper parameters.
  virtual void Init(uint numVersionsStored) = 0;
  // Process all the updates that will be persisted.
  virtual void ProcessUpdates(const categorization::Updates& updates) = 0;
  // Get a serialized object serving as proof for correctness.
  virtual std::string GetProof(const std::string& key) = 0;
  // In case of async processing we use this call to sync and wait for all async tasks to complete.
  virtual void WaitForScheduledTasks() = 0;
};

}  // namespace kvbc
}  // namespace concord
