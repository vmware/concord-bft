// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <set>

#include "IStateTransfer.hpp"

namespace bftEngine {

namespace SimpleInMemoryStateTransfer {

class ISimpleInMemoryStateTransfer : public IStateTransfer {
 public:
  virtual void markUpdate(void* ptrToUpdatedRegion, uint32_t sizeOfUpdatedRegion) = 0;
};

ISimpleInMemoryStateTransfer* create(
    void* ptrToState, uint32_t sizeOfState, uint16_t myReplicaId, uint16_t fVal, uint16_t cVal, bool pedanticChecks);

}  // namespace SimpleInMemoryStateTransfer
}  // namespace bftEngine
