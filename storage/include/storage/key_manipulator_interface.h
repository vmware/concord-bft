// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
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

#include <stdint.h>

#include "db_types.h"
#include "sliver.hpp"

namespace concord::storage {

class IMetadataKeyManipulator {
 public:
  virtual concordUtils::Sliver generateMetadataKey(ObjectId objectId) const { return concordUtils::Sliver(); }
  virtual concordUtils::Sliver generateMetadataKey(const concordUtils::Sliver&) const { return concordUtils::Sliver(); }
  virtual ~IMetadataKeyManipulator() = default;
};

class ISTKeyManipulator {
 public:
  virtual concordUtils::Sliver generateStateTransferKey(ObjectId objectId) const = 0;
  virtual concordUtils::Sliver generateSTPendingPageKey(uint32_t pageid) const = 0;
  virtual concordUtils::Sliver generateSTCheckpointDescriptorKey(uint64_t chkpt) const = 0;
  virtual concordUtils::Sliver generateSTReservedPageDynamicKey(uint32_t pageId, uint64_t chkpt) const = 0;
  virtual concordUtils::Sliver getReservedPageKeyPrefix() const = 0;
  virtual ~ISTKeyManipulator() = default;
};

}  // namespace concord::storage
