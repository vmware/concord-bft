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

#include "s3/key_manipulator.h"
#include "string.hpp"
#include <string>

namespace concord::storage::s3 {

using ::concordUtils::Sliver;

STKeyManipulator::STKeyManipulator(const std::string& prefix)
    : prefix_((prefix.size() ? prefix + std::string("/") : "") + "metadata/st/") {}

Sliver STKeyManipulator::generateStateTransferKey(ObjectId objectId) const {
  LOG_DEBUG(logger(), prefix_ + std::to_string(objectId));
  return prefix_ + std::to_string(objectId);
}

Sliver STKeyManipulator::generateSTPendingPageKey(uint32_t pageid) const {
  throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(" not implemented"));
}

Sliver STKeyManipulator::generateSTCheckpointDescriptorKey(uint64_t chkpt) const {
  LOG_DEBUG(logger(), prefix_ + "chckp_desc/" + std::to_string(chkpt));
  return prefix_ + "chckp_desc/" + std::to_string(chkpt);
}

Sliver STKeyManipulator::generateSTReservedPageDynamicKey(uint32_t pageId, uint64_t chkpt) const {
  throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(" not implemented"));
}
Sliver STKeyManipulator::getReservedPageKeyPrefix() const {
  throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(" not implemented"));
}

}  // namespace concord::storage::s3
