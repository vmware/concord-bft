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

#include "log/logger.hpp"
#include "storage/key_manipulator_interface.h"

namespace concord::storage::s3 {

class STKeyManipulator : public concord::storage::ISTKeyManipulator {
 public:
  STKeyManipulator(const std::string& prefix = "");

  concordUtils::Sliver generateStateTransferKey(ObjectId objectId) const override;
  concordUtils::Sliver generateSTPendingPageKey(uint32_t pageid) const override;
  concordUtils::Sliver generateSTCheckpointDescriptorKey(uint64_t chkpt) const override;
  concordUtils::Sliver generateSTReservedPageDynamicKey(uint32_t pageId, uint64_t chkpt) const override;
  concordUtils::Sliver getReservedPageKeyPrefix() const override;

 protected:
  static logging::Logger& logger() {
    static logging::Logger logger_ = logging::getLogger("concord.storage.s3");
    return logger_;
  }
  std::string prefix_;
};

}  // namespace concord::storage::s3
