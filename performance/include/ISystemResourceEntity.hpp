// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <cstdint>
#include <string>

namespace concord::performance {
class ISystemResourceEntity {
 public:
  virtual int64_t getAvailableResources() const = 0;
  virtual uint64_t getMeasurements() const = 0;
  virtual const std::string& getResourceName() const = 0;
};
}  // namespace concord::performance