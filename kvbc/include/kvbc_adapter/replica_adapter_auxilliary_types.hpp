// Concord
//
// Copyright (c) 2022-2023 VMware, Inc. All Rights Reserved.
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
#include <string>
#include <vector>
#include <memory>

#include "Metrics.hpp"
#include "ISystemResourceEntity.hpp"

namespace concord::kvbc::adapter::aux {

struct AdapterAuxTypes {
  explicit AdapterAuxTypes(std::shared_ptr<concordMetrics::Aggregator> aggregator,
                           concord::performance::ISystemResourceEntity& resource_entity)
      : aggregator_(aggregator), resource_entity_(resource_entity) {}
  std::shared_ptr<concordMetrics::Aggregator> aggregator_;
  concord::performance::ISystemResourceEntity& resource_entity_;
};

}  // namespace concord::kvbc::adapter::aux