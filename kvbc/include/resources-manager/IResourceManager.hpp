// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
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

namespace concord::performance {

struct PruneInfo {
  long double blocksPerSecond{0};
  uint64_t batchSize{0};
};

class IResourceManager {
 public:
  virtual ~IResourceManager() = default;
  /*
    getPruneBlocksPerSecond implementation computes proper pruning pace based on its measuremnts.
  */
  virtual PruneInfo getPruneInfo() = 0;
  virtual void setPeriod(std::uint64_t) = 0;
};

}  // namespace concord::performance
