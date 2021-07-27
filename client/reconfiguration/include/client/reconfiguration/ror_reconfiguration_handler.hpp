// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.
#pragma once

#include "Logger.hpp"
#include "client/reconfiguration/cre_interfaces.hpp"

namespace concord::client::reconfiguration {
class RorReconfigurationHandler : public IStateHandler {
 public:
  RorReconfigurationHandler(std::function<void(uint64_t)> fn);
  bool validate(const State&) const override;
  bool execute(const State&, WriteState&) override;

 private:
  logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("bftengine.RorReconfigurationHandler"));
    return logger_;
  }
  std::function<void(uint64_t)> storeReconfigBlockToMdtCb_;
};
}  // namespace concord::client::reconfiguration