// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "bftclient/config.h"
namespace cre::config {
class Config {
 public:
  bft::client::ClientConfig client_config_;
  uint16_t id_;
  uint64_t interval_timeout_ms_{1000};
};
}  // namespace cre::config