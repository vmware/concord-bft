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
//
// This convenience header combines different block implementations.

#pragma once

#include <vector>
#include <string>
#include <stdint.h>

namespace concord::secretsmanager {

struct KeyParams {
  KeyParams(const std::string& pkey, const std::string& piv);

  std::vector<uint8_t> key;
  std::vector<uint8_t> iv;
};

}  // namespace concord::secretsmanager
