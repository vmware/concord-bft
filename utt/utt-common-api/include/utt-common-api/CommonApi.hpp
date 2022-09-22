// UTT Common API
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
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

#include <map>
#include <string>
#include <vector>

namespace utt {

// [TODO-UTT] All types are tentative

/// @brief The complete configuration required to deploy a UTT instance, contains public and secret data
using Configuration = std::vector<uint8_t>;

/// @brief The public part of a UTT instance configuration visible to all users
using PublicConfig = std::vector<uint8_t>;

}  // namespace utt