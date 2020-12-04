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

#include <string>

namespace concord::kvbc::categorization::detail {

inline const auto SHARED_KV_DATA_CF_SUFFIX = std::string{"_skvdata"};
inline const auto SHARED_KV_LATEST_KEY_VER_CF_SUFFIX = std::string{"_skvlatest"};

inline const auto BLOCKS_CF = std::string{"blocks"};

}  // namespace concord::kvbc::categorization::detail
