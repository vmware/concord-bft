// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
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

namespace concord::kvbc::v4blockchain::detail {

inline const auto BLOCKS_CF = std::string{"v4_blocks"};
inline const auto ST_CHAIN_CF = std::string{"v4_st_chain"};
inline const auto LATEST_KEYS_CF = std::string{"v4_latest_keys"};
inline const auto IMMUTABLE_KEYS_CF = std::string{"v4_immutable_keys"};
inline const auto CATEGORIES_CF = std::string{"v4_categories"};
inline const auto MISC_CF = std::string{"v4_misc"};

}  // namespace concord::kvbc::v4blockchain::detail
