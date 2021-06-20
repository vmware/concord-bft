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

#include "categorization/immutable_kv_category.h"
#include "categorization/block_merkle_category.h"
#include "categorization/versioned_kv_category.h"

namespace concord::kvbc::categorization {

using Category =
    std::variant<detail::ImmutableKeyValueCategory, detail::BlockMerkleCategory, detail::VersionedKeyValueCategory>;
using CategoriesMap = std::map<std::string, Category>;

}  // namespace concord::kvbc::categorization
