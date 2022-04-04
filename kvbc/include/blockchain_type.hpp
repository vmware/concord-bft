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

#include <variant>
#include <optional>
#include <memory>
#include "categorization/kv_blockchain.h"

namespace concord::kvbc {
enum BLOCKCHAIN_VERSION { CATEGORIZED_BLOCKCHAIN = 1, NATURAL_BLOCKCHAIN = 4 };
using CatBCimpl = std::shared_ptr<concord::kvbc::categorization::KeyValueBlockchain>;

using KVBlockChain = std::variant<CatBCimpl>;
}  // end namespace concord::kvbc
