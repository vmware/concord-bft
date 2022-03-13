// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "v4blockchain/v4_blockchain.h"
#include "categorization/base_types.h"

namespace concord::kvbc::v4blockchain {
KeyValueBlockchain::KeyValueBlockchain(
    const std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client,
    bool link_st_chain,
    const std::optional<std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE>>& category_types)
    : native_client_{native_client},
      block_chain_{native_client_},
      state_transfer_chain_{native_client_},
      latest_keys_{native_client_} {}
}  // namespace concord::kvbc::v4blockchain