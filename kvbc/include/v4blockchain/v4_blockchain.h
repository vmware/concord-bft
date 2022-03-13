// Concord
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
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

#include "categorization/updates.h"
#include "rocksdb/native_client.h"
#include "v4blockchain/detail/st_chain.h"
#include "v4blockchain/detail/latest_keys.h"
#include "v4blockchain/detail/blockchain.h"
#include <memory>
#include <string>

namespace concord::kvbc::v4blockchain {

class KeyValueBlockchain {
 public:
  KeyValueBlockchain(const std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client,
                     bool link_st_chain,
                     const std::optional<std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE>>&
                         category_types = std::nullopt);

  /////////////////////// Add Block ///////////////////////

  // BlockId addBlock(Updates&& updates);
 private:
  std::shared_ptr<concord::storage::rocksdb::NativeClient> native_client_;
  v4blockchain::detail::Blockchain block_chain_;
  v4blockchain::detail::StChain state_transfer_chain_;
  v4blockchain::detail::LatestKeys latest_keys_;
};

}  // namespace concord::kvbc::v4blockchain