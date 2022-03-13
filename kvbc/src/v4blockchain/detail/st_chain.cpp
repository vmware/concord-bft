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

#include "v4blockchain/detail/st_chain.h"
#include "v4blockchain/detail/column_families.h"
#include "Logger.hpp"

namespace concord::kvbc::v4blockchain::detail {

StChain::StChain(const std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client)
    : native_client_{native_client} {
  if (v4blockchain::detail::createColumnFamilyIfNotExisting(v4blockchain::detail::ST_CHAIN_CF, *native_client_.get())) {
    LOG_INFO(V4_BLOCK_LOG,
             "Created [" << v4blockchain::detail::ST_CHAIN_CF << "] column family for the state transfer blockchain");
  }
}

}  // namespace concord::kvbc::v4blockchain::detail