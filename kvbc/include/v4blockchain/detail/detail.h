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

#pragma once
#include <chrono>
#include "kvbc_key_types.hpp"
#include "rocksdb/native_client.h"
#include "blockchain_misc.hpp"

namespace concord::kvbc::v4blockchain::detail {

struct ScopedDuration {
  ScopedDuration(const char* msg) : msg_(msg) {}
  ~ScopedDuration() {
    auto jobDuration =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count();
    LOG_INFO(V4_BLOCK_LOG, msg_ << " duration [" << jobDuration << "] micro");
  }

  const char* msg_;
  const std::chrono::time_point<std::chrono::steady_clock> start = std::chrono::steady_clock::now();
};

// Put a dummy KV, flush the CF and delete the KV
inline void persistCf(const std::string& cf, std::shared_ptr<concord::storage::rocksdb::NativeClient>& native_client) {
  native_client->put(cf, kvbc::keyTypes::v4_cf_flush, kvbc::V4Version());
  auto& rocksdb = native_client->rawDB();
  auto* handle = native_client->columnFamilyHandle(cf);
  auto s = rocksdb.Flush(::rocksdb::FlushOptions{}, handle);
  ConcordAssert(s.ok());
  native_client->del(cf, kvbc::keyTypes::v4_cf_flush);
  LOG_INFO(V4_BLOCK_LOG, "Persisted column family " << cf);
}

}  // namespace concord::kvbc::v4blockchain::detail
