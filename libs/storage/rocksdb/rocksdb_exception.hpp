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

#ifdef USE_ROCKSDB

#include <rocksdb/status.h>

#include <stdexcept>

namespace concord::storage::rocksdb {

struct RocksDBException : std::runtime_error {
  RocksDBException(const std::string &what, ::rocksdb::Status &&s) : std::runtime_error{what}, status{std::move(s)} {}
  const ::rocksdb::Status status;
};

}  // namespace concord::storage::rocksdb

#endif  // USE_ROCKSDB
