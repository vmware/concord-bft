// Concord
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
#pragma once
#include <memory>
#include "Logger.hpp"
#include "categorization/kv_blockchain.h"
#include "integrity_checker.hpp"

namespace concord::kvbc::tools {
class DBRestore {
 public:
  DBRestore(std::shared_ptr<IntegrityChecker> checker, logging::Logger logger) : checker_(checker), logger_(logger) {}
  void restore();
  void initRocksDB(const fs::path&);

 protected:
  std::unique_ptr<categorization::KeyValueBlockchain> kv_blockchain_;
  IStorageFactory::DatabaseSet rocksdb_dataset_;
  std::shared_ptr<IntegrityChecker> checker_;
  logging::Logger logger_;
};

}  // namespace concord::kvbc::tools
