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

#include "log/logger.hpp"
#include "kvbc_adapter/replica_adapter.hpp"
#include "integrity_checker.hpp"

namespace concord::kvbc::tools {
class DBRestore {
 public:
  DBRestore(std::shared_ptr<IntegrityChecker> checker, logging::Logger logger) : checker_(checker), logger_(logger) {}
  void restore();
  void initRocksDB(const fs::path&, uint32_t);

 protected:
  std::unique_ptr<concord::kvbc::adapter::ReplicaBlockchain> kv_blockchain_;
  std::shared_ptr<IntegrityChecker> checker_;
  logging::Logger logger_;
};

}  // namespace concord::kvbc::tools
