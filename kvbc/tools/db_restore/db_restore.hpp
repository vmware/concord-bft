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

#include "Logger.hpp"
#include "categorization/kv_blockchain.h"
#include "tools/db_integrity_check/integrity_checker.hpp"

namespace concord::kvbc::tools {
namespace po = boost::program_options;
class DBRestore {
 public:
  DBRestore();
  /** Parse CLI params */
  void parseCLIArgs(int argc, char** argv);
  void restore();

 protected:
  void initRocksDB(const fs::path&);
  std::unique_ptr<categorization::KeyValueBlockchain> kv_blockchain_;
  tools::IntegrityChecker integrity_check_;
  IStorageFactory::DatabaseSet rocksdb_dataset_;
  po::variables_map var_map_;
  po::options_description cli_options_{"\nDB restore options"};
  po::options_description cli_mandatory_options_{"\nDB restore mandatory options"};
  logging::Logger logger_ = logging::getLogger("concord.kvbc.tools.restore");
};

}  // namespace concord::kvbc::tools
