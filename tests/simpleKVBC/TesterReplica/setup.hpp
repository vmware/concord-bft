// Concord
//
// Copyright (c) 2019-2020 VMware, Inc. All Rights Reserved.
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
#include <utility>
#include "ReplicaConfig.hpp"
#include "communication/ICommunication.hpp"
#include "Logger.hpp"
#include "MetricsServer.hpp"
#include "config/test_parameters.hpp"

#ifdef USE_S3_OBJECT_STORE
#include "s3/client.hpp"
#endif

namespace concord::storage {
class IDBClient;
}

namespace concord::kvbc {
class IDbAdapter;

class TestSetup {
 public:
  static std::unique_ptr<TestSetup> ParseArgs(int argc, char** argv);

  std::tuple<std::shared_ptr<concord::storage::IDBClient>, IDbAdapter*> get_db_configuration();

  bftEngine::ReplicaConfig& GetReplicaConfig() { return replica_config_; }
  bft::communication::ICommunication* GetCommunication() const { return communication_.get(); }
  concordMetrics::Server& GetMetricsServer() { return metrics_server_; }
  concordlogger::Logger GetLogger() { return logger_; }
  const bool UsePersistentStorage() const { return use_persistent_storage_; }

 private:
  TestSetup(bftEngine::ReplicaConfig config,
            std::unique_ptr<bft::communication::ICommunication> comm,
            concordlogger::Logger logger,
            uint16_t metrics_port,
            bool use_persistent_storage,
            std::string s3ConfigFile)
      : replica_config_(config),
        communication_(std::move(comm)),
        logger_(logger),
        metrics_server_(metrics_port),
        use_persistent_storage_(use_persistent_storage),
        s3ConfigFile_(s3ConfigFile) {}
  TestSetup() = delete;
#ifdef USE_S3_OBJECT_STORE
  concord::storage::s3::StoreConfig parseS3Config(const std::string& s3ConfigFile);
#endif
  std::tuple<std::shared_ptr<concord::storage::IDBClient>, IDbAdapter*> get_inmem_db_configuration();
  bftEngine::ReplicaConfig replica_config_;
  std::unique_ptr<bft::communication::ICommunication> communication_;
  concordlogger::Logger logger_;
  concordMetrics::Server metrics_server_;
  bool use_persistent_storage_;
  std::string s3ConfigFile_;
};

}  // namespace concord::kvbc
