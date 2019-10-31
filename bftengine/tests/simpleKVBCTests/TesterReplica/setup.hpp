// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
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

#include "ReplicaConfig.hpp"
#include "ICommunication.hpp"
#include "Logger.hpp"
#include "MetricsServer.hpp"

namespace SimpleKVBC {

class TestSetup {
 public:
  static std::unique_ptr<TestSetup> ParseArgs(int argc, char** argv);

  bftEngine::ReplicaConfig& GetReplicaConfig() { return replica_config_; }
  bftEngine::ICommunication* GetCommunication() const { return communication_.get(); }
  concordMetrics::Server& GetMetricsServer() { return metrics_server_; }
  concordlogger::Logger GetLogger() { return logger_; }
  const bool UsePersistentStorage() const { return use_persistent_storage_; }

 private:
  TestSetup(bftEngine::ReplicaConfig config,
            std::unique_ptr<bftEngine::ICommunication> comm,
            concordlogger::Logger logger,
            uint16_t metrics_port,
            bool use_persistent_storage)
      : replica_config_(config),
        communication_(std::move(comm)),
        logger_(logger),
        metrics_server_(metrics_port),
        use_persistent_storage_(use_persistent_storage) {}
  TestSetup() = delete;

  bftEngine::ReplicaConfig replica_config_;
  std::unique_ptr<bftEngine::ICommunication> communication_;
  concordlogger::Logger logger_;
  concordMetrics::Server metrics_server_;
  bool use_persistent_storage_;
};

}  // namespace SimpleKVBC
