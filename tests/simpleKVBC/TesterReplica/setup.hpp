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

#include <chrono>
#include <memory>
#include <optional>
#include <utility>
#include "ReplicaConfig.hpp"
#include "communication/ICommunication.hpp"
#include "Logger.hpp"
#include "MetricsServer.hpp"
#include "config/test_parameters.hpp"
#include "storage_factory_interface.h"
#include "PerformanceManager.hpp"
#include "secrets_manager_impl.h"

#ifdef USE_S3_OBJECT_STORE
#include "s3/client.hpp"
#endif

namespace concord::kvbc {

class TestSetup {
 public:
  static std::unique_ptr<TestSetup> ParseArgs(int argc, char** argv);

  std::unique_ptr<IStorageFactory> GetStorageFactory();
  std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> GetSecretManager() const { return sm_; }
  const bftEngine::ReplicaConfig& GetReplicaConfig() const { return replicaConfig_; }
  bft::communication::ICommunication* GetCommunication() const { return communication_.get(); }
  concordMetrics::Server& GetMetricsServer() { return metricsServer_; }
  logging::Logger GetLogger() { return logger_; }
  const bool UsePersistentStorage() const { return usePersistentStorage_; }
  std::string getLogPropertiesFile() { return logPropsFile_; }
  std::shared_ptr<concord::performance::PerformanceManager> GetPerformanceManager() { return pm_; }
  std::optional<std::uint32_t> GetCronEntryNumberOfExecutes() const { return cronEntryNumberOfExecutes_; }

  static inline constexpr auto kCronTableComponentId = 42;
  static inline constexpr auto kTickGeneratorPeriod = std::chrono::seconds{1};

 private:
  TestSetup(const bftEngine::ReplicaConfig& config,
            std::unique_ptr<bft::communication::ICommunication> comm,
            logging::Logger logger,
            uint16_t metricsPort,
            bool usePersistentStorage,
            const std::string& s3ConfigFile,
            const std::string& logPropsFile,
            const std::optional<std::uint32_t>& cronEntryNumberOfExecutes)
      : replicaConfig_(config),
        communication_(std::move(comm)),
        logger_(logger),
        metricsServer_(metricsPort),
        usePersistentStorage_(usePersistentStorage),
        s3ConfigFile_(s3ConfigFile),
        logPropsFile_(logPropsFile),
        pm_{std::make_shared<concord::performance::PerformanceManager>()},
        cronEntryNumberOfExecutes_{cronEntryNumberOfExecutes} {}

  TestSetup() = delete;

  static std::vector<std::string> getKeyDirectories(const std::string& keysRootPath);

  static void setPublicKeysOfClients(const std::string& principalsMapping,
                                     const std::string& keysRootPath,
                                     std::set<std::pair<const std::string, std::set<uint16_t>>>& publicKeysOfClients);

#ifdef USE_S3_OBJECT_STORE
  concord::storage::s3::StoreConfig ParseS3Config(const std::string& s3ConfigFile);
#endif
  const bftEngine::ReplicaConfig& replicaConfig_;
  std::unique_ptr<bft::communication::ICommunication> communication_;
  logging::Logger logger_;
  concordMetrics::Server metricsServer_;
  bool usePersistentStorage_;
  std::string s3ConfigFile_;
  std::string logPropsFile_;
  std::shared_ptr<concord::performance::PerformanceManager> pm_ = nullptr;
  std::optional<std::uint32_t> cronEntryNumberOfExecutes_;
  std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> sm_;
};

}  // namespace concord::kvbc
