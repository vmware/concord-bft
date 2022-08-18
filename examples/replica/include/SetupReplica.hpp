// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
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
#include <yaml-cpp/yaml.h>
#include "ReplicaConfig.hpp"
#include "communication/ICommunication.hpp"
#include "communication/CommFactory.hpp"
#include "Logger.hpp"
#include "MetricsServer.hpp"
#include "storage_factory_interface.h"
#include "PerformanceManager.hpp"
#include "secrets_manager_impl.h"

namespace concord::osexample {

enum class PersistencyMode {
  Off,       // no persistency at all
  InMemory,  // use in memory module
  File,      // use file as a storage
  RocksDB,   // use RocksDB for storage
  MAX_VALUE = RocksDB
};

// SetupReplica is a class that is responsible for setting up the replica
class SetupReplica {
 public:
  // This function is used to parse the command line arguments and replica config file to setup the replica.
  static std::unique_ptr<SetupReplica> ParseArgs(int argc, char** argv);

  // This function is the helper function used to parse the replica config file.
  static void ParseReplicaConfFile(bftEngine::ReplicaConfig& replicaConfig, const YAML::Node& rconfig_yaml);

  // Template function which read the Node value from yaml replica config file.
  template <typename T>
  static void readYamlField(const YAML::Node& yaml, const std::string& index, T& out, bool value_required = false);

  // This function returns the storage factory based on persistant mode
  std::unique_ptr<concord::kvbc::IStorageFactory> GetStorageFactory();
  std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> GetSecretManager() const { return sm_; }

  // This function returns the replica config object.
  const bftEngine::ReplicaConfig& GetReplicaConfig() const { return replicaConfig_; }

  // This function returns the communication object once created.
  bft::communication::ICommunication* GetCommunication() const { return communication_.get(); }
  concordMetrics::Server& GetMetricsServer() { return metricsServer_; }
  const bool UsePersistentStorage() const { return usePersistentStorage_; }

  // This function is used to get the Log properties file location which is passed from replica config.
  std::string getLogPropertiesFile() { return logPropsFile_; }

  std::shared_ptr<concord::performance::PerformanceManager> GetPerformanceManager() { return pm_; }
  std::optional<std::uint32_t> GetCronEntryNumberOfExecutes() const { return cronEntryNumberOfExecutes_; }
  bool AddAllKeysAsPublic() const { return addAllKeysAsPublic_; }

  static inline constexpr auto kCronTableComponentId = 42;
  static inline constexpr auto kTickGeneratorPeriod = std::chrono::seconds{1};

 private:
  SetupReplica(const bftEngine::ReplicaConfig& config,
               std::unique_ptr<bft::communication::ICommunication> comm,
               uint16_t metricsPort,
               bool usePersistentStorage,
               const std::string& logPropsFile,
               const std::optional<std::uint32_t>& cronEntryNumberOfExecutes,
               bool addAllKeysAsPublic)
      : replicaConfig_(config),
        communication_(std::move(comm)),
        metricsServer_(metricsPort),
        usePersistentStorage_(usePersistentStorage),
        logPropsFile_(logPropsFile),
        pm_{std::make_shared<concord::performance::PerformanceManager>()},
        cronEntryNumberOfExecutes_{cronEntryNumberOfExecutes},
        addAllKeysAsPublic_{addAllKeysAsPublic} {}

  SetupReplica() = delete;

  static logging::Logger getLogger() {
    static logging::Logger logger_(logging::getLogger("osexample::SetupReplica"));
    return logger_;
  }

  // This function is used to get the Transaction signing keys path.
  static std::vector<std::string> getKeyDirectories(const std::string& keysRootPath);

  static bft::communication::ICommunication* createCommunication(
      bftEngine::ReplicaConfig& replicaConfig,
      std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl>& sm,
      std::string& keysFilePrefix,
      std::string& commConfigFile,
      uint16_t& metricsPort);

  static void setPublicKeysOfClients(const std::string& principalsMapping,
                                     const std::string& keysRootPath,
                                     std::set<std::pair<const std::string, std::set<uint16_t>>>& publicKeysOfClients);

  const bftEngine::ReplicaConfig& replicaConfig_;
  std::unique_ptr<bft::communication::ICommunication> communication_;
  concordMetrics::Server metricsServer_;
  bool usePersistentStorage_;
  std::string logPropsFile_;
  std::shared_ptr<concord::performance::PerformanceManager> pm_ = nullptr;
  std::optional<std::uint32_t> cronEntryNumberOfExecutes_;
  std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> sm_;
  bool addAllKeysAsPublic_{false};
};

}  // namespace concord::osexample
