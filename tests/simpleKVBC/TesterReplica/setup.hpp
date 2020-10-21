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
#include "storage_factory_interface.h"

#ifdef USE_S3_OBJECT_STORE
#include "s3/client.hpp"
#endif

namespace concord::kvbc {

class TestSetup {
 public:
  static std::unique_ptr<TestSetup> ParseArgs(int argc, char** argv);

  std::unique_ptr<IStorageFactory> GetStorageFactory();

  const bftEngine::ReplicaConfig& GetReplicaConfig() const { return replicaConfig_; }
  bft::communication::ICommunication* GetCommunication() const { return communication_.get(); }
  concordMetrics::Server& GetMetricsServer() { return metricsServer_; }
  logging::Logger GetLogger() { return logger_; }
  const bool UsePersistentStorage() const { return usePersistentStorage_; }
  std::string getLogPropertiesFile() { return logPropsFile_; }

 private:
  enum class StorageType {
    V1DirectKeyValue,
    V2MerkleTree,
  };

  TestSetup(const bftEngine::ReplicaConfig& config,
            std::unique_ptr<bft::communication::ICommunication> comm,
            logging::Logger logger,
            uint16_t metricsPort,
            bool usePersistentStorage,
            std::string s3ConfigFile,
            StorageType storageType,
            std::string logPropsFile)
      : replicaConfig_(config),
        communication_(std::move(comm)),
        logger_(logger),
        metricsServer_(metricsPort),
        usePersistentStorage_(usePersistentStorage),
        s3ConfigFile_(s3ConfigFile),
        storageType_(storageType),
        logPropsFile_(logPropsFile) {}
  TestSetup() = delete;
#ifdef USE_S3_OBJECT_STORE
  concord::storage::s3::StoreConfig ParseS3Config(const std::string& s3ConfigFile);
#endif
  std::unique_ptr<IStorageFactory> GetInMemStorageFactory() const;
  const bftEngine::ReplicaConfig& replicaConfig_;
  std::unique_ptr<bft::communication::ICommunication> communication_;
  logging::Logger logger_;
  concordMetrics::Server metricsServer_;
  bool usePersistentStorage_;
  std::string s3ConfigFile_;
  StorageType storageType_;
  std::string logPropsFile_;
};

}  // namespace concord::kvbc
