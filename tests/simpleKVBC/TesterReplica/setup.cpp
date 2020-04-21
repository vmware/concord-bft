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

#include <thread>
#include <sys/param.h>
#include <string>
#include <cstring>
#include <getopt.h>
#include <unistd.h>
#include <tuple>
#include <chrono>

#include "Logger.hpp"
#include "setup.hpp"
#include "communication/CommFactory.hpp"
#include "config/test_comm_config.hpp"
#include "commonKVBTests.hpp"
#include "memorydb/client.h"
#include "string.hpp"
#include "config/config_file_parser.hpp"
#include "direct_kv_storage_factory.h"
#include "merkle_tree_storage_factory.h"
namespace concord::kvbc {

std::unique_ptr<TestSetup> TestSetup::ParseArgs(int argc, char** argv) {
  try {
    // used to get info from parsing the key file
    bftEngine::ReplicaConfig replicaConfig;
    replicaConfig.numOfClientProxies = 100;
    replicaConfig.concurrencyLevel = 1;
    replicaConfig.debugStatisticsEnabled = true;
    replicaConfig.viewChangeTimerMillisec = 45 * 1000;
    replicaConfig.statusReportTimerMillisec = 10 * 1000;
    replicaConfig.preExecutionFeatureEnabled = true;

    PersistencyMode persist_mode = PersistencyMode::Off;
    std::string keysFilePrefix;
    std::string commConfigFile;
    std::string s3ConfigFile;

    static struct option long_options[] = {{"replica-id", required_argument, 0, 'i'},
                                           {"key-file-prefix", required_argument, 0, 'k'},
                                           {"network-config-file", required_argument, 0, 'n'},
                                           {"status-report-timeout", required_argument, 0, 's'},
                                           {"view-change-timeout", required_argument, 0, 'v'},
                                           {"auto-primary-rotation-timeout", required_argument, 0, 'a'},
                                           {"s3-config-file", required_argument, 0, '3'},
                                           {"persistence-mode", no_argument, 0, 'p'},
                                           {0, 0, 0, 0}};

    int o = 0;
    int option_index = 0;
    LOG_INFO(GL, "Command line options:");
    while ((o = getopt_long(argc, argv, "i:k:n:s:v:a:3:p", long_options, &option_index)) != -1) {
      switch (o) {
        case 'i': {
          replicaConfig.replicaId = concord::util::to<std::uint16_t>(std::string(optarg));
        } break;
        case 'k': {
          if (optarg[0] == '-') throw std::runtime_error("invalid argument for --key-file-prefix");
          keysFilePrefix = optarg;
        } break;
        case 'n': {
          if (optarg[0] == '-') throw std::runtime_error("invalid argument for --network-config-file");
          commConfigFile = optarg;
        } break;
        case 's': {
          replicaConfig.statusReportTimerMillisec = concord::util::to<std::uint16_t>(std::string(optarg));
        } break;
        case 'v': {
          replicaConfig.viewChangeTimerMillisec = concord::util::to<std::uint16_t>(std::string(optarg));
          replicaConfig.viewChangeProtocolEnabled = true;
        } break;
        case 'a': {
          replicaConfig.autoPrimaryRotationTimerMillisec = concord::util::to<std::uint16_t>(std::string(optarg));
          replicaConfig.autoPrimaryRotationEnabled = true;
        } break;
        case '3': {
          if (optarg[0] == '-') throw std::runtime_error("invalid argument for --s3-config-file");
          s3ConfigFile = optarg;
        } break;
        // We can only toggle persistence on or off. It defaults to InMemory
        // unless -p flag is provided.
        case 'p': {
          persist_mode = PersistencyMode::RocksDB;
        } break;
        case '?': {
          throw std::runtime_error("invalid arguments");
        } break;

        default:
          break;
      }
    }

    if (keysFilePrefix.empty()) throw std::runtime_error("missing --key-file-prefix");

    concordlogger::Logger logger = concordlogger::Log::getLogger("skvbctest.replica");

    TestCommConfig testCommConfig(logger);
    testCommConfig.GetReplicaConfig(replicaConfig.replicaId, keysFilePrefix, &replicaConfig);
    uint16_t numOfReplicas = (uint16_t)(3 * replicaConfig.fVal + 2 * replicaConfig.cVal + 1);

#ifdef USE_COMM_PLAIN_TCP
    bft::communication::PlainTcpConfig conf = testCommConfig.GetTCPConfig(
        true, replicaConfig.replicaId, replicaConfig.numOfClientProxies, numOfReplicas, commConfigFile);
#elif USE_COMM_TLS_TCP
    bft::communication::TlsTcpConfig conf = testCommConfig.GetTlsTCPConfig(
        true, replicaConfig.replicaId, replicaConfig.numOfClientProxies, numOfReplicas, commConfigFile);
#else
    bft::communication::PlainUdpConfig conf = testCommConfig.GetUDPConfig(
        true, replicaConfig.replicaId, replicaConfig.numOfClientProxies, numOfReplicas, commConfigFile);
#endif

    std::unique_ptr<bft::communication::ICommunication> comm(bft::communication::CommFactory::create(conf));

    uint16_t metrics_port = conf.listenPort + 1000;

    LOG_INFO(logger, "\nReplica Configuration: \n" << replicaConfig);

    return std::unique_ptr<TestSetup>(new TestSetup{
        replicaConfig, std::move(comm), logger, metrics_port, persist_mode == PersistencyMode::RocksDB, s3ConfigFile});

  } catch (const std::exception& e) {
    LOG_FATAL(GL, "failed to parse command line arguments: " << e.what());
    throw;
  }
}

std::unique_ptr<IStorageFactory> TestSetup::GetInMemStorageFactory() const {
  return std::make_unique<v1DirectKeyValue::MemoryDBStorageFactory>();
}

#ifdef USE_S3_OBJECT_STORE
concord::storage::s3::StoreConfig TestSetup::ParseS3Config(const std::string& s3ConfigFile) {
  ConfigFileParser parser(logger_, s3ConfigFile);
  if (!parser.Parse()) throw std::runtime_error("failed to parse" + s3ConfigFile);

  concord::storage::s3::StoreConfig config;
  config.bucketName = parser.GetValues("s3-bucket-name")[0];
  config.accessKey = parser.GetValues("s3-access-key")[0];
  config.protocol = parser.GetValues("s3-protocol")[0];
  config.url = parser.GetValues("s3-url")[0];
  config.secretKey = parser.GetValues("s3-secret-key")[0];

  LOG_INFO(logger_,
           "\nS3 Configuration:"
               << "\nbucket:\t\t" << config.bucketName << "\naccess key:\t" << config.accessKey << "\nprotocol:\t"
               << config.protocol << "\nurl:\t\t" << config.url);
  return config;
}
#endif

std::unique_ptr<IStorageFactory> TestSetup::GetStorageFactory() {
#ifndef USE_ROCKSDB
  return GetInMemStorageFactory();
#else

  if (!UsePersistentStorage()) return GetInMemStorageFactory();

  std::stringstream dbPath;
  dbPath << BasicRandomTests::DB_FILE_PREFIX << GetReplicaConfig().replicaId;

#ifdef USE_S3_OBJECT_STORE
  if (GetReplicaConfig().isReadOnly) {
    if (s3ConfigFile_.empty()) throw std::runtime_error("--s3-config-file must be provided");
    const auto s3Config = ParseS3Config(s3ConfigFile_);
    return std::make_unique<v1DirectKeyValue::S3StorageFactory>(dbPath.str(), s3Config);
  }
#endif
  return std::make_unique<v1DirectKeyValue::RocksDBStorageFactory>(dbPath.str());
#endif
}

}  // namespace concord::kvbc
