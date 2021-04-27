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
#include "secrets_manager_plain.h"

#include <boost/algorithm/string.hpp>
#include <experimental/filesystem>

namespace fs = std::experimental::filesystem;

namespace concord::kvbc {

std::unique_ptr<TestSetup> TestSetup::ParseArgs(int argc, char** argv) {
  std::stringstream args;
  for (int i{1}; i < argc; ++i) {
    args << argv[i] << " ";
  }
  LOG_INFO(GL, "Parsing" << KVLOG(argc) << " arguments, args:" << args.str());
  try {
    // used to get info from parsing the key file
    bftEngine::ReplicaConfig& replicaConfig = bftEngine::ReplicaConfig::instance();
    replicaConfig.numOfClientProxies = 0;
    replicaConfig.numOfExternalClients = 30;
    replicaConfig.concurrencyLevel = 3;
    replicaConfig.debugStatisticsEnabled = true;
    replicaConfig.viewChangeTimerMillisec = 45 * 1000;
    replicaConfig.statusReportTimerMillisec = 10 * 1000;
    replicaConfig.preExecutionFeatureEnabled = true;
    replicaConfig.clientBatchingEnabled = true;
    replicaConfig.pruningEnabled_ = true;
    replicaConfig.numBlocksToKeep_ = 10;
    replicaConfig.set("sourceReplicaReplacementTimeoutMilli", 6000);
    const auto persistMode = PersistencyMode::RocksDB;
    std::string keysFilePrefix;
    std::string commConfigFile;
    std::string s3ConfigFile;
    std::string certRootPath = "certs";
    std::string logPropsFile = "logging.properties";
    std::string principalsMapping;
    std::string txnSigningKeysPath;

    static struct option longOptions[] = {{"replica-id", required_argument, 0, 'i'},
                                          {"key-file-prefix", required_argument, 0, 'k'},
                                          {"network-config-file", required_argument, 0, 'n'},
                                          {"status-report-timeout", required_argument, 0, 's'},
                                          {"view-change-timeout", required_argument, 0, 'v'},
                                          {"auto-primary-rotation-timeout", required_argument, 0, 'a'},
                                          {"s3-config-file", required_argument, 0, '3'},
                                          {"log-props-file", required_argument, 0, 'l'},
                                          {"key-exchange-on-start", required_argument, 0, 'e'},
                                          {"cert-root-path", required_argument, 0, 'c'},
                                          {"consensus-batching-policy", required_argument, 0, 'b'},
                                          {"consensus-batching-max-reqs-size", required_argument, 0, 'm'},
                                          {"consensus-batching-max-req-num", required_argument, 0, 'q'},
                                          {"consensus-batching-flush-period", required_argument, 0, 'z'},
                                          {"consensus-concurrency-level", required_argument, 0, 'y'},
                                          {"principals-mapping", optional_argument, 0, 'p'},
                                          {"txn-signing-key-path", optional_argument, 0, 't'},
                                          {0, 0, 0, 0}};
    int o = 0;
    int optionIndex = 0;
    LOG_INFO(GL, "Command line options:");
    while ((o = getopt_long(argc, argv, "i:k:n:s:v:a:3:t:l:c:e:b:m:q:y:z:p:t", longOptions, &optionIndex)) != -1) {
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
        case 'e': {
          replicaConfig.keyExchangeOnStart = true;
        } break;
        case 'y': {
          const auto concurrencyLevel = concord::util::to<std::uint16_t>(std::string(optarg));
          if (concurrencyLevel < 1 || concurrencyLevel > 30)
            throw std::runtime_error{"invalid argument for --consensus-concurrency-level"};
          replicaConfig.concurrencyLevel = concurrencyLevel;
        } break;
        case 'l': {
          logPropsFile = optarg;
          break;
        }
        case 'c': {
          certRootPath = optarg;
          break;
        }
        case 'b': {
          auto policy = concord::util::to<std::uint32_t>(std::string(optarg));
          if (policy < bftEngine::BATCH_SELF_ADJUSTED || policy > bftEngine::BATCH_BY_REQ_NUM)
            throw std::runtime_error{"invalid argument for --consensus-batching-policy"};
          replicaConfig.batchingPolicy = policy;
          break;
        }
        case 'm': {
          replicaConfig.maxBatchSizeInBytes = concord::util::to<std::uint32_t>(std::string(optarg));
          break;
        }
        case 'q': {
          replicaConfig.maxNumOfRequestsInBatch = concord::util::to<std::uint32_t>(std::string(optarg));
          break;
        }
        case 'z': {
          const auto batchFlushPeriod = concord::util::to<std::uint32_t>(std::string(optarg));
          if (!batchFlushPeriod) throw std::runtime_error{"invalid argument for --consensus-batching-flush-period"};
          replicaConfig.batchFlushPeriod = batchFlushPeriod;
          break;
        }
        case 'p': {
          principalsMapping = optarg;
          break;
        }
        case 't': {
          txnSigningKeysPath = optarg;
        } break;
        case '?': {
          throw std::runtime_error("invalid arguments");
        } break;

        default:
          break;
      }
    }

    if (keysFilePrefix.empty()) throw std::runtime_error("missing --key-file-prefix");

    // If -p and -t are set, enable clientTransactionSigningEnabled. If only one of them is set, throw an error
    if (!principalsMapping.empty() && !txnSigningKeysPath.empty()) {
      replicaConfig.clientTransactionSigningEnabled = true;
      TestSetup::setPublicKeysOfClients(principalsMapping, txnSigningKeysPath, replicaConfig.publicKeysOfClients);
    } else if ((!principalsMapping.empty() && txnSigningKeysPath.empty()) ||
               (principalsMapping.empty() && !txnSigningKeysPath.empty())) {
      throw std::runtime_error("Params principals-mapping and txn-signing-key-path must be set simultaneously.");
    }

    logging::Logger logger = logging::getLogger("skvbctest.replica");

    TestCommConfig testCommConfig(logger);
    testCommConfig.GetReplicaConfig(replicaConfig.replicaId, keysFilePrefix, &replicaConfig);
    uint16_t numOfReplicas = (uint16_t)(3 * replicaConfig.fVal + 2 * replicaConfig.cVal + 1);
    auto numOfClients =
        replicaConfig.numOfClientProxies ? replicaConfig.numOfClientProxies : replicaConfig.numOfExternalClients;

#ifdef USE_COMM_PLAIN_TCP
    bft::communication::PlainTcpConfig conf =
        testCommConfig.GetTCPConfig(true, replicaConfig.replicaId, numOfClients, numOfReplicas, commConfigFile);
#elif USE_COMM_TLS_TCP
    bft::communication::TlsTcpConfig conf = testCommConfig.GetTlsTCPConfig(
        true, replicaConfig.replicaId, numOfClients, numOfReplicas, commConfigFile, certRootPath);
#else
    bft::communication::PlainUdpConfig conf =
        testCommConfig.GetUDPConfig(true, replicaConfig.replicaId, numOfClients, numOfReplicas, commConfigFile);
#endif

    std::unique_ptr<bft::communication::ICommunication> comm(bft::communication::CommFactory::create(conf));

    uint16_t metricsPort = conf.listenPort + 1000;

    LOG_INFO(logger, "\nReplica Configuration: \n" << replicaConfig);

    return std::unique_ptr<TestSetup>(new TestSetup{replicaConfig,
                                                    std::move(comm),
                                                    logger,
                                                    metricsPort,
                                                    persistMode == PersistencyMode::RocksDB,
                                                    s3ConfigFile,
                                                    logPropsFile});

  } catch (const std::exception& e) {
    LOG_FATAL(GL, "failed to parse command line arguments: " << e.what());
    throw;
  }
}

#ifdef USE_S3_OBJECT_STORE
concord::storage::s3::StoreConfig TestSetup::ParseS3Config(const std::string& s3ConfigFile) {
  ConfigFileParser parser(logger_, s3ConfigFile);
  if (!parser.Parse()) throw std::runtime_error("failed to parse" + s3ConfigFile);

  auto get_config_value = [&s3ConfigFile, &parser](const std::string& key) {
    std::vector<std::string> v = parser.GetValues(key);
    if (v.size()) {
      return v[0];
    } else {
      throw std::runtime_error("failed to parse " + s3ConfigFile + ": " + key + " is not set.");
    }
  };

  concord::storage::s3::StoreConfig config;
  config.bucketName = get_config_value("s3-bucket-name");
  config.accessKey = get_config_value("s3-access-key");
  config.protocol = get_config_value("s3-protocol");
  config.url = get_config_value("s3-url");
  config.secretKey = get_config_value("s3-secret-key");
  try {
    // TesterReplica is used for Apollo tests. Each test is executed against new blockchain, so we need brand new
    // bucket for the RO replica. To achieve this we use a hack - set the prefix to a uniqe value so each RO replica
    // writes in the same bucket but in different directory.
    // So if s3-path-prefix is NOT SET it is initialised to a unique value based on current time.
    config.pathPrefix = get_config_value("s3-path-prefix");
  } catch (std::runtime_error& e) {
    config.pathPrefix = std::to_string(std::chrono::high_resolution_clock::now().time_since_epoch().count());
  }

  LOG_INFO(logger_,
           "\nS3 Configuration:"
               << "\nbucket:\t\t" << config.bucketName << "\nprotocol:\t" << config.protocol << "\nurl:\t\t"
               << config.url);
  return config;
}
#endif

std::unique_ptr<IStorageFactory> TestSetup::GetStorageFactory() {
  // TODO handle persistancy mode
  std::stringstream dbPath;
  dbPath << BasicRandomTests::DB_FILE_PREFIX << GetReplicaConfig().replicaId;

#ifdef USE_S3_OBJECT_STORE
  if (GetReplicaConfig().isReadOnly) {
    if (s3ConfigFile_.empty()) throw std::runtime_error("--s3-config-file must be provided");
    const auto s3Config = ParseS3Config(s3ConfigFile_);
    return std::make_unique<v1DirectKeyValue::S3StorageFactory>(dbPath.str(), s3Config);
  }
#endif
  return std::make_unique<v2MerkleTree::RocksDBStorageFactory>(dbPath.str());
}

std::vector<std::string> TestSetup::getKeyDirectories(const std::string& path) {
  std::vector<std::string> result;
  if (!fs::exists(path) || !fs::is_directory(path)) {
    throw std::invalid_argument{"Transaction signing keys path doesn't exist at " + path};
  }
  for (auto& dir : fs::directory_iterator(path)) {
    if (fs::exists(dir) && fs::is_directory(dir)) {
      result.push_back(dir.path().string());
    }
  }
  return result;
}

void TestSetup::setPublicKeysOfClients(
    const std::string& principalsMapping,
    const std::string& keysRootPath,
    std::set<std::pair<const std::string, std::set<uint16_t>>>& publicKeysOfClients) {
  // The string principalsMapping looks like:
  // "11 12;13 14;15 16;17 18;19 20", for 10 client ids divided into 5 participants.

  LOG_INFO(GL, "" << KVLOG(principalsMapping, keysRootPath));
  std::vector<std::string> keysDirectories = TestSetup::getKeyDirectories(keysRootPath);
  std::vector<std::string> clientIdChunks;
  boost::split(clientIdChunks, principalsMapping, boost::is_any_of(";"));

  if (clientIdChunks.size() != keysDirectories.size()) {
    std::stringstream msg;
    msg << "Number of keys directory should match the number of sets of clientIds mappings in principals "
           "mapping string "
        << KVLOG(keysDirectories.size(), clientIdChunks.size(), principalsMapping);
    throw std::runtime_error(msg.str());
  }

  // Sort keysDirectories just to ensure ordering of the folders
  std::sort(keysDirectories.begin(), keysDirectories.end());

  // Build publicKeysOfClients of replicaConfig
  for (std::size_t i = 0; i < clientIdChunks.size(); ++i) {
    std::string keyFilePath =
        keysDirectories.at(i) + "/transaction_signing_pub.pem";  // Will be "../1/transaction_signing_pub.pem" etc.

    secretsmanager::SecretsManagerPlain smp;
    std::optional<std::string> keyPlaintext;
    keyPlaintext = smp.decryptFile(keyFilePath);
    if (keyPlaintext.has_value()) {
      // Each clientIdChunk would look like "11 12 13 14"
      std::vector<std::string> clientIds;
      boost::split(clientIds, clientIdChunks.at(i), boost::is_any_of(" "));
      std::set<uint16_t> clientIdsSet;
      for (const std::string clientId : clientIds) {
        clientIdsSet.insert(std::stoi(clientId));
      }
      const std::string key = keyPlaintext.value();
      publicKeysOfClients.insert(std::pair<const std::string, std::set<uint16_t>>(key, clientIdsSet));
    } else {
      throw std::runtime_error("Key public_key.pem not found in directory " + keysDirectories.at(i));
    }
  }
}

}  // namespace concord::kvbc
