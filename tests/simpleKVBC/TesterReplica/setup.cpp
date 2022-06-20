// Concord
//
// Copyright (c) 2019-2022 VMware, Inc. All Rights Reserved.
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
#include <sstream>
#include <cstring>
#include <getopt.h>
#include <unistd.h>
#include <tuple>
#include <chrono>
#include <memory>

#include "Logger.hpp"
#include "setup.hpp"
#include "communication/CommFactory.hpp"
#include "config/test_comm_config.hpp"
#include "commonKVBTests.hpp"
#include "memorydb/client.h"
#include "string.hpp"
#include "config_file_parser.hpp"
#include "direct_kv_storage_factory.h"
#include "merkle_tree_storage_factory.h"
#include "secrets_manager_plain.h"

#include <boost/algorithm/string.hpp>
#include <experimental/filesystem>

#include "strategy/StrategyUtils.hpp"
#include "strategy/ByzantineStrategy.hpp"
#include "strategy/ShufflePrePrepareMsgStrategy.hpp"
#include "strategy/CorruptCheckpointMsgStrategy.hpp"
#include "strategy/DelayStateTransferMsgStrategy.hpp"
#include "strategy/MangledPreProcessResultMsgStrategy.hpp"
#include "WrapCommunication.hpp"
#include "secrets_manager_enc.h"
#include "blockchain_misc.hpp"

#ifdef USE_S3_OBJECT_STORE
#include "s3/config_parser.hpp"
#endif

namespace fs = std::experimental::filesystem;

namespace concord::kvbc {

using bft::communication::WrapCommunication;

std::unique_ptr<TestSetup> TestSetup::ParseArgs(int argc, char** argv) {
  std::stringstream args;
  for (int i{1}; i < argc; ++i) {
    args << argv[i] << " ";
  }
  LOG_INFO(GL, "Parsing" << KVLOG(argc) << " arguments, args:" << args.str());
  try {
    // used to get info from parsing the key file
    bftEngine::ReplicaConfig& replicaConfig = bftEngine::ReplicaConfig::instance();
    logging::Logger logger = logging::getLogger("skvbctest.replica");
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
    replicaConfig.batchedPreProcessEnabled = true;
    replicaConfig.timeServiceEnabled = true;
    replicaConfig.enablePostExecutionSeparation = true;
    replicaConfig.set("sourceReplicaReplacementTimeoutMilli", 6000);
    replicaConfig.set("concord.bft.st.runInSeparateThread", true);
    replicaConfig.set("concord.bft.keyExchage.clientKeysEnabled", false);
    replicaConfig.set("concord.bft.st.fetchRangeSize", 27);
    replicaConfig.set("concord.bft.st.gettingMissingBlocksSummaryWindowSize", 60);
    replicaConfig.set("concord.bft.st.RVT_K", 12);
    replicaConfig.preExecutionResultAuthEnabled = false;
    replicaConfig.numOfClientServices = 1;
    replicaConfig.kvBlockchainVersion = 4;
    const auto persistMode = PersistencyMode::RocksDB;
    std::string keysFilePrefix;
    std::string commConfigFile;
    std::string s3ConfigFile;
    std::string certRootPath = (replicaConfig.useUnifiedCertificates) ? "tls_certs" : "certs";
    std::string logPropsFile = "logging.properties";
    std::string principalsMapping;
    std::string txnSigningKeysPath;
    std::optional<std::uint32_t> cronEntryNumberOfExecutes;
    std::string byzantineStrategies;
    bool is_separate_communication_mode = false;
    int addAllKeysAsPublic = 0;
    int stateTransferMsgDelayMs = 0;
    std::unordered_set<ReplicaId> byzantineReplicaIds{};

    // !!! DO NOT change the order of the next options, as some might use an option index !!!
    static struct option longOptions[] = {
        // !!! ATTENTION !!! long format only options should be put here on top. They all should use val=2 and handled
        // in a single place in
        // the while loop
        {"delay-state-transfer-messages-millisec", required_argument, 0, 2},
        {"corrupt-checkpoint-messages-from-replica-ids", required_argument, 0, 2},
        {"diagnostics-port", required_argument, 0, 2},

        // long/short format options
        {"replica-id", required_argument, 0, 'i'},
        {"key-file-prefix", required_argument, 0, 'k'},
        {"network-config-file", required_argument, 0, 'n'},
        {"status-report-timeout", required_argument, 0, 's'},
        {"view-change-timeout", required_argument, 0, 'v'},
        {"auto-primary-rotation-timeout", required_argument, 0, 'a'},
        {"s3-config-file", required_argument, 0, '3'},
        {"log-props-file", required_argument, 0, 'l'},
        {"key-exchange-on-start", required_argument, 0, 'e'},
        {"publish-client-keys", required_argument, 0, 'w'},
        {"cert-root-path", required_argument, 0, 'c'},
        {"consensus-batching-policy", required_argument, 0, 'b'},
        {"consensus-batching-max-reqs-size", required_argument, 0, 'm'},
        {"consensus-batching-max-req-num", required_argument, 0, 'q'},
        {"consensus-batching-flush-period", required_argument, 0, 'z'},
        {"consensus-concurrency-level", required_argument, 0, 'y'},
        {"replica-block-accumulation", no_argument, 0, 'u'},
        {"send-different-messages-to-different-replica", no_argument, 0, 'd'},
        {"principals-mapping", optional_argument, 0, 'p'},
        {"txn-signing-key-path", optional_argument, 0, 't'},
        {"operator-public-key-path", optional_argument, 0, 'o'},
        {"cron-entry-number-of-executes", optional_argument, 0, 'r'},
        {"replica-byzantine-strategies", optional_argument, 0, 'g'},
        {"pre-exec-result-auth", no_argument, 0, 'x'},
        {"time_service", optional_argument, 0, 'f'},
        {"blockchain-version", optional_argument, 0, 'V'},
        {"enable-db-checkpoint", required_argument, 0, 'h'},

        // direct options - assign directly ro a non-null flag
        {"publish-master-key-on-startup", no_argument, (int*)&replicaConfig.publishReplicasMasterKeyOnStartup, 1},
        {"add-all-keys-as-public", no_argument, &addAllKeysAsPublic, 1},
        {0, 0, 0, 0}};
    int o = 0;
    int optionIndex = 0;
    LOG_INFO(GL, "Command line options:");
    while ((o = getopt_long(
                argc, argv, "i:k:n:s:v:a:3:l:e:w:c:b:m:q:z:y:udp:t:o:r:g:xf:h:j:V:", longOptions, &optionIndex)) !=
           -1) {
      switch (o) {
        // long-options-only first
        case 2:
          switch (optionIndex) {
            case 0: {
              std::string str{optarg};
              std::string::const_iterator it = str.begin();
              while (it != str.end() && std::isdigit(*it)) {
                ++it;
              }
              if (str.empty() || it != str.end()) {
                std::ostringstream ss;
                ss << "invalid argument for --delay-state-transfer-messages-millisec <" << str << ">";
                throw std::runtime_error{ss.str()};
              }
              stateTransferMsgDelayMs = stoi(str);
              byzantineStrategies = concord::kvbc::strategy::DelayStateTransferMsgStrategy(logger, 0).getStrategyName();
            } break;
            case 1: {
              std::string str{optarg};
              if (str.empty()) {
                throw std::runtime_error{"no argument provided for --corrupt-checkpoint-messages-from-replica-ids"};
              }

              std::stringstream ss{str};
              try {
                for (int i = 0; ss >> i;) {
                  LOG_INFO(GL, "Adding replica " + std::to_string(i) + " to the set of byzantine replicas.");
                  byzantineReplicaIds.insert(i);
                  if (ss.peek() == ',') ss.ignore();
                }
              } catch (std::exception&) {
                LOG_INFO(GL, "Failed to parse" << KVLOG(str) << ". Expecting comma separated replica IDs");
                throw std::runtime_error{
                    "invalid argument for --corrupt-checkpoint-messages-from-replica-ids. More information in log."};
              }

              if (byzantineReplicaIds.empty()) {
                throw std::runtime_error{"invalid argument for --corrupt-checkpoint-messages-from-replica-ids"};
              }

              byzantineStrategies = concord::kvbc::strategy::CorruptCheckpointMsgStrategy::strategyName();
            } break;
            case 2: {
              std::string arg{optarg};
              try {
                replicaConfig.diagnosticsServerPort = arg.empty() ? 0 : std::stoi(arg);
              } catch (std::exception&) {
                throw std::runtime_error{
                    "Invalid value for argument --diagnostics-port, argument should be"
                    "a valid available port number"};
              }
            } break;
            default: {
              std::ostringstream ss;
              ss << "invalid option:" << KVLOG(o, optionIndex);
              throw std::runtime_error{ss.str()};
            } break;
          };
          break;

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
        case 'V': {
          auto version = concord::util::to<std::uint16_t>(std::string(optarg));
          if (version != BLOCKCHAIN_VERSION::CATEGORIZED_BLOCKCHAIN && version != BLOCKCHAIN_VERSION::V4_BLOCKCHAIN) {
            std::ostringstream ss;
            ss << "invalid option for blockchain version " << version;
            throw std::runtime_error{ss.str()};
          }
          replicaConfig.kvBlockchainVersion = version;
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
        case 'w': {
          replicaConfig.set("concord.bft.keyExchage.clientKeysEnabled", true);
        } break;
        case 'y': {
          const auto concurrencyLevel = concord::util::to<std::uint16_t>(std::string(optarg));
          if (concurrencyLevel < 1 || concurrencyLevel > 30)
            throw std::runtime_error{"invalid argument for --consensus-concurrency-level"};
          replicaConfig.concurrencyLevel = concurrencyLevel;
        } break;
        case 'u': {
          replicaConfig.blockAccumulation = true;
          break;
        }
        case 'd': {
          is_separate_communication_mode = true;
          break;
        }
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
          if (policy < bftEngine::BATCH_SELF_ADJUSTED || policy > bftEngine::BATCH_ADAPTIVE)
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
          break;
        }
        case 'o': {
          replicaConfig.pathToOperatorPublicKey_ = optarg;
          break;
        }
        case 'r': {
          cronEntryNumberOfExecutes = concord::util::to<std::uint32_t>(optarg);
          break;
        }
        case 'g': {
          byzantineStrategies = optarg;
          break;
        }
        case 'x': {
          replicaConfig.preExecutionResultAuthEnabled = true;
          break;
        }
        case 'f': {
          bool time_service_option = concord::util::to<bool>(std::string(optarg));
          replicaConfig.timeServiceEnabled = time_service_option;
          break;
        }
        case 'h': {
          // enable rocksdb checkpoint with some defaults
          replicaConfig.dbCheckpointFeatureEnabled = false;
          replicaConfig.dbCheckPointWindowSize = 150;
          replicaConfig.dbSnapshotIntervalSeconds = std::chrono::seconds{0};
          replicaConfig.dbCheckpointMonitorIntervalSeconds = std::chrono::seconds{1};
          replicaConfig.maxNumberOfDbCheckpoints = concord::util::to<std::uint32_t>(std::string(optarg));
          if (replicaConfig.maxNumberOfDbCheckpoints) replicaConfig.dbCheckpointFeatureEnabled = true;
          std::stringstream dbSnapshotPath;
          dbSnapshotPath << BasicRandomTests::DB_FILE_PREFIX << "snapshot_" << replicaConfig.replicaId;
          replicaConfig.dbCheckpointDirPath = dbSnapshotPath.str();
          break;
        }
        case 'j': {
          // updating dbCheckPointWindowSize value to test create dbSnapshot operator command
          replicaConfig.dbCheckPointWindowSize = concord::util::to<std::uint32_t>(std::string(optarg));
          break;
        }

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
    } else if (principalsMapping.empty() != txnSigningKeysPath.empty()) {
      throw std::runtime_error("Params principals-mapping and txn-signing-key-path must be set simultaneously.");
    }

    TestCommConfig testCommConfig(logger);
    testCommConfig.GetReplicaConfig(replicaConfig.replicaId, keysFilePrefix, &replicaConfig);
    uint16_t numOfReplicas =
        (uint16_t)(3 * replicaConfig.fVal + 2 * replicaConfig.cVal + 1 + replicaConfig.numRoReplicas);
    auto numOfClients =
        replicaConfig.numOfClientProxies ? replicaConfig.numOfClientProxies : replicaConfig.numOfExternalClients;
    std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> sm_ =
        std::make_shared<concord::secretsmanager::SecretsManagerPlain>();
#ifdef USE_COMM_PLAIN_TCP
    bft::communication::PlainTcpConfig conf =
        testCommConfig.GetTCPConfig(true, replicaConfig.replicaId, numOfClients, numOfReplicas, commConfigFile);
#elif USE_COMM_TLS_TCP
    bft::communication::TlsTcpConfig conf = testCommConfig.GetTlsTCPConfig(
        true, replicaConfig.replicaId, numOfClients, numOfReplicas, commConfigFile, certRootPath);
    if (conf.secretData_.has_value()) {
      sm_ = std::make_shared<concord::secretsmanager::SecretsManagerEnc>(conf.secretData_.value());
    } else {
      sm_ = std::make_shared<concord::secretsmanager::SecretsManagerPlain>();
    }
#else
    bft::communication::PlainUdpConfig conf =
        testCommConfig.GetUDPConfig(true, replicaConfig.replicaId, numOfClients, numOfReplicas, commConfigFile);
#endif
    replicaConfig.certificatesRootPath = certRootPath;
    std::unique_ptr<bft::communication::ICommunication> comm(bft::communication::CommFactory::create(conf));

    if (!byzantineStrategies.empty()) {
      // Initialise all the strategies here at once.
      const std::vector<std::shared_ptr<concord::kvbc::strategy::IByzantineStrategy>> allStrategies = {
          std::make_shared<concord::kvbc::strategy::ShufflePrePrepareMsgStrategy>(logger),
          std::make_shared<concord::kvbc::strategy::MangledPreProcessResultMsgStrategy>(logger),
          std::make_shared<concord::kvbc::strategy::DelayStateTransferMsgStrategy>(logger, stateTransferMsgDelayMs),
          std::make_shared<concord::kvbc::strategy::CorruptCheckpointMsgStrategy>(logger,
                                                                                  std::move(byzantineReplicaIds))};
      WrapCommunication::addStrategies(byzantineStrategies, ',', allStrategies);

      std::unique_ptr<bft::communication::ICommunication> wrappedComm =
          std::make_unique<WrapCommunication>(std::move(comm), is_separate_communication_mode, logger);
      comm.swap(wrappedComm);
      LOG_INFO(logger,
               "Starting the replica with strategies : " << byzantineStrategies << " and randomized send : "
                                                         << is_separate_communication_mode);
    }

    uint16_t metricsPort = conf.listenPort_ + 1000;

    LOG_INFO(logger, "\nReplica Configuration: \n" << replicaConfig);
    std::unique_ptr<TestSetup> setup = nullptr;
    setup.reset(new TestSetup(replicaConfig,
                              std::move(comm),
                              logger,
                              metricsPort,
                              persistMode == PersistencyMode::RocksDB,
                              s3ConfigFile,
                              logPropsFile,
                              cronEntryNumberOfExecutes,
                              addAllKeysAsPublic != 0));
    setup->sm_ = sm_;
    return setup;

  } catch (const std::exception& e) {
    LOG_FATAL(GL, "failed to parse command line arguments: " << e.what());
    throw;
  }
}

std::unique_ptr<IStorageFactory> TestSetup::GetStorageFactory() {
  // TODO handle persistence mode
  std::stringstream dbPath;
  dbPath << BasicRandomTests::DB_FILE_PREFIX << GetReplicaConfig().replicaId;

#ifdef USE_S3_OBJECT_STORE
  if (GetReplicaConfig().isReadOnly) {
    if (s3ConfigFile_.empty()) throw std::runtime_error("--s3-config-file must be provided");
    const auto s3Config = concord::storage::s3::ConfigFileParser(s3ConfigFile_).parse();
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
      for (const std::string& clientId : clientIds) {
        clientIdsSet.insert(std::stoi(clientId));
      }
      const std::string key = keyPlaintext.value();
      publicKeysOfClients.insert(std::pair<const std::string, std::set<uint16_t>>(key, clientIdsSet));
      bftEngine::ReplicaConfig::instance().clientGroups.emplace(i + 1, clientIdsSet);
    } else {
      throw std::runtime_error("Key public_key.pem not found in directory " + keysDirectories.at(i));
    }
  }
  bftEngine::ReplicaConfig::instance().clientsKeysPrefix = keysRootPath;
}

}  // namespace concord::kvbc
