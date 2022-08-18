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

#include <getopt.h>
#include "util/filesystem.hpp"
#include <regex>

#include "SetupReplica.hpp"
#include "common.hpp"
#include "memorydb/client.h"
#include "secrets_manager_plain.h"
#include "secrets_manager_enc.h"
#include "direct_kv_storage_factory.h"
#include "merkle_tree_storage_factory.h"
#include "tests/config/test_comm_config.hpp"

namespace concord::osexample {

// Copy a value from the YAML node to `out`.
// Throws and exception if no value could be read but the value is required.
template <typename T>
void SetupReplica::readYamlField(const YAML::Node& yaml, const std::string& index, T& out, bool value_required) {
  try {
    out = yaml[index].as<T>();
  } catch (const std::exception& e) {
    if (value_required) {
      // We ignore the YAML exceptions because they aren't useful
      std::ostringstream msg;
      msg << "Failed to read \"" << index << "\"";
      throw std::runtime_error(msg.str().data());
    } else {
      LOG_INFO(GL, "No value found for \"" << index << "\"");
    }
  }
}

void SetupReplica::ParseReplicaConfFile(bftEngine::ReplicaConfig& replicaConfig, const YAML::Node& rconfig_yaml) {
  // Preparing replicaConfig object after parsing the replica config file.
  readYamlField(rconfig_yaml, "numOfClientProxies", replicaConfig.numOfClientProxies);
  readYamlField(rconfig_yaml, "numOfExternalClients", replicaConfig.numOfExternalClients);
  readYamlField(rconfig_yaml, "concurrencyLevel", replicaConfig.concurrencyLevel);
  readYamlField(rconfig_yaml, "debugStatisticsEnabled", replicaConfig.debugStatisticsEnabled);
  readYamlField(rconfig_yaml, "viewChangeTimerMillisec", replicaConfig.viewChangeTimerMillisec);
  readYamlField(rconfig_yaml, "statusReportTimerMillisec", replicaConfig.statusReportTimerMillisec);
  readYamlField(rconfig_yaml, "preExecutionFeatureEnabled", replicaConfig.preExecutionFeatureEnabled);
  readYamlField(rconfig_yaml, "clientBatchingEnabled", replicaConfig.clientBatchingEnabled);
  readYamlField(rconfig_yaml, "pruningEnabled_", replicaConfig.pruningEnabled_);
  readYamlField(rconfig_yaml, "numBlocksToKeep_", replicaConfig.numBlocksToKeep_);
  readYamlField(rconfig_yaml, "batchedPreProcessEnabled", replicaConfig.batchedPreProcessEnabled);
  readYamlField(rconfig_yaml, "timeServiceEnabled", replicaConfig.timeServiceEnabled);
  readYamlField(rconfig_yaml, "enablePostExecutionSeparation", replicaConfig.enablePostExecutionSeparation);
  readYamlField(rconfig_yaml, "preExecutionResultAuthEnabled", replicaConfig.preExecutionResultAuthEnabled);
  readYamlField(rconfig_yaml, "numOfClientServices", replicaConfig.numOfClientServices);
  readYamlField(rconfig_yaml, "viewChangeProtocolEnabled", replicaConfig.viewChangeProtocolEnabled);
  readYamlField(rconfig_yaml, "autoPrimaryRotationTimerMillisec", replicaConfig.autoPrimaryRotationTimerMillisec);
  readYamlField(rconfig_yaml, "autoPrimaryRotationEnabled", replicaConfig.autoPrimaryRotationEnabled);
  readYamlField(rconfig_yaml, "keyExchangeOnStart", replicaConfig.keyExchangeOnStart);
  readYamlField(rconfig_yaml, "concurrencyLevel", replicaConfig.concurrencyLevel);
  readYamlField(rconfig_yaml, "batchingPolicy", replicaConfig.batchingPolicy);
  readYamlField(rconfig_yaml, "maxBatchSizeInBytes", replicaConfig.maxBatchSizeInBytes);
  readYamlField(rconfig_yaml, "maxNumOfRequestsInBatch", replicaConfig.maxNumOfRequestsInBatch);
  readYamlField(rconfig_yaml, "batchFlushPeriod", replicaConfig.batchFlushPeriod);
  readYamlField(rconfig_yaml, "maxNumberOfDbCheckpoints", replicaConfig.maxNumberOfDbCheckpoints);
  readYamlField(rconfig_yaml, "dbCheckpointDirPath", replicaConfig.dbCheckpointDirPath);
  readYamlField(rconfig_yaml, "clientTransactionSigningEnabled", replicaConfig.clientTransactionSigningEnabled);
  readYamlField(rconfig_yaml, "numOfClientProxies", replicaConfig.numOfClientProxies);
  readYamlField(rconfig_yaml, "numOfExternalClients", replicaConfig.numOfExternalClients);
  readYamlField(rconfig_yaml, "certificatesRootPath", replicaConfig.certificatesRootPath);
  readYamlField(rconfig_yaml, "publishReplicasMasterKeyOnStartup", replicaConfig.publishReplicasMasterKeyOnStartup);
  readYamlField(rconfig_yaml, "blockAccumulation", replicaConfig.blockAccumulation);
  readYamlField(rconfig_yaml, "pathToOperatorPublicKey_", replicaConfig.pathToOperatorPublicKey_);
  readYamlField(rconfig_yaml, "dbCheckpointFeatureEnabled", replicaConfig.dbCheckpointFeatureEnabled);
  readYamlField(rconfig_yaml, "dbCheckPointWindowSize", replicaConfig.dbCheckPointWindowSize);
  readYamlField(rconfig_yaml, "batchedPreProcessEnabled", replicaConfig.batchedPreProcessEnabled);
  replicaConfig.dbSnapshotIntervalSeconds =
      std::chrono::seconds(rconfig_yaml["dbSnapshotIntervalSeconds"].as<uint64_t>());
  replicaConfig.dbCheckpointMonitorIntervalSeconds =
      std::chrono::seconds(rconfig_yaml["dbCheckpointMonitorIntervalSeconds"].as<uint64_t>());
}

std::unique_ptr<SetupReplica> SetupReplica::ParseArgs(int argc, char** argv) {
  // prepare replica config object from custom replica config file which can be read from ParseReplicaConfFile() and
  // command line arguments
  std::stringstream args;
  for (int i{1}; i < argc; ++i) {
    args << argv[i] << " ";
  }
  LOG_INFO(GL, "Parsing" << KVLOG(argc) << " arguments, args:" << args.str());

  try {
    bftEngine::ReplicaConfig& replicaConfig = bftEngine::ReplicaConfig::instance();

    const auto persistMode = PersistencyMode::RocksDB;
    std::string keysFilePrefix;
    std::string commConfigFile = "";  // default value to empty to setup default nodes
    std::string logPropsFile;
    std::string principalsMapping;
    std::string txnSigningKeysPath;
    std::string replicaSampleConfFilePath;
    std::optional<std::uint32_t> cronEntryNumberOfExecutes;
    int addAllKeysAsPublic = 0;

    static struct option longOptions[] = {{"replica-id", required_argument, 0, 'i'},
                                          {"replicaSampleConfFilePath", required_argument, 0, 'a'}};

    int o = 0;
    int optionIndex = 0;
    LOG_INFO(GL, "Command line options:");
    while ((o = getopt_long(argc, argv, "i:a:", longOptions, &optionIndex)) != -1) {
      switch (o) {
        case 'i': {
          replicaConfig.replicaId = concord::util::to<std::uint16_t>(std::string(optarg));
        } break;
        case 'a': {
          if (optarg[0] == '-') throw std::runtime_error("invalid argument for replica sample config file");
          replicaSampleConfFilePath = optarg;
          LOG_INFO(GL, " " << KVLOG(replicaSampleConfFilePath));
        } break;

        default:
          break;
      }
    }

    // Parsing replica sample config file to update replica config object
    auto yaml = YAML::LoadFile(replicaSampleConfFilePath);
    ParseReplicaConfFile(replicaConfig, yaml);

    replicaConfig.set("sourceReplicaReplacementTimeoutMilli", 6000);
    replicaConfig.set("concord.bft.st.runInSeparateThread", true);
    replicaConfig.set("concord.bft.keyExchage.clientKeysEnabled", false);
    replicaConfig.set("concord.bft.st.fetchRangeSize", 27);
    replicaConfig.set("concord.bft.st.gettingMissingBlocksSummaryWindowSize", 60);
    replicaConfig.set("concord.bft.st.RVT_K", 12);
    replicaConfig.set("concord.bft.keyExchage.clientKeysEnabled", true);

    readYamlField(yaml, "txnSigningKeysPath", txnSigningKeysPath);
    readYamlField(yaml, "principalsMapping", principalsMapping);
    readYamlField(yaml, "logPropsFile", logPropsFile);
    readYamlField(yaml, "keysFilePrefix", keysFilePrefix);
    cronEntryNumberOfExecutes = yaml["cronEntryNumberOfExecutes"].as<std::uint32_t>();

    LOG_INFO(GL, "" << KVLOG(keysFilePrefix));
    LOG_INFO(GL, "" << KVLOG(principalsMapping));
    LOG_INFO(GL, "" << KVLOG(txnSigningKeysPath));

    if (keysFilePrefix.empty()) throw std::runtime_error("missing --key-file-prefix");

    // If principalsMapping and txnSigningKeysPath are set, enable clientTransactionSigningEnabled. If only one of them
    // is set, throw an error
    if (!principalsMapping.empty() && !txnSigningKeysPath.empty()) {
      SetupReplica::setPublicKeysOfClients(principalsMapping, txnSigningKeysPath, replicaConfig.publicKeysOfClients);
    } else if (principalsMapping.empty() != txnSigningKeysPath.empty()) {
      throw std::runtime_error("Params principals-mapping and txn-signing-key-path must be set simultaneously.");
    }

    // Create replica communication
    uint16_t metricsPort;
    std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> sm =
        std::make_shared<concord::secretsmanager::SecretsManagerPlain>();
    std::unique_ptr<bft::communication::ICommunication> comm_ptr(
        createCommunication(replicaConfig, sm, keysFilePrefix, commConfigFile, metricsPort));
    if (!comm_ptr) {
      throw std::runtime_error("Failed to create communication");
    }

    LOG_INFO(getLogger(), "\nReplica Configuration: \n" << replicaConfig);
    std::unique_ptr<SetupReplica> setup = nullptr;
    setup.reset(new SetupReplica(replicaConfig,
                                 std::move(comm_ptr),
                                 metricsPort,
                                 persistMode == PersistencyMode::RocksDB,
                                 logPropsFile,
                                 cronEntryNumberOfExecutes,
                                 addAllKeysAsPublic != 0));
    setup->sm_ = sm;
    return setup;

  } catch (const std::exception& e) {
    LOG_FATAL(GL, "failed to parse command line arguments: " << e.what());
    throw;
  }
  return nullptr;
}

bft::communication::ICommunication* SetupReplica::createCommunication(
    bftEngine::ReplicaConfig& replicaConfig,
    std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl>& sm,
    std::string& keysFilePrefix,
    std::string& commConfigFile,
    uint16_t& metricsPort) {
  logging::Logger logger = getLogger();
  TestCommConfig testCommConfig(logger);
  testCommConfig.GetReplicaConfig(replicaConfig.replicaId, keysFilePrefix, &replicaConfig);
  uint16_t numOfReplicas =
      (uint16_t)(3 * replicaConfig.fVal + 2 * replicaConfig.cVal + 1 + replicaConfig.numRoReplicas);
  auto numOfClients =
      replicaConfig.numOfClientProxies ? replicaConfig.numOfClientProxies : replicaConfig.numOfExternalClients;
#ifdef USE_COMM_PLAIN_TCP
  bft::communication::PlainTcpConfig conf =
      testCommConfig.GetTCPConfig(true, replicaConfig.replicaId, numOfClients, numOfReplicas, commConfigFile);
#elif USE_COMM_TLS_TCP
  bft::communication::TlsTcpConfig conf =
      testCommConfig.GetTlsTCPConfig(true, replicaConfig.replicaId, numOfClients, numOfReplicas, commConfigFile);
  if (conf.secretData_.has_value()) {
    sm = std::make_shared<concord::secretsmanager::SecretsManagerEnc>(conf.secretData_.value());
  } else {
    sm = std::make_shared<concord::secretsmanager::SecretsManagerPlain>();
  }
#else
  bft::communication::PlainUdpConfig conf =
      testCommConfig.GetUDPConfig(true, replicaConfig.replicaId, numOfClients, numOfReplicas, commConfigFile);
#endif
  metricsPort = conf.listenPort_ + 1000;
  return bft::communication::CommFactory::create(conf);
}

std::unique_ptr<concord::kvbc::IStorageFactory> SetupReplica::GetStorageFactory() {
  // TODO handle persistence mode
  std::stringstream dbPath;
  dbPath << CommonConstants::DB_FILE_PREFIX << GetReplicaConfig().replicaId;
  return std::make_unique<concord::kvbc::v2MerkleTree::RocksDBStorageFactory>(dbPath.str());
}

std::vector<std::string> SetupReplica::getKeyDirectories(const std::string& path) {
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

void SetupReplica::setPublicKeysOfClients(
    const std::string& principalsMapping,
    const std::string& keysRootPath,
    std::set<std::pair<const std::string, std::set<uint16_t>>>& publicKeysOfClients) {
  // The string principalsMapping looks like:
  // "11 12;13 14;15 16;17 18;19 20", for 10 client ids divided into 5 participants.

  LOG_INFO(GL, "" << KVLOG(principalsMapping, keysRootPath));
  std::vector<std::string> keysDirectories = SetupReplica::getKeyDirectories(keysRootPath);

  std::regex regex("\\;");
  std::vector<std::string> clientIdChunks(
      std::sregex_token_iterator(principalsMapping.begin(), principalsMapping.end(), regex, -1),
      std::sregex_token_iterator());

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
      regex = " ";
      std::vector<std::string> clientIds(
          std::sregex_token_iterator(clientIdChunks.at(i).begin(), clientIdChunks.at(i).end(), regex, -1),
          std::sregex_token_iterator());

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
}  // namespace concord::osexample
