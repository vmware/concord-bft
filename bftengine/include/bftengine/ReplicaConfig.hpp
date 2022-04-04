// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <stdint.h>
#include <set>
#include <string>
#include <ostream>
#include <vector>
#include <unordered_map>
#include <chrono>
#include "string.hpp"
#include "kvstream.h"

#include "Serializable.h"

namespace bftEngine {

#define CONFIG_PARAM_RO(param, type, default_val, description) \
  type param = default_val;                                    \
  type get##param() const { return param; }

#define CONFIG_PARAM(param, type, default_val, description) \
  CONFIG_PARAM_RO(param, type, default_val, description);   \
  void set##param(const type& val) { param = val; } /* NOLINT(bugprone-macro-parentheses) */

enum BatchingPolicy { BATCH_SELF_ADJUSTED, BATCH_BY_REQ_SIZE, BATCH_BY_REQ_NUM, BATCH_ADAPTIVE };

class ReplicaConfig : public concord::serialize::SerializableFactory<ReplicaConfig> {
 public:
  friend class concord::serialize::SerializableFactory<ReplicaConfig>;

  static ReplicaConfig& instance() {
    static ReplicaConfig config_;
    return config_;
  }

  CONFIG_PARAM(isReadOnly, bool, false, "Am I a read-only replica?");
  CONFIG_PARAM(numReplicas, uint16_t, 0, "number of regular replicas");
  CONFIG_PARAM(numRoReplicas, uint16_t, 0, "number of read-only replicas");
  CONFIG_PARAM(fVal, uint16_t, 0, "F value - max number of faulty/malicious replicas. fVal >= 1");
  CONFIG_PARAM(cVal, uint16_t, 0, "C value. cVal >=0");
  CONFIG_PARAM(replicaId,
               uint16_t,
               0,
               "unique identifier of the replica. "
               "The number of replicas in the system should be N = 3*fVal + 2*cVal + 1. "
               "In the current version, replicaId should be a number between 0 and  N-1. "
               "replicaId should also represent this replica in ICommunication.");
  CONFIG_PARAM(numOfClientProxies, uint16_t, 0, "number of objects that represent clients, numOfClientProxies >= 1");
  CONFIG_PARAM(numOfExternalClients, uint16_t, 0, "number of objects that represent external clients");
  CONFIG_PARAM(numOfClientServices, uint16_t, 0, "number of objects that represent client services");
  CONFIG_PARAM(sizeOfInternalThreadPool, uint16_t, 8, "number of threads in the internal replica thread pool");
  CONFIG_PARAM(statusReportTimerMillisec, uint16_t, 0, "how often the replica sends a status report to other replicas");
  CONFIG_PARAM(clientRequestRetransmissionTimerMilli,
               uint16_t,
               1000,
               "how often the replica tries to retransmit client request received by non primary");
  CONFIG_PARAM(concurrencyLevel,
               uint16_t,
               0,
               "number of consensus operations that can be executed in parallel "
               "1 <= concurrencyLevel <= 30");
  CONFIG_PARAM(numWorkerThreadsForBlockIO,
               uint16_t,
               8,
               "Number of workers threads to be used for blocks IO"
               "operations. When set to 0, std::thread::hardware_concurrency() is set by default");
  CONFIG_PARAM(viewChangeProtocolEnabled, bool, false, "whether the view change protocol enabled");
  CONFIG_PARAM(blockAccumulation, bool, false, "whether the block accumulation enabled");
  CONFIG_PARAM(viewChangeTimerMillisec, uint16_t, 0, "timeout used by the  view change protocol ");
  CONFIG_PARAM(autoPrimaryRotationEnabled, bool, false, "if automatic primary rotation is enabled");
  CONFIG_PARAM(autoPrimaryRotationTimerMillisec, uint16_t, 0, "timeout for automatic primary rotation");
  CONFIG_PARAM(preExecutionFeatureEnabled, bool, false, "enables the pre-execution feature");
  CONFIG_PARAM(batchedPreProcessEnabled,
               bool,
               false,
               "enables send/receive of batched PreProcess request/reply messages");
  CONFIG_PARAM(clientBatchingEnabled, bool, false, "enables the concord-client-batch feature");
  CONFIG_PARAM(clientBatchingMaxMsgsNbr, uint16_t, 10, "Maximum messages number in one client batch");
  CONFIG_PARAM(clientTransactionSigningEnabled,
               bool,
               false,
               "whether concord client requests are signed and should be verified");
  CONFIG_PARAM(preExecReqStatusCheckTimerMillisec,
               uint64_t,
               5000,
               "timeout for detection of timed out "
               "pre-execution requests");
  CONFIG_PARAM(preExecConcurrencyLevel,
               uint16_t,
               0,
               "Number of threads to be used by the PreProcessor to execute "
               "client requests. If equals to 0, a default of "
               "min(thread::hardware_concurrency(), numOfClients) is used ");

  CONFIG_PARAM(batchingPolicy, uint32_t, BATCH_SELF_ADJUSTED, "BFT consensus batching policy for requests");
  CONFIG_PARAM(batchFlushPeriod, uint32_t, 1000, "BFT consensus batching flush period");
  CONFIG_PARAM(maxNumOfRequestsInBatch, uint32_t, 100, "Maximum number of requests in BFT consensus batch");
  CONFIG_PARAM(maxBatchSizeInBytes, uint32_t, 33554432, "Maximum size of all requests in BFT consensus batch");
  CONFIG_PARAM(maxInitialBatchSize,
               uint32_t,
               350,
               "Initial value for a number of requests in the primary replica queue to trigger batching");
  CONFIG_PARAM(batchingFactorCoefficient,
               uint32_t,
               4,
               "Parameter used to heuristically compute the 'optimal' batch size");
  CONFIG_PARAM(adaptiveBatchingIncFactor, std::string, "0.1", "The increase/decrease rate");
  CONFIG_PARAM(adaptiveBatchingMaxIncCond, std::string, "0.95", "The max increase condition");
  CONFIG_PARAM(adaptiveBatchingMidIncCond, std::string, "0.9", "The mid increase condition");
  CONFIG_PARAM(adaptiveBatchingMinIncCond, std::string, "0.75", "The min increase condition");
  CONFIG_PARAM(adaptiveBatchingDecCond, std::string, "0.5", "The decrease condition");

  // Crypto system
  // RSA public keys of all replicas. map from replica identifier to a public key
  std::set<std::pair<uint16_t, const std::string>> publicKeysOfReplicas;

  // RSA public keys of all clients. Each public key holds set of distinct client (principal) ids which are expected to
  // sign with the matching private key
  std::set<std::pair<const std::string, std::set<uint16_t>>> publicKeysOfClients;
  std::unordered_map<uint16_t, std::set<uint16_t>> clientGroups;
  CONFIG_PARAM(clientsKeysPrefix, std::string, "", "the path to the client keys directory");
  CONFIG_PARAM(saveClinetKeyFile,
               bool,
               false,
               "if true, the replica will also updates the client key file on key exchange");
  CONFIG_PARAM(replicaPrivateKey, std::string, "", "RSA private key of the current replica");

  CONFIG_PARAM(certificatesRootPath, std::string, "", "the path to the certificates root directory");

  // Threshold crypto system
  CONFIG_PARAM(thresholdSystemType_, std::string, "", "type of threshold crypto system, [multisig-bls|threshold-bls]");
  CONFIG_PARAM(thresholdSystemSubType_, std::string, "", "sub-type of threshold crypto system [BN-254]");
  CONFIG_PARAM(thresholdPrivateKey_, std::string, "", "threshold crypto system bootstrap private key");
  CONFIG_PARAM(thresholdPublicKey_, std::string, "", "threshold crypto system bootstrap public key");
  std::vector<std::string> thresholdVerificationKeys_;

  // Reconfiguration credentials
  CONFIG_PARAM(pathToOperatorPublicKey_, std::string, "", "Path to the operator public key pem file");
  CONFIG_PARAM(operatorEnabled_, bool, true, "true if operator is enabled");
  // Pruning parameters
  CONFIG_PARAM(pruningEnabled_, bool, false, "Enable pruning");
  CONFIG_PARAM(numBlocksToKeep_, uint64_t, 0, "how much blocks to keep while pruning");

  CONFIG_PARAM(debugPersistentStorageEnabled, bool, false, "whether persistent storage debugging is enabled");
  CONFIG_PARAM(deleteMetricsDumpInterval, uint64_t, 300, "delete metrics dump interval (s)");

  // Messages
  CONFIG_PARAM(maxExternalMessageSize, uint32_t, 131072, "maximum size of external message");
  CONFIG_PARAM(maxReplyMessageSize, uint32_t, 8192, "maximum size of reply message");

  // StateTransfer
  CONFIG_PARAM(maxNumOfReservedPages, uint32_t, 2048, "maximum number of reserved pages managed by State Transfer");
  CONFIG_PARAM(sizeOfReservedPage, uint32_t, 4096, "size of reserved page used by State Transfer");

  CONFIG_PARAM(debugStatisticsEnabled, bool, false, "whether to periodically dump debug statistics");
  CONFIG_PARAM(metricsDumpIntervalSeconds, uint64_t, 600, "Metrics dump interval");

  // Keys Management
  CONFIG_PARAM(keyExchangeOnStart, bool, false, "whether to perform initial key exchange");
  CONFIG_PARAM(keyViewFilePath, std::string, ".", "TODO");
  // Configuration Management
  // Keys Management
  CONFIG_PARAM(configurationViewFilePath,
               std::string,
               ".",
               "The path where we write all previous configuration in a file");
  // Time Service
  CONFIG_PARAM(timeServiceEnabled, bool, false, "whether time service enabled");
  CONFIG_PARAM(timeServiceSoftLimitMillis,
               std::chrono::milliseconds,
               std::chrono::milliseconds{500},
               "if delta between primary's time is greater than the soft limit, a warning log message to be printed");
  CONFIG_PARAM(timeServiceHardLimitMillis,
               std::chrono::milliseconds,
               std::chrono::seconds{1} * 3,
               "if delta between primary's time is greater than the hard limit, the consesnus does not proceed");
  CONFIG_PARAM(timeServiceEpsilonMillis,
               std::chrono::milliseconds,
               std::chrono::milliseconds{1},
               "time provided to execution is max(consensus_time, last_time + timeServiceEpsilonMillis)");

  // Ticks Generator
  CONFIG_PARAM(ticksGeneratorPollPeriod,
               std::chrono::seconds,
               std::chrono::seconds{1},
               "wake up the ticks generator every ticksGeneratorPollPeriod seconds and fire pending ticks");

  CONFIG_PARAM(preExecutionResultAuthEnabled, bool, false, "if PreExecution result authentication is enabled");

  CONFIG_PARAM(prePrepareFinalizeAsyncEnabled, bool, true, "Enabling asynchronous preprepare finishing");

  CONFIG_PARAM(
      threadbagConcurrencyLevel1,
      uint32_t,
      40u,
      "Number of threads given to thread pool that is created for any request processing for the parent of validation");

  CONFIG_PARAM(
      threadbagConcurrencyLevel2,
      uint32_t,
      24u,
      "Number of threads given to thread pool that is created for any request processing for actual validation");

  CONFIG_PARAM(timeoutForPrimaryOnStartupSeconds,
               uint32_t,
               60,
               "timeout we ready to wait for primary to be ready on the system first startup");
  CONFIG_PARAM(waitForFullCommOnStartup, bool, false, "whether to wait for n/n communication on startup");
  CONFIG_PARAM(publishReplicasMasterKeyOnStartup,
               bool,
               false,
               "If true, replicas will publish their master key on startup");
  // Db checkpoint
  CONFIG_PARAM(dbCheckpointFeatureEnabled, bool, false, "Feature flag for rocksDb checkpoints");
  CONFIG_PARAM(maxNumberOfDbCheckpoints, uint32_t, 2u, "Max number of db checkpoints to be created");
  CONFIG_PARAM(dbCheckPointWindowSize, uint32_t, 30000u, "Db checkpoint window size in bft sequence number");
  CONFIG_PARAM(dbCheckpointDirPath, std::string, "", "Db checkpoint directory path");
  CONFIG_PARAM(dbSnapshotIntervalSeconds,
               std::chrono::seconds,
               std::chrono::seconds{3600},
               "Interval time to create db snapshot in seconds");
  CONFIG_PARAM(dbCheckpointMonitorIntervalSeconds,
               std::chrono::seconds,
               std::chrono::seconds{60},
               "Time interval in seconds to monitor disk usage by db checkpoints");
  CONFIG_PARAM(dbCheckpointDiskSpaceThreshold,
               std::string,
               "0.5",
               "Free disk space threshold for db checkpoint cleanup");
  CONFIG_PARAM(enablePostExecutionSeparation, bool, true, "Post-execution thread separation feature flag");
  CONFIG_PARAM(postExecutionQueuesSize, uint16_t, 50, "Post-execution deferred message queues size");

  // Parameter to enable/disable waiting for transaction data to be persisted.
  CONFIG_PARAM(syncOnUpdateOfMetadata,
               bool,
               true,
               "When set to true this parameter will cause endWriteTran to block until "
               "the transaction is persisted every time we update the metadata.");

  CONFIG_PARAM(stateIterationMultiGetBatchSize,
               std::uint32_t,
               30u,
               "Amount of keys to get at once via multiGet when iterating state");

  CONFIG_PARAM(enableMultiplexChannel, bool, false, "whether multiplex communication channel is enabled")

  CONFIG_PARAM(useUnifiedCertificates, bool, false, "A flag to use unified Certificates");

  CONFIG_PARAM(adaptivePruningIntervalDuration,
               std::chrono::milliseconds,
               std::chrono::milliseconds{20000},
               "The polling frequency of pruning info in ms");

  CONFIG_PARAM(adaptivePruningIntervalPeriod,
               std::uint64_t,
               20,
               "Resetting the measurements and starting again after polling X times");

  CONFIG_PARAM(enableEventGroups, bool, false, "A flag to specify whether event groups are enabled or not.");

  CONFIG_PARAM(enablePreProcessorMemoryPool,
               bool,
               true,
               "A flag to specify whether to use a memory pool in PreProcessor or not");

  CONFIG_PARAM(diagnosticsServerPort,
               int,
               0,
               "Port to be used to communicate with the diagnostic server using"
               "the concord-ctl script");
  CONFIG_PARAM(kvBlockchainVersion, std::uint32_t, 1u, "Default version of KV blockchain for this replica");

  // Parameter to enable/disable waiting for transaction data to be persisted.
  // Not predefined configuration parameters
  // Example of usage:
  // repclicaConfig.set(someTimeout, 6000);
  // uint32_t some_timeout = replicaConfig.get("someTimeout", 5000);
  // if set, will return a previously set value for someTimeout, 5000 otherwise

  template <typename T>
  void set(const std::string& param, const T& value) {
    config_params_[param] = std::to_string(value);
  }

  template <typename T>
  T get(const std::string& param, const T& defaultValue) const {
    if (auto it = config_params_.find(param); it != config_params_.end()) return concord::util::to<T>(it->second);
    return defaultValue;
  }
  inline std::set<std::pair<const std::string, std::set<uint16_t>>>* getPublicKeysOfClients() {
    return (clientTransactionSigningEnabled || !clientsKeysPrefix.empty()) ? &publicKeysOfClients : nullptr;
  }

 protected:
  ReplicaConfig() = default;
  // serializable functionality
  const std::string getVersion() const { return "1"; }

  void serializeDataMembers(std::ostream& outStream) const {
    serialize(outStream, isReadOnly);
    serialize(outStream, numReplicas);
    serialize(outStream, numRoReplicas);
    serialize(outStream, fVal);
    serialize(outStream, cVal);
    serialize(outStream, replicaId);
    serialize(outStream, numOfClientProxies);
    serialize(outStream, numOfExternalClients);
    serialize(outStream, numOfClientServices);
    serialize(outStream, statusReportTimerMillisec);
    serialize(outStream, concurrencyLevel);
    serialize(outStream, numWorkerThreadsForBlockIO);
    serialize(outStream, viewChangeProtocolEnabled);
    serialize(outStream, viewChangeTimerMillisec);
    serialize(outStream, autoPrimaryRotationEnabled);
    serialize(outStream, autoPrimaryRotationTimerMillisec);

    serialize(outStream, preExecutionFeatureEnabled);
    serialize(outStream, batchedPreProcessEnabled);
    serialize(outStream, clientBatchingEnabled);
    serialize(outStream, clientBatchingMaxMsgsNbr);
    serialize(outStream, clientTransactionSigningEnabled);
    serialize(outStream, preExecReqStatusCheckTimerMillisec);
    serialize(outStream, preExecConcurrencyLevel);
    serialize(outStream, batchingPolicy);
    serialize(outStream, batchFlushPeriod);
    serialize(outStream, maxNumOfRequestsInBatch);
    serialize(outStream, maxBatchSizeInBytes);
    serialize(outStream, maxInitialBatchSize);
    serialize(outStream, batchingFactorCoefficient);
    serialize(outStream, adaptiveBatchingIncFactor);
    serialize(outStream, adaptiveBatchingMaxIncCond);
    serialize(outStream, adaptiveBatchingMidIncCond);
    serialize(outStream, adaptiveBatchingMinIncCond);
    serialize(outStream, adaptiveBatchingDecCond);

    serialize(outStream, publicKeysOfReplicas);
    serialize(outStream, publicKeysOfClients);
    serialize(outStream, clientGroups);
    serialize(outStream, clientsKeysPrefix);
    serialize(outStream, saveClinetKeyFile);
    serialize(outStream, replicaPrivateKey);
    serialize(outStream, certificatesRootPath);
    serialize(outStream, thresholdSystemType_);
    serialize(outStream, thresholdSystemSubType_);
    serialize(outStream, thresholdPrivateKey_);
    serialize(outStream, thresholdPublicKey_);
    serialize(outStream, thresholdVerificationKeys_);

    serialize(outStream, pathToOperatorPublicKey_);
    serialize(outStream, operatorEnabled_);
    serialize(outStream, pruningEnabled_);
    serialize(outStream, numBlocksToKeep_);

    serialize(outStream, debugPersistentStorageEnabled);
    serialize(outStream, deleteMetricsDumpInterval);
    serialize(outStream, maxExternalMessageSize);
    serialize(outStream, maxReplyMessageSize);
    serialize(outStream, maxNumOfReservedPages);
    serialize(outStream, sizeOfReservedPage);
    serialize(outStream, debugStatisticsEnabled);
    serialize(outStream, metricsDumpIntervalSeconds);
    serialize(outStream, keyExchangeOnStart);
    serialize(outStream, blockAccumulation);
    serialize(outStream, sizeOfInternalThreadPool);
    serialize(outStream, keyViewFilePath);
    serialize(outStream, configurationViewFilePath);
    serialize(outStream, timeServiceEnabled);
    serialize(outStream, timeServiceHardLimitMillis);
    serialize(outStream, timeServiceSoftLimitMillis);
    serialize(outStream, timeServiceEpsilonMillis);
    serialize(outStream, ticksGeneratorPollPeriod);
    serialize(outStream, preExecutionResultAuthEnabled);
    serialize(outStream, prePrepareFinalizeAsyncEnabled);
    serialize(outStream, threadbagConcurrencyLevel1);
    serialize(outStream, threadbagConcurrencyLevel2);
    serialize(outStream, timeoutForPrimaryOnStartupSeconds);
    serialize(outStream, waitForFullCommOnStartup);
    serialize(outStream, publishReplicasMasterKeyOnStartup);
    serialize(outStream, dbCheckpointFeatureEnabled);
    serialize(outStream, maxNumberOfDbCheckpoints);
    serialize(outStream, dbCheckPointWindowSize);
    serialize(outStream, dbCheckpointDirPath);
    serialize(outStream, dbSnapshotIntervalSeconds);
    serialize(outStream, dbCheckpointMonitorIntervalSeconds);
    serialize(outStream, dbCheckpointDiskSpaceThreshold);
    serialize(outStream, enablePostExecutionSeparation);
    serialize(outStream, postExecutionQueuesSize);
    serialize(outStream, stateIterationMultiGetBatchSize);
    serialize(outStream, adaptivePruningIntervalDuration);
    serialize(outStream, adaptivePruningIntervalPeriod);
    serialize(outStream, config_params_);
    serialize(outStream, enableMultiplexChannel);
    serialize(outStream, enableEventGroups);
    serialize(outStream, enablePreProcessorMemoryPool);
    serialize(outStream, diagnosticsServerPort);
    serialize(outStream, useUnifiedCertificates);
    serialize(outStream, kvBlockchainVersion);
  }
  void deserializeDataMembers(std::istream& inStream) {
    deserialize(inStream, isReadOnly);
    deserialize(inStream, numReplicas);
    deserialize(inStream, numRoReplicas);
    deserialize(inStream, fVal);
    deserialize(inStream, cVal);
    deserialize(inStream, replicaId);
    deserialize(inStream, numOfClientProxies);
    deserialize(inStream, numOfExternalClients);
    deserialize(inStream, numOfClientServices);
    deserialize(inStream, statusReportTimerMillisec);
    deserialize(inStream, concurrencyLevel);
    deserialize(inStream, numWorkerThreadsForBlockIO);
    deserialize(inStream, viewChangeProtocolEnabled);
    deserialize(inStream, viewChangeTimerMillisec);
    deserialize(inStream, autoPrimaryRotationEnabled);
    deserialize(inStream, autoPrimaryRotationTimerMillisec);

    deserialize(inStream, preExecutionFeatureEnabled);
    deserialize(inStream, batchedPreProcessEnabled);
    deserialize(inStream, clientBatchingEnabled);
    deserialize(inStream, clientBatchingMaxMsgsNbr);
    deserialize(inStream, clientTransactionSigningEnabled);
    deserialize(inStream, preExecReqStatusCheckTimerMillisec);
    deserialize(inStream, preExecConcurrencyLevel);
    deserialize(inStream, batchingPolicy);
    deserialize(inStream, batchFlushPeriod);
    deserialize(inStream, maxNumOfRequestsInBatch);
    deserialize(inStream, maxBatchSizeInBytes);
    deserialize(inStream, maxInitialBatchSize);
    deserialize(inStream, batchingFactorCoefficient);
    deserialize(inStream, adaptiveBatchingIncFactor);
    deserialize(inStream, adaptiveBatchingMaxIncCond);
    deserialize(inStream, adaptiveBatchingMidIncCond);
    deserialize(inStream, adaptiveBatchingMinIncCond);
    deserialize(inStream, adaptiveBatchingDecCond);

    deserialize(inStream, publicKeysOfReplicas);
    deserialize(inStream, publicKeysOfClients);
    deserialize(inStream, clientGroups);
    deserialize(inStream, clientsKeysPrefix);
    deserialize(inStream, saveClinetKeyFile);
    deserialize(inStream, replicaPrivateKey);
    deserialize(inStream, certificatesRootPath);
    deserialize(inStream, thresholdSystemType_);
    deserialize(inStream, thresholdSystemSubType_);
    deserialize(inStream, thresholdPrivateKey_);
    deserialize(inStream, thresholdPublicKey_);
    deserialize(inStream, thresholdVerificationKeys_);

    deserialize(inStream, pathToOperatorPublicKey_);
    deserialize(inStream, operatorEnabled_);
    deserialize(inStream, pruningEnabled_);
    deserialize(inStream, numBlocksToKeep_);

    deserialize(inStream, debugPersistentStorageEnabled);
    deserialize(inStream, deleteMetricsDumpInterval);
    deserialize(inStream, maxExternalMessageSize);
    deserialize(inStream, maxReplyMessageSize);
    deserialize(inStream, maxNumOfReservedPages);
    deserialize(inStream, sizeOfReservedPage);
    deserialize(inStream, debugStatisticsEnabled);
    deserialize(inStream, metricsDumpIntervalSeconds);
    deserialize(inStream, keyExchangeOnStart);
    deserialize(inStream, blockAccumulation);
    deserialize(inStream, sizeOfInternalThreadPool);
    deserialize(inStream, keyViewFilePath);
    deserialize(inStream, configurationViewFilePath);
    deserialize(inStream, timeServiceEnabled);
    deserialize(inStream, timeServiceHardLimitMillis);
    deserialize(inStream, timeServiceSoftLimitMillis);
    deserialize(inStream, timeServiceEpsilonMillis);
    deserialize(inStream, ticksGeneratorPollPeriod);
    deserialize(inStream, preExecutionResultAuthEnabled);
    deserialize(inStream, prePrepareFinalizeAsyncEnabled);
    deserialize(inStream, threadbagConcurrencyLevel1);
    deserialize(inStream, threadbagConcurrencyLevel2);
    deserialize(inStream, timeoutForPrimaryOnStartupSeconds);
    deserialize(inStream, waitForFullCommOnStartup);
    deserialize(inStream, publishReplicasMasterKeyOnStartup);
    deserialize(inStream, dbCheckpointFeatureEnabled);
    deserialize(inStream, maxNumberOfDbCheckpoints);
    deserialize(inStream, dbCheckPointWindowSize);
    deserialize(inStream, dbCheckpointDirPath);
    deserialize(inStream, dbSnapshotIntervalSeconds);
    deserialize(inStream, dbCheckpointMonitorIntervalSeconds);
    deserialize(inStream, dbCheckpointDiskSpaceThreshold);
    deserialize(inStream, enablePostExecutionSeparation);
    deserialize(inStream, postExecutionQueuesSize);
    deserialize(inStream, stateIterationMultiGetBatchSize);
    deserialize(inStream, adaptivePruningIntervalDuration);
    deserialize(inStream, adaptivePruningIntervalPeriod);
    deserialize(inStream, config_params_);
    deserialize(inStream, enableMultiplexChannel);
    deserialize(inStream, enableEventGroups);
    deserialize(inStream, enablePreProcessorMemoryPool);
    deserialize(inStream, diagnosticsServerPort);
    deserialize(inStream, useUnifiedCertificates);
    deserialize(inStream, kvBlockchainVersion);
  }

 private:
  ReplicaConfig(const ReplicaConfig&) = delete;
  ReplicaConfig& operator=(const ReplicaConfig&) = delete;

  std::unordered_map<std::string, std::string> config_params_;

  friend std::ostream& operator<<(std::ostream&, const ReplicaConfig&);
};

template <>
inline void ReplicaConfig::set<std::string>(const std::string& param, const std::string& value) {
  config_params_[param] = value;
}

inline std::ostream& operator<<(std::ostream& os, const ReplicaConfig& rc) {
  os << KVLOG(rc.isReadOnly,
              rc.numReplicas,
              rc.numRoReplicas,
              rc.fVal,
              rc.cVal,
              rc.replicaId,
              rc.numOfClientProxies,
              rc.numOfExternalClients,
              rc.statusReportTimerMillisec,
              rc.concurrencyLevel,
              rc.numWorkerThreadsForBlockIO,
              rc.viewChangeProtocolEnabled,
              rc.viewChangeTimerMillisec,
              rc.autoPrimaryRotationEnabled,
              rc.autoPrimaryRotationTimerMillisec,
              rc.preExecutionFeatureEnabled);
  os << ",";
  os << KVLOG(rc.preExecReqStatusCheckTimerMillisec,
              rc.preExecConcurrencyLevel,
              rc.batchingPolicy,
              rc.batchFlushPeriod,
              rc.maxNumOfRequestsInBatch,
              rc.maxBatchSizeInBytes,
              rc.maxInitialBatchSize,
              rc.batchingFactorCoefficient,
              rc.debugPersistentStorageEnabled,
              rc.maxExternalMessageSize,
              rc.maxReplyMessageSize,
              rc.maxNumOfReservedPages,
              rc.sizeOfReservedPage,
              rc.debugStatisticsEnabled,
              rc.metricsDumpIntervalSeconds,
              rc.keyExchangeOnStart);
  os << ",";
  os << KVLOG(rc.blockAccumulation,
              rc.clientBatchingEnabled,
              rc.clientBatchingMaxMsgsNbr,
              rc.keyViewFilePath,
              rc.clientTransactionSigningEnabled,
              rc.adaptiveBatchingIncFactor,
              rc.adaptiveBatchingMaxIncCond,
              rc.adaptiveBatchingMidIncCond,
              rc.adaptiveBatchingMinIncCond,
              rc.adaptiveBatchingDecCond,
              rc.sizeOfInternalThreadPool,
              rc.timeServiceEnabled,
              rc.timeServiceSoftLimitMillis.count(),
              rc.timeServiceHardLimitMillis.count(),
              rc.timeServiceEpsilonMillis.count(),
              rc.batchedPreProcessEnabled);
  os << ",";
  os << KVLOG(rc.ticksGeneratorPollPeriod.count(),
              rc.preExecutionResultAuthEnabled,
              rc.prePrepareFinalizeAsyncEnabled,
              rc.threadbagConcurrencyLevel1,
              rc.threadbagConcurrencyLevel2,
              rc.enablePostExecutionSeparation,
              rc.postExecutionQueuesSize,
              rc.stateIterationMultiGetBatchSize,
              rc.numOfClientServices,
              rc.dbCheckpointFeatureEnabled,
              rc.maxNumberOfDbCheckpoints,
              rc.dbCheckPointWindowSize,
              rc.dbCheckpointDirPath,
              rc.adaptivePruningIntervalDuration.count(),
              rc.adaptivePruningIntervalPeriod,
              rc.dbSnapshotIntervalSeconds.count());
  os << ",";
  os << KVLOG(rc.dbCheckpointMonitorIntervalSeconds.count(),
              rc.dbCheckpointDiskSpaceThreshold,
              rc.enableMultiplexChannel,
              rc.enableEventGroups,
              rc.operatorEnabled_,
              rc.enablePreProcessorMemoryPool,
              rc.diagnosticsServerPort,
              rc.useUnifiedCertificates,
              rc.kvBlockchainVersion);
  os << ", ";
  for (auto& [param, value] : rc.config_params_) os << param << ": " << value << "\n";
  return os;
}

}  // namespace bftEngine
