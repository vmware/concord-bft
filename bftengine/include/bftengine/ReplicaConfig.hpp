// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
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
#include "string.hpp"
#include "kvstream.h"

#include "Serializable.h"

namespace bftEngine {

#define CONFIG_PARAM_RO(param, type, default_val, description) \
  type param = default_val;                                    \
  type get##param() const { return param; }

#define CONFIG_PARAM(param, type, default_val, description) \
  CONFIG_PARAM_RO(param, type, default_val, description);   \
  void set##param(const type& val) { param = val; }

enum BatchingPolicy { BATCH_SELF_ADJUSTED, BATCH_BY_REQ_SIZE, BATCH_BY_REQ_NUM };

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
  CONFIG_PARAM(statusReportTimerMillisec, uint16_t, 0, "how often the replica sends a status report to other replicas");
  CONFIG_PARAM(concurrencyLevel,
               uint16_t,
               0,
               "number of consensus operations that can be executed in parallel "
               "1 <= concurrencyLevel <= 30");
  CONFIG_PARAM(viewChangeProtocolEnabled, bool, false, "whether the view change protocol enabled");
  CONFIG_PARAM(blockAccumulation, bool, false, "whether the block accumulation enabled");
  CONFIG_PARAM(viewChangeTimerMillisec, uint16_t, 0, "timeout used by the  view change protocol ");
  CONFIG_PARAM(autoPrimaryRotationEnabled, bool, false, "if automatic primary rotation is enabled");
  CONFIG_PARAM(autoPrimaryRotationTimerMillisec, uint16_t, 0, "timeout for automatic primary rotation");
  CONFIG_PARAM(preExecutionFeatureEnabled, bool, false, "enables the pre-execution feature");
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
  CONFIG_PARAM(maxInitialBatchSize,
               uint32_t,
               350,
               "Initial value for a number of requests in the primary replica queue to trigger batching");
  CONFIG_PARAM(batchingFactorCoefficient,
               uint32_t,
               4,
               "Parameter used to heuristically compute the 'optimal' batch size");

  // Crypto system
  // RSA public keys of all replicas. map from replica identifier to a public key
  std::set<std::pair<uint16_t, const std::string>> publicKeysOfReplicas;

  CONFIG_PARAM(replicaPrivateKey, std::string, "", "RSA private key of the current replica");

  // Threshold crypto system
  CONFIG_PARAM(thresholdSystemType_, std::string, "", "type of threshold crypto system, [multisig-bls|threshold-bls]");
  CONFIG_PARAM(thresholdSystemSubType_, std::string, "", "sub-type of threshold crypto system [BN-254]");
  CONFIG_PARAM(thresholdPrivateKey_, std::string, "", "threshold crypto system bootstrap private key");
  CONFIG_PARAM(thresholdPublicKey_, std::string, "", "threshold crypto system bootstrap public key");
  std::vector<std::string> thresholdVerificationKeys_;

  CONFIG_PARAM(debugPersistentStorageEnabled, bool, false, "whether persistent storage debugging is enabled");

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
    serialize(outStream, statusReportTimerMillisec);
    serialize(outStream, concurrencyLevel);
    serialize(outStream, viewChangeProtocolEnabled);
    serialize(outStream, viewChangeTimerMillisec);
    serialize(outStream, autoPrimaryRotationEnabled);
    serialize(outStream, autoPrimaryRotationTimerMillisec);

    serialize(outStream, preExecutionFeatureEnabled);
    serialize(outStream, preExecReqStatusCheckTimerMillisec);
    serialize(outStream, preExecConcurrencyLevel);
    serialize(outStream, batchingPolicy);
    serialize(outStream, maxInitialBatchSize);
    serialize(outStream, batchingFactorCoefficient);

    serialize(outStream, publicKeysOfReplicas);
    serialize(outStream, replicaPrivateKey);
    serialize(outStream, thresholdSystemType_);
    serialize(outStream, thresholdSystemSubType_);
    serialize(outStream, thresholdPrivateKey_);
    serialize(outStream, thresholdPublicKey_);
    serialize(outStream, thresholdVerificationKeys_);

    serialize(outStream, debugPersistentStorageEnabled);
    serialize(outStream, maxExternalMessageSize);
    serialize(outStream, maxReplyMessageSize);
    serialize(outStream, maxNumOfReservedPages);
    serialize(outStream, sizeOfReservedPage);
    serialize(outStream, debugStatisticsEnabled);
    serialize(outStream, metricsDumpIntervalSeconds);
    serialize(outStream, keyExchangeOnStart);
    serialize(outStream, blockAccumulation);
    serialize(outStream, keyViewFilePath);

    serialize(outStream, config_params_);
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
    deserialize(inStream, statusReportTimerMillisec);
    deserialize(inStream, concurrencyLevel);
    deserialize(inStream, viewChangeProtocolEnabled);
    deserialize(inStream, viewChangeTimerMillisec);
    deserialize(inStream, autoPrimaryRotationEnabled);
    deserialize(inStream, autoPrimaryRotationTimerMillisec);

    deserialize(inStream, preExecutionFeatureEnabled);
    deserialize(inStream, preExecReqStatusCheckTimerMillisec);
    deserialize(inStream, preExecConcurrencyLevel);
    deserialize(inStream, batchingPolicy);
    deserialize(inStream, maxInitialBatchSize);
    deserialize(inStream, batchingFactorCoefficient);

    deserialize(inStream, publicKeysOfReplicas);
    deserialize(inStream, replicaPrivateKey);
    deserialize(inStream, thresholdSystemType_);
    deserialize(inStream, thresholdSystemSubType_);
    deserialize(inStream, thresholdPrivateKey_);
    deserialize(inStream, thresholdPublicKey_);
    deserialize(inStream, thresholdVerificationKeys_);

    deserialize(inStream, debugPersistentStorageEnabled);
    deserialize(inStream, maxExternalMessageSize);
    deserialize(inStream, maxReplyMessageSize);
    deserialize(inStream, maxNumOfReservedPages);
    deserialize(inStream, sizeOfReservedPage);
    deserialize(inStream, debugStatisticsEnabled);
    deserialize(inStream, metricsDumpIntervalSeconds);
    deserialize(inStream, keyExchangeOnStart);
    deserialize(inStream, blockAccumulation);
    deserialize(inStream, keyViewFilePath);

    deserialize(inStream, config_params_);
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
              rc.viewChangeProtocolEnabled,
              rc.viewChangeTimerMillisec,
              rc.autoPrimaryRotationEnabled,
              rc.autoPrimaryRotationTimerMillisec,
              rc.preExecutionFeatureEnabled,
              rc.preExecReqStatusCheckTimerMillisec)
     <<

      KVLOG(rc.preExecConcurrencyLevel,
            rc.batchingPolicy,
            rc.maxInitialBatchSize,
            rc.batchingFactorCoefficient,
            rc.debugPersistentStorageEnabled,
            rc.maxExternalMessageSize,
            rc.maxReplyMessageSize,
            rc.maxNumOfReservedPages,
            rc.sizeOfReservedPage,
            rc.debugStatisticsEnabled,
            rc.metricsDumpIntervalSeconds,
            rc.keyExchangeOnStart,
            rc.blockAccumulation,
            rc.keyViewFilePath);

  for (auto& [param, value] : rc.config_params_) os << param << ": " << value << "\n";

  return os;
}

}  // namespace bftEngine
