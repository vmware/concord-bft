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
#include <mutex>
#include <ostream>
#include <vector>

class IThresholdSigner;
class IThresholdVerifier;

namespace bftEngine {

enum BatchingPolicy { BATCH_SELF_ADJUSTED, BATCH_BY_REQ_SIZE, BATCH_BY_REQ_NUM };

struct ReplicaConfig {
  // Am I a read-only replica?
  bool isReadOnly = false;

  // number of regular replicas
  uint16_t numReplicas = 0;

  // number of read-only replicas
  uint16_t numRoReplicas = 0;

  // F value - max number of faulty/malicious replicas. fVal >= 1
  uint16_t fVal = 0;

  // C value. cVal >=0
  uint16_t cVal = 0;

  // unique identifier of the replica.
  // The number of replicas in the system should be N = 3*fVal + 2*cVal + 1
  // In the current version, replicaId should be a number between 0 and  N-1
  // replicaId should also represent this replica in ICommunication.
  uint16_t replicaId = 0;

  // number of objects that represent clients.
  // numOfClientProxies >= 1
  uint16_t numOfClientProxies = 0;

  // number of objects that represent external clients.
  // numOfExternalClients >= 0
  // By default, numOfExternalClients is 0 unless configured differently.
  uint16_t numOfExternalClients = 0;

  // a time interval in milliseconds. represents how often the replica sends a status report to the other replicas.
  // statusReportTimerMillisec > 0
  uint16_t statusReportTimerMillisec = 0;

  // number of consensus operations that can be executed in parallel
  // 1 <= concurrencyLevel <= 30
  uint16_t concurrencyLevel = 0;

  // viewChangeProtocolEnabled=true, if the view change protocol is enabled at all
  bool viewChangeProtocolEnabled = false;

  // a time interval in milliseconds. represents the timeout used by the  view change protocol
  uint16_t viewChangeTimerMillisec = 0;

  // autoPrimaryRotationEnabled=true, if the automatic primary rotation is enabled
  bool autoPrimaryRotationEnabled = false;

  // a time interval in milliseconds, represents the timeout for automatically replacing the primary
  uint16_t autoPrimaryRotationTimerMillisec = 0;

  // preExecutionFeatureEnabled=true enables the pre-execution feature
  bool preExecutionFeatureEnabled = false;

  // a time interval in milliseconds represents the timeout for the detection of timed out pre-execution requests
  uint64_t preExecReqStatusCheckTimerMillisec = 5000;

  // Number of threads to be used by the PreProcessor to execute client requests
  // If equals to 0, a default number of min(thread::hardware_concurrency(), numOfClients) is used
  uint16_t preExecConcurrencyLevel = 0;

  // BFT consensus batching policy for requests
  uint32_t batchingPolicy = BATCH_SELF_ADJUSTED;

  // Initial value for a number of requests in the primary replica queue to trigger batching
  uint32_t maxInitialBatchSize = 350;

  // Parameter used to heuristically compute the 'optimal' batch size
  uint32_t batchingFactorCoefficient = 4;

  // RSA public keys of all replicas. map from replica identifier to a public key
  std::set<std::pair<uint16_t, const std::string>> publicKeysOfReplicas;

  // RSA private key of the current replica
  std::string replicaPrivateKey;

  // Threshold crypto system
  std::string thresholdSystemType_;
  std::string thresholdSystemSubType_;
  std::string thresholdPrivateKey_;  // bootstrap private key
  std::string thresholdPublicKey_;
  std::vector<std::string> thresholdVerificationKeys_;

  bool debugPersistentStorageEnabled = false;

  // Messages
  uint32_t maxExternalMessageSize = 131072;
  uint32_t maxReplyMessageSize = 8192;

  // StateTransfer
  uint32_t maxNumOfReservedPages = 2048;
  uint32_t sizeOfReservedPage = 4096;

  // If set to true, this replica will periodically log debug statistics such as
  // throughput and number of messages sent.
  bool debugStatisticsEnabled = false;

  // Metrics dump interval
  uint64_t metricsDumpIntervalSeconds = 600;

  bool keyExchangeOnStart = false;
  std::string keyViewFilePath{"."};

  /**
   * create a singleton instance from this object
   * call to this function will have effect only for the first time
   */
  void singletonFromThis();
};

inline std::ostream& operator<<(std::ostream& os, const ReplicaConfig& rc) {
  os << "isReadOnly: " << rc.isReadOnly << "\n"
     << "numReplicas: " << rc.numReplicas << "\n"
     << "numRoReplicas: " << rc.numRoReplicas << "\n"
     << "fVal: " << rc.fVal << "\n"
     << "cVal: " << rc.cVal << "\n"
     << "replicaId: " << rc.replicaId << "\n"
     << "numOfClientProxies: " << rc.numOfClientProxies << "\n"
     << "numOfExternalClients: " << rc.numOfExternalClients << "\n"
     << "statusReportTimerMillisec: " << rc.statusReportTimerMillisec << "\n"
     << "concurrencyLevel: " << rc.concurrencyLevel << "\n"
     << "viewChangeProtocolEnabled: " << rc.viewChangeProtocolEnabled << "\n"
     << "viewChangeTimerMillisec: " << rc.viewChangeTimerMillisec << "\n"
     << "autoPrimaryRotationEnabled: " << rc.autoPrimaryRotationEnabled << "\n"
     << "autoPrimaryRotationTimerMillisec: " << rc.autoPrimaryRotationTimerMillisec << "\n"
     << "preExecutionFeatureEnabled: " << rc.preExecutionFeatureEnabled << "\n"
     << "preExecReqStatusCheckTimerMillisec: " << rc.preExecReqStatusCheckTimerMillisec << "\n"
     << "preExecConcurrencyLevel: " << rc.preExecConcurrencyLevel << "\n"
     << "batchingPolicy: " << rc.batchingPolicy << "\n"
     << "maxInitialBatchSize: " << rc.maxInitialBatchSize << "\n"
     << "batchingFactorCoefficient: " << rc.batchingFactorCoefficient << "\n"
     << "debugPersistentStorageEnabled: " << rc.debugPersistentStorageEnabled << "\n"
     << "maxExternalMessageSize: " << rc.maxExternalMessageSize << "\n"
     << "maxReplyMessageSize: " << rc.maxReplyMessageSize << "\n"
     << "maxNumOfReservedPages: " << rc.maxNumOfReservedPages << "\n"
     << "sizeOfReservedPage: " << rc.sizeOfReservedPage << "\n"
     << "debugStatisticsEnabled: " << rc.debugStatisticsEnabled << "\n"
     << "metricsDumpIntervalSeconds: " << rc.metricsDumpIntervalSeconds << "\n"
     << "keyExchangeOnStart: " << rc.keyExchangeOnStart << "\n"
     << "keyViewFilePath: " << rc.keyViewFilePath << "\n";
  return os;
}

/** System-wide singleton class for accessing replica configuration
 *  Note: ReplicaConfig held by ReplicaConfigSingleton is a COPY of the ReplicaConfig object it was initialized from
 *  This is done to decouple between the life cycles of the two.
 */
class ReplicaConfigSingleton {
 public:
  static ReplicaConfigSingleton& GetInstance() {
    static ReplicaConfigSingleton instance_;
    return instance_;
  }

  uint16_t GetFVal() const { return config_->fVal; }
  uint16_t GetCVal() const { return config_->cVal; }
  uint16_t GetNumReplicas() const { return config_->numReplicas; }
  uint16_t GetReplicaId() const { return config_->replicaId; }
  uint16_t GetNumOfClientProxies() const { return config_->numOfClientProxies; }
  uint16_t GetNumOfExternalClients() const { return config_->numOfExternalClients; }
  uint16_t GetStatusReportTimerMillisec() const { return config_->statusReportTimerMillisec; }
  uint16_t GetConcurrencyLevel() const { return config_->concurrencyLevel; }
  bool GetViewChangeProtocolEnabled() const { return config_->viewChangeProtocolEnabled; }
  uint16_t GetViewChangeTimerMillisec() const { return config_->viewChangeTimerMillisec; }
  bool GetAutoPrimaryRotationEnabled() const { return config_->autoPrimaryRotationEnabled; }

  uint16_t GetAutoPrimaryRotationTimerMillisec() const { return config_->autoPrimaryRotationTimerMillisec; }
  std::set<std::pair<uint16_t, const std::string>> GetPublicKeysOfReplicas() const {
    return config_->publicKeysOfReplicas;
  }
  uint32_t GetBatchingPolicy() const { return config_->batchingPolicy; }
  uint32_t GetMaxInitialBatchSize() const { return config_->maxInitialBatchSize; }
  uint32_t GetBatchingFactorCoefficient() const { return config_->batchingFactorCoefficient; }
  bool GetPreExecutionFeatureEnabled() const { return config_->preExecutionFeatureEnabled; }
  uint64_t GetPreExecReqStatusCheckTimerMillisec() const { return config_->preExecReqStatusCheckTimerMillisec; }
  uint16_t GetPreExecConcurrencyLevel() const { return config_->preExecConcurrencyLevel; }
  std::string GetReplicaPrivateKey() const { return config_->replicaPrivateKey; }

  uint32_t GetMaxExternalMessageSize() const { return config_->maxExternalMessageSize; }
  uint32_t GetMaxReplyMessageSize() const { return config_->maxReplyMessageSize; }
  uint32_t GetMaxNumOfReservedPages() const { return config_->maxNumOfReservedPages; }
  uint32_t GetSizeOfReservedPage() const { return config_->sizeOfReservedPage; }
  uint32_t GetNumOfReplicas() const { return 3 * config_->fVal + 2 * config_->cVal + 1; }
  uint64_t GetMetricsDumpInterval() const { return config_->metricsDumpIntervalSeconds; }

  bool GetDebugStatisticsEnabled() const { return config_->debugStatisticsEnabled; }

  bool GetKeyExchangeOnStart() const { return config_->keyExchangeOnStart; }
  std::string GetKeyViewFilePath() const { return config_->keyViewFilePath; }

 private:
  friend struct ReplicaConfig;
  void init(ReplicaConfig* config) { config_ = new ReplicaConfig(*config); }

  ReplicaConfigSingleton() = default;
  ReplicaConfigSingleton(const ReplicaConfigSingleton&) = delete;
  ReplicaConfigSingleton& operator=(const ReplicaConfigSingleton&) = delete;

  const ReplicaConfig* config_ = nullptr;
};

inline void ReplicaConfig::singletonFromThis() {
  static std::once_flag initialized_;
  std::call_once(initialized_, [this]() { ReplicaConfigSingleton::GetInstance().init(this); });
}

}  // namespace bftEngine
