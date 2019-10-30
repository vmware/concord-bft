// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "ReplicaConfigSingleton.hpp"

#include "ReplicaConfig.hpp"

class IThresholdSigner;
class IThresholdVerifier;

namespace bftEngine {

ReplicaConfigSingleton::ReplicaConfigSingleton() : is_initialized_(false) {}

ReplicaConfigSingleton& ReplicaConfigSingleton::GetInstance(const ReplicaConfig* config) {
  static ReplicaConfigSingleton instance;
  if (!instance.is_initialized_ && config != nullptr) {
    instance.config_ = *config;
    instance.is_initialized_ = true;
  }
  return instance;
}

uint16_t ReplicaConfigSingleton::GetFVal() const { return config_.fVal; }

uint16_t ReplicaConfigSingleton::GetCVal() const { return config_.cVal; }

uint16_t ReplicaConfigSingleton::GetReplicaId() const { return config_.replicaId; }

uint16_t ReplicaConfigSingleton::GetNumOfClientProxies() const { return config_.numOfClientProxies; }

uint16_t ReplicaConfigSingleton::GetStatusReportTimerMillisec() const { return config_.statusReportTimerMillisec; }

uint16_t ReplicaConfigSingleton::GetConcurrencyLevel() const { return config_.concurrencyLevel; }

bool ReplicaConfigSingleton::GetAutoViewChangeEnabled() const { return config_.autoViewChangeEnabled; }

uint16_t ReplicaConfigSingleton::GetViewChangeTimerMillisec() const { return config_.viewChangeTimerMillisec; }

std::set<std::pair<uint16_t, std::string>> ReplicaConfigSingleton::GetPublicKeysOfReplicas() const {
  return config_.publicKeysOfReplicas;
}

std::string ReplicaConfigSingleton::GetReplicaPrivateKey() const { return config_.replicaPrivateKey; }

IThresholdSigner const* ReplicaConfigSingleton::GetThresholdSignerForExecution() const {
  return config_.thresholdSignerForExecution;
}

IThresholdVerifier const* ReplicaConfigSingleton::GetThresholdVerifierForExecution() const {
  return config_.thresholdVerifierForExecution;
}

IThresholdSigner const* ReplicaConfigSingleton::GetThresholdSignerForSlowPathCommit() const {
  return config_.thresholdSignerForSlowPathCommit;
}

IThresholdVerifier const* ReplicaConfigSingleton::GetThresholdVerifierForSlowPathCommit() const {
  return config_.thresholdVerifierForSlowPathCommit;
}

IThresholdSigner const* ReplicaConfigSingleton::GetThresholdSignerForCommit() const {
  return config_.thresholdSignerForCommit;
}

IThresholdVerifier const* ReplicaConfigSingleton::GetThresholdVerifierForCommit() const {
  return config_.thresholdVerifierForCommit;
}

IThresholdSigner const* ReplicaConfigSingleton::GetThresholdSignerForOptimisticCommit() const {
  return config_.thresholdSignerForOptimisticCommit;
}

IThresholdVerifier const* ReplicaConfigSingleton::GetThresholdVerifierForOptimisticCommit() const {
  return config_.thresholdVerifierForOptimisticCommit;
}

uint32_t ReplicaConfigSingleton::GetMaxExternalMessageSize() const { return config_.maxExternalMessageSize; }

uint32_t ReplicaConfigSingleton::GetMaxReplyMessageSize() const { return config_.maxReplyMessageSize; }

uint32_t ReplicaConfigSingleton::GetMaxNumOfReservedPages() const { return config_.maxNumOfReservedPages; }

uint32_t ReplicaConfigSingleton::GetSizeOfReservedPage() const { return config_.sizeOfReservedPage; }

}  // namespace bftEngine
