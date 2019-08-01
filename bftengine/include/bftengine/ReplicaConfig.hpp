//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.

#pragma once

#include <stdint.h>
#include <set>
#include <string>

class IThresholdSigner;
class IThresholdVerifier;

namespace bftEngine {
struct ReplicaConfig {
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

  // a time interval in milliseconds. represents how often the replica sends a status report to the other replicas.
  // statusReportTimerMillisec > 0
  uint16_t statusReportTimerMillisec = 0;

  // number of consensus operations that can be executed in parallel
  // 1 <= concurrencyLevel <= 30
  uint16_t concurrencyLevel = 0;

  // autoViewChangeEnabled=true , if the automatic view change protocol is enabled
  bool autoViewChangeEnabled = false;

  // a time interval in milliseconds. represents the timeout used by the  view change protocol (TODO: add more details)
  uint16_t viewChangeTimerMillisec = 0;

  // public keys of all replicas. map from replica identifier to a public key
  std::set<std::pair<uint16_t, std::string>> publicKeysOfReplicas;

  // private key of the current replica
  std::string replicaPrivateKey;

  // signer and verifier of a threshold signature (for threshold fVal+1 out of N)
  // In the current version, both should be nullptr
  IThresholdSigner *thresholdSignerForExecution = nullptr;
  IThresholdVerifier *thresholdVerifierForExecution = nullptr;

  // signer and verifier of a threshold signature (for threshold N-fVal-cVal out of N)
  IThresholdSigner *thresholdSignerForSlowPathCommit = nullptr;
  IThresholdVerifier *thresholdVerifierForSlowPathCommit = nullptr;

  // signer and verifier of a threshold signature (for threshold N-cVal out of N)
  // If cVal==0, then both should be nullptr
  IThresholdSigner *thresholdSignerForCommit = nullptr;
  IThresholdVerifier *thresholdVerifierForCommit = nullptr;

  // signer and verifier of a threshold signature (for threshold N out of N)
  IThresholdSigner *thresholdSignerForOptimisticCommit = nullptr;
  IThresholdVerifier *thresholdVerifierForOptimisticCommit = nullptr;

  bool usePedanticPersistencyChecks = false;

  // Messages
  uint32_t maxExternalMessageSize = 1 << 16;
  uint32_t maxReplyMessageSize = 1 << 13;

  // StateTransfer
  uint32_t maxNumOfReservedPages = 1 << 11;
  uint32_t sizeOfReservedPage = 1 << 12;
};

}
