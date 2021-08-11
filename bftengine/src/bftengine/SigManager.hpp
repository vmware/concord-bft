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
#include "PrimitiveTypes.hpp"
#include "assertUtils.hpp"
#include "Metrics.hpp"
#include "Crypto.hpp"

#include <utility>
#include <vector>
#include <map>
#include <string>
#include <memory>
#include <shared_mutex>

using concordMetrics::AtomicCounterHandle;

namespace bftEngine {
namespace impl {

class ReplicasInfo;

class SigManager {
 public:
  typedef std::string Key;
  typedef uint16_t KeyIndex;

  // It is the caller responsibility to deallocate (delete) the object
  // NOTICE: sm should be != nullptr ONLY for testing purpose. In that case it will be used as a set function.
  static SigManager* instance(SigManager* sm = nullptr) {
    static SigManager* instance_ = nullptr;

    if (sm) {
      instance_ = sm;
    }

    return instance_;
  }

  // It is the caller responsibility to deallocate (delete) the object
  static SigManager* init(ReplicaId myId,
                          const Key& mySigPrivateKey,
                          const std::set<std::pair<PrincipalId, const std::string>>& publicKeysOfReplicas,
                          KeyFormat replicasKeysFormat,
                          const std::set<std::pair<const std::string, std::set<uint16_t>>>* publicKeysOfClients,
                          KeyFormat clientsKeysFormat,
                          ReplicasInfo& replicasInfo);

  // returns 0 if pid is invalid - caller might consider throwing an exception
  uint16_t getSigLength(PrincipalId pid) const;
  // returns false if actual verification failed, or if pid is invalid
  bool verifySig(PrincipalId pid, const char* data, size_t dataLength, const char* sig, uint16_t sigLength) const;
  void sign(const char* data, size_t dataLength, char* outSig, uint16_t outSigLength) const;
  uint16_t getMySigLength() const;
  bool isClientTransactionSigningEnabled() { return clientTransactionSigningEnabled_; }
  void SetAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) {
    metrics_component_.SetAggregator(aggregator);
  }
  void setClientPublicKey(const std::string& key, PrincipalId, KeyFormat);
  bool hasVerifier(PrincipalId pid);
  SigManager(const SigManager&) = delete;
  SigManager& operator=(const SigManager&) = delete;
  SigManager(SigManager&&) = delete;
  SigManager& operator=(SigManager&&) = delete;

  std::string getClientsPublicKeys();

 protected:
  static constexpr uint16_t updateMetricsAggregatorThresh = 1000;

  SigManager(PrincipalId myId,
             uint16_t numReplicas,
             const std::pair<Key, KeyFormat>& mySigPrivateKey,
             const std::vector<std::pair<Key, KeyFormat>>& publickeys,
             const std::map<PrincipalId, KeyIndex>& publicKeysMapping,
             bool clientTransactionSigningEnabled,
             ReplicasInfo& replicasInfo);

  static SigManager* initImpl(ReplicaId myId,
                              const Key& mySigPrivateKey,
                              const std::set<std::pair<PrincipalId, const std::string>>& publicKeysOfReplicas,
                              KeyFormat replicasKeysFormat,
                              const std::set<std::pair<const std::string, std::set<uint16_t>>>* publicKeysOfClients,
                              KeyFormat clientsKeysFormat,
                              ReplicasInfo& replicasInfo);

  const PrincipalId myId_;
  std::unique_ptr<RSASigner> mySigner_;
  std::map<PrincipalId, std::shared_ptr<RSAVerifier>> verifiers_;
  bool clientTransactionSigningEnabled_ = true;
  ReplicasInfo& replicasInfo_;

  struct Metrics {
    AtomicCounterHandle externalClientReqSigVerificationFailed_;
    AtomicCounterHandle externalClientReqSigVerified_;

    AtomicCounterHandle replicaSigVerificationFailed_;
    AtomicCounterHandle replicaSigVerified_;

    AtomicCounterHandle sigVerificationFailedOnUnrecognizedParticipantId_;
  };

  mutable concordMetrics::Component metrics_component_;
  mutable Metrics metrics_;
  mutable std::shared_mutex mutex_;
  // These methods bypass the singelton, and can be used (STRICTLY) for testing.
  // Define the below flag in order to use them in your test.
#ifdef CONCORD_BFT_TESTING
 public:
  // It is the caller responsibility to deallocate (delete) the object
  static SigManager* initInTesting(
      ReplicaId myId,
      const Key& mySigPrivateKey,
      const std::set<std::pair<PrincipalId, const std::string>>& publicKeysOfReplicas,
      KeyFormat replicasKeysFormat,
      const std::set<std::pair<const std::string, std::set<uint16_t>>>* publicKeysOfClients,
      KeyFormat clientsKeysFormat,
      ReplicasInfo& replicasInfo) {
    return initImpl(myId,
                    mySigPrivateKey,
                    publicKeysOfReplicas,
                    replicasKeysFormat,
                    publicKeysOfClients,
                    clientsKeysFormat,
                    replicasInfo);
  }
#endif
};

}  // namespace impl
}  // namespace bftEngine
