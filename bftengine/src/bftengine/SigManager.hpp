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
#include "util/assertUtils.hpp"
#include "util/Metrics.hpp"
#include "crypto/crypto.hpp"
#include "crypto/signer.hpp"
#include "crypto/verifier.hpp"

#include <utility>
#include <vector>
#include <map>
#include <string>
#include <memory>
#include <shared_mutex>
#include <optional>

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
                          concord::crypto::KeyFormat replicasKeysFormat,
                          const std::set<std::pair<const std::string, std::set<uint16_t>>>* publicKeysOfClients,
                          concord::crypto::KeyFormat clientsKeysFormat,
                          const std::optional<std::tuple<PrincipalId, Key, concord::crypto::KeyFormat>>& operatorKey,
                          ReplicasInfo& replicasInfo);

  // It is the caller responsibility to deallocate (delete) the object
  static SigManager* init(ReplicaId myId,
                          const Key& mySigPrivateKey,
                          const std::set<std::pair<PrincipalId, const std::string>>& publicKeysOfReplicas,
                          concord::crypto::KeyFormat replicasKeysFormat,
                          const std::set<std::pair<const std::string, std::set<uint16_t>>>* publicKeysOfClients,
                          concord::crypto::KeyFormat clientsKeysFormat,
                          ReplicasInfo& replicasInfo);

  // returns 0 if pid is invalid - caller might consider throwing an exception
  uint16_t getSigLength(PrincipalId pid) const;
  // returns false if actual verification failed, or if pid is invalid
  bool verifySig(PrincipalId pid,
                 const concord::Byte* data,
                 size_t dataLength,
                 const concord::Byte* sig,
                 uint16_t sigLength) const;

  template <typename DataContainer, typename SignatureContainer>
  bool verifySig(PrincipalId pid, const DataContainer& data, const SignatureContainer& sig) const {
    static_assert(sizeof(typename DataContainer::value_type) == sizeof(concord::Byte),
                  "data elements are not byte-sized");
    static_assert(sizeof(typename SignatureContainer::value_type) == sizeof(concord::Byte),
                  "signature elements are not byte-sized");
    return verifySig(pid,
                     reinterpret_cast<const concord::Byte*>(data.data()),
                     data.size(),
                     reinterpret_cast<const concord::Byte*>(sig.data()),
                     static_cast<uint16_t>(sig.size()));
  }

  size_t sign(const concord::Byte* data, size_t dataLength, concord::Byte* outSig) const;
  size_t sign(const char* data, size_t dataLength, char* outSig) const;
  uint16_t getMySigLength() const;
  bool isClientTransactionSigningEnabled() { return clientTransactionSigningEnabled_; }
  void SetAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) {
    metrics_component_.SetAggregator(aggregator);
  }
  void setClientPublicKey(const std::string& key, PrincipalId, concord::crypto::KeyFormat);
  bool hasVerifier(PrincipalId pid);
  SigManager(const SigManager&) = delete;
  SigManager& operator=(const SigManager&) = delete;
  SigManager(SigManager&&) = delete;
  SigManager& operator=(SigManager&&) = delete;

  concord::crypto::SignatureAlgorithm getMainKeyAlgorithm() const;
  concord::crypto::ISigner& getSigner();
  const concord::crypto::IVerifier& getVerifier(PrincipalId otherPrincipal) const;

  std::string getClientsPublicKeys();
  std::string getPublicKeyOfVerifier(uint32_t id) const {
    if (!verifiers_.count(id)) return std::string();
    return verifiers_.at(id)->getPubKey();
  }
  std::string getSelfPrivKey() const { return mySigner_->getPrivKey(); }

 protected:
  static constexpr uint16_t updateMetricsAggregatorThresh = 1000;

  SigManager(PrincipalId myId,
             uint16_t numReplicas,
             const std::pair<Key, concord::crypto::KeyFormat>& mySigPrivateKey,
             const std::vector<std::pair<Key, concord::crypto::KeyFormat>>& publickeys,
             const std::map<PrincipalId, KeyIndex>& publicKeysMapping,
             bool clientTransactionSigningEnabled,
             const std::optional<std::tuple<PrincipalId, Key, concord::crypto::KeyFormat>>& operatorKey,
             ReplicasInfo& replicasInfo);

  static SigManager* initImpl(
      ReplicaId myId,
      const Key& mySigPrivateKey,
      const std::set<std::pair<PrincipalId, const std::string>>& publicKeysOfReplicas,
      concord::crypto::KeyFormat replicasKeysFormat,
      const std::set<std::pair<const std::string, std::set<uint16_t>>>* publicKeysOfClients,
      concord::crypto::KeyFormat clientsKeysFormat,
      const std::optional<std::tuple<PrincipalId, Key, concord::crypto::KeyFormat>>& operatorKey,
      ReplicasInfo& replicasInfo);

  const PrincipalId myId_;
  std::unique_ptr<concord::crypto::ISigner> mySigner_;
  std::map<PrincipalId, std::shared_ptr<concord::crypto::IVerifier>> verifiers_;
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
      concord::crypto::KeyFormat replicasKeysFormat,
      const std::set<std::pair<const std::string, std::set<uint16_t>>>* publicKeysOfClients,
      concord::crypto::KeyFormat clientsKeysFormat,
      ReplicasInfo& replicasInfo) {
    return initImpl(myId,
                    mySigPrivateKey,
                    publicKeysOfReplicas,
                    replicasKeysFormat,
                    publicKeysOfClients,
                    clientsKeysFormat,
                    std::nullopt,
                    replicasInfo);
  }
#endif
};

}  // namespace impl
}  // namespace bftEngine
