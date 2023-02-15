
// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
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
#include "util/memory.hpp"
#include "SysConsts.hpp"
#include <utility>
#include <vector>
#include <map>
#include <string>
#include <memory>
#include <shared_mutex>
#include <optional>

using concordMetrics::AtomicCounterHandle;

class EdDSAMultisigSigner;
class EdDSAMultisigVerifier;

namespace bftEngine {
namespace impl {

class ReplicasInfo;

class SigManager {
 public:
  using Key = std::string;
  using KeyIndex = uint16_t;

  virtual ~SigManager() = default;
  static SigManager* instance();
  static void reset(std::shared_ptr<SigManager> other);

  // It is the caller responsibility to deallocate (delete) the object
  // This method is assumed to be called by a single thread
  static std::shared_ptr<SigManager> init(
      PrincipalId myId,
      const Key& mySigPrivateKey,
      const std::set<std::pair<PrincipalId, const std::string>>& publicKeysOfReplicas,
      concord::crypto::KeyFormat replicasKeysFormat,
      const std::set<std::pair<const std::string, std::set<uint16_t>>>* publicKeysOfClients,
      concord::crypto::KeyFormat clientsKeysFormat,
      const std::optional<std::tuple<PrincipalId, Key, concord::crypto::KeyFormat>>& operatorKey,
      const ReplicasInfo& replicasInfo);

  // returns 0 if pid is invalid - caller might consider throwing an exception
  uint16_t getSigLength(PrincipalId pid) const;

  // returns false if actual verification failed, or if pid is invalid
  virtual bool verifySig(PrincipalId replicaID,
                         const concord::Byte* data,
                         size_t dataLength,
                         const concord::Byte* sig,
                         uint16_t sigLength) const;

  template <typename DataContainer, typename SignatureContainer>
  bool verifySig(PrincipalId replicaID, const DataContainer& data, const SignatureContainer& sig) const {
    static_assert(sizeof(typename DataContainer::value_type) == sizeof(concord::Byte),
                  "data elements are not byte-sized");
    static_assert(sizeof(typename SignatureContainer::value_type) == sizeof(concord::Byte),
                  "signature elements are not byte-sized");
    return verifySig(replicaID,
                     reinterpret_cast<const concord::Byte*>(data.data()),
                     data.size(),
                     reinterpret_cast<const concord::Byte*>(sig.data()),
                     static_cast<uint16_t>(sig.size()));
  }

  // A replica may change (key rotation) its private key starting from a certain sequence number
  // Only replicas sign using SigManager
  size_t sign(SeqNum seq, const concord::Byte* data, size_t dataLength, concord::Byte* outSig) const;
  size_t sign(SeqNum seq, const char* data, size_t dataLength, char* outSig) const;

  bool verifyReplicaSig(PrincipalId replicaID,
                        const concord::Byte* data,
                        size_t dataLength,
                        const concord::Byte* sig,
                        uint16_t sigLength) const;

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
  std::shared_ptr<EdDSAMultisigSigner> getCurrentReplicaSigner() const;
  std::shared_ptr<EdDSAMultisigSigner> getLastReplicaSigner() const;
  const concord::crypto::IVerifier& getVerifier(PrincipalId otherPrincipal) const;

  std::string getClientsPublicKeys();

  // Hex format
  std::pair<SeqNum, std::string> getMyLatestPublicKey() const;

  // Used by AsyncTLSConnection to verify tls certificates which replicas sign using their main key
  // Up to replicaIdentityHistoryCount keys are returned by replicas
  std::array<std::string, replicaIdentityHistoryCount> getPublicKeyOfVerifier(uint32_t id) const;

  // Used only by replicas
  std::string getSelfPrivKey() const;

  void setReplicaLastExecutedSeq(SeqNum seq);
  SeqNum getReplicaLastExecutedSeq() const;

  bool verifyOwnSignature(const concord::Byte* data, size_t dataLength, const concord::Byte* expectedSignature) const;

 protected:
  static constexpr uint16_t updateMetricsAggregatorThresh = 1000;

  SigManager(PrincipalId myId,
             const std::pair<Key, concord::crypto::KeyFormat>& mySigPrivateKey,
             const std::vector<std::pair<Key, concord::crypto::KeyFormat>>& publickeys,
             const std::map<PrincipalId, KeyIndex>& publicKeysMapping,
             bool clientTransactionSigningEnabled,
             const std::optional<std::tuple<PrincipalId, Key, concord::crypto::KeyFormat>>& operatorKey,
             const ReplicasInfo& replicasInfo);

  bool verifyNonReplicaSig(PrincipalId pid,
                           const concord::Byte* data,
                           size_t dataLength,
                           const concord::Byte* sig,
                           uint16_t sigLength) const;

  const PrincipalId myId_;
  SeqNum replicaLastExecutedSeq_{0};
  std::unique_ptr<concord::crypto::ISigner> mySigner_;
  std::map<PrincipalId, std::shared_ptr<concord::crypto::IVerifier>> verifiers_;
  bool clientTransactionSigningEnabled_ = true;
  const ReplicasInfo& replicasInfo_;

  // The ownership model of a SigManager object depends on its use
  static std::shared_ptr<SigManager> s_sm;

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
};

}  // namespace impl
}  // namespace bftEngine
