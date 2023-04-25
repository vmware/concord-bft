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

#include "SigManager.hpp"
#include "util/assertUtils.hpp"
#include "ReplicasInfo.hpp"

#include <algorithm>
#include "keys_and_signatures.cmf.hpp"
#include "ReplicaConfig.hpp"
#include "util/hex_tools.hpp"
#include "crypto/factory.hpp"
#include "CryptoManager.hpp"
#include "crypto/threshsign/eddsa/EdDSAMultisigVerifier.h"
#include "crypto/threshsign/eddsa/EdDSAMultisigSigner.h"

using namespace std;

namespace bftEngine {
namespace impl {

using concord::crypto::IVerifier;
using concord::crypto::Factory;
using concord::crypto::KeyFormat;

concord::messages::keys_and_signatures::ClientsPublicKeys clientsPublicKeys_;

std::shared_ptr<SigManager> SigManager::s_sm;

std::string SigManager::getClientsPublicKeys() {
  std::shared_lock lock(mutex_);
  std::vector<uint8_t> output;
  concord::messages::keys_and_signatures::serialize(output, clientsPublicKeys_);
  return std::string(output.begin(), output.end());
}

SigManager* SigManager::instance() {
  ConcordAssertNE(s_sm.get(), nullptr);
  return s_sm.get();
}

std::shared_ptr<SigManager> SigManager::owningInstance() {
  ConcordAssertNE(s_sm.get(), nullptr);
  return s_sm;
}

void SigManager::reset(std::shared_ptr<SigManager> other) { s_sm = other; }

std::shared_ptr<SigManager> SigManager::init(
    ReplicaId myId,
    const Key& mySigPrivateKey,
    const std::set<std::pair<PrincipalId, const std::string>>& publicKeysOfReplicas,
    KeyFormat replicasKeysFormat,
    const std::set<std::pair<const std::string, std::set<uint16_t>>>* publicKeysOfClients,
    KeyFormat clientsKeysFormat,
    const std::optional<std::tuple<PrincipalId, Key, concord::crypto::KeyFormat>>& operatorKey,
    const ReplicasInfo& replicasInfo) {
  vector<pair<Key, KeyFormat>> publickeys;
  map<PrincipalId, SigManager::KeyIndex> publicKeysMapping;
  size_t lowBound, highBound;
  auto numReplicas = replicasInfo.getNumberOfReplicas();
  auto numRoReplicas = replicasInfo.getNumberOfRoReplicas();
  auto numOfClientProxies = replicasInfo.getNumOfClientProxies();
  auto numOfExternalClients = replicasInfo.getNumberOfExternalClients();
  auto numOfInternalClients = replicasInfo.getNumberOfInternalClients();
  auto numOfClientServices = replicasInfo.getNumberOfClientServices();

  LOG_INFO(
      GL,
      "Compute publicKeysMapping and publickeys: " << KVLOG(
          myId, numReplicas, numRoReplicas, numOfClientProxies, numOfExternalClients, publicKeysOfReplicas.size()));

  SigManager::KeyIndex i{0};
  highBound = numReplicas + numRoReplicas - 1;
  for (const auto& repIdToKeyPair : publicKeysOfReplicas) {
    // each replica sign with a unique private key (1 to 1 relation)
    ConcordAssert(repIdToKeyPair.first <= highBound);
    publickeys.push_back(make_pair(repIdToKeyPair.second, replicasKeysFormat));
    publicKeysMapping.insert({repIdToKeyPair.first, i++});
  }

  if (publicKeysOfClients) {
    // Multiple clients might be signing with the same private key (1 to many relation)
    // Also, we do not enforce to have all range between [lowBound, highBound] constructed. We might want to have less
    // principal ids mapped to keys than what is stated in the range.
    lowBound = numRoReplicas + numReplicas + numOfClientProxies;
    highBound = lowBound + numOfExternalClients + numOfInternalClients + numOfClientServices - 1;
    for (const auto& [publicKey, idSet] : (*publicKeysOfClients)) {
      ConcordAssert(!publicKey.empty());
      publickeys.push_back(make_pair(publicKey, clientsKeysFormat));
      for (const auto participantId : idSet) {
        if ((participantId < lowBound) || (participantId > highBound)) {
          LOG_FATAL(GL, "Invalid participant id " << KVLOG(participantId, lowBound, highBound));
          std::terminate();
        }
        publicKeysMapping.insert({participantId, i});
      }
      ++i;
    }
  }

  LOG_INFO(GL, "Done Compute Start ctor for SigManager with " << KVLOG(publickeys.size(), publicKeysMapping.size()));
  auto ret = std::shared_ptr<SigManager>{
      new SigManager(myId,
                     make_pair(mySigPrivateKey, replicasKeysFormat),
                     publickeys,
                     publicKeysMapping,
                     ((ReplicaConfig::instance().clientTransactionSigningEnabled) && (publicKeysOfClients != nullptr)),
                     operatorKey,
                     replicasInfo)};

  reset(ret);
  return ret;
}

SigManager::SigManager(PrincipalId myId,
                       const pair<Key, KeyFormat>& mySigPrivateKey,
                       const vector<pair<Key, KeyFormat>>& publickeys,
                       const map<PrincipalId, KeyIndex>& publicKeysMapping,
                       bool clientTransactionSigningEnabled,
                       const std::optional<std::tuple<PrincipalId, Key, concord::crypto::KeyFormat>>& operatorKey,
                       const ReplicasInfo& replicasInfo)
    : myId_(myId),
      clientTransactionSigningEnabled_(clientTransactionSigningEnabled),
      replicasInfo_(replicasInfo),

      metrics_component_{
          concordMetrics::Component("signature_manager", std::make_shared<concordMetrics::Aggregator>())},

      metrics_{
          metrics_component_.RegisterAtomicCounter("external_client_request_signature_verification_failed"),
          metrics_component_.RegisterAtomicCounter("external_client_request_signatures_verified"),
          metrics_component_.RegisterAtomicCounter("peer_replicas_signature_verification_failed"),
          metrics_component_.RegisterAtomicCounter("peer_replicas_signatures_verified"),
          metrics_component_.RegisterAtomicCounter("signature_verification_failed_on_unrecognized_participant_id")} {
  map<KeyIndex, std::shared_ptr<IVerifier>> publicKeyIndexToVerifier;
  size_t numPublickeys = publickeys.size();

  ConcordAssert(publicKeysMapping.size() >= numPublickeys);
  if (!mySigPrivateKey.first.empty()) {
    mySigner_ = Factory::getSigner(
        mySigPrivateKey.first, ReplicaConfig::instance().replicaMsgSigningAlgo, mySigPrivateKey.second);
  }
  for (const auto& p : publicKeysMapping) {
    ConcordAssert(verifiers_.count(p.first) == 0);
    ConcordAssert(p.second < numPublickeys);

    auto iter = publicKeyIndexToVerifier.find(p.second);
    const auto& [key, format] = publickeys[p.second];
    if (iter == publicKeyIndexToVerifier.end()) {
      verifiers_[p.first] = std::shared_ptr<IVerifier>(
          Factory::getVerifier(key, ReplicaConfig::instance().replicaMsgSigningAlgo, format));
      publicKeyIndexToVerifier[p.second] = verifiers_[p.first];
    } else {
      verifiers_[p.first] = iter->second;
    }
    if (replicasInfo_.isIdOfExternalClient(p.first)) {
      clientsPublicKeys_.ids_to_keys[p.first] = concord::messages::keys_and_signatures::PublicKey{key, (uint8_t)format};
      LOG_DEBUG(KEY_EX_LOG, "Adding key of client " << p.first << " key size " << key.size());
    }
  }
  if (operatorKey.has_value()) {
    auto& [operator_id, operator_pub_key, operator_key_fmt] = operatorKey.value();
    if (operator_id > 0 && !operator_pub_key.empty()) {
      verifiers_[operator_id] = std::shared_ptr<IVerifier>(
          Factory::getVerifier(operator_pub_key, ReplicaConfig::instance().operatorMsgSigningAlgo, operator_key_fmt));
    }
  }

  /* version `1` suggests RSAVerifier.
   * version `2` suggests EdDSAVerifier. */
  clientsPublicKeys_.version = 2;
  LOG_DEBUG(KEY_EX_LOG, "Map contains " << clientsPublicKeys_.ids_to_keys.size() << " public clients keys");
  metrics_component_.Register();

  // This is done mainly for debugging and sanity check:
  // compute a vector which counts how many participants and which are per each key:
  vector<set<PrincipalId>> keyIndexToPrincipalIds(publickeys.size());
  for (auto& principalIdToKeyIndex : publicKeysMapping) {
    ConcordAssert(principalIdToKeyIndex.second < keyIndexToPrincipalIds.size());
    keyIndexToPrincipalIds[principalIdToKeyIndex.second].insert(principalIdToKeyIndex.first);
  }
  size_t i{0};
  for (auto& ids : keyIndexToPrincipalIds) {
    // Knowing how deplyment works, we assume a continuous ids per key. If not, the next log line is not sufficient
    LOG_INFO(GL,
             "Key index " << i << " is used by " << ids.size() << " principal IDs"
                          << " from " << (*std::min_element(ids.begin(), ids.end())) << " to "
                          << (*std::max_element(ids.begin(), ids.end())));
    ++i;
  }

  LOG_INFO(GL, "SigManager initialized: " << KVLOG(myId_, verifiers_.size(), publicKeyIndexToVerifier.size()));
  ConcordAssert(verifiers_.size() >= publickeys.size());
}

uint16_t SigManager::getSigLength(PrincipalId pid) const {
  if (pid == myId_) {
    return (uint16_t)mySigner_->signatureLength();
  } else {
    std::shared_lock lock(mutex_);
    if (auto pos = verifiers_.find(pid); pos != verifiers_.end()) {
      auto result = pos->second->signatureLength();
      return result;
    } else {
      LOG_ERROR(GL, "Unrecognized pid " << pid);
      return 0;
    }
  }
}

bool SigManager::verifySigUsingInternalMap(
    PrincipalId pid, const concord::Byte* data, size_t dataLength, const concord::Byte* sig, uint16_t sigLength) const {
  LOG_DEBUG(GL, "Validating signature using internal map: " << KVLOG(myId_, pid, sigLength));
  ConcordAssert(!replicasInfo_.isIdOfReplica(myId_) || !ReplicaConfig::instance().singleSignatureScheme ||
                !replicasInfo_.isIdOfReplica(pid));
  bool result = false;
  {
    std::shared_lock lock(mutex_);

    if (auto pos = verifiers_.find(pid); pos != verifiers_.end()) {
      result = pos->second->verifyBuffer(data, dataLength, sig, sigLength);
    } else {
      LOG_ERROR(GL, "Unrecognized pid " << pid);
      metrics_.sigVerificationFailedOnUnrecognizedParticipantId_++;
      metrics_component_.UpdateAggregator();
      return false;
    }
  }
  bool idOfReplica = false, idOfExternalClient = false, idOfReadOnlyReplica = false;
  idOfExternalClient = replicasInfo_.isIdOfExternalClient(pid);
  if (!idOfExternalClient) {
    idOfReplica = replicasInfo_.isIdOfReplica(pid);
  }
  idOfReadOnlyReplica = replicasInfo_.isIdOfPeerRoReplica(pid);
  ConcordAssert(idOfReplica || idOfExternalClient || idOfReadOnlyReplica);
  if (!result) {  // failure
    if (idOfExternalClient)
      metrics_.externalClientReqSigVerificationFailed_++;
    else
      metrics_.replicaSigVerificationFailed_++;
    metrics_component_.UpdateAggregator();
  } else {  // success
    if (idOfExternalClient) {
      metrics_.externalClientReqSigVerified_++;
      if ((metrics_.externalClientReqSigVerified_.Get().Get() % updateMetricsAggregatorThresh) == 0)
        metrics_component_.UpdateAggregator();
    } else {
      metrics_.replicaSigVerified_++;
      if ((metrics_.replicaSigVerified_.Get().Get() % updateMetricsAggregatorThresh) == 0)
        metrics_component_.UpdateAggregator();
    }
  }
  LOG_DEBUG(GL,
            "Internal map validation result: " << KVLOG(
                result, myId_, pid, sigLength, idOfReadOnlyReplica, idOfExternalClient));
  return result;
}

size_t SigManager::sign(SeqNum seq, const concord::Byte* data, size_t dataLength, concord::Byte* outSig) const {
  ConcordAssert(replicasInfo_.isIdOfReplica(myId_) || replicasInfo_.isRoReplica());
  const concord::crypto::ISigner* rawSigner = nullptr;
  std::shared_ptr<IThresholdSigner> signer;
  if (ReplicaConfig::instance().singleSignatureScheme) {
    signer = CryptoManager::instance().getSigner(seq);
    rawSigner = &extractSignerFromMultisig(signer);
  } else {
    rawSigner = mySigner_.get();
  }
  auto result = rawSigner->signBuffer(data, dataLength, outSig);
  LOG_DEBUG(GL, "Signing as replica with " << KVLOG(myId_, seq, rawSigner->signatureLength(), result));
  return result;
}

size_t SigManager::sign(SeqNum seq, const char* data, size_t dataLength, char* outSig) const {
  return sign(seq, reinterpret_cast<const uint8_t*>(data), dataLength, reinterpret_cast<uint8_t*>(outSig));
}

bool SigManager::verifyReplicaSigUsingMultisigVerifier(PrincipalId replicaID,
                                                       const concord::Byte* data,
                                                       size_t dataLength,
                                                       const concord::Byte* sig,
                                                       uint16_t sigLength) const {
  ConcordAssert(ReplicaConfig::instance().singleSignatureScheme);
  ConcordAssert(replicasInfo_.isIdOfReplica(myId_) || replicasInfo_.isRoReplica());
  ConcordAssert(replicasInfo_.isIdOfReplica(replicaID));
  int lastVerifierIndex = 0;
  for (auto [seq, multisigVerifier] : CryptoManager::instance().getLatestVerifiers()) {
    UNUSED(seq);
    if (multisigVerifier.get() == nullptr) {
      continue;
    }
    auto& verifier = extractVerifierFromMultisig(multisigVerifier, replicaID);
    LOG_DEBUG(GL,
              "Validating replica signature with: " << KVLOG(
                  myId_, replicaID, lastVerifierIndex, verifier.getPubKey(), sigLength, verifier.signatureLength()));
    if (verifier.verifyBuffer(data, dataLength, sig, sigLength)) {
      LOG_DEBUG(GL,
                "Replica signature validation Successful "
                    << KVLOG(myId_, replicaID, lastVerifierIndex, sigLength, verifier.signatureLength()));
      return true;
    } else {
      LOG_DEBUG(GL,
                "Replica signature validation failed: " << KVLOG(
                    myId_, replicaID, lastVerifierIndex, sigLength, verifier.signatureLength()));
    }
    lastVerifierIndex++;
  }
  LOG_WARN(GL, "Validation failed using all cryptosystems" << KVLOG(myId_, replicaID, lastVerifierIndex));
  return false;
}

// verify using the two last keys, once the last key's checkpoint is reached, the previous key is removed
bool SigManager::verifySig(
    PrincipalId pid, const concord::Byte* data, size_t dataLength, const concord::Byte* sig, uint16_t sigLength) const {
  if (ReplicaConfig::instance().singleSignatureScheme && replicasInfo_.isIdOfReplica(pid)) {
    return verifyReplicaSigUsingMultisigVerifier(pid, data, dataLength, sig, sigLength);
  }

  return verifySigUsingInternalMap(pid, data, dataLength, sig, sigLength);
}

bool SigManager::verifyOwnSignature(const concord::Byte* data,
                                    size_t dataLength,
                                    const concord::Byte* expectedSignature) const {
  std::vector<concord::Byte> sig(getMySigLength());
  if (ReplicaConfig::instance().singleSignatureScheme) {
    auto signers = CryptoManager::instance().getLatestSigners();
    for (auto& signer : signers) {
      extractSignerFromMultisig(signer).signBuffer(data, dataLength, sig.data());

      if (std::memcmp(sig.data(), expectedSignature, getMySigLength()) == 0) {
        LOG_DEBUG(GL, "Self-sig validation succeeded");
        return true;
      } else {
        LOG_DEBUG(GL, "Self-sig validation failed");
      }
    }
    return false;
  }

  mySigner_->signBuffer(data, dataLength, sig.data());
  return std::memcmp(sig.data(), expectedSignature, getMySigLength()) == 0;
}

const concord::crypto::ISigner& SigManager::extractSignerFromMultisig(
    std::shared_ptr<IThresholdSigner> thresholdSigner) const {
  ConcordAssert(ReplicaConfig::instance().singleSignatureScheme);
  ConcordAssertNE(thresholdSigner.get(), nullptr);
  return reinterpret_cast<EdDSAMultisigSigner&>(*thresholdSigner.get());
}

const concord::crypto::IVerifier& SigManager::extractVerifierFromMultisig(
    std::shared_ptr<IThresholdVerifier> thresholdVerifier, PrincipalId id) const {
  ConcordAssert(ReplicaConfig::instance().singleSignatureScheme);
  ConcordAssertNE(thresholdVerifier.get(), nullptr);
  return reinterpret_cast<EdDSAMultisigVerifier&>(*thresholdVerifier.get()).getVerifier(id);
}

uint16_t SigManager::getMySigLength() const {
  if (ReplicaConfig::instance().singleSignatureScheme &&
      (replicasInfo_.isIdOfReplica(myId_) || replicasInfo_.isRoReplica())) {
    auto currentSigner = getCurrentReplicaSigner();
    return extractSignerFromMultisig(currentSigner).signatureLength();
  }
  return (uint16_t)mySigner_->signatureLength();
}

void SigManager::setClientPublicKey(const std::string& key, PrincipalId id, KeyFormat format) {
  LOG_INFO(KEY_EX_LOG, "client: " << id << " key: " << key << " format: " << (uint16_t)format);
  if (replicasInfo_.isIdOfExternalClient(id) || replicasInfo_.isIdOfClientService(id)) {
    try {
      std::unique_lock lock(mutex_);
      verifiers_.insert_or_assign(id,
                                  std::shared_ptr<IVerifier>(Factory::getVerifier(
                                      key, ReplicaConfig::instance().replicaMsgSigningAlgo, format)));
    } catch (const std::exception& e) {
      LOG_ERROR(KEY_EX_LOG, "failed to add a key for client: " << id << " reason: " << e.what());
      throw;
    }
    clientsPublicKeys_.ids_to_keys[id] = concord::messages::keys_and_signatures::PublicKey{key, (uint8_t)format};
  } else {
    LOG_WARN(KEY_EX_LOG, "Illegal id for client " << id);
  }
}
bool SigManager::hasVerifier(PrincipalId pid) {
  if (ReplicaConfig::instance().singleSignatureScheme && replicasInfo_.isIdOfReplica(pid)) {
    return true;
  }
  return verifiers_.find(pid) != verifiers_.end();
}

concord::crypto::SignatureAlgorithm SigManager::getMainKeyAlgorithm() const { return concord::crypto::EdDSA; }

std::shared_ptr<IThresholdSigner> SigManager::getCurrentReplicaSigner() const {
  ConcordAssert(ReplicaConfig::instance().singleSignatureScheme);
  ConcordAssert(replicasInfo_.isIdOfReplica(myId_) || replicasInfo_.isRoReplica());
  auto signer = CryptoManager::instance().getSigner(getReplicaLastExecutedSeq());
  return signer;
}

std::pair<std::string, std::string> SigManager::getMyLatestKeyPair() const {
  ConcordAssert(replicasInfo_.isIdOfReplica(myId_));

  if (!ReplicaConfig::instance().singleSignatureScheme) {
    return {mySigner_->getPrivKey(), getVerifier(myId_).getPubKey()};
  }

  auto& system = CryptoManager::instance().getLatestCryptoSystem();
  std::pair<std::string, std::string> ret = {system->getPrivateKey(replicasInfo_.myId()),
                                             system->getMyVerificationKey()};
  return ret;
}

std::string SigManager::getSelfPrivKey() const {
  if (ReplicaConfig::instance().singleSignatureScheme) {
    auto currentSigner = getCurrentReplicaSigner();
    return extractSignerFromMultisig(currentSigner).getPrivKey();
  }
  return mySigner_->getPrivKey();
}

std::array<std::string, replicaIdentityHistoryCount> SigManager::getPublicKeyOfVerifier(uint32_t id) const {
  std::array<std::string, replicaIdentityHistoryCount> publicKeys;

  if (ReplicaConfig::instance().singleSignatureScheme && replicasInfo_.isIdOfReplica(id)) {
    int i = 0;
    for (auto [seq, multisigVerifier] : CryptoManager::instance().getLatestVerifiers()) {
      UNUSED(seq);
      if (multisigVerifier.get() == nullptr) {
        continue;
      }
      auto& verifier = extractVerifierFromMultisig(multisigVerifier, id);
      publicKeys[i] = verifier.getPubKey();
      ++i;
    }
  } else if (verifiers_.count(id)) {
    publicKeys[0] = verifiers_.at(id)->getPubKey();
  }

  return publicKeys;
}

const concord::crypto::IVerifier& SigManager::getVerifier(PrincipalId otherPrincipal) const {
  return *verifiers_.at(otherPrincipal);
}

void SigManager::setReplicaLastExecutedSeq(SeqNum seq) {
  ConcordAssert(replicasInfo_.isIdOfReplica(myId_));
  replicaLastExecutedSeq_ = seq;
}

SeqNum SigManager::getReplicaLastExecutedSeq() const {
  ConcordAssert(replicasInfo_.isIdOfReplica(myId_) || replicasInfo_.isRoReplica());
  return replicaLastExecutedSeq_;
}
const ReplicasInfo& SigManager::getReplicasInfo() const { return replicasInfo_; }

}  // namespace impl
}  // namespace bftEngine
