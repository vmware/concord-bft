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
#include "assertUtils.hpp"
#include "ReplicasInfo.hpp"

#include <algorithm>
#include "keys_and_signatures.cmf.hpp"
#include "ReplicaConfig.hpp"

using namespace std;

namespace bftEngine {
namespace impl {

concord::messages::keys_and_signatures::ClientsPublicKeys clientsPublicKeys_;

std::string SigManager::getClientsPublicKeys() {
  std::shared_lock lock(mutex_);
  std::vector<uint8_t> output;
  concord::messages::keys_and_signatures::serialize(output, clientsPublicKeys_);
  return std::string(output.begin(), output.end());
}

SigManager* SigManager::initImpl(ReplicaId myId,
                                 const Key& mySigPrivateKey,
                                 const std::set<std::pair<PrincipalId, const std::string>>& publicKeysOfReplicas,
                                 concord::util::crypto::KeyFormat replicasKeysFormat,
                                 const std::set<std::pair<const std::string, std::set<uint16_t>>>* publicKeysOfClients,
                                 concord::util::crypto::KeyFormat clientsKeysFormat,
                                 ReplicasInfo& replicasInfo) {
  vector<pair<Key, concord::util::crypto::KeyFormat>> publickeys;
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
    // Also, we do not enforce to have all range between [lowBound, highBound] construcred. We might want to have less
    // principal ids mapped to keys than what is stated in the range.
    lowBound = numRoReplicas + numReplicas + numOfClientProxies;
    highBound = lowBound + numOfExternalClients + numOfInternalClients + numOfClientServices - 1;
    for (const auto& p : (*publicKeysOfClients)) {
      ConcordAssert(!p.first.empty());
      publickeys.push_back(make_pair(p.first, clientsKeysFormat));
      for (const auto e : p.second) {
        if ((e < lowBound) || (e > highBound)) {
          LOG_FATAL(GL, "Invalid participant id " << KVLOG(e, lowBound, highBound));
          std::terminate();
        }
        publicKeysMapping.insert({e, i});
      }
      ++i;
    }
  }

  LOG_INFO(GL, "Done Compute Start ctor for SigManager with " << KVLOG(publickeys.size(), publicKeysMapping.size()));
  return new SigManager(
      myId,
      numReplicas,
      make_pair(mySigPrivateKey, replicasKeysFormat),
      publickeys,
      publicKeysMapping,
      ((ReplicaConfig::instance().clientTransactionSigningEnabled) && (publicKeysOfClients != nullptr)),
      replicasInfo);
}

SigManager* SigManager::init(ReplicaId myId,
                             const Key& mySigPrivateKey,
                             const std::set<std::pair<PrincipalId, const std::string>>& publicKeysOfReplicas,
                             concord::util::crypto::KeyFormat replicasKeysFormat,
                             const std::set<std::pair<const std::string, std::set<uint16_t>>>* publicKeysOfClients,
                             concord::util::crypto::KeyFormat clientsKeysFormat,
                             ReplicasInfo& replicasInfo) {
  SigManager* sm = initImpl(myId,
                            mySigPrivateKey,
                            publicKeysOfReplicas,
                            replicasKeysFormat,
                            publicKeysOfClients,
                            clientsKeysFormat,
                            replicasInfo);
  return SigManager::instance(sm);
}

SigManager::SigManager(PrincipalId myId,
                       uint16_t numReplicas,
                       const pair<Key, concord::util::crypto::KeyFormat>& mySigPrivateKey,
                       const vector<pair<Key, concord::util::crypto::KeyFormat>>& publickeys,
                       const map<PrincipalId, KeyIndex>& publicKeysMapping,
                       bool clientTransactionSigningEnabled,
                       ReplicasInfo& replicasInfo)
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
  map<KeyIndex, std::shared_ptr<concord::util::crypto::IVerifier>> publicKeyIndexToVerifier;
  size_t numPublickeys = publickeys.size();

  ConcordAssert(publicKeysMapping.size() >= numPublickeys);
  if (!mySigPrivateKey.first.empty())
    mySigner_.reset(new concord::util::crypto::RSASigner(mySigPrivateKey.first.c_str(), mySigPrivateKey.second));
  for (const auto& p : publicKeysMapping) {
    ConcordAssert(verifiers_.count(p.first) == 0);
    ConcordAssert(p.second < numPublickeys);

    auto iter = publicKeyIndexToVerifier.find(p.second);
    const auto& [key, format] = publickeys[p.second];
    if (iter == publicKeyIndexToVerifier.end()) {
      verifiers_[p.first] = std::make_shared<concord::util::crypto::RSAVerifier>(key.c_str(), format);
      publicKeyIndexToVerifier[p.second] = verifiers_[p.first];
    } else {
      verifiers_[p.first] = iter->second;
    }
    if (replicasInfo_.isIdOfExternalClient(p.first)) {
      clientsPublicKeys_.ids_to_keys[p.first] = concord::messages::keys_and_signatures::PublicKey{key, (uint8_t)format};
      LOG_DEBUG(KEY_EX_LOG, "Adding key of client " << p.first << " key size " << key.size());
    }
  }
  clientsPublicKeys_.version = 1;  // version `1` suggests RSAVerifier.
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

  LOG_INFO(GL,
           "Signature Manager initialized with own private key, "
               << verifiers_.size() << " verifiers and " << publicKeyIndexToVerifier.size() << " other principals");
  ConcordAssert(verifiers_.size() >= publickeys.size());
}

uint16_t SigManager::getSigLength(PrincipalId pid) const {
  if (pid == myId_) {
    return (uint16_t)mySigner_->signatureLength();
  } else {
    std::shared_lock lock(mutex_);
    if (auto pos = verifiers_.find(pid); pos != verifiers_.end()) {
      return pos->second->signatureLength();
    } else {
      LOG_ERROR(GL, "Unrecognized pid " << pid);
      return 0;
    }
  }
}

bool SigManager::verifySig(
    PrincipalId pid, const char* data, size_t dataLength, const char* sig, uint16_t sigLength) const {
  bool result = false;
  {
    std::string str_data(data, dataLength);
    std::string str_sig(sig, sigLength);
    std::shared_lock lock(mutex_);
    if (auto pos = verifiers_.find(pid); pos != verifiers_.end()) {
      result = pos->second->verify(str_data, str_sig);
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
  return result;
}

void SigManager::sign(const char* data, size_t dataLength, char* outSig, uint16_t outSigLength) const {
  std::string str_data(data, dataLength);
  std::string sig;
  sig = mySigner_->sign(str_data);
  outSigLength = sig.size();
  std::memcpy(outSig, sig.c_str(), outSigLength);
}

uint16_t SigManager::getMySigLength() const { return (uint16_t)mySigner_->signatureLength(); }

void SigManager::setClientPublicKey(const std::string& key, PrincipalId id, concord::util::crypto::KeyFormat format) {
  LOG_INFO(KEY_EX_LOG, "client: " << id << " key: " << key << " format: " << (uint16_t)format);
  if (replicasInfo_.isIdOfExternalClient(id) || replicasInfo_.isIdOfClientService(id)) {
    try {
      std::unique_lock lock(mutex_);
      verifiers_.insert_or_assign(id, std::make_shared<concord::util::crypto::RSAVerifier>(key.c_str(), format));
    } catch (const std::exception& e) {
      LOG_ERROR(KEY_EX_LOG, "failed to add a key for client: " << id << " reason: " << e.what());
      throw;
    }
    clientsPublicKeys_.ids_to_keys[id] = concord::messages::keys_and_signatures::PublicKey{key, (uint8_t)format};
  } else {
    LOG_WARN(KEY_EX_LOG, "Illegal id for client " << id);
  }
}
bool SigManager::hasVerifier(PrincipalId pid) { return verifiers_.find(pid) != verifiers_.end(); }

}  // namespace impl
}  // namespace bftEngine
