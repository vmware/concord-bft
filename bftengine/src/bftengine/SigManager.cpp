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
#include "Crypto.hpp"
#include "assertUtils.hpp"
#include "ReplicasInfo.hpp"

#include <vector>
#include <algorithm>

using namespace std;

namespace bftEngine {
namespace impl {

SigManager* SigManager::initImpl(ReplicaId myId,
                                 const Key& mySigPrivateKey,
                                 const std::set<std::pair<PrincipalId, const std::string>>& publicKeysOfReplicas,
                                 KeyFormat replicasKeysFormat,
                                 const std::set<std::pair<const std::string, std::set<uint16_t>>>* publicKeysOfClients,
                                 KeyFormat clientsKeysFormat,
                                 ReplicasInfo& replicasInfo) {
  vector<pair<Key, KeyFormat>> publickeys;
  map<PrincipalId, SigManager::KeyIndex> publicKeysMapping;
  size_t lowBound, highBound;
  auto numReplicas = replicasInfo.getNumberOfReplicas();
  auto numRoReplicas = replicasInfo.getNumberOfRoReplicas();
  auto numOfClientProxies = replicasInfo.getNumOfClientProxies();
  auto numOfExternalClients = replicasInfo.getNumberOfExternalClients();

  LOG_INFO(
      GL,
      "Compute publicKeysMapping and publickeys: " << KVLOG(
          myId, numReplicas, numRoReplicas, numOfClientProxies, numOfExternalClients, publicKeysOfReplicas.size()));

  SigManager::KeyIndex i{0};
  highBound = numReplicas + numRoReplicas - 1;
  for (const auto& repIdToKeyPair : publicKeysOfReplicas) {
    // each replica sign with a unique private key (1 to 1 relation)
    ConcordAssert(repIdToKeyPair.first <= highBound);
    if (myId == repIdToKeyPair.first)
      // don't insert my own public key
      continue;
    publickeys.push_back(make_pair(repIdToKeyPair.second, replicasKeysFormat));
    publicKeysMapping.insert({repIdToKeyPair.first, i++});
  }

  if (publicKeysOfClients) {
    // Multiple clients might be signing with the same private key (1 to many relation)
    // Also, we do not enforce to have all range between [lowBound, highBound] construcred. We might want to have less
    // principal ids mapped to keys than what is stated in the range.
    lowBound = numRoReplicas + numReplicas + numOfClientProxies;
    highBound = lowBound + numOfExternalClients - 1;
    for (const auto& p : (*publicKeysOfClients)) {
      ConcordAssert(!p.first.empty());
      publickeys.push_back(make_pair(p.first, clientsKeysFormat));
      for (const auto e : p.second) {
        ConcordAssert((e >= lowBound) && (e <= highBound));
        publicKeysMapping.insert({e, i});
      }
      ++i;
    }
  }

  LOG_INFO(GL, "Done Compute Start ctor for SigManager with " << KVLOG(publickeys.size(), publicKeysMapping.size()));
  return new SigManager(myId,
                        numReplicas,
                        make_pair(mySigPrivateKey, replicasKeysFormat),
                        publickeys,
                        publicKeysMapping,
                        (publicKeysOfClients != nullptr),
                        replicasInfo);
}

SigManager* SigManager::init(ReplicaId myId,
                             const Key& mySigPrivateKey,
                             const std::set<std::pair<PrincipalId, const std::string>>& publicKeysOfReplicas,
                             KeyFormat replicasKeysFormat,
                             const std::set<std::pair<const std::string, std::set<uint16_t>>>* publicKeysOfClients,
                             KeyFormat clientsKeysFormat,
                             ReplicasInfo& replicasInfo) {
  SigManager* sm = initImpl(myId,
                            mySigPrivateKey,
                            publicKeysOfReplicas,
                            replicasKeysFormat,
                            publicKeysOfClients,
                            clientsKeysFormat,
                            replicasInfo);
  return SigManager::getInstance(sm);
}

SigManager::SigManager(PrincipalId myId,
                       uint16_t numReplicas,
                       const pair<Key, KeyFormat>& mySigPrivateKey,
                       const vector<pair<Key, KeyFormat>>& publickeys,
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
          metrics_component_.RegisterAtomicCounter("signature_verification_failed_on_unrecognized_participant_id")},
      updateAggregatorCounter(0) {
  map<KeyIndex, RSAVerifier*> publicKeyIndexToVerifier;
  size_t numPublickeys = publickeys.size();

  ConcordAssert(publicKeysMapping.size() >= numPublickeys);
  mySigner_ = new RSASigner(mySigPrivateKey.first.c_str(), mySigPrivateKey.second);
  for (const auto& p : publicKeysMapping) {
    ConcordAssert(verifiers_.count(p.first) == 0);
    ConcordAssert(p.second < numPublickeys);
    ConcordAssert(p.first != myId_);

    auto iter = publicKeyIndexToVerifier.find(p.second);
    if (iter == publicKeyIndexToVerifier.end()) {
      const auto& keyPair = publickeys[p.second];
      verifiers_[p.first] = new RSAVerifier(keyPair.first.c_str(), keyPair.second);
      publicKeyIndexToVerifier[p.second] = verifiers_[p.first];
    } else
      verifiers_[p.first] = iter->second;
  }

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

SigManager::~SigManager() {
  delete mySigner_;
  set<RSAVerifier*> alreadyDeleted;
  for (pair<PrincipalId, RSAVerifier*> v : verifiers_) {
    if (alreadyDeleted.find(v.second) == alreadyDeleted.end()) {
      delete v.second;
      alreadyDeleted.insert(v.second);
    }
  }
}

uint16_t SigManager::getSigLength(PrincipalId pid) const {
  if (pid == myId_) {
    return (uint16_t)mySigner_->signatureLength();
  } else {
    auto pos = verifiers_.find(pid);
    if (pos == verifiers_.end()) {
      LOG_ERROR(GL, "Unrecognized pid " << pid);
      return 0;
    }

    RSAVerifier* verifier = pos->second;
    return (uint16_t)verifier->signatureLength();
  }
}

bool SigManager::verifySig(
    PrincipalId pid, const char* data, size_t dataLength, const char* sig, uint16_t sigLength) const {
  auto pos = verifiers_.find(pid);
  bool idOfPeerReplica = false, idOfExternalClient = false, result;

  if (pos == verifiers_.end()) {
    LOG_ERROR(GL, "Unrecognized pid " << pid);
    metrics_.sigVerificationFailedOnUnrecognizedParticipantId_.Get().Inc();
    metrics_component_.UpdateAggregator();
    updateAggregatorCounter = 0;
    return false;
  }

  RSAVerifier* verifier = pos->second;
  result = verifier->verify(data, dataLength, sig, sigLength);
  idOfExternalClient = replicasInfo_.isIdOfExternalClient(pid);
  if (!idOfExternalClient) {
    idOfPeerReplica = replicasInfo_.isIdOfPeerReplica(pid);
  }
  ConcordAssert(idOfPeerReplica || idOfExternalClient);
  if (!result) {  // failure
    metrics_component_.UpdateAggregator();
    updateAggregatorCounter = 0;
    if (idOfExternalClient)
      metrics_.externalClientReqSigVerificationFailed_.Get().Inc();
    else
      metrics_.replicaSigVerificationFailed_.Get().Inc();
  } else {  // success
    if (idOfExternalClient)
      metrics_.externalClientReqSigVerified_.Get().Inc();
    else
      metrics_.replicaSigVerified_.Get().Inc();
    ++updateAggregatorCounter;
    if ((updateAggregatorCounter % updateMetricsAggregatorThresh) == 0) {
      metrics_component_.UpdateAggregator();
    }
  }
  return result;
}

void SigManager::sign(const char* data, size_t dataLength, char* outSig, uint16_t outSigLength) const {
  size_t actualSigSize = 0;
  mySigner_->sign(data, dataLength, outSig, outSigLength, actualSigSize);
  ConcordAssert(outSigLength == actualSigSize);
}

uint16_t SigManager::getMySigLength() const { return (uint16_t)mySigner_->signatureLength(); }

}  // namespace impl
}  // namespace bftEngine
