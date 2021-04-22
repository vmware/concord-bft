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

#include <utility>
#include <vector>
#include <map>
#include <string>
#include <memory>

namespace bftEngine {
namespace impl {

class RSASigner;
class RSAVerifier;

class SigManager {
 public:
  typedef std::string Key;
  typedef uint16_t KeyIndex;

  static SigManager* getInstance(SigManager* sm = nullptr) {
    static SigManager* instance_ = sm;
    return instance_;
  }

  // It is the caller responsibility to deallocate (delete) the object
  static SigManager* init(ReplicaId myId,
                          const Key& mySigPrivateKey,
                          const std::set<std::pair<PrincipalId, const std::string>>& publicKeysOfReplicas,
                          KeyFormat replicasKeysFormat,
                          const std::set<std::pair<const std::string, std::set<uint16_t>>>* publicKeysOfClients,
                          KeyFormat clientsKeysFormat,
                          uint16_t numReplicas,
                          uint16_t numRoReplicas,
                          uint16_t numOfClientProxies,
                          uint16_t numOfExternalClients);

  ~SigManager();

  // returns 0 if pid is invalid
  uint16_t getSigLength(PrincipalId pid) const;
  // returns false if actual verification failed, or if pid is invalid
  bool verifySig(PrincipalId pid, const char* data, size_t dataLength, const char* sig, uint16_t sigLength) const;
  void sign(const char* data, size_t dataLength, char* outSig, uint16_t outSigLength) const;
  uint16_t getMySigLength() const;
  bool isClientTransactionSigningEnabled() { return clientTransactionSigningEnabled_; }

  SigManager(const SigManager&) = delete;
  SigManager& operator=(const SigManager&) = delete;
  SigManager(SigManager&&) = delete;
  SigManager& operator=(SigManager&&) = delete;

 protected:
  const PrincipalId myId_;
  RSASigner* mySigner_;
  std::map<PrincipalId, RSAVerifier*> verifiers_;
  bool clientTransactionSigningEnabled_ = true;

  SigManager(PrincipalId myId,
             uint16_t numReplicas,
             const std::pair<Key, KeyFormat>& mySigPrivateKey,
             const std::vector<std::pair<Key, KeyFormat>>& publickeys,
             const std::map<PrincipalId, KeyIndex>& publicKeysMapping,
             bool clientTransactionSigningEnabled);

  static SigManager* initImpl(ReplicaId myId,
                              const Key& mySigPrivateKey,
                              const std::set<std::pair<PrincipalId, const std::string>>& publicKeysOfReplicas,
                              KeyFormat replicasKeysFormat,
                              const std::set<std::pair<const std::string, std::set<uint16_t>>>* publicKeysOfClients,
                              KeyFormat clientsKeysFormat,
                              uint16_t numReplicas,
                              uint16_t numRoReplicas,
                              uint16_t numOfClientProxies,
                              uint16_t numOfExternalClients);

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
      uint16_t numReplicas,
      uint16_t numRoReplicas,
      uint16_t numOfClientProxies,
      uint16_t numOfExternalClients) {
    return initImpl(myId,
                    mySigPrivateKey,
                    publicKeysOfReplicas,
                    replicasKeysFormat,
                    publicKeysOfClients,
                    clientsKeysFormat,
                    numReplicas,
                    numRoReplicas,
                    numOfClientProxies,
                    numOfExternalClients);
  }
#endif

};  // namespace impl

}  // namespace impl
}  // namespace bftEngine
