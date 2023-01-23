// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "helper.hpp"
#include "ReplicaConfig.hpp"

typedef std::pair<uint16_t, std::string> IdToKeyPair;

using concord::crypto::SignatureAlgorithm;
using bftEngine::ReplicaConfig;

const std::string replicaEdDSAPrivateKey = {"09a30490ebf6f6685556046f2497fd9c7df4a552998c9a9b6ebec742e8183174"};
const std::string replicaEdDSAPubKey = {"7363bc5ab96d7f85e71a5ffe0b284405ae38e2e0f032fb3ffe805d9f0e2d117b"};

void loadPrivateAndPublicKeys(std::string& myPrivateKey,
                              std::set<std::pair<uint16_t, const std::string>>& publicKeysOfReplicas,
                              size_t numReplicas) {
  ConcordAssert(numReplicas <= 7);

  std::string pubKey;
  ConcordAssertEQ(ReplicaConfig::instance().replicaMsgSigningAlgo, SignatureAlgorithm::EdDSA);
  myPrivateKey = replicaEdDSAPrivateKey;

  for (size_t i{0}; i < numReplicas; ++i) {
    publicKeysOfReplicas.insert(IdToKeyPair(i, replicaEdDSAPubKey.c_str()));
  }
}

bftEngine::ReplicaConfig& createReplicaConfig(uint16_t fVal, uint16_t cVal) {
  bftEngine::ReplicaConfig& config = bftEngine::ReplicaConfig::instance();
  config.numReplicas = 3 * fVal + 2 * cVal + 1;
  config.fVal = fVal;
  config.cVal = cVal;
  config.replicaId = 0;
  config.numOfClientProxies = 0;
  config.statusReportTimerMillisec = 15;
  config.concurrencyLevel = 5;
  config.viewChangeProtocolEnabled = true;
  config.viewChangeTimerMillisec = 12;
  config.autoPrimaryRotationEnabled = false;
  config.autoPrimaryRotationTimerMillisec = 42;
  config.maxExternalMessageSize = 2000000;
  config.maxNumOfReservedPages = 256;
  config.maxReplyMessageSize = 1024;
  config.sizeOfReservedPage = 2048;
  config.debugStatisticsEnabled = true;
  config.threadbagConcurrencyLevel1 = 16;
  config.threadbagConcurrencyLevel2 = 8;

  loadPrivateAndPublicKeys(config.replicaPrivateKey, config.publicKeysOfReplicas, config.numReplicas);

  bftEngine::CryptoManager::init(std::make_unique<TestCryptoSystem>());

  return config;
}

TestMultisigCryptoSystem::TestMultisigCryptoSystem(NodeIdType id,
                                                   const std::vector<std::string>& hexStringEdDSAPublicKeys,
                                                   const std::string& hexStringEdDSAPrivateKey)
    : id_{id},
      hexStringEdDSAPrivateKey_{hexStringEdDSAPrivateKey},
      hexStringEdDSAPublicKeys_{hexStringEdDSAPublicKeys} {}
IThresholdVerifier* TestMultisigCryptoSystem::createThresholdVerifier(uint16_t threshold) {
  return factory_.newVerifier(threshold, hexStringEdDSAPublicKeys_.size(), "", hexStringEdDSAPublicKeys_);
}
IThresholdSigner* TestMultisigCryptoSystem::createThresholdSigner() {
  return factory_.newSigner(id_, hexStringEdDSAPrivateKey_.c_str());
}

class TestSigManager : public bftEngine::impl::SigManager {
 public:
  TestSigManager(PrincipalId myId,
                 const std::pair<Key, concord::crypto::KeyFormat>& mySigPrivateKey,
                 const std::vector<std::pair<Key, concord::crypto::KeyFormat>>& publickeys,
                 const std::map<PrincipalId, KeyIndex>& publicKeysMapping,
                 const ReplicasInfo& replicasInfo,
                 bool transactionSigningEnabled = false)
      : bftEngine::impl::SigManager(
            myId, mySigPrivateKey, publickeys, publicKeysMapping, transactionSigningEnabled, replicasInfo) {}

  static std::shared_ptr<TestSigManager> init(size_t myId,
                                              std::string& myPrivateKey,
                                              std::set<std::pair<uint16_t, const std::string>>& publicKeys,
                                              ReplicasInfo& replicasInfo) {
    std::vector<std::pair<Key, concord::crypto::KeyFormat>> publicKeysWithFormat(publicKeys.size());
    std::vector<Key> replicaPublicKeys(replicasInfo.getNumberOfReplicas());

    std::map<PrincipalId, KeyIndex> publicKeysMapping;
    for (auto& [id, key] : publicKeys) {
      publicKeysWithFormat[id] = {key, concord::crypto::KeyFormat::HexaDecimalStrippedFormat};
      publicKeysMapping.emplace(id, id);
      ConcordAssert(replicasInfo.isIdOfReplica(id));
      replicaPublicKeys[id] = key;
    }

    bftEngine::CryptoManager::init(std::make_unique<TestMultisigCryptoSystem>(myId, replicaPublicKeys, myPrivateKey));
    auto manager =
        std::make_shared<TestSigManager>(myId,
                                         std::pair<std::string, concord::crypto::KeyFormat>{
                                             myPrivateKey, concord::crypto::KeyFormat::HexaDecimalStrippedFormat},
                                         publicKeysWithFormat,
                                         publicKeysMapping,
                                         replicasInfo);
    SigManager::reset(manager);
    return manager;
  }
};

std::shared_ptr<bftEngine::impl::SigManager> createSigManager(
    size_t myId,
    std::string& myPrivateKey,
    concord::crypto::KeyFormat replicasKeysFormat,
    std::set<std::pair<uint16_t, const std::string>>& publicKeysOfReplicas,
    ReplicasInfo& replicasInfo) {
  UNUSED(replicasKeysFormat);
  return TestSigManager::init(myId, myPrivateKey, publicKeysOfReplicas, replicasInfo);
}
