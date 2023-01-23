// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "SigManager.hpp"
#include "helper.hpp"

#include <random>

#include "gtest/gtest.h"
#include "crypto/factory.hpp"
#include "crypto/crypto.hpp"

using namespace std;
using concord::crypto::KeyFormat;
constexpr size_t RANDOM_DATA_SIZE = 1000U;

std::default_random_engine generator;

using concord::crypto::ISigner;
using concord::crypto::IVerifier;
using concord::crypto::Factory;
using bftEngine::ReplicaConfig;
using bftEngine::CryptoManager;
using concord::crypto::SignatureAlgorithm;
using concord::crypto::generateEdDSAKeyPair;

std::vector<std::pair<std::string, std::string>> generateKeyPairs(size_t count) {
  std::vector<std::pair<std::string, std::string>> result;
  for (size_t i = 0; i < count; i++) {
    result.push_back(generateEdDSAKeyPair(KeyFormat::HexaDecimalStrippedFormat));
  }
  return result;
}

void generateRandomData(char* data, size_t len) {
  std::uniform_int_distribution<int> distribution(0, 0xFF);
  for (size_t i{0}; i < len; ++i) {
    data[i] = static_cast<char>(distribution(generator));
  }
}

void corrupt(concord::Byte* data, size_t len) {
  for (size_t i{0}; i < len; ++i) {
    data[i] = ~data[i];
  }
}

void corrupt(char* data, size_t len) { corrupt(reinterpret_cast<concord::Byte*>(data), len); }

TEST(SignerAndVerifierTest, LoadSignVerifyFromHexKeyPair) {
  char data[RANDOM_DATA_SIZE]{0};

  const auto keyPair = generateEdDSAKeyPair();
  generateRandomData(data, RANDOM_DATA_SIZE);

  const auto signer_ = Factory::getSigner(keyPair.first, ReplicaConfig::instance().replicaMsgSigningAlgo);
  const auto verifier_ = Factory::getVerifier(keyPair.second, ReplicaConfig::instance().replicaMsgSigningAlgo);

  // sign with replica signer.
  size_t expectedSignerSigLen = signer_->signatureLength();
  std::vector<concord::Byte> sig(expectedSignerSigLen);
  size_t lenRetData;
  std::string str_data(data, RANDOM_DATA_SIZE);
  lenRetData = signer_->sign(str_data, sig.data());
  ASSERT_EQ(lenRetData, expectedSignerSigLen);

  // validate with replica verifier.
  ASSERT_TRUE(verifier_->verify(str_data, sig));

  // change data randomally, expect failure
  char data1[RANDOM_DATA_SIZE];
  std::copy(std::begin(data), std::end(data), std::begin(data1));
  corrupt(data1 + 10, 1);
  std::string str_data1(data1, RANDOM_DATA_SIZE);
  ASSERT_FALSE(verifier_->verify(str_data1, sig));

  // change signature randomally, expect failure
  corrupt(sig.data(), 1);
  str_data = std::string(data, RANDOM_DATA_SIZE);
  ASSERT_FALSE(verifier_->verify(str_data, sig));
}

TEST(SignerAndVerifierTest, LoadSignVerifyFromPemfiles) {
  char data[RANDOM_DATA_SIZE]{0};
  auto keys = generateKeyPairs(1);
  auto& [privKey, pubkey] = keys[0];

  generateRandomData(data, RANDOM_DATA_SIZE);

  const auto signer_ = Factory::getSigner(
      privKey, ReplicaConfig::instance().replicaMsgSigningAlgo, KeyFormat::HexaDecimalStrippedFormat);
  const auto verifier_ = Factory::getVerifier(
      pubkey, ReplicaConfig::instance().replicaMsgSigningAlgo, KeyFormat::HexaDecimalStrippedFormat);

  // sign with replica signer.
  size_t expectedSignerSigLen = signer_->signatureLength();
  std::vector<concord::Byte> sig(expectedSignerSigLen);
  std::string str_data(data, RANDOM_DATA_SIZE);
  uint32_t lenRetData = signer_->sign(str_data, sig.data());
  ASSERT_EQ(lenRetData, signer_->signatureLength());

  // validate with replica verifier.
  ASSERT_TRUE(verifier_->verify(str_data, sig));

  // change data randomally, expect failure
  char data1[RANDOM_DATA_SIZE];
  std::copy(std::begin(data), std::end(data), std::begin(data1));
  corrupt(data1 + 10, 1);
  std::string str_data1(data1, RANDOM_DATA_SIZE);
  ASSERT_FALSE(verifier_->verify(str_data1, sig));

  // change signature randomally, expect failure
  corrupt(sig.data(), 1);
  str_data = std::string(data, RANDOM_DATA_SIZE);
  ASSERT_FALSE(verifier_->verify(str_data, sig));
}

class SigManagerTest : public ::testing::Test {
 public:
  SigManagerTest() : config{createReplicaConfig()}, replicaInfo{config, false, false} {}

  void SetUp() override {
    hexKeyPairs = generateKeyPairs(config.numReplicas);
    std::set<std::pair<PrincipalId, const std::string>> publicKeysOfReplicas;
    std::vector<std::string> hexReplicaPublicKeys;

    // generateKeyPairs(config.numReplicas + config.numRoReplicas + config.)
    for (size_t i = 0; i < config.numReplicas; ++i) {
      publicKeysOfReplicas.emplace(i, concord::crypto::EdDSAHexToPem(hexKeyPairs[i]).second);
      hexReplicaPublicKeys.push_back(hexKeyPairs[i].second);
    }

    sigManager = SigManager::init(config.replicaId,
                                  concord::crypto::EdDSAHexToPem(hexKeyPairs[config.replicaId]).first,
                                  publicKeysOfReplicas,
                                  KeyFormat::PemFormat,
                                  nullptr,
                                  KeyFormat::PemFormat,
                                  replicaInfo);

    bftEngine::CryptoManager::init(std::make_unique<TestMultisigCryptoSystem>(
        config.replicaId, hexReplicaPublicKeys, hexKeyPairs[config.replicaId].first));
  }

 protected:
  std::vector<std::pair<std::string, std::string>> hexKeyPairs;
  std::shared_ptr<SigManager> sigManager;
  const ReplicaConfig& config;
  ReplicasInfo replicaInfo;
};


TEST_F(SigManagerTest, TestMySigLength) {
  ASSERT_GT(sigManager->getMySigLength(), 0);
}

TEST_F(SigManagerTest, ReplicasOnlyCheckVerify) {
  const size_t numReplicas = config.numReplicas;
  std::vector<unique_ptr<ISigner>> signers(numReplicas);

  // Load signers to simulate other replicas
  for (size_t i = 0; i < numReplicas; ++i) {
    signers[i] = Factory::getSigner(
        hexKeyPairs[i].first, ReplicaConfig::instance().replicaMsgSigningAlgo, KeyFormat::HexaDecimalStrippedFormat);
  }

  std::vector<char> data(RANDOM_DATA_SIZE);
  generateRandomData(data.data(), RANDOM_DATA_SIZE);

  for (size_t i = 0; i < numReplicas; ++i) {
    const auto& signer = signers[i];

    // sign with replica signer (other replicas, mock)
    std::vector<concord::Byte> sig(signer->signatureLength());
    ASSERT_EQ(signer->sign(data, sig.data()), sig.size());

    // Validate with SigManager (my replica)
    ASSERT_EQ(sig.size(), sigManager->getSigLength(i));
    ASSERT_TRUE(sigManager->verifySig(i, data, sig));

    size_t offset = i % data.size();
    corrupt(data.data() + offset, 1);
    ASSERT_FALSE(sigManager->verifySig(i, data, sig));
  }
}

// Check 1 more replica + 1200 clients on 6 additional participants
// where each participant hols a client pool of 200 clients
TEST(SigManagerTestWithClients, ReplicasAndClientsCheckVerify) {
  constexpr size_t numReplicas{7};
  constexpr size_t numRoReplicas{2};
  constexpr size_t numOfClientProxies{36};  // (numRoReplicas+numRoReplicas) * 4
  constexpr size_t numParticipantNodes{6};
  constexpr size_t numBftClientsInParticipantNodes{200};
  constexpr size_t totalNumberofExternalBftClients{1200};  // numOfExternaClients * numBftClientsInExternalClient
  constexpr PrincipalId myId{0};
  size_t signerIndex{0};
  unique_ptr<ISigner> signers[numReplicas + numParticipantNodes];  // only external clients and consensus replicas sign

  set<pair<PrincipalId, const string>> publicKeysOfReplicas;
  set<pair<const string, set<uint16_t>>> publicKeysOfClients;
  unordered_map<PrincipalId, size_t> principalIdToSignerIndex;

  auto keyPairs = generateKeyPairs(numReplicas + numParticipantNodes);

  // Load replica signers to simulate other replicas
  PrincipalId currPrincipalId{0};
  for (currPrincipalId = 0; currPrincipalId < numReplicas; ++currPrincipalId) {
    signers[signerIndex] = Factory::getSigner(keyPairs[currPrincipalId].first,
                                              ReplicaConfig::instance().replicaMsgSigningAlgo,
                                              KeyFormat::HexaDecimalStrippedFormat);

    publicKeysOfReplicas.insert(make_pair(currPrincipalId, keyPairs[currPrincipalId].second));
    principalIdToSignerIndex.emplace(currPrincipalId, signerIndex);
    ++signerIndex;
  }

  // Load another group of replica signers to simulate other clients
  currPrincipalId = numReplicas + numRoReplicas + numOfClientProxies;
  for (size_t i = numReplicas; i < numReplicas + numParticipantNodes; ++i) {
    signers[signerIndex] = Factory::getSigner(keyPairs[signerIndex].first,
                                              ReplicaConfig::instance().replicaMsgSigningAlgo,
                                              KeyFormat::HexaDecimalStrippedFormat);
    set<PrincipalId> principalIds;
    for (size_t j{0}; j < numBftClientsInParticipantNodes; ++j) {
      principalIds.insert(principalIds.end(), currPrincipalId);
      principalIdToSignerIndex.insert(make_pair(currPrincipalId, signerIndex));
      ++currPrincipalId;
    }
    publicKeysOfClients.insert(make_pair(keyPairs[signerIndex].second, std::move(principalIds)));
    ++signerIndex;
  }

  auto& config = createReplicaConfig(2, 0);
  config.numReplicas = numReplicas;
  config.numRoReplicas = numRoReplicas;
  config.numOfClientProxies = numOfClientProxies;
  config.numOfExternalClients = totalNumberofExternalBftClients;
  ReplicasInfo replicaInfo(config, false, false);

  shared_ptr<SigManager> sigManager(SigManager::init(myId,
                                                     keyPairs[config.replicaId].first,
                                                     publicKeysOfReplicas,
                                                     KeyFormat::HexaDecimalStrippedFormat,
                                                     &publicKeysOfClients,
                                                     KeyFormat::HexaDecimalStrippedFormat,
                                                     replicaInfo));
  std::vector<std::string> replicaHexPublicKeys(config.numReplicas);
  for (int j = 0; j < config.numReplicas; j++) {
    replicaHexPublicKeys[j] = keyPairs[j].second;
  }
  CryptoManager::init(std::make_unique<TestMultisigCryptoSystem>(
      config.replicaId, replicaHexPublicKeys, keyPairs[config.replicaId].second));

  // principalIdToSignerIndex carries all principal ids for replica, read only replicas and bft-clients.
  // There are some principal Ids in the range [minKey(principalIdToSignerIndex), maxKey(principalIdToSignerIndex)]
  // which are not recognized by SigManager - these are the principal ids of Proxy Clients.
  // In the next loop we will sign 30K times. Every iteration a random principal ID between minKey and maxKey is
  // generated. If the ID is valid, we locate the right signer using principalIdToSignerIndex. If not - sign with
  // another signer and expect a failure.
  PrincipalId minPidInclusive = 0;
  PrincipalId maxPidInclusive = currPrincipalId;
  std::uniform_int_distribution<int> distribution(minPidInclusive, maxPidInclusive);
  std::vector<char> data(RANDOM_DATA_SIZE);
  std::vector<concord::Byte> sig(1024u);

  for (size_t principalId = 0; principalId < maxPidInclusive; ++principalId) {
    size_t lenRetData, signerOffset;

    auto iter = principalIdToSignerIndex.find(principalId);
    if (iter == principalIdToSignerIndex.end()) {
      ASSERT_FALSE(sigManager->hasVerifier(principalId));
      ASSERT_EQ(sigManager->getSigLength(principalId), 0);
      continue;
    }
    signerOffset = iter->second;

    // sign
    LOG_DEBUG(GL, KVLOG(principalId, signerOffset, config.replicaId));
    auto expectedSignerSigLen = signers[signerOffset]->signatureLength();
    ASSERT_GE(sig.size(), expectedSignerSigLen);
    generateRandomData(data.data(), RANDOM_DATA_SIZE);
    lenRetData = signers[signerOffset]->sign(data, sig.data());
    ASSERT_EQ(lenRetData, expectedSignerSigLen);

    // Validate with SigManager (my replica)
    ASSERT_TRUE(sigManager->getSigLength(principalId) > 0);
    bool signatureValid = sigManager->verifySig(
        principalId, data, string_view{reinterpret_cast<char*>(sig.data()), sigManager->getSigLength(principalId)});
    ASSERT_TRUE(signatureValid);
  }
}
