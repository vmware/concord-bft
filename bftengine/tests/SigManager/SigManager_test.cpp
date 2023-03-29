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

constexpr char KEYS_BASE_PARENT_PATH[] = "/tmp/";
constexpr char KEYS_BASE_PATH[] = "/tmp/transaction_signing_keys";
constexpr char PRIV_KEY_NAME[] = "privkey.pem";
constexpr char PUB_KEY_NAME[] = "pubkey.pem";
constexpr char KEYS_GEN_SCRIPT_PATH[] = "../../../../scripts/linux/create_concord_clients_transaction_signing_keys.sh";
constexpr size_t RANDOM_DATA_SIZE = 1000U;

std::default_random_engine generator;

using concord::crypto::ISigner;
using concord::crypto::IVerifier;
using concord::crypto::Factory;
using bftEngine::ReplicaConfig;
using concord::crypto::SignatureAlgorithm;
using concord::crypto::generateEdDSAKeyPair;

void generateKeyPairs(size_t count) {
  ostringstream cmd;

  ASSERT_EQ(0, system(cmd.str().c_str()));
  cmd << "rm -rf " << KEYS_BASE_PATH;
  ASSERT_EQ(0, system(cmd.str().c_str()));

  cmd.str("");
  cmd.clear();

  std::string algo;
  if (ReplicaConfig::instance().replicaMsgSigningAlgo == SignatureAlgorithm::EdDSA) {
    algo = "eddsa";
  }
  cmd << KEYS_GEN_SCRIPT_PATH << " -n " << count << " -r " << PRIV_KEY_NAME << " -u " << PUB_KEY_NAME << " -o "
      << KEYS_BASE_PARENT_PATH << " -a " << algo;
  ASSERT_EQ(0, system(cmd.str().c_str()));
}

void readFile(string_view path, string& keyOut) {
  stringstream stream;
  ifstream file(path.data());
  ASSERT_TRUE(file.good());
  stream << file.rdbuf();
  keyOut = stream.str();
}

void generateRandomData(char* data, size_t len) {
  std::uniform_int_distribution<int> distribution(0, 0xFF);
  for (size_t i{0}; i < len; ++i) {
    data[i] = static_cast<char>(distribution(generator));
  }
}

void corrupt(concord::Byte* data, size_t len) {
  for (size_t i{0}; i < len; ++i) {
    ++data[i];
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
  string publicKeyFullPath({string(KEYS_BASE_PATH) + string("/1/") + PUB_KEY_NAME});
  string privateKeyFullPath({string(KEYS_BASE_PATH) + string("/1/") + PRIV_KEY_NAME});

  string privKey, pubkey;
  char data[RANDOM_DATA_SIZE]{0};

  generateKeyPairs(1);

  generateRandomData(data, RANDOM_DATA_SIZE);
  readFile(privateKeyFullPath, privKey);
  readFile(publicKeyFullPath, pubkey);

  const auto signer_ =
      Factory::getSigner(privKey, ReplicaConfig::instance().replicaMsgSigningAlgo, KeyFormat::PemFormat);
  const auto verifier_ =
      Factory::getVerifier(pubkey, ReplicaConfig::instance().replicaMsgSigningAlgo, KeyFormat::PemFormat);

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

TEST(SigManagerTest, ReplicasOnlyCheckVerify) {
  constexpr size_t numReplicas{4};
  constexpr PrincipalId myId{0};
  string myPrivKey;
  unique_ptr<ISigner> signers[numReplicas];
  set<pair<PrincipalId, const string>> publicKeysOfReplicas;

  generateKeyPairs(numReplicas);

  // Load signers to simulate other replicas
  for (size_t i{1}; i <= numReplicas; ++i) {
    string privKey, pubKey;
    string privateKeyFullPath({string(KEYS_BASE_PATH) + string("/") + to_string(i) + string("/") + PRIV_KEY_NAME});
    readFile(privateKeyFullPath, privKey);
    PrincipalId pid = i - 1;  // folders are 1-indexed

    if (pid == myId) {
      myPrivKey = privKey;
      continue;
    }
    signers[pid] = Factory::getSigner(privKey, ReplicaConfig::instance().replicaMsgSigningAlgo, KeyFormat::PemFormat);
    string pubKeyFullPath({string(KEYS_BASE_PATH) + string("/") + to_string(i) + string("/") + PUB_KEY_NAME});
    readFile(pubKeyFullPath, pubKey);
    publicKeysOfReplicas.insert(make_pair(pid, pubKey));
  }

  ReplicasInfo replicaInfo(createReplicaConfig(), false, false);
  unique_ptr<SigManager> sigManager(SigManager::init(
      myId, myPrivKey, publicKeysOfReplicas, KeyFormat::PemFormat, nullptr, KeyFormat::PemFormat, replicaInfo));

  for (size_t i{0}; i < numReplicas; ++i) {
    const auto& signer = signers[i];

    char data[RANDOM_DATA_SIZE]{0};
    size_t lenRetData;
    size_t expectedSignerSigLen;

    if (i == myId) continue;

    // sign with replica signer (other replicas, mock)
    expectedSignerSigLen = signer->signatureLength();
    std::vector<concord::Byte> sig(expectedSignerSigLen);

    generateRandomData(data, RANDOM_DATA_SIZE);
    std::string str_data(data, RANDOM_DATA_SIZE);
    lenRetData = signer->sign(str_data, sig.data());
    ASSERT_EQ(lenRetData, expectedSignerSigLen);

    // Validate with SigManager (my replica)
    ASSERT_EQ(sig.size(), sigManager->getSigLength(i));
    ASSERT_TRUE(sigManager->verifySig(i, std::string_view{data, RANDOM_DATA_SIZE}, sig));

    // change data randomally, expect failure
    char data1[RANDOM_DATA_SIZE];
    std::copy(std::begin(data), std::end(data), std::begin(data1));
    corrupt(data1 + 10, 1);
    ASSERT_FALSE(sigManager->verifySig(i, std::string_view{data1, RANDOM_DATA_SIZE}, sig));

    // change signature randomally, expect failure
    corrupt(sig.data(), 1);
    ASSERT_FALSE(sigManager->verifySig(i, std::string_view{data, RANDOM_DATA_SIZE}, sig));
  }
}

TEST(SigManagerTest, ReplicasOnlyCheckSign) {
  constexpr size_t numReplicas{4};
  constexpr PrincipalId myId{0};
  string myPrivKey, privKey, pubKey, sig;
  unique_ptr<IVerifier> verifier;
  set<pair<PrincipalId, const string>> publicKeysOfReplicas;
  char data[RANDOM_DATA_SIZE]{0};
  size_t expectedSignerSigLen;

  generateKeyPairs(numReplicas);

  // Load my private key
  string privateKeyFullPath({string(KEYS_BASE_PATH) + string("/") + to_string(1) + string("/") + PRIV_KEY_NAME});
  readFile(privateKeyFullPath, myPrivKey);

  // Load single other replica's verifier (mock)
  string pubKeyFullPath({string(KEYS_BASE_PATH) + string("/") + to_string(1) + string("/") + PUB_KEY_NAME});
  readFile(pubKeyFullPath, pubKey);

  verifier = Factory::getVerifier(pubKey, ReplicaConfig::instance().replicaMsgSigningAlgo, KeyFormat::PemFormat);

  // load public key of other replicas, must be done for SigManager ctor
  for (size_t i{2}; i <= numReplicas; ++i) {
    pubKeyFullPath = string(KEYS_BASE_PATH) + string("/") + to_string(i) + string("/") + PUB_KEY_NAME;
    readFile(pubKeyFullPath, pubKey);
    publicKeysOfReplicas.insert(make_pair(i - 1, pubKey));
  }

  ReplicasInfo replicaInfo(createReplicaConfig(), false, false);
  unique_ptr<SigManager> sigManager(SigManager::init(
      myId, myPrivKey, publicKeysOfReplicas, KeyFormat::PemFormat, nullptr, KeyFormat::PemFormat, replicaInfo));
  // sign with SigManager
  expectedSignerSigLen = sigManager->getSigLength(myId);
  generateRandomData(data, RANDOM_DATA_SIZE);
  sig.resize(expectedSignerSigLen);
  sigManager->sign(data, RANDOM_DATA_SIZE, sig.data());

  // Validate with replica verifier (mock)
  std::string str_data(data, RANDOM_DATA_SIZE);
  ASSERT_TRUE(verifier->verify(str_data, sig));

  // change data randomally, expect failure
  char data1[RANDOM_DATA_SIZE];
  std::copy(std::begin(data), std::end(data), std::begin(data1));
  corrupt(data1 + 10, 1);
  std::string str_data1(data1, RANDOM_DATA_SIZE);
  ASSERT_FALSE(verifier->verify(str_data1, sig));

  // change signature randomally, expect failure
  corrupt(sig.data(), 1);
  ASSERT_FALSE(verifier->verify(str_data1, sig));
}

// Check 1 more replica + 1200 clients on 6 additional participants
// where each participant hols a client pool of 200 clients
TEST(SigManagerTest, ReplicasAndClientsCheckVerify) {
  constexpr size_t numReplicas{7};
  constexpr size_t numRoReplicas{2};
  constexpr size_t numOfClientProxies{36};  // (numRoReplicas+numRoReplicas) * 4
  constexpr size_t numParticipantNodes{6};
  constexpr size_t numBftClientsInParticipantNodes{200};
  constexpr size_t totalNumberofExternalBftClients{1200};  // numOfExternaClients * numBftClientsInExternalClient
  constexpr PrincipalId myId{0};
  string myPrivKey;
  size_t i, signerIndex{0};
  unique_ptr<ISigner> signers[numReplicas + numParticipantNodes];  // only external clients and consensus replicas sign

  set<pair<PrincipalId, const string>> publicKeysOfReplicas;
  set<pair<const string, set<uint16_t>>> publicKeysOfClients;
  unordered_map<PrincipalId, size_t> principalIdToSignerIndex;

  generateKeyPairs(numReplicas + numParticipantNodes);

  // Load replica signers to simulate other replicas
  PrincipalId currPrincipalId{0};
  for (i = 1; i <= numReplicas; ++i, ++currPrincipalId) {
    string privKey, pubKey;
    string privateKeyFullPath({string(KEYS_BASE_PATH) + string("/") + to_string(i) + string("/") + PRIV_KEY_NAME});
    readFile(privateKeyFullPath, privKey);

    if (currPrincipalId == myId) {
      myPrivKey = privKey;
      continue;
    }
    signers[signerIndex] =
        Factory::getSigner(privKey, ReplicaConfig::instance().replicaMsgSigningAlgo, KeyFormat::PemFormat);

    string pubKeyFullPath({string(KEYS_BASE_PATH) + string("/") + to_string(i) + string("/") + PUB_KEY_NAME});
    readFile(pubKeyFullPath, pubKey);
    publicKeysOfReplicas.insert(make_pair(currPrincipalId, pubKey));
    principalIdToSignerIndex.insert(make_pair(currPrincipalId, signerIndex));
    ++signerIndex;
  }

  // Load another group of replica signers to simulate other clients
  currPrincipalId = numReplicas + numRoReplicas + numOfClientProxies;
  for (; i <= numReplicas + numParticipantNodes; ++i) {
    string privKey, pubKey;
    string privateKeyFullPath({string(KEYS_BASE_PATH) + string("/") + to_string(i) + string("/") + PRIV_KEY_NAME});
    readFile(privateKeyFullPath, privKey);
    signers[signerIndex] =
        Factory::getSigner(privKey, ReplicaConfig::instance().replicaMsgSigningAlgo, KeyFormat::PemFormat);
    string pubKeyFullPath({string(KEYS_BASE_PATH) + string("/") + to_string(i) + string("/") + PUB_KEY_NAME});
    set<PrincipalId> principalIds;
    for (size_t j{0}; j < numBftClientsInParticipantNodes; ++j) {
      principalIds.insert(principalIds.end(), currPrincipalId);
      principalIdToSignerIndex.insert(make_pair(currPrincipalId, signerIndex));
      ++currPrincipalId;
    }
    readFile(pubKeyFullPath, pubKey);
    publicKeysOfClients.insert(make_pair(std::move(pubKey), std::move(principalIds)));
    ++signerIndex;
  }

  auto& config = createReplicaConfig(2, 0);
  config.numReplicas = numReplicas;
  config.numRoReplicas = numRoReplicas;
  config.numOfClientProxies = numOfClientProxies;
  config.numOfExternalClients = totalNumberofExternalBftClients;
  ReplicasInfo replicaInfo(config, false, false);

  unique_ptr<SigManager> sigManager(SigManager::init(myId,
                                                     myPrivKey,
                                                     publicKeysOfReplicas,
                                                     KeyFormat::PemFormat,
                                                     &publicKeysOfClients,
                                                     KeyFormat::PemFormat,
                                                     replicaInfo));

  // principalIdToSignerIndex carries all principal ids for replica, read only replicas and bft-clients.
  // There are some principal Ids in the range [minKey(principalIdToSignerIndex), maxKey(principalIdToSignerIndex)]
  // which are not recognized by SigManager - these are the principal ids of Proxy Clients.
  // In the next loop we will sign 30K times. Every iteration a random principal ID between minKey and maxKey is
  // generated. If the ID is valid, we locate the right signer using principalIdToSignerIndex. If not - sign with
  // another signer and expect a failure.
  PrincipalId minPidInclusive = 1, maxPidInclusive = currPrincipalId - 1;
  std::uniform_int_distribution<int> distribution(minPidInclusive, maxPidInclusive);
  for (size_t i{0}; i < 3E4; ++i) {
    char data[RANDOM_DATA_SIZE]{0};
    size_t lenRetData, expectedVerifierSigLen, expectedSignerSigLen, signerIndex;
    bool expectFailure = false;

    PrincipalId signerPrincipalId = static_cast<PrincipalId>(distribution(generator));
    auto iter = principalIdToSignerIndex.find(signerPrincipalId);
    if (iter != principalIdToSignerIndex.end()) {
      signerIndex = iter->second;
    } else {
      signerIndex = 1;  // sign with signer index 1, so we can check the target SigManager
      expectFailure = true;
    }

    // sign
    expectedSignerSigLen = signers[signerIndex]->signatureLength();
    std::vector<concord::Byte> sig(expectedSignerSigLen);
    generateRandomData(data, RANDOM_DATA_SIZE);
    std::string str_data(data, RANDOM_DATA_SIZE);
    lenRetData = signers[signerIndex]->sign(str_data, sig.data());
    ASSERT_EQ(lenRetData, expectedSignerSigLen);

    // Validate with SigManager (my replica)
    expectedVerifierSigLen = sigManager->getSigLength(signerPrincipalId);
    ASSERT_TRUE((expectFailure && expectedVerifierSigLen == 0) || (!expectFailure && expectedVerifierSigLen > 0));
    bool signatureValid = sigManager->verifySig(signerPrincipalId, std::string_view{data, RANDOM_DATA_SIZE}, sig);
    ASSERT_TRUE((expectFailure && !signatureValid) || (!expectFailure && signatureValid));
  }
}
