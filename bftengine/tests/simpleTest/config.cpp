// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <utility>
#include <string>

#include "CommFactory.hpp"
#include "ReplicaConfig.hpp"
#include <threshsign/ThresholdSignaturesSchemes.h>

using BLS::Relic::BlsThresholdFactory;
using namespace std;
using namespace bftEngine;


//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////

#define MAX_ITEM_LENGTH (4096)
#define MAX_ITEM_LENGTH_STR "4096"

const char* nameOfPublicFile = "public_replicas_data";
const char* namesOfPrivateFiles[] = {
  "private_replica_0",
  "private_replica_1",
  "private_replica_2",
  "private_replica_3"
};

const int numOfReplicas = 4;
const int fVal = 1;

const uint16_t basePort = 3710;

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////


static void readOneLine(FILE *f, int capacity, char * buf) {
  fgets(buf, capacity - 1, f);
  buf[strlen(buf) - 1] = 0;  // strip newline
}

static void ignoreOneLine(FILE *f) {
  char buf[8192];
  readOneLine(f, 8191, buf);
}


static IThresholdSigner * createThresholdSigner(FILE * f,
                                                int id,
                                                IThresholdFactory* factory) {
  if (id < 1) {
    throw std::runtime_error(
        "Expected replica's ID to be strictly greater than zero!");
  }

  char secretKey[MAX_ITEM_LENGTH];
  ignoreOneLine(f);

  readOneLine(f, MAX_ITEM_LENGTH, secretKey);
  return factory->newSigner(id, secretKey);
}


static IThresholdFactory* createThresholdFactory(FILE* f, int n) {
  char buf[MAX_ITEM_LENGTH];
  char * curveType = buf;
  char cryptosys[MAX_ITEM_LENGTH];

  ignoreOneLine(f);
  readOneLine(f, MAX_ITEM_LENGTH, cryptosys);
  readOneLine(f, MAX_ITEM_LENGTH, buf);

  if (strcmp(cryptosys, MULTISIG_BLS_SCHEME) == 0) {
    return new BlsThresholdFactory(
        BLS::Relic::PublicParametersFactory::getByCurveType(curveType));
  } else if (strcmp(cryptosys, THRESHOLD_BLS_SCHEME) == 0) {
    return new BlsThresholdFactory(
        BLS::Relic::PublicParametersFactory::getByCurveType(curveType));
  } else {
    printf("ERROR: Unsupported cryptosystem: %s\n", cryptosys);
    throw std::runtime_error("ERROR: Unsupported cryptosystem");
  }
}

static IThresholdVerifier * createThresholdVerifier(
    FILE * f, int k, int n, IThresholdFactory* factory) {
  char publicKey[MAX_ITEM_LENGTH];
  char verifKey[MAX_ITEM_LENGTH];

  readOneLine(f, MAX_ITEM_LENGTH, publicKey);

  ignoreOneLine(f);
  std::vector<std::string> verifKeys;
  verifKeys.push_back("");  // signer 0 does not exist
  for (int i = 0; i < n; i++) {
    readOneLine(f, MAX_ITEM_LENGTH, verifKey);
    verifKeys.push_back(std::string(verifKey));
  }

  // Ignore last comment/empty line
  ignoreOneLine(f);

  return factory->newVerifier(k, n, publicKey, verifKeys);
}


static void ignoreThresholdSigner(FILE * f) {
  ignoreOneLine(f);
  ignoreOneLine(f);
}


static bool parseReplicaConfig(uint16_t replicaId,
                               FILE* publicKeysFile,
                               FILE* privateKeysFile,
                               ReplicaConfig* outConfig) {
  int tempInt = 0;
  char tempString[MAX_ITEM_LENGTH];

  std::set <pair<uint16_t, string>> publicKeysOfReplicas;

  // read from publicKeysFile

  for (int i = 0; i < numOfReplicas; i++) {
    fscanf(publicKeysFile, "%" MAX_ITEM_LENGTH_STR "s\n", tempString);

    string name(tempString);

    string expectedName = string("replica") + std::to_string(i);

    if (expectedName != name) return false;

    fscanf(publicKeysFile, "%" MAX_ITEM_LENGTH_STR "s\n", tempString);

    string publicKeyString(tempString);

    publicKeysOfReplicas.insert({i, publicKeyString});
  }

  // Read threshold signatures
  IThresholdFactory* slowCommitFactory = nullptr;
  IThresholdSigner* thresholdSignerForSlowPathCommit = nullptr;
  IThresholdVerifier* thresholdVerifierForSlowPathCommit = nullptr;

  IThresholdFactory* optimisticCommitFactory = nullptr;
  IThresholdSigner* thresholdSignerForOptimisticCommit = nullptr;
  IThresholdVerifier* thresholdVerifierForOptimisticCommit = nullptr;

  slowCommitFactory = createThresholdFactory(publicKeysFile, numOfReplicas);
  thresholdVerifierForSlowPathCommit =
      createThresholdVerifier(publicKeysFile,
                              numOfReplicas - fVal,
                              numOfReplicas,
                              slowCommitFactory);

  optimisticCommitFactory = createThresholdFactory(publicKeysFile,
                                                   numOfReplicas);
  thresholdVerifierForOptimisticCommit =
      createThresholdVerifier(publicKeysFile,
                              numOfReplicas,
                              numOfReplicas,
                              optimisticCommitFactory);

  // read from privateKeysFile

  fscanf(privateKeysFile, "%d\n", &tempInt);
  if (tempInt != replicaId) return false;

  fscanf(privateKeysFile, "%" MAX_ITEM_LENGTH_STR "s\n", tempString);

  string sigPrivateKey(tempString);

  thresholdSignerForSlowPathCommit =
      createThresholdSigner(privateKeysFile, replicaId+1, slowCommitFactory);

  thresholdSignerForOptimisticCommit =
      createThresholdSigner(privateKeysFile,
                            replicaId+1,
                            optimisticCommitFactory);

  delete slowCommitFactory;
  delete optimisticCommitFactory;

  // set configuration

  outConfig->replicaId = replicaId;
  outConfig->fVal = 1;
  outConfig->cVal = 0;
  outConfig->numOfClientProxies = 1;
  outConfig->statusReportTimerMillisec = 2000;
  outConfig->concurrencyLevel = 1;
  outConfig->autoViewChangeEnabled = false;
  outConfig->viewChangeTimerMillisec = 60000;

  outConfig->publicKeysOfReplicas = publicKeysOfReplicas;
  outConfig->replicaPrivateKey = sigPrivateKey;

  outConfig->thresholdSignerForExecution = nullptr;
  outConfig->thresholdVerifierForExecution = nullptr;

  outConfig->thresholdSignerForSlowPathCommit =
      thresholdSignerForSlowPathCommit;
  outConfig->thresholdVerifierForSlowPathCommit =
      thresholdVerifierForSlowPathCommit;

  outConfig->thresholdSignerForCommit = nullptr;
  outConfig->thresholdVerifierForCommit = nullptr;

  outConfig->thresholdSignerForOptimisticCommit =
      thresholdSignerForOptimisticCommit;
  outConfig->thresholdVerifierForOptimisticCommit =
      thresholdVerifierForOptimisticCommit;

  return true;
}

void getReplicaConfig(uint16_t replicaId, ReplicaConfig* outConfig) {
  FILE* publicKeysFile = fopen(nameOfPublicFile, "r");
  if (!publicKeysFile)
    throw std::runtime_error("Unable to read public keys");

  FILE* privateKeysFile = fopen(namesOfPrivateFiles[replicaId], "r");
  if (!privateKeysFile)
    throw std::runtime_error("Unable to read private keys");

  bool succ = parseReplicaConfig(
      replicaId, publicKeysFile, privateKeysFile, outConfig);
  if (!succ)
    throw std::runtime_error("Unable to parse configuration");
}

PlainUdpConfig getUDPConfig(uint16_t id) {
  std::string ip = "127.0.0.1";
  uint16_t port = basePort + id*2;
  uint32_t bufLength = 64000;

  std::unordered_map <NodeNum, std::tuple<std::string, uint16_t>> nodes;
  for (int i = 0; i < (numOfReplicas+1); i++)
    nodes.insert({ i, std::make_tuple(ip, basePort + i*2) });

  PlainUdpConfig retVal(ip, port, bufLength, nodes);

  return retVal;
}
