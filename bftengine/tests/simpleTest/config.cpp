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

// This file includes functionality that both the client and the replica use, to
// set up communications and signatures.

#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "CommFactory.hpp"
#include "ReplicaConfig.hpp"
#include "threshsign/ThresholdSignaturesSchemes.h"

using bftEngine::PlainUdpConfig;
using bftEngine::ReplicaConfig;
using BLS::Relic::BlsThresholdFactory;
using std::pair;
using std::string;

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////

// Maximum length of a line in the config file.
#define MAX_ITEM_LENGTH (4096)

// Stringified max line length, for use in fscanf.
#define MAX_ITEM_LENGTH_STR "4096"

// Filenames of config files.
const char* nameOfPublicFile = "public_replicas_data";
const char* namesOfPrivateFiles[] = {
  "private_replica_0",
  "private_replica_1",
  "private_replica_2",
  "private_replica_3"
};

// Number of replicas - must be less than or equal to the length of
// namesOfPrivateFiles.
const int numOfReplicas = 4;

// Number of client proxies.
const int numOfClientProxies = 1;

// Number of failed nodes allowed. numOfReplicas must be at least (3*fVal)+1.
const int fVal = 1;

// Network port of the first replica. Other replicas use ports
// basePort+(2*index).
const uint16_t basePort = 3710;

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////

// Read a line from file `f` into buffer `buf`, not exceeding `capacity`
// bytes. Trailing newline is assumed, and removed.
static void readOneLine(FILE *f, int capacity, char * buf) {
  fgets(buf, capacity - 1, f);
  buf[strlen(buf) - 1] = 0;  // strip newline
}

// Read a line from file `f` and discard it.
static void ignoreOneLine(FILE *f) {
  char buf[8192];
  readOneLine(f, 8191, buf);
}

// Read signer parameters from file `f`, and use them to create a threshold
// signer with index `id`.
//
// Two lines are read from the file:
//  * The first is discareded.
//  * The second is expected to be the private key in hex.
static IThresholdSigner* createThresholdSigner(FILE* f,
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

// Read verifier parameters from file `f`, and use them to create a threshold
// verifier with index `k`.
//
// 3+`n` lines are read from the file:
//  * The first line is expected to be the verifier's public key in hex.
//  * The second line is ignored.
//  * Each of the next `n` lines is expected to be a threshold key in hex.
//  * The final line is ignored.
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

// Initialize the BLS threshold library, and prepare it to create threshold
// signers and verifiers.
//
// Three lines are read from the file.
//  * The first is discarded.
//  * The second is expected to be the name of a crypto system, either
//    "multisig-bls" or "threshold-bls".
//  * The third is the name of a curve type, one of "BN-P254", "BN-P256",
//    "B12-P381", "BN-P382", "B12-P455", "B24-P477", "KSS-P508", "BN-P638", or
//    "B12-P638".
static IThresholdFactory* createThresholdFactory(FILE* f) {
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

// Create a replica configuration for the replica with index `replicaId`, using
// data from `publicKeysFile` and `privateKeysFiles`. Returns `true` if the
// files were read and all names matched (see below), or `false` if names did
// not match.
//
// From `publicKeysFile`, first `numOfReplicas`*2 lines are read
//  * The first of each pair is the name of a replica. Must be "replica"
//    followed by the index of the replica in decimal. For example, "replica0",
//    "replica1", ...
//  * The second of each pair is the public key of that replica in hex.
//
// After that, two threshold verifier configs are read (see
// createThresholdVerifier for line descriptions). The first verifier is used
// for slow path commit, and the second is used for optimistic path commit.
//
// Next, from `privateKeysFiles`, two lines are read:
//  * The first is the index of the replica. This must match `replicaId` in
//    decimal. For example "0", "1", ...
//  * The second is the private key for the replica.
//
// After that, two threshold signer configs are read (see createThresholdSigner
// for line descriptions). The first signer is used for the slow patch, and the
// second signer is used for the optimistic path.
static bool parseReplicaConfig(uint16_t replicaId,
                               FILE* publicKeysFile,
                               FILE* privateKeysFile,
                               ReplicaConfig* outConfig) {
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

  slowCommitFactory = createThresholdFactory(publicKeysFile);
  thresholdVerifierForSlowPathCommit =
      createThresholdVerifier(publicKeysFile,
                              numOfReplicas - fVal,
                              numOfReplicas,
                              slowCommitFactory);

  optimisticCommitFactory = createThresholdFactory(publicKeysFile);
  thresholdVerifierForOptimisticCommit =
      createThresholdVerifier(publicKeysFile,
                              numOfReplicas,
                              numOfReplicas,
                              optimisticCommitFactory);

  // read from privateKeysFile

  int tempInt = 0;
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
  outConfig->fVal = fVal;
  outConfig->cVal = 0;
  outConfig->numOfClientProxies = numOfClientProxies;
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

// Create a replica config for the replica with index `replicaId`. This is a
// wrapper around parseReplicaConfig that handles opening the correct files.
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

// Create a UDP communication configuration for the node (replica or client)
// with index `id`.
PlainUdpConfig getUDPConfig(uint16_t id) {
  std::string ip = "127.0.0.1";
  uint16_t port = basePort + id*2;
  uint32_t bufLength = 64000;

  // Create a map of where the port for each node is.
  std::unordered_map <NodeNum, NodeInfo> nodes;
  for (int i = 0; i < (numOfReplicas + numOfClientProxies); i++)
    nodes.insert({
      i,
      NodeInfo{ip, (uint16_t)(basePort + i*2), i < numOfReplicas} });

  PlainUdpConfig retVal(ip, port, bufLength, nodes, id);
  return retVal;
}
