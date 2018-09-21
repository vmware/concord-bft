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
#include <iostream>
#include <fstream>

#include "CommFactory.hpp"
#include "ReplicaConfig.hpp"
#include "threshsign/ThresholdSignaturesSchemes.h"
#include "KeyfileIOUtils.hpp"

using bftEngine::PlainUdpConfig;
using bftEngine::PlainTcpConfig;
using bftEngine::ReplicaConfig;
using BLS::Relic::BlsThresholdFactory;
using std::pair;
using std::string;

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////

const char* namesOfKeyfiles[] = {
  "private_replica_0",
  "private_replica_1",
  "private_replica_2",
  "private_replica_3"
};

// Number of replicas - must be less than or equal to the length of
// namesOfKeyfiles.
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

// Create a replica config for the replica with index `replicaId`.
// inputReplicaKeyfile is used to read the keys for this replica, and default
// values are loaded for non-cryptographic configuration parameters.
void getReplicaConfig(uint16_t replicaId, ReplicaConfig* outConfig) {
  
  std::string keyfileName = namesOfKeyfiles[replicaId];
  std::ifstream keyfile(keyfileName);
  if (!keyfile.is_open()) {
    throw std::runtime_error("Unable to read replica keyfile.");
  }
  
  bool succ = inputReplicaKeyfile(keyfile, keyfileName, *outConfig);
  if (!succ)
    throw std::runtime_error("Unable to parse replica keyfile.");

  // set non-cryptographic configuration

  outConfig->numOfClientProxies = numOfClientProxies;
  outConfig->statusReportTimerMillisec = 2000;
  outConfig->concurrencyLevel = 1;
  outConfig->autoViewChangeEnabled = false;
  outConfig->viewChangeTimerMillisec = 60000;
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

// Create a UDP communication configuration for the node (replica or client)
// with index `id`.
PlainTcpConfig getTCPConfig(uint16_t id) {
  std::string ip = "127.0.0.1";
  uint16_t port = basePort + id*2;
  uint32_t bufLength = 64000;

  // Create a map of where the port for each node is.
  std::unordered_map <NodeNum, NodeInfo> nodes;
  for (int i = 0; i < (numOfReplicas + numOfClientProxies); i++)
    nodes.insert({
                     i,
                     NodeInfo{ip, (uint16_t)(basePort + i*2), i < numOfReplicas} });

  PlainTcpConfig retVal(ip, port, bufLength, nodes, numOfReplicas -1, id);
  return retVal;
}
