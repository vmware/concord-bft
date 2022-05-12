// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#define CONCORD_BFT_TESTING

#include "communication/CommDefs.hpp"
#include "communication/CommFactory.hpp"
#include "diagnostics.h"
#include <gtest/gtest.h>

using namespace bft::communication;
using namespace std;

namespace {

const string host = "128.0.0.1";
const uint16_t port = 0;
const uint32_t bufLength = 1024;
const int32_t maxServerId = 20;
const uint32_t clientsNum = 5;
const uint32_t replicasNum = 4;
const uint32_t clientServiceNum = 55;
const string certRootPath = " ";
const string cipherSuite = " ";

TlsMultiplexCommunication* communicatorPtr = nullptr;

class DummyReceiver : public IReceiver {
 public:
  virtual ~DummyReceiver() = default;

  void onNewMessage(const NodeNum sourceNode,
                    const char* const message,
                    const size_t messageLength,
                    NodeNum endpointNum) override {}
  void onConnectionStatusChanged(const NodeNum node, const ConnectionStatus newStatus) override {}
};

shared_ptr<DummyReceiver> msgsReceiver = make_shared<DummyReceiver>();

tuple<unordered_map<NodeNum, NodeInfo>, unordered_map<NodeNum, NodeNum>> createClientNodesConfig() {
  unordered_map<NodeNum, NodeInfo> nodes;
  unordered_map<NodeNum, NodeNum> endpointIdToNodeIdMap;
  NodeInfo replicaInfo{host, port, true};
  for (uint32_t i = 0; i < replicasNum; i++) {
    nodes[i] = replicaInfo;
    endpointIdToNodeIdMap[i] = i;
  }
  return make_tuple(nodes, endpointIdToNodeIdMap);
}

void setUpCommunicationOnReplica(NodeNum selfId, shared_ptr<IReceiver> receiver) {
  delete communicatorPtr;
  auto [nodes, endpointIdToNodeIdMap] = createClientNodesConfig();
  NodeInfo clientInfo{host, port, false};
  for (uint32_t i = replicasNum; i < replicasNum + clientsNum; i++) {
    nodes[i] = clientInfo;
    endpointIdToNodeIdMap[i] = clientServiceNum;
  }
  auto configuration = TlsMultiplexConfig(
      host, port, bufLength, nodes, maxServerId, selfId, certRootPath, cipherSuite, false, endpointIdToNodeIdMap);
  ASSERT_TRUE(configuration.amIReplica_);
  int clients = 0;
  int replicas = 0;
  for (auto const& node : configuration.nodes_) {
    if (node.second.isReplica) {
      ASSERT_FALSE(configuration.isClient(node.first));
      replicas++;
    } else {
      ASSERT_TRUE(configuration.isClient(node.first));
      clients++;
    }
  }
  ASSERT_EQ(clients, clientsNum);
  ASSERT_EQ(replicas, replicasNum);
  communicatorPtr = dynamic_cast<TlsMultiplexCommunication*>(CommFactory::create(configuration));
  communicatorPtr->setReceiver(selfId, receiver.get());
}

void setUpCommunicationOnClient(NodeNum selfId, shared_ptr<IReceiver> receiver) {
  delete communicatorPtr;
  auto [nodes, endpointIdToNodeIdMap] = createClientNodesConfig();
  auto configuration = TlsMultiplexConfig(
      host, port, bufLength, nodes, maxServerId, selfId, certRootPath, cipherSuite, false, endpointIdToNodeIdMap);
  for (auto const& node : configuration.nodes_) ASSERT_TRUE(node.second.isReplica);
  ASSERT_FALSE(configuration.amIReplica_);
  communicatorPtr = dynamic_cast<TlsMultiplexCommunication*>(CommFactory::create(configuration));
  communicatorPtr->setReceiver(selfId, receiver.get());
}

void clearDiagnosticsHandlers() {
  auto& registrar = concord::diagnostics::RegistrarSingleton::getInstance();
  registrar.perf.clear();
  registrar.status.clear();
}

TEST(TlsMultiplexCommunication, verifyConfigurationParameters) {
  const NodeNum myReplicaId = 1;
  setUpCommunicationOnReplica(myReplicaId, msgsReceiver);
  const NodeNum destNode = 2;
  const NodeNum clientEndpoint = 8;
  ASSERT_EQ(communicatorPtr->getConnectionByEndpointNum(destNode, clientEndpoint), clientServiceNum);
  const NodeNum replicaEndpoint = 2;
  ASSERT_EQ(communicatorPtr->getConnectionByEndpointNum(destNode, replicaEndpoint), replicaEndpoint);
  ASSERT_EQ(communicatorPtr->getConnectionByEndpointNum(MAX_ENDPOINT_NUM, replicaEndpoint), replicaEndpoint);

  NodeNum myClientId = 8;
  setUpCommunicationOnClient(myClientId, msgsReceiver);
  ASSERT_NE(communicatorPtr->getConnectionByEndpointNum(destNode, clientEndpoint), clientServiceNum);
  ASSERT_EQ(communicatorPtr->getConnectionByEndpointNum(destNode, replicaEndpoint), replicaEndpoint);
  clearDiagnosticsHandlers();
}

}  // end namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  logging::initLogger("logging.properties");
  int res = RUN_ALL_TESTS();
  return res;
}
