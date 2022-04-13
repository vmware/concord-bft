// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <iostream>
#include <fstream>
#include <thread>

#include "config/test_comm_config.hpp"
#include "config/test_parameters.hpp"
#include "communication/CommFactory.hpp"
#include "bftclient/config.h"
#include "bftclient/bft_client.h"
#include "bftclient/seq_num_generator.h"
#include "utt_messages.cmf.hpp"

using namespace bftEngine;
using namespace bft::communication;
using std::string;
using namespace utt::messages;
using namespace bft::client;

uint64_t nextSeqNum() {
  static SeqNumberGenerator gen{ClientId{0}};  // ClientId used just for logging
  return gen.unique();
}

ClientParams setupClientParams(int argc, char** argv) {
  ClientParams clientParams;
  clientParams.clientId = UINT16_MAX;
  clientParams.numOfFaulty = UINT16_MAX;
  clientParams.numOfSlow = UINT16_MAX;
  clientParams.numOfOperations = UINT16_MAX;
  char argTempBuffer[PATH_MAX + 10];
  int o = 0;
  while ((o = getopt(argc, argv, "i:f:c:p:n:")) != EOF) {
    switch (o) {
      case 'i': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        string idStr = argTempBuffer;
        int tempId = std::stoi(idStr);
        if (tempId >= 0 && tempId < UINT16_MAX) clientParams.clientId = (uint16_t)tempId;
      } break;

      case 'f': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        string fStr = argTempBuffer;
        int tempfVal = std::stoi(fStr);
        if (tempfVal >= 1 && tempfVal < UINT16_MAX) clientParams.numOfFaulty = (uint16_t)tempfVal;
      } break;

      case 'c': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        string cStr = argTempBuffer;
        int tempcVal = std::stoi(cStr);
        if (tempcVal >= 0 && tempcVal < UINT16_MAX) clientParams.numOfSlow = (uint16_t)tempcVal;
      } break;

      case 'p': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        string numOfOpsStr = argTempBuffer;
        uint32_t tempPVal = std::stoul(numOfOpsStr);
        if (tempPVal >= 1 && tempPVal < UINT32_MAX) clientParams.numOfOperations = tempPVal;
      } break;

      case 'n': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        clientParams.configFileName = argTempBuffer;
      } break;

      default:
        break;
    }
  }
  return clientParams;
}

auto logger = logging::getLogger("uttdemo.payment-service");

/////////////////////////////////////////////////////////////////////////////////////////////////////
ICommunication* setupCommunicationParams(ClientParams& cp) {
  TestCommConfig testCommConfig(logger);
  uint16_t numOfReplicas = cp.get_numOfReplicas();
#ifdef USE_COMM_PLAIN_TCP
  PlainTcpConfig conf =
      testCommConfig.GetTCPConfig(false, cp.clientId, cp.numOfClients, numOfReplicas, cp.configFileName);
#elif USE_COMM_TLS_TCP
  TlsTcpConfig conf =
      testCommConfig.GetTlsTCPConfig(false, cp.clientId, cp.numOfClients, numOfReplicas, cp.configFileName);
#else
  PlainUdpConfig conf =
      testCommConfig.GetUDPConfig(false, cp.clientId, cp.numOfClients, numOfReplicas, cp.configFileName);
#endif

  return CommFactory::create(conf);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
std::unique_ptr<ICommunication> setupCommunicationForPaymetService() {
  // [TODO-UTT] Support for other communcation modes like TCP/TLS (if needed)

  // PaymentService 1
  std::string listenAddr = "127.0.0.1";
  uint64_t listenPort = 3720;
  int32_t msgMaxSize = 128 * 1024;  // 128 kB -- Same as TestCommConfig
  NodeNum selfId = 0;               // The payment service is always node 0 from the point of view of the wallets

  std::unordered_map<NodeNum, NodeInfo> walletNodes;
  walletNodes.emplace(1, NodeInfo{"127.0.0.1", 3722, false});  // Wallet 1
  walletNodes.emplace(2, NodeInfo{"127.0.0.1", 3724, false});  // Wallet 2
  walletNodes.emplace(3, NodeInfo{"127.0.0.1", 3726, false});  // Wallet 3

  PlainUdpConfig conf(listenAddr, listenPort, msgMaxSize, walletNodes, selfId);

  return std::unique_ptr<ICommunication>(CommFactory::create(conf));
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
bft::client::Reply sendUTTRequest(Client& client, const UTTRequest& msg) {
  if (std::holds_alternative<TxRequest>(msg.request)) {
    // Send write

    // Ensure we only wait for F+1 replies (ByzantineSafeQuorum)
    WriteConfig writeConf{RequestConfig{false, nextSeqNum()}, ByzantineSafeQuorum{}};

    Msg reqBytes;
    serialize(reqBytes, msg);
    return client.send(writeConf, std::move(reqBytes));  // Sync send

  } else if (std::holds_alternative<GetLastBlockRequest>(msg.request) ||
             std::holds_alternative<GetBlockDataRequest>(msg.request)) {
    // Send read

    ReadConfig readConf{RequestConfig{false, nextSeqNum()}, LinearizableQuorum{}};

    Msg reqBytes;
    serialize(reqBytes, msg);
    return client.send(readConf, std::move(reqBytes));  // Sync send
  }

  throw std::runtime_error("Unhandled UTTRequest type!");
}

class PaymentService : public IReceiver {
 public:
  PaymentService(logging::Logger& logger) : logger_{logger} {}

  // Invoked when a new message is received
  // Notice that the memory pointed by message may be freed immediately
  // after the execution of this method.
  void onNewMessage(NodeNum sourceNode, const char* const message, size_t messageLength, NodeNum endpointNum) override {
    // This is called from the listening thread of the communication
    LOG_INFO(logger_, "onNewMessage from: " << sourceNode << " msgLen: " << messageLength);
  }

  // Invoked when the known status of a connection is changed.
  // For each NodeNum, this method will never be concurrently
  // executed by two different threads.
  void onConnectionStatusChanged(NodeNum node, ConnectionStatus newStatus) override {
    // Not applicable to UDP
    LOG_INFO(logger_, "onConnectionStatusChanged from: " << node << " newStatus: " << (int)newStatus);
  }

 private:
  logging::Logger& logger_;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
int main(int argc, char** argv) {
  logging::initLogger("config/logging.properties");

  // [TODO-UTT] Need to create 3 separate bft clients with ids depending on the id
  // of the payment service.
  // |replicas| + (payment_service_id - 1) * 3 + i

  // Mapping from account id to bft client id in payment services
  // PaymentService 1 | {1, 2, 3} -> {4, 5, 6}
  // PaymentService 2 | {4, 5, 6} -> {7, 8, 9}
  // PaymentService 3 | {7, 8, 9} -> {10, 11, 12}

  ClientParams clientParams = setupClientParams(argc, argv);

  if (clientParams.clientId == UINT16_MAX || clientParams.numOfFaulty == UINT16_MAX) {
    std::cout << "Wrong usage! Required parameters: " << argv[0] << " -f <numFaulty> -i <id>";
    exit(-1);
  }

  SharedCommPtr bftClientComm = SharedCommPtr(setupCommunicationParams(clientParams));

  ClientConfig clientConfig;
  clientConfig.f_val = clientParams.numOfFaulty;
  for (uint16_t i = 0; i < clientParams.numOfReplicas; ++i) clientConfig.all_replicas.emplace(ReplicaId{i});
  clientConfig.id = ClientId{clientParams.clientId};

  Client client(bftClientComm, clientConfig);

  // [TODO-UTT] Pass the payment service id
  auto comm = setupCommunicationForPaymetService();
  if (comm) {
    PaymentService paymentService(logger);

    comm->setReceiver(NodeNum{0}, &paymentService);

    comm->start();

    LOG_INFO(logger, "PaymentService is running...");

    // [TODO-UTT] Some way to break out of the loop
    // Maybe we will wait for a terminate signal since a payment service
    // is always listening for client messages
    while (true) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }

  } else {
    LOG_FATAL(logger, "Failed to create communcation for payment service!");
    return 1;
  }

  comm->stop();

  return 0;
}
