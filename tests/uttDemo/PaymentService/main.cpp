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

  SharedCommPtr comm = SharedCommPtr(setupCommunicationParams(clientParams));

  ClientConfig clientConfig;
  clientConfig.f_val = clientParams.numOfFaulty;
  for (uint16_t i = 0; i < clientParams.numOfReplicas; ++i) clientConfig.all_replicas.emplace(ReplicaId{i});
  clientConfig.id = ClientId{clientParams.clientId};

  Client client(comm, clientConfig);

  // [TODO-UTT] Listen on known port for messages
  // and pass to the appropriate bft client depending on the account id
}
