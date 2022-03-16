// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
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

#include "config/test_comm_config.hpp"
#include "config/test_parameters.hpp"
#include "communication/CommFactory.hpp"
#include "bftclient/config.h"
#include "bftclient/bft_client.h"
#include "utt_messages.cmf.hpp"

#include "app_state.hpp"

using namespace bftEngine;
using namespace bft::communication;
using std::string;
using namespace utt::messages;
using namespace bft::client;

uint64_t nextSeqNum() {
  static uint64_t nextSeqNum = 1;
  return nextSeqNum++;
}

std::vector<uint8_t> StrToBytes(const std::string& str) { return std::vector<uint8_t>(str.begin(), str.end()); }

std::string BytesToStr(const std::vector<uint8_t>& bytes) { return std::string{bytes.begin(), bytes.end()}; }

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

auto logger = logging::getLogger("uttdemo.client");

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

int main(int argc, char** argv) {
  try {
    logging::initLogger("logging.properties");
    ClientParams clientParams = setupClientParams(argc, argv);

    if (clientParams.clientId == UINT16_MAX || clientParams.numOfFaulty == UINT16_MAX ||
        clientParams.numOfSlow == UINT16_MAX || clientParams.numOfOperations == UINT32_MAX) {
      std::cout << "Wrong usage! Required parameters: " << argv[0] << " -f F -c C -p NUM_OPS -i ID";
      exit(-1);
    }

    SharedCommPtr comm = SharedCommPtr(setupCommunicationParams(clientParams));

    ClientConfig clientConfig;
    clientConfig.f_val = clientParams.numOfFaulty;
    for (uint16_t i = 0; i < clientParams.numOfReplicas; ++i) clientConfig.all_replicas.emplace(ReplicaId{i});
    clientConfig.id = ClientId{clientParams.clientId};

    Client client(comm, clientConfig);

    AppState state;

    while (true) {
      std::cout << "\nEnter command (type 'h' for commands, 'q' to exit):\n";
      std::string cmd;
      std::getline(std::cin, cmd);
      try {
        if (std::cin.eof() || cmd == "q") {
          std::cout << "Quit!\n";
          client.stop();
          return 0;
        } else if (cmd == "h") {
          std::cout << "list of commands is empty (NYI)\n";
        } else if (cmd == "balance") {
          for (const auto& kvp : state.accounts_) {
            const auto& acc = kvp.second;
            std::cout << acc.getId() << " : " << acc.getBalancePublic() << '\n';
          }
        } else if (cmd == "ledger") {
          // ToDo: check for new blocks

          // ToDo: send last block request

          // ToDo: fetch missing blocks
          state.printLedger();
        } else if (auto tx = parseTx(cmd)) {
          state.validateTx(*tx);
          // ToDo: send client request
          state.executeNextTx(*tx);
          std::cout << "Ok\n";
        } else {
          std::cout << "Unknown command '" << cmd << "'\n";
        }
      } catch (const bft::client::TimeoutException& e) {
        std::cout << "Request timeout: " << e.what() << '\n';
      } catch (const std::domain_error& e) {
        std::cout << "Validation error: " << e.what() << '\n';
      }
    }
  } catch (const std::exception& e) {
    std::cout << "Exception: " << e.what() << '\n';
  }
}
