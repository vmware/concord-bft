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
#include "skvbc_messages.cmf.hpp"

#include "app_state.hpp"

using namespace bftEngine;
using namespace bft::communication;
using std::string;
using namespace skvbc::messages;
using namespace bft::client;

uint64_t nextSeqNum() {
  static uint64_t nextSeqNum = 1;
  return nextSeqNum++;
}

std::vector<uint8_t> StrToBytes(const std::string& str) { return std::vector<uint8_t>(str.begin(), str.end()); }

std::string BytesToStr(const std::vector<uint8_t>& bytes) { return std::string{bytes.begin(), bytes.end()}; }

void SimpleSKVBCTest(bft::client::Client& client) {
  std::vector<std::pair<std::string, std::string>> state;
  state.emplace_back("test-key-1", "test-value-1");
  state.emplace_back("test-key-2", "test-value-2");
  state.emplace_back("test-key-3", "test-value-3");

  // Write test key-value pairs
  {
    SKVBCWriteRequest writeReq;
    // The expected values by the execution engine in the kv pair are strings
    for (int i = 0; i < (int)state.size(); ++i)
      writeReq.writeset.emplace_back(StrToBytes(state[i].first), StrToBytes(state[i].second));

    SKVBCRequest req;
    req.request = std::move(writeReq);

    // Ensure we only wait for F+1 replies (ByzantineSafeQuorum)
    WriteConfig writeConf{RequestConfig{false, nextSeqNum()}, ByzantineSafeQuorum{}};

    Msg reqBytes;
    serialize(reqBytes, req);
    auto replyBytes = client.send(writeConf, std::move(reqBytes));  // Sync send

    SKVBCReply reply;
    deserialize(replyBytes.matched_data, reply);

    const auto& writeReply = std::get<SKVBCWriteReply>(reply.reply);  // throws if unexpected variant
    std::cout << "Got SKVBCWriteReply, success=" << writeReply.success << " latest_block=" << writeReply.latest_block
              << '\n';

    if (!writeReply.success) throw std::runtime_error("Failed to write values to the BC!\n");
  }

  // Read back the account balances
  {
    SKVBCReadRequest readReq;
    readReq.read_version = 1;
    // The expected values by the execution engine in the kv pair are strings
    for (const auto& kvp : state) readReq.keys.emplace_back(StrToBytes(kvp.first));

    SKVBCRequest req;
    req.request = std::move(readReq);

    // Ensure we wait for 2F+1 replies
    ReadConfig readConf{RequestConfig{false, nextSeqNum()}, LinearizableQuorum{}};

    Msg reqBytes;
    serialize(reqBytes, req);
    auto replyBytes = client.send(readConf, std::move(reqBytes));  // Sync send

    SKVBCReply reply;
    deserialize(replyBytes.matched_data, reply);

    const auto& readReply = std::get<SKVBCReadReply>(reply.reply);  // throws if unexpected variant
    std::cout << "Got SKVBCReadReply with reads:\n";

    for (const auto& kvp : readReply.reads) {
      auto key = BytesToStr(kvp.first);
      auto value = BytesToStr(kvp.second);

      std::cout << '\t' << key << " : " << value << '\n';

      // Assert we got the same values that we wrote
      auto it = std::find_if(state.begin(), state.end(), [&key](const std::pair<std::string, std::string>& kvp) {
        return kvp.first == key;
      });

      ConcordAssert(it != state.end());
      ConcordAssert(it->second == value);
    }
  }
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
        } else if (cmd == "test") {
          SimpleSKVBCTest(client);
        } else if (cmd == "ledger") {
          // ToDo: check for new blocks

          // ToDo: send last block request

          // ToDo: fetch missing blocks
          state.printLedger();
        } else if (auto tx = parseTx(cmd)) {
          std::cout << "Successfully parsed transaction: " << *tx << '\n';
          std::cout << "Transaction is valid: " << state.validateTx(*tx) << '\n';

          // ToDo: send client request

          // ToDo: execute transaction

        } else {
          std::cout << "Unknown command '" << cmd << "'\n";
        }
      } catch (const bft::client::TimeoutException& e) {
        std::cout << "Request timeout: " << e.what() << '\n';
      }
    }
  } catch (const std::exception& e) {
    std::cout << "Exception: " << e.what() << '\n';
  }
}
