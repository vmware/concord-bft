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

// Sync state by fetching missing blocks and executing them
void syncState(AppState& state, Client& client) {
  std::cout << "Sync state...\n";

  // Request last block id
  {
    UTTRequest req;
    req.request = GetLastBlockRequest();
    ReadConfig readConf{RequestConfig{false, nextSeqNum()}, LinearizableQuorum{}};

    Msg reqBytes;
    serialize(reqBytes, req);
    auto replyBytes = client.send(readConf, std::move(reqBytes));  // Sync send

    UTTReply reply;
    deserialize(replyBytes.matched_data, reply);

    const auto& lastBlockReply = std::get<GetLastBlockReply>(reply.reply);  // throws if unexpected variant
    std::cout << "Got GetLastBlockReply, last_block_id=" << lastBlockReply.last_block_id << '\n';

    state.setLastKnownBlockId(lastBlockReply.last_block_id);
  }

  // Sync missing blocks
  auto missingBlockId = state.sync();
  while (missingBlockId) {
    // Request missing block
    GetBlockDataRequest blockDataReq;
    blockDataReq.block_id = *missingBlockId;

    UTTRequest req;
    req.request = std::move(blockDataReq);
    ReadConfig readConf{RequestConfig{false, nextSeqNum()}, LinearizableQuorum{}};

    Msg reqBytes;
    serialize(reqBytes, req);
    auto replyBytes = client.send(readConf, std::move(reqBytes));  // Sync send

    UTTReply reply;
    deserialize(replyBytes.matched_data, reply);

    const auto& blockDataReply = std::get<GetBlockDataReply>(reply.reply);  // throws if unexpected variant
    std::cout << "Got GetBlockDataReply, block_id=" << blockDataReply.block_id << '\n';

    if (blockDataReply.id != *missingBlockId) throw std::runtime_error("Received missing block id differs!");
    auto tx = parseTx(BytesToStr(blockDataReply.tx));
    if (!tx) throw std::runtime_error("Failed to parse tx from missing block!");

    state.appendBlock(Block{std::move(*tx)});
    missingBlockId = state.sync();
  }
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
          syncState(state, client);
          for (const auto& kvp : state.GetAccounts()) {
            const auto& acc = kvp.second;
            std::cout << acc.getId() << " : " << acc.getBalancePublic() << '\n';
          }
        } else if (cmd == "ledger") {
          syncState(state, client);
          for (const auto& block : state.GetBlocks()) {
            std::cout << block << '\n';
          }
        } else if (auto tx = parseTx(cmd)) {
          state.validateTx(*tx);

          // Send transaction request
          TxRequest txReq;
          txReq.tx = StrToBytes(cmd);

          UTTRequest req;
          req.request = std::move(txReq);

          // Ensure we only wait for F+1 replies (ByzantineSafeQuorum)
          WriteConfig writeConf{RequestConfig{false, nextSeqNum()}, ByzantineSafeQuorum{}};

          Msg reqBytes;
          serialize(reqBytes, req);
          auto replyBytes = client.send(writeConf, std::move(reqBytes));  // Sync send

          UTTReply reply;
          deserialize(replyBytes.matched_data, reply);

          const auto& txReply = std::get<TxReply>(reply.reply);  // throws if unexpected variant
          std::cout << "Got TxReply, success=" << txReply.success << " last_block_id=" << txReply.last_block_id << '\n';

          if (txReply.success) {
            state.setLastKnownBlockId(txReply.last_block_id);
            std::cout << "Ok.\n";
          } else {
            std::cout << "Failed to execute transaction!\n";
          }
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
