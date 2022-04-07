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

#include "app_state.hpp"
#include "utt_config.hpp"

#include <utt/Client.h>

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

void initAccounts(AppState& state) {
  AppState::initUTTLibrary();

  if (!state.GetAccounts().empty()) throw std::runtime_error("Accounts already exist");
  for (int i = 1; i <= 3; ++i) {
    const std::string fileName = "utt_client_" + std::to_string(i);
    std::ifstream ifs(fileName);
    if (!ifs.is_open()) throw std::runtime_error("Missing config: " + fileName);

    UTTClientConfig cfg;
    ifs >> cfg;

    std::cout << "Successfully loaded UTT wallet '" << cfg.wallet_.getUserPid() << "'\n";

    state.addAccount(Account{std::move(cfg.wallet_)});
  }
}

TxReply sendTxRequest(Client& client, const Tx& tx) {
  UTTRequest req;

  if (const auto* uttTransfer = std::get_if<TxUttTransfer>(&tx)) {
    std::stringstream ss;
    ss << uttTransfer->uttTx_;
    UttTx uttTx;
    uttTx.tx = ss.str();
    req.request = std::move(uttTx);
  } else {
    std::stringstream ss;
    ss << tx;
    PublicTx publicTx;
    publicTx.tx = ss.str();
    req.request = std::move(publicTx);
  }

  // Ensure we only wait for F+1 replies (ByzantineSafeQuorum)
  WriteConfig writeConf{RequestConfig{false, nextSeqNum()}, ByzantineSafeQuorum{}};

  Msg reqBytes;
  serialize(reqBytes, req);
  auto replyBytes = client.send(writeConf, std::move(reqBytes));  // Sync send

  UTTReply reply;
  deserialize(replyBytes.matched_data, reply);

  const auto& txReply = std::get<TxReply>(reply.reply);  // throws if unexpected variant
  std::cout << "Got TxReply, success=" << txReply.success << " last_block_id=" << txReply.last_block_id << '\n';

  return txReply;
}

GetLastBlockReply sendGetLastBlockRequest(Client& client) {
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

  return lastBlockReply;
}

GetBlockDataReply sendGetBlockDataRequest(Client& client, BlockId blockId, ReplicaSigShares& outSigShares) {
  GetBlockDataRequest blockDataReq;
  blockDataReq.block_id = blockId;

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

  // Deserialize sign shares
  // [TODO-UTT]: Need to collect F+1 (out of 2F+1) shares that combine to a valid RandSig
  // Here, we assume naively that all replicas are honest
  for (const auto& kvp : replyBytes.rsi) {
    outSigShares.signerIds_.emplace_back(kvp.first.val);  // ReplicaId

    std::stringstream ss(BytesToStr(kvp.second));
    size_t size = 0;  // The size reflects the number of output coins
    ss >> size;
    // Add this replica share to the list for i-th coin (the order is defined by signerIds)
    for (size_t i = 0; i < size; ++i) {
      outSigShares.sigShares_[i].emplace_back(libutt::RandSigShare(ss));
    }
  }

  return blockDataReply;
}

// Sync state by fetching missing blocks and executing them
void syncState(AppState& state, Client& client) {
  std::cout << "Sync state...\n";

  auto lastBlockReply = sendGetLastBlockRequest(client);
  state.setLastKnownBlockId(lastBlockReply.last_block_id);

  // Sync missing blocks
  auto missingBlockId = state.executeBlocks();
  while (missingBlockId) {
    // Request missing block
    ReplicaSigShares sigShares;
    auto blockDataReply = sendGetBlockDataRequest(client, *missingBlockId, sigShares);

    if (blockDataReply.block_id != *missingBlockId) throw std::runtime_error("Received missing block id differs!");

    std::optional<Tx> tx;
    if (const auto* uttTx = std::get_if<UttTx>(&blockDataReply.tx)) {
      std::stringstream ss(uttTx->tx);
      tx = TxUttTransfer(libutt::Tx(ss), std::move(sigShares));
    } else if (const auto* publicTx = std::get_if<PublicTx>(&blockDataReply.tx)) {
      tx = parsePublicTx(publicTx->tx);
      if (!tx) throw std::runtime_error("Failed to parse public tx from missing block!");
    } else {
      throw std::runtime_error("Unhandled tx type in GetBlockDataReply! blockId=" +
                               std::to_string(blockDataReply.block_id));
    }

    state.appendBlock(Block{std::move(*tx)});
    missingBlockId = state.executeBlocks();
  }
}

struct UttPayment {
  UttPayment(Account& from, const Account& to, size_t payment) : from_(from), to_(to), payment_(payment) {}

  Account& from_;
  const Account& to_;
  size_t payment_;
};

std::optional<UttPayment> createUttPayment(const std::string& cmd, AppState& state) {
  std::vector<std::string> tokens;
  std::string token;
  std::stringstream ss(cmd);
  while (std::getline(ss, token, ' ')) tokens.emplace_back(std::move(token));

  if (tokens.size() == 4 && tokens[0] == "utt") {
    const auto& from = tokens[1];
    const auto& to = tokens[2];
    int payment = std::atoi(tokens[3].c_str());
    if (payment <= 0) throw std::domain_error("utt payment amount must be positive!");

    // [TODO-UTT] This logic will be modified once we have a separate payment process and an account process.
    // The utt transaction will be made from the point of view of the account and it will only know
    // that the receiving pid exists (is previously registered by the registry authority)
    auto* fromAccount = state.getAccountById(from);
    if (!fromAccount) throw std::domain_error("utt sender account not found!");
    const auto* toAccount = state.getAccountById(to);
    if (!toAccount) throw std::domain_error("utt receiver account not found!");

    if (fromAccount->getUttBalance() < payment) throw std::domain_error("insufficient balance for utt payment!");
    if (fromAccount->getUttBudget() < payment)
      throw std::domain_error("insufficient anonymous budget for utt payment!");

    return UttPayment(*fromAccount, *toAccount, payment);
  }

  return std::nullopt;
}

int main(int argc, char** argv) {
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

  try {
    initAccounts(state);

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
          std::cout << "\nCommands:\n";
          std::cout << "balance\t\t\t\t-- print all account's public and utt balances\n";
          std::cout << "ledger\t\t\t\t-- print the transactions that happened on the blockchain\n";
          std::cout << "deposit [account] [amount]\t-- public money deposit to account\n";
          std::cout << "withdraw [account] [amount]\t-- public money withdraw from account\n";
          std::cout << "transfer [from] [to] [amount]\t-- public money transfer\n";
          std::cout << "utt [from] [to] [amount]\t-- anonymous money transfer\n";
        } else if (cmd == "balance") {
          syncState(state, client);
          for (const auto& kvp : state.GetAccounts()) {
            const auto& acc = kvp.second;
            std::cout << acc.getId() << " | public: " << acc.getPublicBalance();
            std::cout << " utt: " << acc.getUttBalance() << '\n';
          }
        } else if (cmd == "ledger") {
          syncState(state, client);
          for (const auto& block : state.GetBlocks()) {
            std::cout << block << '\n';
          }
        } else if (cmd == "checkpoint") {
          for (int i = 0; i < 150; ++i) {
            auto reply = sendTxRequest(client, TxPublicDeposit("user_1", 1));
            if (reply.success) {
              state.setLastKnownBlockId(reply.last_block_id);
            } else {
              std::cout << "Checkpoint transaction " << (i + 1) << " failed: " << reply.err << '\n';
              break;
            }
          }
        } else if (auto uttPayment = createUttPayment(cmd, state)) {
          std::cout << "Starting a UTT payment from " << uttPayment->from_.getId();
          std::cout << " to " << uttPayment->to_.getId() << " for " << uttPayment->payment_;
          std::cout << " ...\n";

          // [TODO-UTT] check if this is a coin merge and repeat
          auto wallet = uttPayment->from_.getWallet();
          ConcordAssert(wallet != nullptr);

          auto uttTx = libutt::Client::createTxForPayment(*wallet, uttPayment->to_.getId(), uttPayment->payment_);

          Tx tx = TxUttTransfer(std::move(uttTx));

          auto reply = sendTxRequest(client, tx);
          if (reply.success) {
            state.setLastKnownBlockId(reply.last_block_id);
            std::cout << "Ok.\n";
          } else {
            std::cout << "Transaction failed: " << reply.err << '\n';
          }

          // We need to sync the state so we obtain any merged coins
          syncState(state, client);

        } else if (auto tx = parsePublicTx(cmd)) {
          auto reply = sendTxRequest(client, *tx);
          if (reply.success) {
            state.setLastKnownBlockId(reply.last_block_id);
            std::cout << "Ok.\n";
          } else {
            std::cout << "Transaction failed: " << reply.err << '\n';
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
    client.stop();
    std::cout << "Exception: " << e.what() << '\n';
  }
}
