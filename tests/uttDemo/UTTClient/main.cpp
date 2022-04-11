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

/////////////////////////////////////////////////////////////////////////////////////////////////////
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

/////////////////////////////////////////////////////////////////////////////////////////////////////
TxReply sendTxRequest(Client& client, const Tx& tx) {
  // [TODO-UTT] Debug output
  if (const auto* txUtt = std::get_if<TxUtt>(&tx)) {
    std::cout << "Sending UTT Tx " << txUtt->utt_.getHashHex() << '\n';
  }

  std::stringstream ss;
  ss << tx;

  TxRequest txRequest;
  txRequest.tx = ss.str();

  UTTRequest req;
  req.request = std::move(txRequest);

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

/////////////////////////////////////////////////////////////////////////////////////////////////////
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

/////////////////////////////////////////////////////////////////////////////////////////////////////
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
  std::cout << "Got GetBlockDataReply, success=" << blockDataReply.success << " block_id=" << blockDataReply.block_id
            << '\n';

  if (!blockDataReply.success) {
    return blockDataReply;
  }

  // Deserialize sign shares
  // [TODO-UTT]: Need to collect F+1 (out of 2F+1) shares that combine to a valid RandSig
  // Here, we assume naively that all replicas are honest

  for (const auto& kvp : replyBytes.rsi) {
    if (outSigShares.signerIds_.size() == 2) break;  // Pick first F+1 signers

    outSigShares.signerIds_.emplace_back(kvp.first.val);  // ReplicaId

    std::stringstream ss(BytesToStr(kvp.second));
    size_t size = 0;  // The size reflects the number of output coins
    ss >> size;
    ss.ignore(1, '\n');  // skip newline

    ConcordAssert(size <= 3);
    // Add this replica share to the list for i-th coin (the order is defined by signerIds)
    for (size_t i = 0; i < size; ++i) {
      if (outSigShares.sigShares_.size() == i)  // Resize to accommodate up to 3 coins
        outSigShares.sigShares_.emplace_back();

      outSigShares.sigShares_[i].emplace_back(libutt::RandSigShare(ss));
    }
  }

  ConcordAssert(outSigShares.signerIds_.size() == 2);
  for (size_t i = 0; i < outSigShares.sigShares_.size(); ++i)  // Check for each coin we have F+1 signers
    ConcordAssert(outSigShares.signerIds_.size() == outSigShares.sigShares_[i].size());

  return blockDataReply;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
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
    const auto replyBlockId = blockDataReply.block_id;

    if (!blockDataReply.success) throw std::runtime_error("Requested block does not exist!");

    if (replyBlockId != *missingBlockId) throw std::runtime_error("Requested missing block id differs from reply!");

    auto tx = parseTx(blockDataReply.tx);
    if (!tx) throw std::runtime_error("Failed to parse tx for block " + std::to_string(replyBlockId));

    if (auto* txUtt = std::get_if<TxUtt>(&(*tx))) {
      // [TODO-UTT] Debug output
      std::cout << "Received UTT Tx " << txUtt->utt_.getHashHex() << " for block " << replyBlockId << '\n';

      // Add sig shares
      txUtt->sigShares_ = std::move(sigShares);
    }

    state.appendBlock(Block{std::move(*tx)});
    missingBlockId = state.executeBlocks();
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct UttPayment {
  UttPayment(Account& from, const Account& to, size_t amount) : from_(from), to_(to), amount_(amount) {}

  Account& from_;
  const Account& to_;
  size_t amount_;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
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

    return UttPayment(*fromAccount, *toAccount, payment);
  }

  return std::nullopt;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void printHelp() {
  std::cout << "\nCommands:\n";
  std::cout << "balance\t\t\t\t-- print all account's public and utt balances\n";
  std::cout << "ledger\t\t\t\t-- print the transactions that happened on the blockchain\n";
  std::cout << "deposit [account] [amount]\t-- public money deposit to account\n";
  std::cout << "withdraw [account] [amount]\t-- public money withdraw from account\n";
  std::cout << "transfer [from] [to] [amount]\t-- public money transfer\n";
  std::cout << "utt [from] [to] [amount]\t-- anonymous money transfer\n";
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void checkBalance(AppState& state, Client& client) {
  syncState(state, client);
  std::cout << '\n';
  for (const auto& kvp : state.GetAccounts()) {
    const auto& acc = kvp.second;
    std::cout << acc.getId() << " | public: " << acc.getPublicBalance();
    std::cout << " utt: " << acc.getUttBalance() << " / " << acc.getUttBudget() << '\n';
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void checkLedger(AppState& state, Client& client) {
  syncState(state, client);
  std::cout << '\n';
  for (const auto& block : state.GetBlocks()) {
    std::cout << block << '\n';
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void runUttPayment(const UttPayment& payment, AppState& state, Client& client) {
  std::cout << "Running a UTT payment from " << payment.from_.getId();
  std::cout << " to " << payment.to_.getId() << " for " << payment.amount_ << '\n';

  // Precondition: a valid UTT payment
  auto wallet = payment.from_.getWallet();
  ConcordAssert(wallet != nullptr);

  while (true) {
    // We do a sync before each attempt to do a utt payment since only the client
    // knows if it has the right coins to do it.
    // This also handles the case where we do repeated splits or merges to arrive at a final payment
    // and need to obtain the resulting coins before we proceed.
    syncState(state, client);

    // Check that we can still do the payment and this holds:
    // payment <= balance && payment <= budget
    if (payment.from_.getUttBalance() < payment.amount_)
      throw std::domain_error("Insufficient balance for utt payment!");
    if (payment.from_.getUttBudget() < payment.amount_)
      throw std::domain_error("Insufficient anonymous budget for utt payment!");

    auto uttTx = libutt::Client::createTxForPayment(*wallet, payment.to_.getId(), payment.amount_);

    // We assume that any tx with a budget coin must be an actual payment
    // and not a coin split or merge.
    // We assume that any valid utt payment terminates with a paying transaction.
    const bool isPayment = uttTx.isBudgeted();

    Tx tx = TxUtt(std::move(uttTx));

    auto reply = sendTxRequest(client, tx);
    if (reply.success) {
      state.setLastKnownBlockId(reply.last_block_id);
      std::cout << "Ok.\n";
    } else {
      std::cout << "Transaction failed: " << reply.err << '\n';
      break;  // Stop payment if we encounter an error
    }

    if (isPayment) {
      std::cout << "Payment completed.\n";
      break;  // Done
    }
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void sendPublicTx(const Tx& tx, AppState& state, Client& client) {
  auto reply = sendTxRequest(client, tx);
  if (reply.success) {
    state.setLastKnownBlockId(reply.last_block_id);
    std::cout << "Ok.\n";
  } else {
    std::cout << "Transaction failed: " << reply.err << '\n';
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void dbgForceCheckpoint(AppState& state, Client& client) {
  // [TODO-UTT] Pick the user and the number of txs
  for (int i = 0; i < 150; ++i) {
    auto reply = sendTxRequest(client, TxPublicDeposit("user_1", 1));
    if (reply.success) {
      state.setLastKnownBlockId(reply.last_block_id);
    } else {
      std::cout << "Checkpoint transaction " << (i + 1) << " failed: " << reply.err << '\n';
      break;
    }
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
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
          printHelp();
        } else if (cmd == "balance") {
          checkBalance(state, client);
        } else if (cmd == "ledger") {
          checkLedger(state, client);
        } else if (cmd == "checkpoint") {
          dbgForceCheckpoint(state, client);
        } else if (auto uttPayment = createUttPayment(cmd, state)) {
          runUttPayment(*uttPayment, state, client);
        } else if (auto tx = parseTx(cmd)) {
          sendPublicTx(*tx, state, client);
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
