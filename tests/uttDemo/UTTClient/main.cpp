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
// This module creates an instance of ClientImp class using input
// parameters and launches a bunch of tests created by TestsBuilder towards
// concord::consensus::ReplicaImp objects.

#include <cstdio>
#include <cstring>
#include <sys/param.h>
#include <unistd.h>
#include <iostream>

#include "config/test_comm_config.hpp"
#include "config/test_parameters.hpp"
#include "communication/CommFactory.hpp"
#include "bftclient/config.h"
#include "bftclient/bft_client.h"
#include "skvbc_messages.cmf.hpp"

using namespace bftEngine;
using namespace bft::communication;
using std::string;
using namespace skvbc::messages;
using namespace bft::client;

class Account {
 public:
  Account(std::string id) : id_(std::move(id)) {}

  const std::string getId() const { return id_; }

  int getBalancePublic() const { return publicBalance_; }

  void depositPublic(int val) { publicBalance_ += val; }

  int withdrawPublic(int val) {
    val = std::min<int>(publicBalance_, val);
    publicBalance_ -= val;
    return val;
  }

 private:
  std::string id_;
  int publicBalance_ = 0;
  // To-Do: add UTT wallet
};

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

void SimpleSKVBCTest(bft::client::Client& client) {
  std::vector<Account> accounts;
  accounts.emplace_back("Account_1");
  accounts.emplace_back("Account_2");
  accounts.emplace_back("Account_3");

  // Deposit initial balances
  {
    std::vector<int> deposits = {101, 202, 303};

    SKVBCWriteRequest writeReq;
    // The expected values by the execution engine in the kv pair are strings
    for (int i = 0; i < 3; ++i)
      writeReq.writeset.emplace_back(StrToBytes(accounts[i].getId()), StrToBytes(std::to_string(deposits[i])));

    SKVBCRequest req;
    req.request = std::move(writeReq);

    // Ensure we only wait for F+1 replies (ByzantineSafeQuorum)
    WriteConfig writeConf{RequestConfig{false, nextSeqNum()}, ByzantineSafeQuorum{}};
    writeConf.request.timeout = 5000ms;

    Msg reqBytes;
    serialize(reqBytes, req);
    auto replyBytes = client.send(writeConf, std::move(reqBytes));  // Sync send

    SKVBCReply reply;
    deserialize(replyBytes.matched_data, reply);

    const auto& writeReply = std::get<SKVBCWriteReply>(reply.reply);  // throws if unexpected variant
    std::cout << "Got SKVBCWriteReply, success=" << writeReply.success << " latest_block=" << writeReply.latest_block
              << '\n';

    if (writeReply.success) {
      for (int i = 0; i < 3; ++i) accounts[i].depositPublic(deposits[i]);
    } else {
      throw std::runtime_error("Failed to write values to the BC!\n");
    }
  }

  // Read back the account balances
  {
    SKVBCReadRequest readReq;
    readReq.read_version = 1;
    // The expected values by the execution engine in the kv pair are strings
    for (const auto& account : accounts) readReq.keys.emplace_back(StrToBytes(account.getId()));

    SKVBCRequest req;
    req.request = std::move(readReq);

    // Ensure we wait for 2F+1 replies
    ReadConfig readConf{RequestConfig{false, nextSeqNum()}, LinearizableQuorum{}};
    // To-Do: the request timeouts frequently, maybe due to
    // initialization of the database
    readConf.request.timeout = 5000ms;

    Msg reqBytes;
    serialize(reqBytes, req);
    auto replyBytes = client.send(readConf, std::move(reqBytes));  // Sync send

    SKVBCReply reply;
    deserialize(replyBytes.matched_data, reply);

    const auto& readReply = std::get<SKVBCReadReply>(reply.reply);  // throws if unexpected variant
    std::cout << "Got SKVBCReadReply with reads:\n";

    for (const auto& kvp : readReply.reads) {
      auto accountId = BytesToStr(kvp.first);
      auto publicBalanceStr = BytesToStr(kvp.second);
      int publicBalance = std::atoi(publicBalanceStr.c_str());

      std::cout << '\t' << accountId << " : " << publicBalance << '\n';

      // Assert we got the same balance values that we wrote
      auto it = std::find_if(
          accounts.begin(), accounts.end(), [&](const Account& account) { return account.getId() == accountId; });

      ConcordAssert(it != accounts.end());
      ConcordAssert(it->getBalancePublic() == publicBalance);
    }
  }
}

struct TxPublicDeposit {
  TxPublicDeposit(std::string accId, int amount) : amount_{amount}, toAccountId_{std::move(accId)} {}

  int amount_ = 0;
  std::string toAccountId_;
};
std::ostream& operator<<(std::ostream& os, const TxPublicDeposit& tx) {
  os << "deposit " << tx.toAccountId_ << ' ' << tx.amount_;
  return os;
}

struct TxPublicWithdraw {
  TxPublicWithdraw(std::string accId, int amount) : amount_{amount}, toAccountId_{std::move(accId)} {}

  int amount_ = 0;
  std::string toAccountId_;
};
std::ostream& operator<<(std::ostream& os, const TxPublicWithdraw& tx) {
  os << "withdraw " << tx.toAccountId_ << ' ' << tx.amount_;
  return os;
}

struct TxPublicTransfer {
  TxPublicTransfer(std::string fromAccId, std::string toAccId, int amount)
      : amount_{amount}, fromAccountId_{std::move(fromAccId)}, toAccountId_{std::move(toAccId)} {}

  int amount_ = 0;
  std::string fromAccountId_;
  std::string toAccountId_;
};
std::ostream& operator<<(std::ostream& os, const TxPublicTransfer& tx) {
  os << "transfer " << tx.fromAccountId_ << ' ' << tx.toAccountId_ << ' ' << tx.amount_;
  return os;
}

struct TxUttTransfer {
  std::string data_;  // some opaque data
};

using Tx = std::variant<TxPublicDeposit, TxPublicWithdraw, TxPublicTransfer, TxUttTransfer>;

std::ostream& operator<<(std::ostream& os, const Tx& tx) {
  std::visit([&os](const auto& tx) { os << tx; }, tx);
  return os;
}

std::optional<Tx> parseTx(const std::string& str) {
  std::vector<std::string> tokens;
  std::string token;
  std::stringstream ss(str);
  while (std::getline(ss, token, ' ')) tokens.emplace_back(std::move(token));

  if (tokens.size() == 3) {
    if (tokens[0] == "deposit")
      return TxPublicDeposit(std::move(tokens[1]), std::atoi(tokens[2].c_str()));
    else if (tokens[0] == "withdraw")
      return TxPublicWithdraw(std::move(tokens[1]), std::atoi(tokens[2].c_str()));
  } else if (token.size() == 4) {
    if (tokens[0] == "transfer")
      return TxPublicTransfer(std::move(tokens[1]), std::move(tokens[2]), std::atoi(tokens[3].c_str()));
  }

  return std::nullopt;
}

struct Block {
  int id_ = 0;
  std::map<std::string, Tx> tx_;
  std::set<std::string> nullifiers_;
};

std::ostream& operator<<(std::ostream& os, const Block& b) {
  os << "Block " << b.id_ << "\n";
  os << "---------------------------\n";
  for (const auto& kvp : b.tx_) os << kvp.first << " : " << kvp.second << '\n';
  // To-Do: print nullifiers?
  return os;
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

    while (true) {
      std::cout << "\nEnter command (type 'h' for commands, 'q' to exit):\n";
      std::string cmd;
      std::getline(std::cin, cmd);
      try {
        if (std::cin.eof() || cmd == "q") {
          std::cout << "Finished!\n";
          client.stop();
          return 0;
        } else if (cmd == "h") {
          std::cout << "list of commands is empty (NYI)\n";
        } else if (cmd == "test") {
          SimpleSKVBCTest(client);
        } else if (auto tx = parseTx(cmd)) {
          std::cout << "Successfully parsed transaction: " << *tx << '\n';
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
