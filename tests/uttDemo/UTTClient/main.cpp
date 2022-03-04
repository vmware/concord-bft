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

std::vector<uint8_t> StrToBytes(const std::string &str) { return std::vector<uint8_t>(str.begin(), str.end()); }

std::string BytesToStr(const std::vector<uint8_t> &bytes) { return std::string{bytes.begin(), bytes.end()}; }

ClientParams setupClientParams(int argc, char **argv) {
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

auto logger = logging::getLogger("skvbtest.client");

ICommunication *setupCommunicationParams(ClientParams &cp) {
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

int main(int argc, char **argv) {
  try {
    logging::initLogger("logging.properties");
    ClientParams clientParams = setupClientParams(argc, argv);

    if (clientParams.clientId == UINT16_MAX || clientParams.numOfFaulty == UINT16_MAX ||
        clientParams.numOfSlow == UINT16_MAX || clientParams.numOfOperations == UINT32_MAX) {
      LOG_ERROR(logger, "Wrong usage! Required parameters: " << argv[0] << " -f F -c C -p NUM_OPS -i ID");
      exit(-1);
    }

    std::vector<Account> accounts;
    accounts.emplace_back("Account_1");
    accounts.emplace_back("Account_2");
    accounts.emplace_back("Account_3");

    SharedCommPtr comm = SharedCommPtr(setupCommunicationParams(clientParams));

    ClientConfig clientConfig;
    clientConfig.f_val = clientParams.numOfFaulty;
    for (uint16_t i = 0; i < clientParams.numOfReplicas; ++i) clientConfig.all_replicas.emplace(ReplicaId{i});
    clientConfig.id = ClientId{clientParams.clientId};

    LOG_INFO(GL, "Starting bft client id=" << clientConfig.id.val);

    Client client(comm, clientConfig);

    // Deposit initial balances
    {
      std::vector<int> deposits = {88, 99, 101};

      SKVBCWriteRequest writeReq;
      // The expected values by the execution engine in the kv pair are strings
      for (int i = 0; i < 3; ++i)
        writeReq.writeset.emplace_back(StrToBytes(accounts[i].getId()), StrToBytes(std::to_string(deposits[i])));

      SKVBCRequest req;
      req.request = std::move(writeReq);

      // Ensure we only wait for F+1 replies (ByzantineSafeQuorum)
      WriteConfig writeConf{RequestConfig{false, nextSeqNum()}, ByzantineSafeQuorum{}};
      // To-Do: the request fails with only 500ms timeout, but maybe that's due to
      // initialization of the database
      writeConf.request.timeout = 5000ms;

      Msg reqBytes;
      serialize(reqBytes, req);
      auto replyBytes = client.send(writeConf, std::move(reqBytes));  // Sync send

      SKVBCReply reply;
      deserialize(replyBytes.matched_data, reply);

      const auto &writeReply = std::get<SKVBCWriteReply>(reply.reply);  // throws if unexpected variant
      LOG_INFO(GL,
               "Got SKVBCWriteReply, success=" << writeReply.success << " latest_block=" << writeReply.latest_block);

      if (writeReply.success) {
        for (int i = 0; i < 3; ++i) accounts[i].depositPublic(deposits[i]);
      }
    }

    // Read back the account balances
    {
      SKVBCReadRequest readReq;
      readReq.read_version = 1;
      // The expected values by the execution engine in the kv pair are strings
      for (const auto &account : accounts) readReq.keys.emplace_back(StrToBytes(account.getId()));

      SKVBCRequest req;
      req.request = std::move(readReq);

      // Ensure we wait for 2F+1 replies
      ReadConfig readConf{RequestConfig{false, nextSeqNum()}, LinearizableQuorum{}};
      readConf.request.timeout = 500ms;

      Msg reqBytes;
      serialize(reqBytes, req);
      auto replyBytes = client.send(readConf, std::move(reqBytes));  // Sync send

      SKVBCReply reply;
      deserialize(replyBytes.matched_data, reply);

      const auto &readReply = std::get<SKVBCReadReply>(reply.reply);  // throws if unexpected variant
      LOG_INFO(GL, "Got SKVBCReadReply with reads:");

      for (const auto &kvp : readReply.reads) {
        auto accountId = BytesToStr(kvp.first);
        auto publicBalanceStr = BytesToStr(kvp.second);
        int publicBalance = std::atoi(publicBalanceStr.c_str());

        LOG_INFO(GL, '\t' << accountId << " : " << publicBalance);

        // Assert we got the same balance values that we wrote
        auto it = std::find_if(
            accounts.begin(), accounts.end(), [&](const Account &account) { return account.getId() == accountId; });

        ConcordAssert(it != accounts.end());
        ConcordAssert(it->getBalancePublic() == publicBalance);
      }
    }

    client.stop();

  } catch (const std::exception &e) {
    LOG_ERROR(GL, "Exception: " << e.what());
  }
}
