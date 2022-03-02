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

#include "basicRandomTestsRunner.hpp"
#include "KVBCInterfaces.h"
#include "config/test_comm_config.hpp"
#include "config/test_parameters.hpp"
#include "communication/CommFactory.hpp"
#include "bftclient/config.h"
#include "bftclient/bft_client.h"
#include "skvbc_messages.cmf.hpp"

using namespace bftEngine;
using namespace bft::communication;
using concord::kvbc::test::BasicRandomTestsRunner;

using std::string;

using concord::kvbc::BlockId;
using concord::kvbc::ClientConfig;

std::vector<uint8_t> CopyBytes(const char *str) {
  return str ? std::vector<uint8_t>(str, str + strlen(str)) : std::vector<uint8_t>{};
}

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

ClientConfig setupConsensusParams(ClientParams &clientParams) {
  ClientConfig clientConfig;
  clientConfig.clientId = clientParams.clientId;
  clientConfig.fVal = clientParams.numOfFaulty;
  clientConfig.cVal = clientParams.numOfSlow;
  return clientConfig;
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

    // This is the test fixture
    // BasicRandomTestsRunner testsRunner(
    //     concord::kvbc::createClient(setupConsensusParams(clientParams), setupCommunicationParams(clientParams)));
    // testsRunner.run(clientParams.numOfOperations);

    // Create and run some commmands from a bft client

    // unique_ptr<FakeCommunication> comm(new FakeCommunication(WriteBehavior));

    bft::client::SharedCommPtr comm = bft::client::SharedCommPtr(setupCommunicationParams(clientParams));

    bft::client::ClientConfig clientConfig;
    clientConfig.f_val = clientParams.numOfFaulty;
    for (uint16_t i = 0; i < clientParams.numOfReplicas; ++i)
      clientConfig.all_replicas.emplace(bft::client::ReplicaId{i});
    clientConfig.id = bft::client::ClientId{clientParams.clientId};

    LOG_INFO(GL, "Starting bft client id=" << clientConfig.id.val);

    bft::client::Client client(comm, clientConfig);

    // Write account balances as kv pairs
    skvbc::messages::SKVBCWriteRequest writeReq;
    // The expected values by the execution engine in the kv pair are strings
    writeReq.writeset.emplace_back(CopyBytes("Account-1"), CopyBytes("100"));
    writeReq.writeset.emplace_back(CopyBytes("Account-2"), CopyBytes("200"));
    writeReq.writeset.emplace_back(CopyBytes("Account-3"), CopyBytes("300"));

    skvbc::messages::SKVBCRequest req;
    req.request = std::move(writeReq);

    // Ensure we only wait for F+1 replies (ByzantineSafeQuorum)
    bft::client::WriteConfig config{bft::client::RequestConfig{false, 1}, bft::client::ByzantineSafeQuorum{}};
    config.request.timeout = 500ms;

    bft::client::Msg reqBytes;
    skvbc::messages::serialize(reqBytes, req);

    // Test deserialize
    {
      skvbc::messages::SKVBCRequest req2;
      skvbc::messages::deserialize(reqBytes, req2);
      LOG_INFO(GL, "test desserialize id=" << req2.id);
    }

    auto replyBytes = client.send(config, std::move(reqBytes));  // Sync send

    skvbc::messages::SKVBCReply reply;
    skvbc::messages::deserialize(replyBytes.matched_data, reply);

    const auto &writeReply = std::get<skvbc::messages::SKVBCWriteReply>(reply.reply);  // throws if unexpected variant
    LOG_INFO(GL, "Got SKVBCWriteReply, success=" << writeReply.success << " latest_block=" << writeReply.latest_block);

    // To-Do: Check written values

    client.stop();

  } catch (const std::exception &e) {
    LOG_ERROR(GL, "Exception: " << e.what());
  }
}
