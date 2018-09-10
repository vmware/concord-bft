// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <stdio.h>
#include <string.h>
#include <cassert>
#include <string>
#include <thread>

#include "CommFactory.hpp"

#include "commonDefs.h"
#include "SimpleClient.hpp"

using bftEngine::ICommunication;
using bftEngine::PlainUDPCommunication;
using bftEngine::PlainUdpConfig;
using bftEngine::SeqNumberGeneratorForClientRequests;
using bftEngine::SimpleClient;

PlainUdpConfig getUDPConfig(uint16_t id);

int main(int argc, char **argv) {
  const int16_t id = 4;
  const int numOfOperations = 2800;
  const int readMod = 7;

  SeqNumberGeneratorForClientRequests* pSeqGen =
      SeqNumberGeneratorForClientRequests::
      createSeqNumberGeneratorForClientRequests();

  PlainUdpConfig udpConf = getUDPConfig(id);

  ICommunication* comm = PlainUDPCommunication::create(udpConf);
  SimpleClient* client = SimpleClient::createSimpleClient(comm, id, 1, 0);

  comm->Start();

  uint64_t expectedStateNum = 0;
  bool hasExpectedStateNum = false;
  uint64_t expectedLastValue = 0;
  bool hasExpectedLastValue = false;

  for (int i = 1; i <= numOfOperations; i++) {
    if (i % readMod == 0) {
      // if read
      char replyBuffer[sizeof(uint64_t)];

      uint64_t reqId = READ_VAL_REQ;
      uint32_t actualReplyLength = 0;
      client->sendRequest(true,
                          reinterpret_cast<char*>(&reqId),
                          sizeof(uint64_t),
                          pSeqGen->generateUniqueSequenceNumberForRequest(),
                          SimpleClient::INFINITE_TIMEOUT,
                          sizeof(uint64_t),
                          replyBuffer,
                          actualReplyLength);

      assert(actualReplyLength == sizeof(uint64_t));

      uint64_t retVal = *reinterpret_cast<uint64_t*>(replyBuffer);

      if (hasExpectedLastValue)
        assert(retVal == expectedLastValue);
    } else {
      // if write
      char requestBuffer[sizeof(uint64_t) * 2];
      char replyBuffer[sizeof(uint64_t)];

      if (hasExpectedStateNum) expectedStateNum++;

      if (!hasExpectedLastValue) hasExpectedLastValue = true;

      expectedLastValue = (i + 1)*(i + 7)*(i + 18);

      uint64_t* pReqId  = reinterpret_cast<uint64_t*>(requestBuffer);
      uint64_t* pReqVal = (pReqId + 1);
      *pReqId = SET_VAL_REQ;
      *pReqVal = expectedLastValue;
      uint32_t actualReplyLength = 0;
      client->sendRequest(false, requestBuffer, sizeof(uint64_t)*2,
                          pSeqGen->generateUniqueSequenceNumberForRequest(),
                          SimpleClient::INFINITE_TIMEOUT,
                          sizeof(uint64_t),
                          replyBuffer,
                          actualReplyLength);

      assert(actualReplyLength == sizeof(uint64_t));

      uint64_t retVal = *reinterpret_cast<uint64_t*>(replyBuffer);

      if (hasExpectedStateNum) {
        assert(retVal == expectedStateNum);
      } else {
        hasExpectedStateNum = true;
        expectedStateNum = retVal;
      }
    }
  }

  comm->Stop();

  delete pSeqGen;
  delete client;
  delete comm;

  return 0;
}
