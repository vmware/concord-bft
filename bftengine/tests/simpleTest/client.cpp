#include <stdio.h>
#include <string.h>
#include <cassert>
#include <string>
#include <thread>

#include "CommFactory.hpp"

#include "commonDefs.h"
#include "SimpleClient.hpp"

using namespace std;
using namespace bftEngine;

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
      client->sendRequest(true, (char*)&reqId, sizeof(uint64_t),
                          pSeqGen->generateUniqueSequenceNumberForRequest(),
                          SimpleClient::INFINITE_TIMEOUT,
                          sizeof(uint64_t),
                          replyBuffer,
                          actualReplyLength);

      assert(actualReplyLength == sizeof(uint64_t));

      uint64_t retVal = *((uint64_t*)replyBuffer);

      if (hasExpectedLastValue)
        assert(retVal == expectedLastValue);
    } else {
      // if write
      char requestBuffer[sizeof(uint64_t) * 2];
      char replyBuffer[sizeof(uint64_t)];

      if (hasExpectedStateNum) expectedStateNum++;

      if (!hasExpectedLastValue) hasExpectedLastValue = true;

      expectedLastValue = (i + 1)*(i + 7)*(i + 18);

      uint64_t* pReqId  = (uint64_t*)requestBuffer;
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

      uint64_t retVal = *((uint64_t*)replyBuffer);

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
