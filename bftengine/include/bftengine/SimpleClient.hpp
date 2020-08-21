//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.

#pragma once

#include  <cstddef>
#include <stdint.h>
#include <string>
#include <set>
#include "ICommunication.hpp"

using namespace std;

namespace bftEngine
{
	//	 parameters // TODO(GG): move to client configuration
	struct SimpleClientParams {
  		uint64_t clientInitialRetryTimeoutMilli = 150;
  		uint64_t clientMinRetryTimeoutMilli = 50;
  		uint64_t clientMaxRetryTimeoutMilli = 1000;
  		uint16_t clientSendsRequestToAllReplicasFirstThresh = 4;
  		uint16_t clientSendsRequestToAllReplicasPeriodThresh = 2;
  		uint16_t clientPeriodicResetThresh = 30;
	};

	class SimpleClient
	{
	public:

		static const uint64_t INFINITE_TIMEOUT = UINT64_MAX;

		static SimpleClient* createSimpleClient(ICommunication* communication, uint16_t clientId, uint16_t fVal, uint16_t cVal);

	   static SimpleClient* createSimpleClient(ICommunication* communication,
	   		uint16_t clientId, uint16_t fVal, uint16_t cVal,
	   		SimpleClientParams p);
		
		static SimpleClient* createArchipelagoSimpleClient(ICommunication* communication,
	   		uint16_t clientId, uint16_t fVal, uint16_t cVal,
	   		SimpleClientParams p);

		virtual ~SimpleClient() ;

		virtual int sendRequest(bool isReadOnly, const char* request, uint32_t lengthOfRequest, uint64_t reqSeqNum, uint64_t timeoutMilli, uint32_t lengthOfReplyBuffer, char* replyBuffer, uint32_t& actualReplyLength) = 0;

		virtual int sendRequestToResetSeqNum() = 0;		
		virtual int sendRequestToReadLatestSeqNum(uint64_t timeoutMilli, uint64_t& outLatestReqSeqNum) = 0;		
	};

        // This class is mainly for testing and SimpleClient applications.
        // Users are allowed to generate their own sequence numbers (they do not
        // have to use this class). Other examples of ways to do this include:
        // (1) A simple counter + store the last counter in a persistent storage
        // (2) An approach that utilizes the functions
        //     SimpleClient::sendRequestToResetSeqNum() or
        //     SimpleClient::sendRequestToReadLatestSeqNum(..)
        //     [These functions are not yet supported, but if necessary, we will
        //     support them.]
        class SeqNumberGeneratorForClientRequests {
          public:
            static SeqNumberGeneratorForClientRequests*
              createSeqNumberGeneratorForClientRequests();

            virtual uint64_t generateUniqueSequenceNumberForRequest() = 0;

            virtual ~SeqNumberGeneratorForClientRequests() {};
        };
}
