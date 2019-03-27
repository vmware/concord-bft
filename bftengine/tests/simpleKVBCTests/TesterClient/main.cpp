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
#include <signal.h>
#include <stdlib.h>
#include <thread>

#include "KVBCInterfaces.h"
#include "simpleKVBCTests.h"
#include "CommFactory.hpp"
#include "test_parameters.hpp"
#include "test_comm_config.hpp"
#include "TestDefs.h"

#ifndef _WIN32
#include <sys/param.h>
#include <unistd.h>
#else
#include "winUtils.h"
#endif

#ifdef USE_LOG4CPP
#include <log4cplus/configurator.h>
#endif

using namespace SimpleKVBC;
using namespace bftEngine;

using std::string;

concordlogger::Logger clientLogger =
		concordlogger::Logger::getLogger("skvbctest.client");

int main(int argc, char **argv) {
#if defined(_WIN32)
	initWinSock();
#endif

#ifdef USE_LOG4CPP
	using namespace log4cplus;
  initialize();
  BasicConfigurator logConfig;
  logConfig.configure();
#endif

	char argTempBuffer[PATH_MAX + 10];
	ClientParams cp;
	cp.clientId = UINT16_MAX;
	cp.numOfFaulty = UINT16_MAX;
	cp.numOfSlow = UINT16_MAX;
	cp.numOfOperations = UINT16_MAX;

	int o = 0;
	while ((o = getopt(argc, argv, "i:f:c:p:n:")) != EOF) {
		switch (o) {
		case 'i':
		{
			strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
			argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
			string idStr = argTempBuffer;
			int tempId = std::stoi(idStr);
			if (tempId >= 0 && tempId < UINT16_MAX)
				cp.clientId = (uint16_t)tempId;
			// TODO: check clientId
		}
		break;

		case 'f':
		{
			strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
			argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
			string fStr = argTempBuffer;
			int tempfVal = std::stoi(fStr);
			if (tempfVal >= 1 && tempfVal < UINT16_MAX)
				cp.numOfFaulty = (uint16_t)tempfVal;
			// TODO: check fVal
		}
		break;

		case 'c':
		{
			strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
			argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
			string cStr = argTempBuffer;
			int tempcVal = std::stoi(cStr);
			if (tempcVal >= 0 && tempcVal < UINT16_MAX)
				cp.numOfSlow = (uint16_t)tempcVal;
			// TODO: check cVal
		}
		break;

		case 'p':
		{
			strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
			argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
			string numOfOpsStr = argTempBuffer;
			int tempfVal = std::stoi(numOfOpsStr);
			if (tempfVal >= 1 && tempfVal < UINT32_MAX)
				cp.numOfOperations = (uint32_t)tempfVal;
			// TODO: check numOfOps
		}
		break;

		case 'n':
		{
			strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
			argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
			cp.configFileName = argTempBuffer;
		}
		break;

		default:
			// nop
			break;
		}
	}

	if (cp.clientId == UINT16_MAX ||
		cp.numOfFaulty == UINT16_MAX ||
		cp.numOfSlow == UINT16_MAX	||
		cp.numOfOperations == UINT32_MAX
		)
	{
		fprintf(stderr, "%s -f F -c C -p NUM_OPS -i ID -n "
                  "COMM_CONFIG_FILE_NAME", argv[0]);
		exit(-1);
	}

	// TODO: check arguments

	TestCommConfig testCommConfig(clientLogger);
	uint16_t numOfReplicas = cp.get_numOfReplicas();
#ifdef USE_COMM_PLAIN_TCP
	PlainTcpConfig conf = testCommConfig.GetTCPConfig(true, cp.clientId,
                                                    cp.numOfClients,
                                                    numOfReplicas,
                                                    cp.configFileName);
#elif USE_COMM_TLS_TCP
	TlsTcpConfig conf = testCommConfig.GetTlsTCPConfig(true, cp.clientId,
													   cp.numOfClients,
													   numOfReplicas,
													   cp.configFileName);
#else
	PlainUdpConfig conf = testCommConfig.GetUDPConfig(true, cp.clientId,
                                                    cp.numOfClients,
                                                    numOfReplicas,
                                                    cp.configFileName);
#endif

	ICommunication* comm = bftEngine::CommFactory::create(conf);
	SimpleKVBC::ClientConfig config;

	config.clientId = cp.clientId;
	config.fVal = cp.numOfFaulty;
	config.cVal = cp.numOfSlow;
	config.maxReplySize = maxMsgSize;

	IClient* c = createClient(config, comm);		 

	BasicRandomTests::run(c, cp.numOfOperations);
}