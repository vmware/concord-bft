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
#include <sstream>
#include <signal.h>
#include <stdlib.h>
#include <thread>

#include "KVBCInterfaces.h"
#include "simpleKVBCTests.h"
#include "CommFactory.hpp"
#include "test_comm_config.hpp"
#include "test_parameters.hpp"
#include "MetricsServer.hpp"
#include "ReplicaImp.h"

#ifndef _WIN32
#include <sys/param.h>
#include <unistd.h>
#else
#include "winUtils.h"
#endif

using namespace SimpleKVBC;
using namespace bftEngine;

using std::string;
using ::TestCommConfig;

IReplica* r = nullptr;
ReplicaParams rp;
concordlogger::Logger replicaLogger =
		concordlogger::Log::getLogger("skvbctest.replica");

int main(int argc, char **argv) {
#if defined(_WIN32)
	initWinSock();
#endif

	rp.replicaId = UINT16_MAX;
        rp.viewChangeEnabled = false;
        rp.viewChangeTimeout = 45*1000;

	// allows to attach debugger
	if(rp.debug) {
          std::this_thread::sleep_for(std::chrono::seconds(20));
        }

	char argTempBuffer[PATH_MAX+10];
	string idStr;

	int o = 0;
	while ((o = getopt(argc, argv, "r:i:k:n:s:v:")) != EOF) {
		switch (o) {
		case 'i':
		{
			strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
			argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
			idStr = argTempBuffer;
			int tempId = std::stoi(idStr);
			if (tempId >= 0 && tempId < UINT16_MAX) 
				rp.replicaId = (uint16_t)tempId;
			// TODO: check repId
		}
		break;

		case 'k':
		{
			strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
			argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
			rp.keysFilePrefix = argTempBuffer;
			// TODO: check keysFilePrefix
		}
		break;

		case 'n':
		{
			strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
			argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
			rp.configFileName = argTempBuffer;
		}
		break;
		case 's':
		{
			strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
			argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
			idStr = argTempBuffer;
			int tempId = std::stoi(idStr);
			if (tempId >= 0 && tempId < UINT16_MAX) 
				rp.statusReportTimerMillisec = (uint16_t)tempId;
		}
                break;
                case 'v':
                {
			strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
			argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
			idStr = argTempBuffer;
			int tempId = std::stoi(idStr);
			if (tempId >= 0 && (uint32_t)tempId < UINT32_MAX) {
                          rp.viewChangeTimeout = (uint32_t)tempId;
                          rp.viewChangeEnabled = true;
                        }
                }
                break;

		default:
			// nop
			break;
		}
	}

	if(rp.replicaId == UINT16_MAX || rp.keysFilePrefix.empty())
	{
		fprintf(stderr, "%s -k KEYS_FILE_PREFIX -i ID -n COMM_CONFIG_FILE",
				argv[0]);
		exit(-1);
	}

	// TODO: check arguments

    //used to get info from parsing the key file
	bftEngine::ReplicaConfig replicaConfig;

    TestCommConfig testCommConfig(replicaLogger);
	testCommConfig.GetReplicaConfig(
			rp.replicaId, rp.keysFilePrefix, &replicaConfig);

        // This allows more concurrency and only affects known ids in the
        // communication classes.
	replicaConfig.numOfClientProxies = 100;
	replicaConfig.autoViewChangeEnabled = rp.viewChangeEnabled;
	replicaConfig.viewChangeTimerMillisec = rp.viewChangeTimeout;

	uint16_t numOfReplicas =
			(uint16_t)(3 * replicaConfig.fVal + 2 * replicaConfig.cVal + 1);
#ifdef USE_COMM_PLAIN_TCP
	PlainTcpConfig conf = testCommConfig.GetTCPConfig(true, rp.replicaId,
                                                      replicaConfig.numOfClientProxies,
                                                      numOfReplicas,
                                                      rp.configFileName);
#elif USE_COMM_TLS_TCP
	TlsTcpConfig conf = testCommConfig.GetTlsTCPConfig(true, rp.replicaId,
                                                       replicaConfig.numOfClientProxies,
                                                       numOfReplicas,
                                                       rp.configFileName);
#else
        PlainUdpConfig conf = testCommConfig.GetUDPConfig(true, 
                                                          rp.replicaId,
                                                          replicaConfig.numOfClientProxies,
                                                          numOfReplicas,
                                                          rp.configFileName);
#endif
	//used to run tests. TODO(IG): use the standard config structs for all tests
	SimpleKVBC::ReplicaConfig c;

	ICommunication *comm = CommFactory::create(conf);

	c.pathOfKeysfile = rp.keysFilePrefix + std::to_string(rp.replicaId);
	c.replicaId = rp.replicaId;
	c.fVal = replicaConfig.fVal;
	c.cVal = replicaConfig.cVal;
	c.numOfClientProxies = replicaConfig.numOfClientProxies;
        // Allow triggering of things like state transfer to occur faster in
        // tests.
	c.statusReportTimerMillisec = rp.statusReportTimerMillisec;
	c.concurrencyLevel = 1;
	c.autoViewChangeEnabled = rp.viewChangeEnabled;
	c.viewChangeTimerMillisec =  rp.viewChangeTimeout;
	c.maxBlockSize = 2 * 1024 * 1024;  // 2MB


        // UDP MetricsServer only used in tests.
        uint16_t metricsPort = conf.listenPort + 1000;
        concordMetrics::Server server(metricsPort);
        server.Start();

	r = createReplica(c, comm, BasicRandomTests::commandsHandler(), server.GetAggregator());
	r->start();
	while (r->isRunning())
		std::this_thread::sleep_for(std::chrono::seconds(1));
}
