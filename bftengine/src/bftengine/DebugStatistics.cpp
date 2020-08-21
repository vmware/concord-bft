//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.

#include "DebugStatistics.hpp"


#ifdef DEBUG_STATISTICS

#if defined(_WIN32)
#include <winsock2.h>
#include <WinBase.h>
#else
#include <sys/time.h>
#endif
#include <stdio.h>
#include "MsgCode.hpp"

namespace bftEngine
{
	namespace impl
	{


		void DebugStatistics::onCycleCheck()
		{
			DebugStatDesc& d = getDebugStatDesc();

			if (!d.initialized) // if this is the first time
			{
				d.initialized = true;
				d.lastCycleTime = getMonotonicTime();
				clearCounters(d);
				return;
			}

			Time currTime = getMonotonicTime();

			long long durationMicroSec = subtract(currTime, d.lastCycleTime);

			double durationSec = durationMicroSec / 1000000.0;

			if (durationSec < DEBUG_STAT_PERIOD_SECONDS)
				return;

			double readThruput = 0;
			double writeThruput = 0;

			if (d.completedReadOnlyRequests > 0)
				readThruput = (d.completedReadOnlyRequests / durationSec);

			if (d.completedReadWriteRequests > 0)
				writeThruput = (d.completedReadWriteRequests / durationSec);

#if defined(_WIN32)

			SYSTEMTIME  sysTime;
			GetLocalTime(&sysTime); // TODO(GG): GetSystemTime ???

			uint32_t hour = sysTime.wHour;
			uint32_t minute = sysTime.wMinute;
			uint32_t seconds = sysTime.wSecond;
			uint32_t milli = sysTime.wMilliseconds;

#else

			timeval t;
			gettimeofday(&t, nullptr);
			uint32_t secondsInDay = t.tv_sec % (3600 * 24);
			uint32_t hour = secondsInDay / 3600;
			uint32_t minute = (secondsInDay % 3600) / 60;
			uint32_t seconds = secondsInDay % 60;
			uint32_t milli = t.tv_usec / 1000;

#endif
/*
			fprintf(stdout, "\n %02u:%02u:%02u.%03u STAT:\t", hour, minute, seconds, milli);
			fprintf(stdout, "ReadOnlyThruput = %7.2f\t", readThruput);
			fprintf(stdout, "WriteThruput = %7.2f\t", writeThruput);
			fprintf(stdout, "receivedMessages = %zd\t", d.receivedMessages);
			fprintf(stdout, "sendMessages = %zd\t", d.sendMessages);
			fprintf(stdout, "numberOfReceivedSTMessages = %zd\t", d.numberOfReceivedSTMessages);
			fprintf(stdout, "numberOfReceivedStatusMessages = %zd\t", d.numberOfReceivedStatusMessages);
			fprintf(stdout, "numberOfReceivedCommitMessages = %zd\t", d.numberOfReceivedCommitMessages);
			fprintf(stdout, "\n");
*/
			if (d.completedReadOnlyRequests > 0 || d.completedReadWriteRequests > 0)
			{
				/*
				fprintf(stdout, "\n %02u:%02u:%02u.%03u SHRES:\t", hour, minute, seconds, milli);
				fprintf(stdout, "ReadOnlyThruput = %7.2f\t", readThruput);
				fprintf(stdout, "WriteThruput = %7.2f\t", writeThruput);
				fprintf(stdout, "\n\n\n");

				fprintf(stderr, "\n %02u:%02u:%02u.%03u SHRES:\t", hour, minute, seconds, milli);
				fprintf(stderr, "ReadOnlyThruput = %7.2f\t", readThruput);
				fprintf(stderr, "WriteThruput = %7.2f\t", writeThruput);
				fprintf(stderr, "\n\n\n");
				*/
			}


			d.lastCycleTime = currTime;
			clearCounters(d);
		}


		void DebugStatistics::onReceivedExMessage(uint16_t type)
		{
			DebugStatDesc& d = getDebugStatDesc();

			d.receivedMessages++;

			switch (type)
			{
			case MsgCode::ReplicaStatus:
				d.numberOfReceivedStatusMessages++;
				break;
			case MsgCode::StateTransfer: // TODO(GG): TBD?
				d.numberOfReceivedSTMessages++;
				break;
			case MsgCode::CommitPartial:
			case MsgCode::CommitFull:
				d.numberOfReceivedCommitMessages++;
				break;
			default:
				break;
			}
		}

		void DebugStatistics::onSendExMessage(uint16_t type)
		{
			DebugStatDesc& d = getDebugStatDesc();

			d.sendMessages++;
		}


		void DebugStatistics::onRequestCompleted(bool isReadOnly)
		{
			DebugStatDesc& d = getDebugStatDesc();

			if (isReadOnly) d.completedReadOnlyRequests++;
			else d.completedReadWriteRequests++;
		}

		void DebugStatistics::clearCounters(DebugStatDesc& d)
		{
			d.receivedMessages = 0;
			d.sendMessages = 0;
			d.completedReadOnlyRequests = 0;
			d.completedReadWriteRequests = 0;
			d.numberOfReceivedSTMessages = 0;
			d.numberOfReceivedStatusMessages = 0;
			d.numberOfReceivedCommitMessages = 0;
		}

#ifdef USE_TLS
		void DebugStatistics::initDebugStatisticsData()
		{
			ThreadLocalData* l = ThreadLocalData::Get();
			Assert(l->debugStatData == nullptr);
			DebugStatDesc* dsd = new DebugStatDesc;
			l->debugStatData = dsd;
		}

		void DebugStatistics::freeDebugStatisticsData()
		{
			ThreadLocalData* l = ThreadLocalData::Get();
			DebugStatDesc* dsd = (DebugStatDesc*)l->debugStatData;
			delete dsd;
			l->debugStatData = nullptr;
		}
#else

		DebugStatistics::DebugStatDesc DebugStatistics::globalDebugStatDesc;

		void DebugStatistics::initDebugStatisticsData()
		{
		}

		void DebugStatistics::freeDebugStatisticsData()
		{
		}

	}
}


#endif

#endif

