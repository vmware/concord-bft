//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.

#include "Logger.hpp"
#include <stdio.h>
#include <stdarg.h>
#include <stdio.h>
#include <signal.h>
#include <stdlib.h>
#include <stdint.h>

#if defined(_WIN32)
#include <windows.h>
#include <crtdbg.h>
#else
#include <execinfo.h>
#include <unistd.h>
#include <sys/time.h>
#endif


//TODO(GG): use a standard c++ log

static const bool infoEnabled = true;
static const bool warnEnabled = true;
static const bool includeTime = true;


static void printLogMsg(const char* prefix, FILE *stream, const char *format, va_list arg)
{
	if(includeTime)
	{
#if defined(_WIN32)

		SYSTEMTIME  sysTime;
		GetLocalTime(&sysTime); // TODO(GG): GetSystemTime ???

		uint32_t hour = sysTime.wHour;
		uint32_t minute = sysTime.wMinute;
		uint32_t seconds = sysTime.wSecond;
		uint32_t milli = sysTime.wMilliseconds;
#else
		timeval t;
		gettimeofday(&t, NULL);

		uint32_t secondsInDay = t.tv_sec % (3600 * 24);

		uint32_t hour = secondsInDay / 3600;
		uint32_t minute = (secondsInDay % 3600) / 60;
		uint32_t seconds = secondsInDay % 60;
		uint32_t milli = t.tv_usec / 1000;

#endif
		
   	   fprintf(stdout, "\n %02u:%02u:%02u.%03u %s", hour, minute, seconds, milli, prefix);
		
	}
	else
	{
	  fprintf(stdout, "\n%s", prefix);
	}
	
	vfprintf(stdout, format, arg);
}


void Logger::printInfo(const char *format, ...)
{
	if (!infoEnabled) return;
	va_list arg;
	va_start(arg, format);
	printLogMsg("INFO: ", stdout, format, arg);
	va_end(arg);
}

void Logger::printWarn(const char *format, ...)
{
	if (!warnEnabled) return;
	va_list arg;
	{
		va_start(arg, format);
		printLogMsg("WARNING: ", stdout, format, arg);
		va_end(arg);
	}
	{
		va_start(arg, format);
		printLogMsg("WARNING: ", stderr, format, arg);
		va_end(arg);
	}
}

void Logger::printError(const char *format, ...)
{
	va_list arg;
	{
		va_start(arg, format);
		printLogMsg("ERROR: ", stdout, format, arg);
		va_end(arg);
	}
	{
		va_start(arg, format);
		printLogMsg("ERROR: ", stderr, format, arg);
		va_end(arg);
	}
}



void Logger::simpleAssert(bool cond, const char* msg) // TODO(GG): improve
{
#if defined(_WIN32)
	if (!cond) {
		Logger::printError(msg);
		_ASSERT(false);
	}
#else
	if (!cond) {
		Logger::printLastStackFrames();
		Logger::printError(msg);
		exit(1);
	}
#endif

}


void Logger::printLastStackFrames()
{
	// TODO(GG): replace this method

#if !defined(_WIN32)
	int j, nptrs;
	void *buffer[20];
	char **strings;

	nptrs = backtrace(buffer, 20);
	printf("backtrace() returned %d addresses\n", nptrs);

	strings = backtrace_symbols(buffer, nptrs);
	if (strings == NULL) {
		perror("backtrace_symbols");
		exit(EXIT_FAILURE);
	}

	for (j = 0; j < nptrs; j++)
		printf("%s\n", strings[j]);

	free(strings);
#endif
	
}
