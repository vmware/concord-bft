/*
 *  Log.h
 *
 *  Created on: Oct 17, 2014
 *      Author: Alin Tomescu <alinush@mit.edu>
 */
#pragma once

#include <iostream>
#include <string>
#include <iomanip>
#include <cerrno>
#include <sys/types.h>
#ifndef _WIN32
# include <unistd.h>
# include <sys/syscall.h>
#endif

// TODO: Things like this don't work: std::copy(verifKeys.begin(), verifKeys.end(), std::ostream_iterator(loginfo, "\n"));

/**************
 * FUNCTIONS  *
 **************/

/**
 * Returns the current time in "2019-08-26 17:10:33" format.
 */
std::string timeToString();
std::ostream& logErrNo();
long int getCurrentThreadId();
std::ostream& coredumpOstream();
#ifdef _WIN32
int64_t getpid();
#endif

/************
 * DEFINES  *
 ***********/

/**************
 * LOG MACROS *
 **************/
# define LOG_PREFIX timeToString() << " " << std::setw(5) << getpid() << " " << std::setw(5) << getCurrentThreadId() << " " << std::setw(5)

# define LOG_SUFFIX " " << std::setw(20) << __FUNCTION__ << ":" << std::setw(4) << __LINE__ << " | "

# define loginfo std::clog  << LOG_PREFIX << "INFO" << LOG_SUFFIX
# define logwarn std::clog  << LOG_PREFIX << "WARN" << LOG_SUFFIX
# define logerror std::cerr << LOG_PREFIX << "ERROR" << LOG_SUFFIX
# define logperf std::clog << LOG_PREFIX << "PERF" << LOG_SUFFIX
# define logerrno logErrNo()

# ifndef NDEBUG
#  define logdbg std::clog << LOG_PREFIX << "DEBUG" << LOG_SUFFIX
# else
#  define logdbg if(false) coredumpOstream()
# endif

# ifdef TRACE
#  define logtrace std::clog << LOG_PREFIX << "TRACE" << LOG_SUFFIX
# else
#  define logtrace if(false) coredumpOstream()
# endif

# ifdef LOG_ALLOC
# define logalloc std::clog << LOG_PREFIX << "ALLOC" << LOG_SUFFIX
# else
#  define logalloc if(false) coredumpOstream()
# endif
