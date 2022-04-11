/*
 * Log.cpp
 *
 *  Created on: Oct 17, 2014
 *      Author: Alin Tomescu <alinush@mit.edu>
 */

#include <cstring>
#ifdef __APPLE__
#include <pthread.h>
#endif
#ifdef _WIN32
#include <windows.h>
#endif

#include <xutils/Log.h>
#include <xassert/XAssert.h>  // coredump

namespace libutt {

std::ostream& coredumpOstream() {
  logerror << "Oops, looks like there's a either a logic error in your logdbg/logtrace/log[.*] #define's or your "
              "compiler is not optimizing out log[.*] calls when they are disabled"
           << std::endl;
  XAssert::coredump();
  return std::cout;
}

std::string timeToString() {
  char buf[512];
  time_t t = time(NULL);
  struct tm tm;

#if defined(_WIN32)
  localtime_s(&tm, &t);
#elif defined(__STDC_LIB_EXT1__)
  localtime_s(&t, &tm);
#else
  localtime_r(&t, &tm);
#endif

  if (strftime(buf, 512, "%F %T", &tm) != 0)
    return std::string(buf);
  else
    return std::string("strftime failed, small buffer size?");
}
std::ostream& logErrNo() {
#if defined(__APPLE__) || ((_POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600) && !_GNU_SOURCE)
  int errNo = errno;
  char logerrnobuf[256];
  snprintf(logerrnobuf, 256, "???");
  char* logerrnomsg;
  strerror_r(errNo, logerrnobuf, 256);
  logerrnomsg = logerrnobuf;
  std::cerr << LOG_PREFIX << "ERROR" << LOG_SUFFIX << "(errno=" << logerrnobuf << ") ";
  return std::cerr;
#elif defined(__STDC_LIB_EXT1__) || defined(_WIN32)
  int errNo = errno;
  char logerrnobuf[256];

  if (strerror_s(logerrnobuf, 256, errNo) == 0)
    std::cerr << LOG_PREFIX << "ERROR" << LOG_SUFFIX << "(errno=" << logerrnobuf << ") ";
  else
    std::cerr << LOG_PREFIX << "ERROR" << LOG_SUFFIX << "(errno=" << errNo << ") ";

  return std::cerr;
#else
  int errNo = errno;
  char logerrnobuf[256];
  char* logerrnomsg = strerror_r(errNo, logerrnobuf, 256);
  std::cerr << LOG_PREFIX << "ERROR" << LOG_SUFFIX << "(errno=" << logerrnomsg << ") ";
  return std::cerr;
#endif
}

long int getCurrentThreadId() {
#ifdef __APPLE__
  uint64_t tid;
  pthread_threadid_np(NULL, &tid);
  return static_cast<long int>(tid);
#elif defined(_WIN32)
  return GetCurrentThreadId();
#else
  return syscall(SYS_gettid);
#endif
}

#ifdef _WIN32
int64_t getpid() { return GetCurrentProcessId(); }

#endif

/**
 * Makes sure the compiler doesn't evaluate expressions passed in to logX << expr() calls when logging is disabled.
 * If it does, the program will "segfault" instantly to indicate something's wrong.
 */
class LogInitializer {
 private:
  static LogInitializer logInit;

 private:
  LogInitializer() {
    // If we're in "no logging" mode, make sure we never evaluate 'expr' we logdbg!
#ifdef NDEBUG
    logdbg << "asd" << 4 << shouldNotBeCalled();
    // loginfo << "Compiler successfully avoids evaluating expressions in 'logdbg << expr()'" << std::endl;
#endif

    // If we're in "no trace" mode, make sure we never evaluate 'expr' we logtrace!
#ifndef TRACE
    logtrace << 2 << "xyz" << shouldNotBeCalled();
    // loginfo << "Compiler successfully avoids evaluating expressions in 'logtrace << expr()'" << std::endl;
#endif
  }

  bool shouldNotBeCalled() {
    fprintf(
        stderr,
        "Oops, your compiler should not be evaluating expressions passed into optimized out 'log << expr' calls.\n");
    return XAssert::coredump();
  }
};

LogInitializer LogInitializer::logInit;

}  // namespace libutt
