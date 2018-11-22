// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#ifndef CONCORD_BFT_LOGGING_HPP
#define CONCORD_BFT_LOGGING_HPP

#include <string>
#include <sstream>
#include <chrono>
#include <iomanip>
#include <stdarg.h>

#ifdef USE_LOG4CPP
#include <log4cplus/loggingmacros.h>
typedef log4cplus::Logger LoggerImpl;
#endif

namespace concordlogger {

#ifndef USE_LOG4CPP

// log levels as defined in log4cpp
enum LogLevel {
  trace,
  debug,
  info,
  warn,
  error,
  fatal,
  off,
  all = trace
};

constexpr LogLevel CURRENT_LEVEL = LogLevel::info;
#define CHECK_ENABLED(l) \
if (CURRENT_LEVEL == LogLevel::off || l < CURRENT_LEVEL) return;

class SimpleLoggerImpl {
  std::string _name;
  std::string LEVELS_STRINGS[6] =
      {"TRACE",
       "DEBUG",
       "INFO",
       "WARN",
       "ERROR",
       "FATAL"};

  inline void get_time(std::stringstream &ss) {
    using namespace std::chrono;
    auto now = system_clock::now();
    auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
    auto timer = system_clock::to_time_t(now);
    std::tm bt = *std::localtime(&timer);
    ss << std::put_time(&bt, "%F %T")
       << "."
       << std::setfill('0') << std::setw(3) << ms.count();
  }

 public:
  explicit SimpleLoggerImpl(std::string name) : _name{std::move(name)} {}

  inline void print(concordlogger::LogLevel l, std::string text) {
    std::stringstream time;
    get_time(time);
    printf("%s %s (%s) %s\n",
           SimpleLoggerImpl::LEVELS_STRINGS[l].c_str(),
           time.str().c_str(),
           _name.c_str(),
           text.c_str());
  }
};

typedef SimpleLoggerImpl LoggerImpl;
#endif // USE_LOG4CPP

class Logger {
 private:
  LoggerImpl _impl;
  Logger(LoggerImpl impl) : _impl(std::move(impl)) {}

  inline std::string prepare(const char *format, va_list &args) {
    va_list args2;
    va_copy(args2, args);
    auto size = std::vsnprintf(nullptr, 0, format, args2);
    va_end(args2);
    std::string output(size + 1, '\0');
    std::vsnprintf((char *) output.c_str(), size + 1, format, args);
    return output;
  }

 public:
  inline LoggerImpl getImpl() {
    return _impl;
  }

  inline void fatal(std::string msg) {
#ifdef USE_LOG4CPP
    LOG4CPLUS_FATAL(_impl, msg);
#else
    CHECK_ENABLED(LogLevel::fatal);
    _impl.print(LogLevel::fatal, msg);
#endif
  }

  inline void fatal(const char *format, ...) {
    va_list args;
    va_start (args, format);
#ifdef USE_LOG4CPP
    LOG4CPLUS_FATAL_FMT(_impl, format, args);
#else
    CHECK_ENABLED(LogLevel::fatal);
    std::string output = prepare(format, args);
    fatal(output);
#endif
    va_end(args);
  }

  inline void fatal(std::ostringstream &os) {
    fatal(os.str());
  }

  inline void error(std::string msg) {
#ifdef USE_LOG4CPP
    LOG4CPLUS_ERROR(_impl, msg);
#else
    CHECK_ENABLED(LogLevel::error);
    _impl.print(LogLevel::error, msg);
#endif
  }

  inline void error(const char *format, ...) {
    va_list args;
    va_start (args, format);
#ifdef USE_LOG4CPP
    LOG4CPLUS_ERROR_FMT(_impl, format, args);
#else
    CHECK_ENABLED(LogLevel::error);
    std::string output = prepare(format, args);
    error(output);
#endif
    va_end(args);
  }

  inline void error(std::ostringstream &os) {
    error(os.str());
  }

  inline void warn(std::string msg) {
#ifdef USE_LOG4CPP
    LOG4CPLUS_WARN(_impl, msg);
#else
    CHECK_ENABLED(LogLevel::warn);
    _impl.print(LogLevel::warn, msg);
#endif
  }

  inline void warn(const char *format, ...) {
    va_list args;
    va_start (args, format);
#ifdef USE_LOG4CPP
    LOG4CPLUS_WARN_FMT(_impl, format, args);
#else
    CHECK_ENABLED(LogLevel::warn);
    std::string output = prepare(format, args);
    warn(output);
#endif
    va_end(args);
  }

  inline void warn(std::ostringstream &os) {
    warn(os.str());
  }

  inline void info(std::string msg) {
#ifdef USE_LOG4CPP
    LOG4CPLUS_INFO(_impl, msg);
#else
    CHECK_ENABLED(LogLevel::info);
    _impl.print(LogLevel::info, msg);
#endif
  }

  inline void info(const char *format, ...) {
    va_list args;
    va_start (args, format);
#ifdef USE_LOG4CPP
    LOG4CPLUS_INFO_FMT(_impl, format, args);
#else
    CHECK_ENABLED(LogLevel::info);
    std::string output = prepare(format, args);
    info(output);
#endif
    va_end(args);
  }

  inline void info(std::ostringstream &os) {
    info(os.str());
  }

  inline void debug(std::string msg) {
#ifdef USE_LOG4CPP
    LOG4CPLUS_DEBUG(_impl, msg);
#else
    CHECK_ENABLED(LogLevel::debug);
    _impl.print(LogLevel::debug, msg);
#endif
  }

  inline void debug(const char *format, ...) {
    va_list args;
    va_start (args, format);
#ifdef USE_LOG4CPP
    LOG4CPLUS_DEBUG_FMT(_impl, format, args);
#else
    CHECK_ENABLED(LogLevel::debug);
    std::string output = prepare(format, args);
    debug(output);
#endif
    va_end(args);
  }

  inline void debug(std::ostringstream &os) {
    debug(os.str());
  }

  inline void trace(std::string msg) {
#ifdef USE_LOG4CPP
    LOG4CPLUS_FATAL(_impl, msg);
#else
    CHECK_ENABLED(LogLevel::trace);
    _impl.print(LogLevel::trace, msg);
#endif
  }

  inline void trace(const char *format, ...) {
    va_list args;
    va_start (args, format);
#ifdef USE_LOG4CPP
    LOG4CPLUS_FATAL_FMT(_impl, format, args);
#else
    CHECK_ENABLED(LogLevel::trace);
    std::string output = prepare(format, args);
    trace(output);
#endif
    va_end(args);
  }

  inline void trace(std::ostringstream &os) {
    trace(os.str());
  }

  static Logger getLogger(std::string name) {
#ifdef USE_LOG4CPP
    auto l = log4cplus::Logger::getInstance(name);
    return Logger(l);
#else
    return Logger(SimpleLoggerImpl(name));
#endif // USE_LOG4CPP
  }
}; // Logger

#ifdef USE_LOG4CPP
#define LOG_TRACE(l, s) LOG4CPLUS_TRACE(l.getImpl(),s)
#define LOG_TRACE_F(l, ...) LOG4CPLUS_TRACE_FMT(l.getImpl(), __VA_ARGS__)

#define LOG_DEBUG(l, s)LOG4CPLUS_DEBUG(l.getImpl(),s)
#define LOG_DEBUG_F(l,...) LOG4CPLUS_DEBUG_FMT(l.getImpl(),__VA_ARGS__)

#define LOG_INFO(l, s) LOG4CPLUS_INFO(l.getImpl(),s)
#define LOG_INFO_F(l,...) LOG4CPLUS_INFO_FMT(l.getImpl(), __VA_ARGS__)

#define LOG_WARN(l, s) LOG4CPLUS_WARN(l.getImpl(),s)
#define LOG_WARN_F(logger,...) LOG4CPLUS_WARN_FMT(logger.getImpl(),__VA_ARGS__)

#define LOG_ERROR(l, s) LOG4CPLUS_ERROR(l.getImpl(),s)
#define LOG_ERROR_F(l,...) LOG4CPLUS_ERROR_FMT(l.getImpl(),__VA_ARGS__)

#define LOG_FATAL(l, s) LOG4CPLUS_FATAL(l.getImpl(),s)
#define LOG_FATAL_F(l,...) LOG4CPLUS_FATAL_FMT(l.getImpl(),__VA_ARGS__)
#else
#define LOG_TRACE(l, s) {std::ostringstream os; os << s; l.trace (os);}
#define LOG_TRACE_F(l, ...) l.trace(__VA_ARGS__)

#define LOG_DEBUG(l, s) {std::ostringstream os; os << s; l.debug (os);}
#define LOG_DEBUG_F(l, ...) l.debug(__VA_ARGS__)

#define LOG_INFO(l, s) {std::ostringstream os; os << s; l.info (os);}
#define LOG_INFO_F(l, ...) l.info(__VA_ARGS__)

#define LOG_WARN(l, s) {std::ostringstream os; os << s; l.warn(os);}
#define LOG_WARN_F(l, ...) l.warn(__VA_ARGS__)

#define LOG_ERROR(l, s) {std::ostringstream os; os << s; l.error(os);}
#define LOG_ERROR_F(l, ...) l.error(__VA_ARGS__)

#define LOG_FATAL(l, s) {std::ostringstream os; os << s; l.fatal(os);}
#define LOG_FATAL_F(l, ...) l.fatal(__VA_ARGS__)
#endif

} // namespace
#endif //CONCORD_BFT_LOGGING_HPP
