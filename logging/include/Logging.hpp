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

#pragma once

#include <string>
#include <sys/time.h>
#include <iomanip>
#include <iostream>
#include <map>
#include <array>
namespace logging {

/**
 *  Logging Levels
 */
enum LogLevel { trace, debug, info, warn, error, fatal };

/**
 * Mapped Diagnostic Context
 */
class MDC {
 public:
  void put(const std::string& key, const std::string& val) { mdc_map_.insert_or_assign(key, val); }
  std::string get(const std::string& key) { return mdc_map_[key]; }
  void remove(const std::string& key) { mdc_map_.erase(key); }
  void clear() { mdc_map_.clear(); }

 private:
  std::map<std::string, std::string> mdc_map_;
};

/**
 * Logger Thread Context
 * thread local
 */
class ThreadContext {
 public:
  MDC& getMDC() { return mdc_; }

 private:
  MDC mdc_;
};

/**
 * Logger implementation
 */
class LoggerImpl {
 public:
  LoggerImpl(const std::string& name) : name_(name) {}
  LoggerImpl(const LoggerImpl&) = delete;
  LoggerImpl& operator=(const LoggerImpl&) = delete;
  ~LoggerImpl() = default;

  std::ostream& print(logging::LogLevel l, const char* func) const {
    struct timeval cur_time;
    gettimeofday(&cur_time, NULL);

    // clang-format off
    std::cout << std::put_time(std::localtime(&cur_time.tv_sec), "%FT%T.") << (int)cur_time.tv_usec / 1000
       << "|" << LoggerImpl::LEVELS_STRINGS[l]
       << "|" << getThreadContext().getMDC().get(MDC_REPLICA_ID_KEY)
       << "|" << name_
       << "|" << getThreadContext().getMDC().get(MDC_THREAD_KEY)
       << "|" << getThreadContext().getMDC().get(MDC_CID_KEY)
       << "|" << getThreadContext().getMDC().get(MDC_SEQ_NUM_KEY)
       << "|" << func
       << "|";
    // clang-format on
    return std::cout;
  }

 private:
  friend class Logger;

  static ThreadContext& getThreadContext() {
    static thread_local ThreadContext t_;
    return t_;
  }

  std::string name_;
  LogLevel level_ = LogLevel::info;
  static std::array<std::string, 6> LEVELS_STRINGS;
};

/**
 * Main logger class - is a wrapper around LoggerImpl
 * since this class is copied around.
 */
class Logger {
 public:
  Logger(LoggerImpl& logger) : logger_{&logger} {}
  std::ostream& print(logging::LogLevel l, const char* func) const { return logger_->print(l, func); }
  LogLevel getLogLevel() const { return logger_->level_; }
  void setLogLevel(LogLevel l) { logger_->level_ = l; }
  static ThreadContext& getThreadContext() { return LoggerImpl::getThreadContext(); }
  static bool config(const std::string& configFileName);

 private:
  LoggerImpl* logger_;
};

}  // namespace logging

#define LOG_COMMON(logger, level, s)                                                                              \
  if ((logger).getLogLevel() <= level) {                                                                          \
    (logger).print(level, __PRETTY_FUNCTION__) << s << " | [SQ:" << std::to_string(getSeq()) << "]" << std::endl; \
  }

#define LOG_TRACE(l, s) LOG_COMMON(l, logging::LogLevel::trace, s)
#define LOG_DEBUG(l, s) LOG_COMMON(l, logging::LogLevel::debug, s)
#define LOG_INFO(l, s) LOG_COMMON(l, logging::LogLevel::info, s)
#define LOG_WARN(l, s) LOG_COMMON(l, logging::LogLevel::warn, s)
#define LOG_ERROR(l, s) LOG_COMMON(l, logging::LogLevel::error, s)
#define LOG_FATAL(l, s) LOG_COMMON(l, logging::LogLevel::fatal, s)

#define MDC_PUT(k, v) logging::Logger::getThreadContext().getMDC().put(k, v);
#define MDC_REMOVE(k) logging::Logger::getThreadContext().getMDC().remove(k);
#define MDC_CLEAR logging::Logger::getThreadContext().getMDC().clear();
#define MDC_GET(k) logging::Logger::getThreadContext().getMDC().get(k)

#define LOG_CONFIGURE_AND_WATCH(config_file, millis) \
  {                                                  \
    logging::initLogger(config_file);                \
    (void)(millis);                                  \
  }
