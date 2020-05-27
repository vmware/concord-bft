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
#include <sstream>
#include <chrono>
#include <iomanip>
#include <stdarg.h>
#include <cassert>
#include <iostream>
#include <unordered_map>
#include <mutex>

#ifndef USE_LOG4CPP
namespace concordlogger {

// log levels as defined in log4cpp
enum LogLevel { trace, debug, info, warn, error, fatal, off, all = trace };

extern LogLevel CURRENT_LEVEL;
/**
 * Mapped Diagnostic Context
 */
class MDC {
 public:
  typedef std::unordered_map<std::string, std::string> MDCMap;

  MDC() = default;

  void put(const std::string& key, const std::string& val) { mdc_map_.insert_or_assign(key, val); }
  std::string get(const std::string& key) { return mdc_map_[key]; }
  void remove(const std::string& key) { mdc_map_.erase(key); }
  void clear() { mdc_map_.clear(); }

  MDCMap const& getContext() const { return mdc_map_; }

 private:
  MDCMap mdc_map_;
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
 * Main Logger Facility
 */
class Logger {
  std::string name_;
  std::array<std::string, 6> LEVELS_STRINGS = {"TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"};

 public:
  explicit Logger(std::string name) : name_{std::move(name)} {}
  Logger(const Logger& l) {
    name_ = l.name_;
    LEVELS_STRINGS = l.LEVELS_STRINGS;
  }

  Logger& operator=(const Logger& rhs) {
    if (this == &rhs) return *this;
    name_ = rhs.name_;
    LEVELS_STRINGS = rhs.LEVELS_STRINGS;

    return *this;
  }

  void print(concordlogger::LogLevel l, const char* func, const std::string& s) const {
    std::stringstream time;
    get_time(time);
    std::cout << getThreadContext().getMDC().get(MDC_REPLICA_ID_KEY) << "|" << time.str() << "|"
              << Logger::LEVELS_STRINGS[l].c_str() << "|" << name_ << "|"
              << getThreadContext().getMDC().get(MDC_THREAD_KEY) << "|" << getThreadContext().getMDC().get(MDC_CID_KEY)
              << "|" << getThreadContext().getMDC().get(MDC_SEQ_NUM_KEY) << "|" << func << "|" << s << std::endl;
  }

  static ThreadContext& getThreadContext() {
    static thread_local ThreadContext t_;
    return t_;
  }

 private:
  inline void get_time(std::stringstream& ss) const {
    using namespace std::chrono;
    auto now = system_clock::now();
    auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
    auto timer = system_clock::to_time_t(now);
    std::tm bt = *std::localtime(&timer);
    ss << std::put_time(&bt, "%F %T") << "." << std::setfill('0') << std::setw(3) << ms.count();
  }
};

}  // namespace concordlogger

#define LOG_COMMON(logger, level, s)                                                                           \
  if (concordlogger::CURRENT_LEVEL != concordlogger::LogLevel::off && level >= concordlogger::CURRENT_LEVEL) { \
    std::ostringstream oss;                                                                                    \
    oss << s;                                                                                                  \
    logger.print(level, __PRETTY_FUNCTION__, oss.str());                                                       \
  }

#define LOG_TRACE(l, s) LOG_COMMON(l, concordlogger::LogLevel::trace, s)

#define LOG_DEBUG(l, s) LOG_COMMON(l, concordlogger::LogLevel::debug, s)

#define LOG_INFO(l, s) LOG_COMMON(l, concordlogger::LogLevel::info, s)

#define LOG_WARN(l, s) LOG_COMMON(l, concordlogger::LogLevel::warn, s)

#define LOG_ERROR(l, s) LOG_COMMON(l, concordlogger::LogLevel::error, s)

#define LOG_FATAL(l, s) LOG_COMMON(l, concordlogger::LogLevel::fatal, s)

#define MDC_PUT(k, v) concordlogger::Logger::getThreadContext().getMDC().put(k, v);
#define MDC_REMOVE(k) concordlogger::Logger::getThreadContext().getMDC().remove(k);

#endif
