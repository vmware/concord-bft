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

constexpr LogLevel CURRENT_LEVEL = LogLevel::info;

class Logger {
  std::string _name;
  std::array<std::string, 6> LEVELS_STRINGS = {"TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"};
  std::unordered_map<std::string, std::string> mdc_;
  mutable std::mutex mdc_mutex_;
  // If you add new members, don't forget to handle them in copy constructor and operator=

 public:
  explicit Logger(std::string name) : _name{std::move(name)} {}
  Logger(const Logger& l) {
    std::lock_guard<std::mutex> lock(l.mdc_mutex_);

    _name = l._name;
    LEVELS_STRINGS = l.LEVELS_STRINGS;
    mdc_ = l.mdc_;
  }

  Logger& operator=(const Logger& rhs) {
    if (this == &rhs) {
      return *this;
    }

    std::lock_guard<std::mutex> lock_rhs(rhs.mdc_mutex_);
    std::lock_guard<std::mutex> lock(mdc_mutex_);

    _name = rhs._name;
    LEVELS_STRINGS = rhs.LEVELS_STRINGS;
    mdc_ = rhs.mdc_;

    return *this;
  }

  void print(concordlogger::LogLevel l, const std::string& s) const {
    std::stringstream time;
    get_time(time);
    std::cout << Logger::LEVELS_STRINGS[l].c_str() << " " << time.str() << " "
              << "(" << _name << ")"
              << " " << mdcToStr() << " " << s << std::endl;
  }

  void print(concordlogger::LogLevel l, const char* format, ...) __attribute__((format(__printf__, 3, 4))) {
    std::stringstream time;
    get_time(time);
    va_list args;
    va_start(args, format);
    static constexpr size_t size = 1024;
    std::string output(size, '\0');
    std::vsnprintf(const_cast<char*>(output.c_str()), size, format, args);
    va_end(args);

    printf("%s %s (%s) %s %s\n",
           Logger::LEVELS_STRINGS[l].c_str(),
           time.str().c_str(),
           _name.c_str(),
           mdcToStr().c_str(),
           output.c_str());
  }

  void putMdc(const std::string& key, const std::string& val) {
    std::lock_guard<std::mutex> lock(mdc_mutex_);
    mdc_.emplace(key, val);
  }

  void removeMdc(const std::string& key) {
    std::lock_guard<std::mutex> lock(mdc_mutex_);
    mdc_.erase(key);
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

  inline const std::string mdcToStr() const {
    std::lock_guard<std::mutex> lock(mdc_mutex_);
    if (mdc_.empty()) return "";
    std::stringstream s;
    s << "%";
    for (auto& p : mdc_) {
      s << "{" << p.first << "," << p.second << "}";
    }
    s << "%";
    return s.str();
  }
};

class Log {
 public:
  static Logger getLogger(std::string name) { return Logger(name); }
};

}  // namespace concordlogger

#define LOG_COMMON_F(logger, level, ...)                                                                       \
  if (concordlogger::CURRENT_LEVEL != concordlogger::LogLevel::off && level >= concordlogger::CURRENT_LEVEL) { \
    logger.print(level, __VA_ARGS__);                                                                          \
  }

#define LOG_COMMON(logger, level, s)                                                                           \
  if (concordlogger::CURRENT_LEVEL != concordlogger::LogLevel::off && level >= concordlogger::CURRENT_LEVEL) { \
    std::ostringstream oss;                                                                                    \
    oss << s;                                                                                                  \
    logger.print(level, oss.str());                                                                            \
  }

#define LOG_TRACE(l, s) LOG_COMMON(l, concordlogger::LogLevel::trace, s)
#define LOG_TRACE_F(l, ...) LOG_COMMON_F(l, concordlogger::LogLevel::trace, __VA_ARGS__)

#define LOG_DEBUG(l, s) LOG_COMMON(l, concordlogger::LogLevel::debug, s)
#define LOG_DEBUG_F(l, ...) LOG_COMMON_F(l, concordlogger::LogLevel::debug, __VA_ARGS__)

#define LOG_INFO(l, s) LOG_COMMON(l, concordlogger::LogLevel::info, s)
#define LOG_INFO_F(l, ...) LOG_COMMON_F(l, concordlogger::LogLevel::info, __VA_ARGS__)

#define LOG_WARN(l, s) LOG_COMMON(l, concordlogger::LogLevel::warn, s)
#define LOG_WARN_F(l, ...) LOG_COMMON_F(l, concordlogger::LogLevel::warn, __VA_ARGS__)

#define LOG_ERROR(l, s) LOG_COMMON(l, concordlogger::LogLevel::error, s)
#define LOG_ERROR_F(l, ...) LOG_COMMON_F(l, concordlogger::LogLevel::error, __VA_ARGS__)

#define LOG_FATAL(l, s) LOG_COMMON(l, concordlogger::LogLevel::fatal, s)
#define LOG_FATAL_F(l, ...) LOG_COMMON_F(l, concordlogger::LogLevel::fatal, __VA_ARGS__)

#endif
