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

namespace concordlogger {

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

class Logger
{
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


  explicit Logger(std::string name) : _name{std::move(name)} {}

  void print(concordlogger::LogLevel l, const std::string& s) {
    std::stringstream time;
    get_time(time);
    std::cout << Logger::LEVELS_STRINGS[l].c_str() << " "
              << time.str() << " "
              << "(" << _name << ")"
              << s
              << std::endl;
  }

  void print(concordlogger::LogLevel l, const char* format, ...) __attribute__ ((format (__printf__, 3, 4))){
    std::stringstream time;
    get_time(time);
    va_list args;
    va_start (args, format);
    static constexpr size_t size = 1024;
    std::string output(size, '\0');
    //int res = 
    std::vsnprintf(const_cast<char*>(output.c_str()), size, format, args);
    //assert(res >= 0);
    va_end(args);

    printf("%s %s (%s) %s\n",
           Logger::LEVELS_STRINGS[l].c_str(),
           time.str().c_str(),
           _name.c_str(),
           output.c_str());

  }
};

class Log
{
 public:
  static Logger getLogger(std::string name) { return Logger(name);}
};

} // namespace

#define LOG_COMMON_F(logger, level, ...)                                \
    if (concordlogger::CURRENT_LEVEL != concordlogger::LogLevel::off && \
        level >= concordlogger::CURRENT_LEVEL){                         \
      logger.print(level, __VA_ARGS__);                                 \
  }

#define LOG_COMMON(logger, level, s)                                    \
    if (concordlogger::CURRENT_LEVEL != concordlogger::LogLevel::off && \
        level >= concordlogger::CURRENT_LEVEL){                         \
      std::ostringstream oss;                                           \
      oss << s;                                                         \
      logger.print(level, oss.str());                                   \
  }

#define LOG_TRACE(l, s)     LOG_COMMON(l, concordlogger::LogLevel::trace, s)
#define LOG_TRACE_F(l, ...) LOG_COMMON_F(l, concordlogger::LogLevel::trace, __VA_ARGS__)

#define LOG_DEBUG(l, s)     LOG_COMMON(l, concordlogger::LogLevel::debug, s)
#define LOG_DEBUG_F(l, ...) LOG_COMMON_F(l, concordlogger::LogLevel::debug, __VA_ARGS__)

#define LOG_INFO(l, s)      LOG_COMMON(l, concordlogger::LogLevel::info, s)
#define LOG_INFO_F(l, ...)  LOG_COMMON_F(l, concordlogger::LogLevel::info, __VA_ARGS__)

#define LOG_WARN(l, s)      LOG_COMMON(l, concordlogger::LogLevel::warn, s)
#define LOG_WARN_F(l, ...)  LOG_COMMON_F(l, concordlogger::LogLevel::warn, __VA_ARGS__)

#define LOG_ERROR(l, s)     LOG_COMMON(l, concordlogger::LogLevel::error, s)
#define LOG_ERROR_F(l, ...) LOG_COMMON_F(l, concordlogger::LogLevel::error, __VA_ARGS__)

#define LOG_FATAL(l, s)     LOG_COMMON(l, concordlogger::LogLevel::fatal, s)
#define LOG_FATAL_F(l, ...) LOG_COMMON_F(l, concordlogger::LogLevel::fatal, __VA_ARGS__)

