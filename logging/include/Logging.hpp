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

}  // namespace concordlogger

#define LOG_COMMON(logger, level, s)                                                                           \
  if (concordlogger::CURRENT_LEVEL != concordlogger::LogLevel::off && level >= concordlogger::CURRENT_LEVEL) { \
    std::ostringstream oss;                                                                                    \
    oss << s;                                                                                                  \
    logger.print(level, oss.str());                                                                            \
  }

#define LOG_TRACE(l, s) LOG_COMMON(l, concordlogger::LogLevel::trace, s)

#define LOG_DEBUG(l, s) LOG_COMMON(l, concordlogger::LogLevel::debug, s)

#define LOG_INFO(l, s) LOG_COMMON(l, concordlogger::LogLevel::info, s)

#define LOG_WARN(l, s) LOG_COMMON(l, concordlogger::LogLevel::warn, s)

#define LOG_ERROR(l, s) LOG_COMMON(l, concordlogger::LogLevel::error, s)

#define LOG_FATAL(l, s) LOG_COMMON(l, concordlogger::LogLevel::fatal, s)

#endif
