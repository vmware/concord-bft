// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license,
// as noted in the LICENSE file.

#include "Logger.hpp"
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
namespace logging {

std::array<std::string, 6> LoggerImpl::LEVELS_STRINGS = {"TRACE", "DEBUG", "INFO ", "WARN ", "ERROR", "FATAL"};

/**
 * This function is usually called at startup so there will be no contention on mutex during runtime.
 * It could be implemented using static thread_local map without a mutex,
 * but then the on-the-fly configuration change would be more complex.
 */
Logger getLogger(const std::string& name) {
  static std::map<std::string, LoggerImpl> logmap_;
  static std::mutex mux;
  std::lock_guard g(mux);
  return logmap_.try_emplace(name, name).first->second;
}

void initLogger(const std::string& configFileName) {
  if (!Logger::config(configFileName)) {
    std::cout << "using default configuration" << std::endl;
  }
}

/**
 * simple configuration
 * logger_name:logging_level
 * TODO [TK]: extend
 */
bool Logger::config(const std::string& configFileName) {
  std::ifstream infile(configFileName);
  if (!infile.is_open()) {
    std::cerr << __PRETTY_FUNCTION__ << ": can't open " << configFileName << std::endl;
    return false;
  }
  std::string line;
  while (std::getline(infile, line)) {
    // concord::util::trim_inplace(line); TODO [TK]
    if (line[0] == '#') continue;              // comment
    if (line.compare(0, 4, "log.")) continue;  // not my configuration
    line.erase(0, 4);
    if (size_t pos = line.find(":"); pos != line.npos) {
      std::string logger = line.substr(0, pos);
      std::string levelStr = line.substr(pos + 1);
      LogLevel level;
      if (!levelStr.compare("TRACE"))
        level = LogLevel::trace;
      else if (!levelStr.compare("DEBUG"))
        level = LogLevel::debug;
      else if (!levelStr.compare("INFO"))
        level = LogLevel::info;
      else if (!levelStr.compare("WARN"))
        level = LogLevel::warn;
      else if (!levelStr.compare("ERROR"))
        level = LogLevel::error;
      else if (!levelStr.compare("FATAL"))
        level = LogLevel::fatal;
      else {
        std::cerr << __PRETTY_FUNCTION__ << ": ignoring invalid log level " << levelStr << std::endl;
        continue;
      }
      std::cout << __PRETTY_FUNCTION__ << ": " << logger << " -> " << levelStr << std::endl;
      getLogger(logger).setLogLevel(level);
    }
  }
  return true;
}

}  // namespace logging
