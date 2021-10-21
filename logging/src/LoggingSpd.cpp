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
#include "LoggingSpd.hpp"
#include <memory>
#include <fstream>
#include <iostream>

#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"
#include "spdlog/async.h"  //support for async logging.
#include "spdlog/sinks/basic_file_sink.h"

namespace logging {

static const char* logPattern = "%d{%Y-%m-%dT%H:%M:%S,%qZ}|%-5p|%X{rid}|%c|%X{thread}|%X{cid}|%X{sn}|%b:%L|%M|%m%n";

bool defaultInit() {
  (void)logPattern;
  spdlog::init_thread_pool(65536, 1);
  return true;
}

void initLogger(const std::string& configFileName) {}

Logger getLogger(const std::string& name) {
  static bool __logging_init__ = defaultInit();  // one time initialization
  (void)__logging_init__;
  Logger theLogger = spdlog::get(name);
  if (theLogger == nullptr) {
    spdlog::stdout_logger_mt<spdlog::async_factory>(name);
    theLogger = spdlog::get(name);
  }
  return theLogger;
}

std::string get(const std::string& key) {
  (void)key;
  return "";
}

}  // namespace logging