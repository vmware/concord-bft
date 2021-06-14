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

#include <log4cplus/logger.h>
#include <log4cplus/helpers/property.h>
#include <log4cplus/consoleappender.h>
#include <log4cplus/fileappender.h>
#include <log4cplus/initializer.h>

using namespace log4cplus;

namespace logging {

static const char* logPattern = "%d{%Y-%m-%dT%H:%M:%S,%qZ}|%-5p|%X{rid}|%c|%X{thread}|%X{cid}|%X{sn}|%b:%L|%M|%m%n";

void initLogger(const std::string& configFileName) {
  std::ifstream infile(configFileName);
  if (!infile.is_open()) {
    std::cerr << __PRETTY_FUNCTION__ << ": can't open " << configFileName << " using default configuration."
              << std::endl;
    return;
  }
  infile.close();
  helpers::Properties props(configFileName);
  PropertyConfigurator propConfig(props);
  propConfig.configure();
}

bool defaultInit() {
  log4cplus::initialize();
  SharedAppenderPtr ca_ptr = SharedAppenderPtr(new ConsoleAppender(false, true));
  ca_ptr->setLayout(std::unique_ptr<Layout>(new PatternLayout(logPattern)));

  Logger::getRoot().addAppender(ca_ptr);
  Logger::getRoot().setLogLevel(INFO_LOG_LEVEL);
  return true;
}

Logger getLogger(const std::string& name) {
  static bool __logging_init__ = defaultInit();  // one time initialization
  (void)__logging_init__;
  return log4cplus::Logger::getInstance(name);
}

std::string get(const std::string& key) {
  std::string result;
  log4cplus::getMDC().get(&result, key);
  return result;
}

}  // namespace logging
