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
#include <iostream>

#ifndef USE_LOG4CPP

namespace concordlogger {

LogLevel CURRENT_LEVEL = LogLevel::info;

Logger Log::getLogger(const std::string& name) { return Logger(name); }

void Log::initLogger(const std::string& configFileName) { /* TBD */
}

static Logger defaultInitLogger() { return concordlogger::Log::getLogger(DEFAULT_LOGGER_NAME); }

}  // namespace concordlogger
#else
#include <log4cplus/logger.h>
#include <log4cplus/configurator.h>
#include <log4cplus/helpers/property.h>
#include <log4cplus/consoleappender.h>
#include <log4cplus/fileappender.h>

using namespace log4cplus;

namespace concordlogger {

const char* logPattern = "%X{rid}|%d{%m-%d-%Y %H:%M:%S.%q}|%-5p|%c|%X{thread}|%M|%m%n";

static Logger defaultInitLogger() {
  SharedAppenderPtr ca_ptr = SharedAppenderPtr(new ConsoleAppender(false, true));
  ca_ptr->setLayout(std::auto_ptr<Layout>(new PatternLayout(logPattern)));

  Logger::getRoot().addAppender(ca_ptr);
  Logger::getRoot().setLogLevel(INFO_LOG_LEVEL);
  return Logger::getInstance(LOG4CPLUS_TEXT(DEFAULT_LOGGER_NAME));
}

// first lookup a configuration file in the current directory
// if not found - use default configuration
void Log::initLogger(const std::string& configFileName) {
  // PropertyConfigurator propConfig (configFileName);
  helpers::Properties props(configFileName);
  PropertyConfigurator propConfig(props);
  propConfig.configure();
}
Logger Log::getLogger(const std::string& name) { return log4cplus::Logger::getInstance(name); }

}  // namespace concordlogger
#endif

namespace concordlogger {

ScopedMdc::ScopedMdc(const std::string& key, const std::string& val) : key_{key} { MDC_PUT(key, val); }
ScopedMdc::~ScopedMdc() { MDC_REMOVE(key_); }

}  // namespace concordlogger
concordlogger::Logger GL = concordlogger::defaultInitLogger();
