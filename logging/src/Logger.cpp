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

Logger Log::getLogger(const std::string &name) { return Logger(name); }

void Log::initLogger(const std::string &configFileName) { /* TBD */
}

static Logger defaultInitLogger() { return concordlogger::Log::getLogger(DEFAULT_LOGGER_NAME); }

MDC::MDC(concordlogger::Logger &logger, const std::string &key, const std::string &value) : logger_(logger), key_(key) {
  logger_.putMdc(key, value);
}

MDC::~MDC() { logger_.removeMdc(key_); }

}  // namespace concordlogger
#else
#include <log4cplus/logger.h>
#include <log4cplus/configurator.h>
#include <log4cplus/helpers/property.h>
#include <log4cplus/consoleappender.h>
#include <log4cplus/fileappender.h>
#include <log4cplus/mdc.h>
#if __has_include(<filesystem>)
#include <filesystem>
namespace fs = std::filesystem;
#elif __has_include(<experimental/filesystem>)
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#else
#error "Missing filesystem support"
#endif

using namespace log4cplus;

namespace concordlogger {

const char* logPattern = "%X{rid}|%d{%m-%d-%Y %H:%M:%S.%q}|%t|%-5p|%c|%M|%m%n";

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
  if (!fs::exists(configFileName)) {
    std::cerr << __PRETTY_FUNCTION__ << ": log4cplus properties file " << configFileName
              << " not found in the current dir" << std::endl;
    return;
  }
  // PropertyConfigurator propConfig (configFileName);
  helpers::Properties props(configFileName);
  PropertyConfigurator propConfig(props);
  propConfig.configure();
}
Logger Log::getLogger(const std::string& name) { return log4cplus::Logger::getInstance(name); }

MDC::MDC(concordlogger::Logger& logger, const std::string& key, const std::string& value) : logger_(logger), key_(key) {
  (void)logger_;
  getMDC().put(key_, value);
}

MDC::~MDC() {
  (void)logger_;
  getMDC().remove(key_);
}

}  // namespace concordlogger
#endif

concordlogger::Logger GL = concordlogger::defaultInitLogger();
