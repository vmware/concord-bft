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
#ifndef USE_LOG4CPP
concordlogger::Logger initLogger() { return concordlogger::Log::getLogger(DEFAULT_LOGGER_NAME); }
concordlogger::MDC::MDC(concordlogger::Logger &logger, const std::string &key, const std::string &value)
    : logger_(logger), key_(key) {
  logger_.putMdc(key, value);
}
concordlogger::MDC::~MDC() { logger_.removeMdc(key_); }
#else
#include <log4cplus/logger.h>
#include <log4cplus/configurator.h>
#include <log4cplus/consoleappender.h>
#include <log4cplus/mdc.h>

concordlogger::Logger initLogger() {
  log4cplus::SharedAppenderPtr ca_ptr = log4cplus::SharedAppenderPtr(new log4cplus::ConsoleAppender(false, true));
  ca_ptr->setLayout(
      std::auto_ptr<log4cplus::Layout>(new log4cplus::PatternLayout("[Node %X{rid}] [%t] %%%X%% %-5p|%c||%M|%m|%n ")));
  log4cplus::Logger::getRoot().addAppender(ca_ptr);
  log4cplus::Logger::getRoot().setLogLevel(log4cplus::INFO_LOG_LEVEL);
  return log4cplus::Logger::getInstance(LOG4CPLUS_TEXT(DEFAULT_LOGGER_NAME));
}

concordlogger::MDC::MDC(concordlogger::Logger &logger, const std::string &key, const std::string &value)
    : logger_(logger), key_(key) {
  (void)logger_;
  log4cplus::getMDC().put(key_, value);
}
concordlogger::MDC::~MDC() {
  (void)logger_;
  log4cplus::getMDC().remove(key_);
  ;
}

#endif

concordlogger::Logger GL = initLogger();
