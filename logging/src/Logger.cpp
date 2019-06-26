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
concordlogger::Logger initLogger()
{
  return concordlogger::Log::getLogger(DEFAULT_LOGGER_NAME);
}
#else
#include <log4cplus/logger.h>
#include <log4cplus/configurator.h>

concordlogger::Logger initLogger()
{
  log4cplus::initialize();
  log4cplus::BasicConfigurator config;
  config.configure();
  return log4cplus::Logger::getInstance( LOG4CPLUS_TEXT(DEFAULT_LOGGER_NAME));
}


#endif

concordlogger::Logger GL = initLogger();
