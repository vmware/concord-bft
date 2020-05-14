// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to
// the terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#pragma once

#ifndef USE_LOG4CPP
#include "Logging.hpp"
#else
#include "Logging4cplus.hpp"
#endif

/**
 * For development purposes anyone can link with $<TARGET_OBJECTS:logging_dev>
 * and get a default initializations for both loggers.
 */

extern concordlogger::Logger GL;

namespace concordlogger {

class Log {
 public:
  static Logger getLogger(const std::string& name);
  static void initLogger(const std::string& configFileName);
};

class MDC {
  concordlogger::Logger& logger_;
  std::string key_;

 public:
  MDC(concordlogger::Logger& logger, const std::string& key, const std::string& value);
  MDC(concordlogger::Logger& logger, const std::string& key, int value) : MDC(logger, key, std::to_string(value)) {}
  ~MDC();
};

}  // namespace concordlogger

/*
 * These macros are meant to append temporary key-value pairs to the log messages.
 * The duration of this adding is the scope where MDC_PUT was called - when reaching to the end of this scope,
 * the key-value will be automatically removed.
 * When using the internal logger of concord-bft, the temporary key-value will be added to the given logger.
 * When using log4cpp, the key-value pair will be added to the current thread logger.
 */
#define MDC_PUT(l, k, v) concordlogger::MDC mdc_((l), (k), (v))
#define CID_KEY "cid"
#define MDC_CID_PUT(l, v) MDC_PUT(l, CID_KEY, v)
#define SEQ_NUM_KEY "sn"
