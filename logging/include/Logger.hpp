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

#define MDC_THREAD_KEY "thread"
#define MDC_REPLICA_ID_KEY "rid"
#define MDC_CID_KEY "cid"
#define MDC_SEQ_NUM_KEY "sn"

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

class ScopedMdc {
 public:
  ScopedMdc(const std::string& key, const std::string& val);
  ~ScopedMdc();

 private:
  const std::string key_;
};

}  // namespace concordlogger

/*
 * These macros are meant to append temporary key-value pairs to the log messages.
 * The duration of this adding is the scope where SCOPED_MDC was called - when reaching to the end of this scope,
 * the key-value will be automatically removed.
 */

#define SCOPED_MDC(k, v) concordlogger::ScopedMdc __s_mdc__(k, v)
#define SCOPED_MDC_CID(v) concordlogger::ScopedMdc __s_mdc_cid__(MDC_CID_KEY, v)
#define SCOPED_MDC_SEQ_NUM(v) concordlogger::ScopedMdc __s_mdc_seq_num__(MDC_SEQ_NUM_KEY, v)
