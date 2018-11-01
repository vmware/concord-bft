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

#ifndef BFT_LOGGER_HPP_1
#define BFT_LOGGER_HPP_1

#include "Logging.hpp"

//TODO(GG): use a standard c++ log

// globals to support easy logging
extern concordlogger::Logger bftGlobalLogger;
#define GL bftGlobalLogger

class Logger {
 public:
  static void printInfo(const char *format, ...);
  static void printWarn(const char *format, ...);
  static void printError(const char *format, ...);

  static void printLastStackFrames();

  static void simpleAssert(bool cond, const char *msg);
};

#endif // BFT_LOGGER_HPP_1

