// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#pragma once

#include <string>
#include <sstream>
#include "spdlog/spdlog.h"

#ifdef USE_SPDLOG

namespace logging {

// typedef log4cplus::Logger Logger;
using Logger = std::shared_ptr<spdlog::logger>;

std::string get(const std::string& key);

}  // namespace logging

#define LOG_TRACE(l, s)                        \
  {                                            \
    if (l->should_log(spdlog::level::trace)) { \
      std::ostringstream ss;                   \
      ss << s;                                 \
      l->trace(ss.str());                      \
    }                                          \
  }
#define LOG_DEBUG(l, s)                        \
  {                                            \
    if (l->should_log(spdlog::level::debug)) { \
      std::ostringstream ss;                   \
      ss << s;                                 \
      l->debug(ss.str());                      \
    }                                          \
  }
#define LOG_INFO(l, s)                        \
  {                                           \
    if (l->should_log(spdlog::level::info)) { \
      std::ostringstream ss;                  \
      ss << s;                                \
      l->info(ss.str());                      \
    }                                         \
  }
#define LOG_WARN(l, s)                        \
  {                                           \
    if (l->should_log(spdlog::level::warn)) { \
      std::ostringstream ss;                  \
      ss << s;                                \
      l->warn(ss.str());                      \
    }                                         \
  }
#define LOG_ERROR(l, s)                      \
  {                                          \
    if (l->should_log(spdlog::level::err)) { \
      std::ostringstream ss;                 \
      ss << s;                               \
      l->error(ss.str());                    \
    }                                        \
  }
#define LOG_FATAL(l, s)                           \
  {                                               \
    if (l->should_log(spdlog::level::critical)) { \
      std::ostringstream ss;                      \
      ss << s;                                    \
      l->critical(ss.str());                      \
    }                                             \
  }

#define MDC_PUT(k, v) \
  {}
#define MDC_REMOVE(k) \
  {}
#define MDC_CLEAR \
  {}
#define MDC_GET(k) std::string("temporary out of order");

#define LOG_CONFIGURE_AND_WATCH(config_file, millis) \
  {                                                  \
    (void)config_file;                               \
    (void)millis;                                    \
  }

#endif
