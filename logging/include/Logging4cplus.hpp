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
#include <log4cplus/loggingmacros.h>
#include <log4cplus/mdc.h>
#include <log4cplus/configurator.h>

#ifdef USE_LOG4CPP

namespace logging {

typedef log4cplus::Logger Logger;

std::string get(const std::string& key);

}  // namespace logging
#define LOG_FUNC(s) __func__ << "|" << s

#define LOG_TRACE(l, s) LOG4CPLUS_TRACE(l, LOG_FUNC(s))
#define LOG_DEBUG(l, s) LOG4CPLUS_DEBUG(l, LOG_FUNC(s))
#define LOG_INFO(l, s) LOG4CPLUS_INFO(l, LOG_FUNC(s))
#define LOG_WARN(l, s) LOG4CPLUS_WARN(l, LOG_FUNC(s))
#define LOG_ERROR(l, s) LOG4CPLUS_ERROR(l, LOG_FUNC(s))
#define LOG_FATAL(l, s) LOG4CPLUS_FATAL(l, LOG_FUNC(s))

#define MDC_PUT(k, v) log4cplus::getMDC().put(k, v)
#define MDC_REMOVE(k) log4cplus::getMDC().remove(k)
#define MDC_CLEAR log4cplus::getMDC().clear()
#define MDC_GET(k) logging::get(k)

#define LOG_CONFIGURE_AND_WATCH(config_file, millis) \
  log4cplus::ConfigureAndWatchThread configureThread(config_file, millis)

#endif
