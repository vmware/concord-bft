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
#ifdef USE_LOG4CPP

namespace concordlogger {

typedef log4cplus::Logger Logger;

}  // namespace concordlogger

#define LOG_TRACE(l, s) LOG4CPLUS_TRACE(l, s)

#define LOG_DEBUG(l, s) LOG4CPLUS_DEBUG(l, s)

#define LOG_INFO(l, s) LOG4CPLUS_INFO(l, s)

#define LOG_WARN(l, s) LOG4CPLUS_WARN(l, s)

#define LOG_ERROR(l, s) LOG4CPLUS_ERROR(l, s)

#define LOG_FATAL(l, s) LOG4CPLUS_FATAL(l, s)

#define MDC_PUT(k, v) log4cplus::getMDC().put(k, v)
#define MDC_REMOVE(k) log4cplus::getMDC().remove(k)

#endif
