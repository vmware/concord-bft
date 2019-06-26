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
 * GL and initLogger() have to be defined by a user in a following way:
 * concordlogger::Logger GL = initLogger();
 *
 * For development purposes anyone can link with $<TARGET_OBJECTS:logging_dev>
 * and get a default initializations for both loggers.
 */
extern concordlogger::Logger GL;
extern concordlogger::Logger initLogger();
