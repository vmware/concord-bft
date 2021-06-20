// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "status_handlers.h"
#include "Logger.hpp"

namespace concord::diagnostics {

static logging::Logger DIAG_LOGGER = logging::getLogger("concord.diag.status");

void StatusHandlers::registerHandler(const StatusHandler& handler) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (status_handlers_.count(handler.name)) {
    LOG_FATAL(DIAG_LOGGER, "StatusHandler already exists: " << handler.name);
    std::terminate();
  }
  status_handlers_.insert({handler.name, handler});
}

std::string StatusHandlers::get(const std::string& name) const {
  std::lock_guard<std::mutex> guard(mutex_);
  if (status_handlers_.count(name)) {
    return status_handlers_.at(name).f();
  }
  return "*--STATUS_NOT_FOUND--*";
}

std::string StatusHandlers::describe(const std::string& name) const {
  std::lock_guard<std::mutex> guard(mutex_);
  if (status_handlers_.count(name)) {
    return status_handlers_.at(name).description;
  }
  return "*--DESCRIPTION_NOT_FOUND--*";
}

std::string StatusHandlers::describe() const {
  std::string output;
  std::lock_guard<std::mutex> guard(mutex_);
  for (const auto& [_, handler] : status_handlers_) {
    (void)_;  // undefined variable hack
    output += "\n" + handler.name + "\n  ";
    output += handler.description + "\n";
  }
  return output;
}

std::string StatusHandlers::listKeys() const {
  std::lock_guard<std::mutex> guard(mutex_);
  std::string output;
  for (const auto& [key, _] : status_handlers_) {
    (void)_;  // unused variable hack
    output += key + "\n";
  }
  return output;
}

}  // namespace concord::diagnostics
