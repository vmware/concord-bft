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

#pragma once
#include <functional>
#include <map>
#include <mutex>
#include <stdexcept>

namespace concord::diagnostics {

// Every component that can return status should register a status handler with the Registrar.
struct StatusHandler {
  StatusHandler(std::string name, std::string description, std::function<std::string()> fun)
      : name(std::move(name)), description(std::move(description)), f(std::move(fun)) {}
  std::string name;
  std::string description;
  std::function<std::string()> f;
};

class Registrar {
 public:
  void registerStatusHandler(StatusHandler handler) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (status_handlers_.count(handler.name)) {
      throw std::invalid_argument(std::string("StatusHandler already exists: ") + handler.name);
    }
    status_handlers_.insert({handler.name, handler});
  }

  std::string getStatus(const std::string& name) const {
    std::lock_guard<std::mutex> guard(mutex_);
    if (status_handlers_.count(name)) {
      return status_handlers_.at(name).f();
    }
    return "*--STATUS_NOT_FOUND--*";
  }

  std::string describeStatus(const std::string& name) const {
    std::lock_guard<std::mutex> guard(mutex_);
    if (status_handlers_.count(name)) {
      return status_handlers_.at(name).description;
    }
    return "*--DESCRIPTION_NOT_FOUND--*";
  }

  std::string describeStatus() const {
    std::lock_guard<std::mutex> guard(mutex_);
    std::string output;
    for (const auto& [_, handler]: status_handlers_) {
      (void)_; // undefined variable hack
      output += "\n" + handler.name + "\n  ";
      output += handler.description + "\n";
    }
    return output;
  }

  std::string listStatusKeys() const {
    std::lock_guard<std::mutex> guard(mutex_);
    std::string output;
    for (const auto& [key, _]: status_handlers_) {
      (void)_; // unused variable hack
      output += key + "\n";
    }
    return output;
  }

 private:
  std::map<std::string, StatusHandler> status_handlers_;
  mutable std::mutex mutex_;
};

}  // namespace concord::diagnostics

