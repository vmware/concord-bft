
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
  StatusHandler(const std::string& name, const std::string& description, std::function<std::string()> fun)
      : name(name), description(description), f(std::move(fun)) {}
  std::string name;
  std::string description;
  std::function<std::string()> f;
};

class StatusHandlers {
 public:
  void registerHandler(const StatusHandler& handler);
  std::string get(const std::string& name) const;
  std::string describe(const std::string& name) const;
  std::string describe() const;
  std::string listKeys() const;

  // DO NOT USE THIS IN PRODUCTION. THIS IS ONLY FOR TESTING, SO THAT WE CAN CLEAR THE SINGLETON AND REREGISTER.
  void clear() {
    std::lock_guard<std::mutex> guard(mutex_);
    status_handlers_.clear();
  }

  std::map<std::string, StatusHandler> status_handlers_;
  mutable std::mutex mutex_;
};

}  // namespace concord::diagnostics
