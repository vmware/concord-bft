// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once
#include "callback_registry.hpp"
#include <functional>
namespace bft::client {
class StateControl {
 public:
  static StateControl& instance() {
    static StateControl sc;
    return sc;
  }

  void setRestartFunc(const std::function<void()>& f) { restart_registry_.add(f); }

  void restart() { restart_registry_.invokeAll(); }

 private:
  concord::util::CallbackRegistry<> restart_registry_;
};
}  // namespace bft::client