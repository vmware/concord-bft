// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "environment.h"
#include "orrery_msgs.cmf.hpp"

namespace concord::orrery {

// A World is an abstraction that allows an orrery component to interact with other orrery
// components in the same process.
class World {
 public:
  World(const Environment& env, ComponentId sender) : env_(env), sender_(sender) {}

  // Send a message to a component or all components.
  //
  // This is the common case for normal protocol behavior.
  //
  // If `to == broadcast` then send to every component.
  //
  // Msg must be a top level msg in `Envelope`
  template <typename Msg>
  void send(ComponentId to, Msg&& msg) {
    if (to == ComponentId::broadcast) {
      return broadcast(std::forward<decltype(msg)>(msg));
    }
    auto& mailbox = env_.mailbox(to);
    mailbox.put(Envelope{to, sender_, AllMsgs{std::forward<decltype(msg)>(msg)}});
  }

  // Send a message to all components
  //
  // This is mostly useful for things like overload alarms, status, metrics, or shutdown events.
  // It's an explicit form to help readers of the code see that a message is a broadcast.
  //
  // Msg must be a top level msg in `Envelope`
  template <typename Msg>
  void broadcast(Msg&& msg) {
    auto envelope = Envelope{ComponentId::broadcast, sender_, AllMsgs{std::forward<decltype(msg)>(msg)}};
    for (auto& [_, mailbox] : env_.executors()) {
      (void)_;
      auto copy = envelope;
      mailbox.put(std::move(copy));
    }
  }

 private:
  Environment env_;
  ComponentId sender_;
};

}  // namespace concord::orrery
