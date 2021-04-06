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

#include "orrery/executor.h"
#include <variant>
#include "assertUtils.hpp"
#include "orrery_msgs.cmf.hpp"

using namespace std::chrono_literals;

namespace concord::orrery {

static constexpr std::chrono::milliseconds TIMEOUT = 100ms;

std::thread Executor::start() && {
  return std::thread([executor{std::move(*this)}]() mutable {
    while (true) {
      if (auto envelope = executor.queue_->pop(TIMEOUT)) {
        if (envelope->to == ComponentId::broadcast) {
          for (auto& component : executor.components_) {
            if (component) {
              auto copy = envelope->all_msgs;
              component->handle(envelope->from, std::move(copy));
            }
          }
          if (std::holds_alternative<ControlMsg>(envelope->all_msgs.msg)) {
            if (std::get<ControlMsg>(envelope->all_msgs.msg).cmd == ControlCmd::shutdown) {
              LOG_INFO(executor.logger_, "Shutting down executor: " << executor.name());
              return;
            }
          }
        } else {
          auto* component = executor.get(envelope->to);
          ConcordAssertNE(nullptr, component);
          component->handle(envelope->from, std::move(envelope->all_msgs));
        }
      }
    }
  });
}

}  // namespace concord::orrery
