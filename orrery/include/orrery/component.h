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

#include <utility>
#include <type_traits>

#include "kvstream.h"
#include "orrery_msgs.cmf.hpp"
#include "Logger.hpp"
#include <iostream>

namespace concord::orrery {

// A component provides an interface to handle `AllMsgs`
class IComponent {
 public:
  virtual ~IComponent() = default;
  virtual void handle(ComponentId from, AllMsgs&&) = 0;
};

template <typename ComponentImpl>
class Component : public IComponent {
 public:
  // Trait to ensure that a ComponentImpl has a `handle` method that takes a message of type `Msg`.
  template <typename Impl, typename Msg, typename = std::void_t<>>
  struct CanHandleMsgT : std::false_type {};

  template <typename Impl, typename Msg>
  struct CanHandleMsgT<
      Impl,
      Msg,
      std::void_t<decltype(std::declval<Impl>().handle(std::declval<ComponentId>(), std::declval<Msg>()))>>
      : std::true_type {};

  Component(ComponentImpl&& impl) : impl_(std::move(impl)) {}

  // TODO: Recursive visitor to also check for non-top-level messages?
  void handle(ComponentId from_id, AllMsgs&& msg_variant) override {
    std::visit(
        [from_id, this](auto&& msg) {
          if constexpr (CanHandleMsgT<ComponentImpl, decltype(msg)>::value) {
            this->impl_.handle(from_id, std::forward<decltype(msg)>(msg));
          } else {
            size_t to = static_cast<uint8_t>(impl_.id);
            size_t from = static_cast<uint8_t>(from_id);
            LOG_ERROR(logger_, "Component cannot handle message: " << KVLOG(from, to, msg.id));
          }
        },
        std::move(msg_variant.msg));
  }

 private:
  ComponentImpl impl_;
  logging::Logger logger_ = logging::getLogger("orrery.component");
};

}  // namespace concord::orrery
