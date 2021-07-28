// Concord
//
// Copyright (c) 2018-2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include "TimeUtils.hpp"
#include "messages/MessageBase.hpp"
#include "messages/InternalMessage.hpp"
#include "messages/IncomingMsg.hpp"

#include <functional>
#include <memory>

namespace bftEngine::impl {

class IncomingMsgsStorage {
 public:
  explicit IncomingMsgsStorage() = default;
  virtual ~IncomingMsgsStorage() = default;

  virtual void start() = 0;
  virtual void stop() = 0;

  virtual bool isRunning() const = 0;

  using Callback = std::function<void()>;

  // Below methods return true if message is pushed and false otherwise (e.g. queue capacity has been reached).
  // The optional `onMsgPopped` function is called when the given message is popped from the consumer/replica.
  virtual bool pushExternalMsg(std::unique_ptr<MessageBase> msg) = 0;
  virtual bool pushExternalMsg(std::unique_ptr<MessageBase> msg, Callback onMsgPopped) = 0;
  virtual bool pushExternalMsgRaw(char* msg, size_t size) = 0;
  virtual bool pushExternalMsgRaw(char* msg, size_t size, Callback onMsgPopped) = 0;

  virtual void pushInternalMsg(InternalMessage&& msg) = 0;
};

}  // namespace bftEngine::impl
