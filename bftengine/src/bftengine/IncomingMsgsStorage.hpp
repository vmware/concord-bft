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

#include <memory>

namespace bftEngine::impl {

class IncomingMsgsStorage {
 public:
  explicit IncomingMsgsStorage() = default;
  virtual ~IncomingMsgsStorage() = default;

  virtual void pushExternalMsg(std::unique_ptr<MessageBase> msg) = 0;
  virtual void pushInternalMsg(std::unique_ptr<InternalMessage> msg) = 0;
  virtual IncomingMsg popMsgForProcessing(std::chrono::milliseconds timeout) = 0;
};

}  // namespace bftEngine::impl
