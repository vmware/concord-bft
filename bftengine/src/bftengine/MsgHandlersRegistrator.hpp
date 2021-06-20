// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include "messages/MessageBase.hpp"
#include "messages/InternalMessage.hpp"
#include <functional>
#include <unordered_map>

namespace bftEngine::impl {

typedef std::function<void(MessageBase*)> MsgHandlerCallback;
typedef std::function<void(InternalMessage&&)> InternalMsgHandlerCallback;

// MsgHandlersRegistrator class contains message handling callback functions.
// Logically it's a singleton - only one message handler could be registered for every message type,
// but practically we launch a number of replicas in the same process in the tests and each ReplicaImp instance
// needs to have its own callbacks being called.
// For each new message corresponding function of type MsgHandlerCallback should be registered via this class
// otherwise the message could not be handled later.

class MsgHandlersRegistrator {
 public:
  void registerMsgHandler(uint16_t msgId, const MsgHandlerCallback& callbackFunc) {
    msgHandlers_[msgId] = callbackFunc;
  }

  void registerInternalMsgHandler(const InternalMsgHandlerCallback& cb) { internalMsgHandler_ = cb; }

  MsgHandlerCallback getCallback(uint16_t msgId) {
    auto iterator = msgHandlers_.find(msgId);
    if (iterator != msgHandlers_.end()) return iterator->second;
    return nullptr;
  }

  void handleInternalMsg(InternalMessage&& msg) { internalMsgHandler_(std::move(msg)); }

 private:
  std::unordered_map<uint16_t, MsgHandlerCallback> msgHandlers_;
  InternalMsgHandlerCallback internalMsgHandler_;
};

}  // namespace bftEngine::impl
