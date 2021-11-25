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
#include <functional>
#include <unordered_map>

#include "messages/MessageBase.hpp"
#include "messages/InternalMessage.hpp"
#include "messages/ValidatedMessageCarrierInternalMsg.hpp"

namespace bftEngine::impl {

template <typename T>
using CallbackTypeWithPtrArg = std::function<void(T*)>;

template <typename T>
using CallbackTypeWithRefArg = std::function<void(T&&)>;

using MsgHandlerCallback = CallbackTypeWithPtrArg<MessageBase>;
using ValidatedMsgHandlerCallback = CallbackTypeWithPtrArg<CarrierMesssage>;
using InternalMsgHandlerCallback = CallbackTypeWithRefArg<InternalMessage>;

// MsgHandlersRegistrator class contains message handling callback functions.
// Logically it's a singleton - only one message handler could be registered for every message type,
// but practically we launch a number of replicas in the same process in the tests and each ReplicaImp instance
// needs to have its own callbacks being called.
// For each new message corresponding function of type MsgHandlerCallback should be registered via this class
// otherwise the message could not be handled later.
// MsgHandlersRegistrator class contains three kinds of callback functions:
//  1) Default callback handler which will be called by the external message dispatcher.
//  2) Validated callback handler which will be called when redispatching of internal
//     message will happen.
//  3) Internal message callback handler which will be called by the Internal message
//     dispatcher.
// MsgHandlersRegistrator is a repository of all kinds of callbacks.

class MsgHandlersRegistrator {
 public:
  void registerMsgHandler(uint16_t msgId, const MsgHandlerCallback& callbackFunc) {
    msgHandlers_[msgId] = callbackFunc;
  }

  // This variation will allow addition of validated message callback function.
  void registerMsgHandler(uint16_t msgId,
                          const MsgHandlerCallback& callbackFunc,
                          const ValidatedMsgHandlerCallback& validatedMsgCallbackFunc) {
    msgHandlers_[msgId] = callbackFunc;
    validatedMsgHandlers_[msgId] = validatedMsgCallbackFunc;
  }

  void registerInternalMsgHandler(const InternalMsgHandlerCallback& cb) { internalMsgHandler_ = cb; }

  MsgHandlerCallback getCallback(uint16_t msgId) {
    auto iterator = msgHandlers_.find(msgId);
    if (iterator != msgHandlers_.end()) return iterator->second;
    return nullptr;
  }

  // Returns the validated callback message handler for the given message id.
  // Validated callback is possible only for External Messages which were validated,
  // so they are the subset of message which has a valid message type (msgId).
  ValidatedMsgHandlerCallback getValidatedMsgCallback(uint16_t msgId) {
    auto iterator = validatedMsgHandlers_.find(msgId);
    if (iterator != validatedMsgHandlers_.end()) return iterator->second;
    return nullptr;
  }

  void handleInternalMsg(InternalMessage&& msg) { internalMsgHandler_(std::move(msg)); }

 private:
  std::unordered_map<uint16_t, MsgHandlerCallback> msgHandlers_;
  std::unordered_map<uint16_t, ValidatedMsgHandlerCallback> validatedMsgHandlers_;
  InternalMsgHandlerCallback internalMsgHandler_;
};

}  // namespace bftEngine::impl
