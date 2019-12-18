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

#include <unordered_map>

namespace bftEngine::impl {

typedef std::function<void(MessageBase*)> MsgHandlerCallback;

// MsgHandlersRegistrator class contains message handling callback functions. One instance of this class should be
// created per instance of the class that registers message callback functions.
// For each new message corresponding function of type MsgHandlerCallback should be registered via this class
// otherwise the message could not be handled later.

class MsgHandlersRegistrator {
 public:
  void registerMsgHandler(uint16_t msgId, MsgHandlerCallback callbackFunc) { msgHandlers_[msgId] = std::move(callbackFunc); }

  MsgHandlerCallback getCallback(uint16_t msgId) {
    auto iterator = msgHandlers_.find(msgId);
    if (iterator != msgHandlers_.end()) return iterator->second;
    return nullptr;
  }

 private:
  std::unordered_map<uint16_t, MsgHandlerCallback> msgHandlers_;
};

}  // namespace bftEngine::impl
