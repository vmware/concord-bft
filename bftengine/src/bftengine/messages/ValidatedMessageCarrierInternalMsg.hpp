// Concord
//
// Copyright (c) 2021-2023 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <type_traits>

#include "MessageBase.hpp"

namespace bftEngine {
namespace impl {

// Any Incoming message is external and can be translated into internal message.
// CarrierMessage class will contain the knowledge of type of the translated message that
// Carried from internal message into external message
class CarrierMessage {
 public:
  CarrierMessage(MsgType msgType) : msgType_(msgType) {}
  virtual ~CarrierMessage() {}
  MsgType getMsgType() const { return msgType_; }

 private:
  MsgType msgType_;
};

// One use case of translation of message is doing validation in a separate thread and then
// use the validated message as an internal message. This message is encapsulated by
// ValidatedMessageCarrierInternalMsg<T> class.
// This class assumes that the translated message is a subclass of MessageBase
template <typename MSG, typename = std::enable_if_t<std::is_base_of_v<MessageBase, MSG>>>
class ValidatedMessageCarrierInternalMsg : public CarrierMessage {
 public:
  // This ctor will get a ptr to the message, and it is not responsible for allocation or deallocation of the message.
  // It just carries the message.
  ValidatedMessageCarrierInternalMsg(MSG*& msg) : CarrierMessage(msg->type()), msg_(std::move(msg)) {}

  // Once the message is returned to the owner of the message, it will never be taken by anyone or
  // given to anyone.
  // This is a onetime transaction. Subsequent call to this function will be nullptr.
  MSG* returnMessageToOwner() {
    MSG* retMsg = msg_;
    msg_ = nullptr;
    return retMsg;
  }
  ~ValidatedMessageCarrierInternalMsg() { msg_ = nullptr; }
  typedef MSG type;

 private:
  MSG* msg_ = nullptr;
};

}  // namespace impl
}  // namespace bftEngine
