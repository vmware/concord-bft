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

#include <memory>

#include "messages/InternalMessage.hpp"

namespace bftEngine::impl {

class MessageBase;

// This is needed because we can't safely cast unique_ptrs to void pointers
// We also can't use a union because it would require custom deleters and
// could possibly result in unsafe destruction.
//
// As a result, we have the overhead of an extra pointer here. However, the
// structure is on the stack and only one is active at a time in the main loop,
// so the overhead is negligible. One of the unique_ptrs will be moved out for
// use depending upon the tag.
//
// This is probably a good use case for std::variant, but we are on c++11 and
// variant is only available in c++17.
class IncomingMsg {
 public:
  enum { EXTERNAL, INTERNAL, INVALID } tag;

  IncomingMsg() : tag(IncomingMsg::INVALID) {}
  explicit IncomingMsg(std::unique_ptr<MessageBase> msg) : tag(IncomingMsg::EXTERNAL), external(std::move(msg)) {}
  explicit IncomingMsg(InternalMessage&& msg) : tag(IncomingMsg::INTERNAL), internal(std::move(msg)) {}

  std::unique_ptr<MessageBase> external;
  InternalMessage internal;
};

}  // namespace bftEngine::impl