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

namespace bftEngine::impl {

class IncomingMsg {
 public:
  enum { EXTERNAL, INTERNAL, INVALID } tag;

  IncomingMsg() : tag(IncomingMsg::INVALID) {}
  explicit IncomingMsg(std::unique_ptr<MessageBase> msg) : tag(IncomingMsg::EXTERNAL), external(std::move(msg)) {}
  explicit IncomingMsg(std::unique_ptr<InternalMessage> msg) : tag(IncomingMsg::INTERNAL), internal(std::move(msg)) {}

  std::unique_ptr<MessageBase> external;
  std::unique_ptr<InternalMessage> internal;
};

}  // namespace bftEngine::impl
