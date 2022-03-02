// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <cstdint>
#include "IRequestHandler.hpp"

namespace bftEngine::impl {

struct RemovePendingForExecutionRequest {
  uint16_t clientProxyId;
  ReqId requestSeqNum;
  RemovePendingForExecutionRequest(uint16_t cpid, ReqId rsn) : clientProxyId{cpid}, requestSeqNum{rsn} {}
};

}  // namespace bftEngine::impl
