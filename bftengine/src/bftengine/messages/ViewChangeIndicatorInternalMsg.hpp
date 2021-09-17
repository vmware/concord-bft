// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "ReplicaAsksToLeaveViewMsg.hpp"

namespace bftEngine::impl {
struct ViewChangeIndicatorInternalMsg {
  const bftEngine::impl::ReplicaAsksToLeaveViewMsg::Reason reasonToLeave_;

  ViewChangeIndicatorInternalMsg(bftEngine::impl::ReplicaAsksToLeaveViewMsg::Reason viewChangeReason)
      : reasonToLeave_(viewChangeReason) {}
};
}  // namespace bftEngine::impl
