// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "StateTransferMsg.hpp"
#include "assertUtils.hpp"

namespace bftEngine {
namespace impl {
bool StateTransferMsg::ToActualMsgType(const ReplicasInfo& repInfo,
                                       MessageBase* inMsg,
                                       StateTransferMsg*& outMsg) {
  Assert(inMsg->type() == MsgCode::StateTransfer);
  if (inMsg->size() < sizeof(MessageBase::Header)) return false;

  outMsg = (StateTransferMsg*)inMsg;

  return true;
}

}  // namespace impl
}  // namespace bftEngine
