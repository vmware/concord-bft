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

#include "RetransmissionProcResultInternalMsg.hpp"
#include "RetransmissionsManager.hpp"

namespace bftEngine::impl {

RetransmissionProcResultInternalMsg::~RetransmissionProcResultInternalMsg() {
  auto *p = (std::forward_list<RetSuggestion> *)suggestedRetransmissions_;
  delete p;
}

void RetransmissionProcResultInternalMsg::handle() {
  replica_->onRetransmissionsProcessingResults(lastStableSeqNum_, view_, suggestedRetransmissions_);
  retransmissionsMgr_->onProcessingComplete();
}

}  // namespace bftEngine::impl