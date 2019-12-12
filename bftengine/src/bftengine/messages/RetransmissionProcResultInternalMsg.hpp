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

#include "InternalMessage.hpp"
#include "InternalReplicaApi.hpp"

namespace bftEngine::impl {

class RetransmissionsManager;

class RetransmissionProcResultInternalMsg : public InternalMessage {
 public:
  RetransmissionProcResultInternalMsg(InternalReplicaApi *replica,
                                      SeqNum seqNum,
                                      ViewNum view,
                                      std::forward_list<RetSuggestion> *suggestedRetransmissions,
                                      RetransmissionsManager *retransmissionsManager)
      : replica_{replica},
        lastStableSeqNum_{seqNum},
        view_{view},
        suggestedRetransmissions_{suggestedRetransmissions},
        retransmissionsMgr_{retransmissionsManager} {}

  ~RetransmissionProcResultInternalMsg() override;

  void handle() override;

 private:
  InternalReplicaApi *const replica_;
  const SeqNum lastStableSeqNum_;
  const ViewNum view_;
  const std::forward_list<RetSuggestion> *const suggestedRetransmissions_;
  RetransmissionsManager *const retransmissionsMgr_;
};

}  // namespace bftEngine::impl