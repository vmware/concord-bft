// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub--component's license, as noted in the LICENSE
// file.

#pragma once

#include "RetransmissionsManager.hpp"
#include "RetransmissionLogic.hpp"
#include "SimpleThreadPool.hpp"

namespace bftEngine::impl {

class RetransmissionProcessingJob : public util::SimpleThreadPool::Job {
 public:
  RetransmissionProcessingJob(InternalReplicaApi *const replica,
                              RetransmissionsManager *const retransmissionManager,
                              std::shared_ptr<MsgsCommunicator> &msgsCommunicator,
                              std::vector<RetransmissionEvent> *events,
                              RetransmissionLogic *retransmissionLogic,
                              SeqNum lastStableSeqNum,
                              ViewNum view,
                              bool clearPendingRetransmissions)
      : replica_{replica},
        retransmissionManager_{retransmissionManager},
        msgsCommunicator_{msgsCommunicator},
        setOfEvents_{events},
        logic_{retransmissionLogic},
        lastStable_{lastStableSeqNum},
        lastView_{view},
        clearPending_{clearPendingRetransmissions} {}

  virtual ~RetransmissionProcessingJob() = default;

  void release() override;
  void execute() override;

 private:
  InternalReplicaApi *const replica_;
  RetransmissionsManager *const retransmissionManager_;
  std::shared_ptr<MsgsCommunicator> msgsCommunicator_;
  std::vector<RetransmissionEvent> *const setOfEvents_;
  RetransmissionLogic *const logic_;
  const SeqNum lastStable_;
  const ViewNum lastView_;
  const bool clearPending_;
};

}  // namespace bftEngine::impl
