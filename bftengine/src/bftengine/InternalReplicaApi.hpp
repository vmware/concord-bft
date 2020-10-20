// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <set>
#include <forward_list>

#include "PrimitiveTypes.hpp"
#include "IncomingMsgsStorage.hpp"

class IThresholdVerifier;
class ReplicasInfo;
namespace util {
class SimpleThreadPool;
}

namespace bftEngine {
namespace impl {

class PrePrepareMsg;

class InternalReplicaApi  // TODO(GG): rename + clean + split to several classes
{
 public:
  virtual const ReplicasInfo& getReplicasInfo() const = 0;
  virtual bool isValidClient(NodeIdType clientId) const = 0;
  virtual bool isIdOfReplica(NodeIdType id) const = 0;
  virtual const std::set<ReplicaId>& getIdsOfPeerReplicas() const = 0;
  virtual ViewNum getCurrentView() const = 0;
  virtual ReplicaId currentPrimary() const = 0;
  virtual bool isCurrentPrimary() const = 0;
  virtual bool currentViewIsActive() const = 0;
  virtual ReqId seqNumberOfLastReplyToClient(NodeIdType clientId) const = 0;
  virtual bool isClientRequestInProcess(NodeIdType clientId, ReqId reqSeqNum) const = 0;
  virtual SeqNum getPrimaryLastUsedSeqNum() const = 0;
  virtual uint64_t getRequestsInQueue() const = 0;
  virtual SeqNum getLastExecutedSeqNum() const = 0;
  virtual PrePrepareMsg* buildPrePrepareMessage() = 0;

  virtual IncomingMsgsStorage& getIncomingMsgsStorage() = 0;
  virtual util::SimpleThreadPool& getInternalThreadPool() = 0;

  virtual bool isCollectingState() const = 0;

  virtual const ReplicaConfig& getReplicaConfig() const = 0;

  virtual ~InternalReplicaApi() = default;
};

}  // namespace impl
}  // namespace bftEngine
