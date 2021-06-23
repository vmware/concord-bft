// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include "messages/ReplicaAsksToLeaveViewMsg.hpp"
#include "ReplicaConfig.hpp"

#include <memory>
#include <unordered_map>

namespace bftEngine::impl {
class ReplicasAskedToLeaveViewInfo {
 public:
  ReplicasAskedToLeaveViewInfo(const int16_t fVal) : f_val(fVal) {}

  ReplicasAskedToLeaveViewInfo(ReplicasAskedToLeaveViewInfo&& rhs) : f_val(rhs.f_val), msgs(std::move(rhs.msgs)) {}

  ReplicasAskedToLeaveViewInfo& operator=(ReplicasAskedToLeaveViewInfo&& rhs) {
    msgs.clear();
    msgs = std::move(rhs.msgs);
    return *this;
  }

  bool hasQuorumToLeaveView() const { return msgs.size() >= f_val + 1U; }

  void store(std::unique_ptr<ReplicaAsksToLeaveViewMsg>&& msg) {
    msgs.emplace(msg->idOfGeneratedReplica(), std::move(msg));
  }

  void clear() { msgs.clear(); }

  bool empty() const { return msgs.empty(); }

  std::shared_ptr<ReplicaAsksToLeaveViewMsg> getComplaintFromReplica(ReplicaId replicaId) {
    auto msg = msgs.find(replicaId);
    if (msg != msgs.end()) {
      return msg->second;
    }
    return nullptr;
  }

  const auto& getAllMsgs() const { return msgs; }

 private:
  const int16_t f_val;
  std::unordered_map<NodeIdType, std::shared_ptr<ReplicaAsksToLeaveViewMsg>> msgs;
};

}  // namespace bftEngine::impl
