// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <forward_list>

#include "PrimitiveTypes.hpp"

namespace bftEngine::impl {

struct RetSuggestion {
  ReplicaId replicaId;
  uint16_t msgType;
  SeqNum msgSeqNum;
};

struct RetranProcResultInternalMsg {
  const SeqNum lastStableSeqNum;
  const ViewNum view;
  const std::forward_list<RetSuggestion> suggestedRetransmissions;

 public:
  RetranProcResultInternalMsg(SeqNum s, ViewNum v, std::forward_list<RetSuggestion>&& suggestedRetran)
      : lastStableSeqNum{s}, view{v}, suggestedRetransmissions{std::move(suggestedRetran)} {}
};

}  // namespace bftEngine::impl
