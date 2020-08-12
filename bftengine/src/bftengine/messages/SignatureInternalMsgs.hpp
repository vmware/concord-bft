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

#include <set>
#include <vector>

#include "OpenTracing.hpp"
#include "PrimitiveTypes.hpp"

namespace bftEngine::impl {

struct CombinedSigFailedInternalMsg {
  const SeqNum seqNumber;
  const ViewNum view;
  const std::set<uint16_t> replicasWithBadSigs;

  CombinedSigFailedInternalMsg(SeqNum s, ViewNum v, const std::set<uint16_t>& repsWithBadSigs)
      : seqNumber{s}, view{v}, replicasWithBadSigs{repsWithBadSigs} {}
};

struct CombinedSigSucceededInternalMsg {
  const SeqNum seqNumber;
  const ViewNum view;
  const std::vector<char> combinedSig;
  concordUtils::SpanContext span_context_;

  CombinedSigSucceededInternalMsg(
      SeqNum s, ViewNum v, const char* sig, uint16_t sigLen, const concordUtils::SpanContext& span_context)
      : seqNumber{s}, view{v}, combinedSig(sig, sig + sigLen), span_context_{span_context} {}
};

struct VerifyCombinedSigResultInternalMsg {
  const SeqNum seqNumber;
  const ViewNum view;
  const bool isValid;

  VerifyCombinedSigResultInternalMsg(SeqNum s, ViewNum v, bool result) : seqNumber{s}, view{v}, isValid{result} {}
};

struct CombinedCommitSigSucceededInternalMsg {
  const SeqNum seqNumber;
  const ViewNum view;
  const std::vector<char> combinedSig;
  concordUtils::SpanContext span_context_;

  CombinedCommitSigSucceededInternalMsg(
      SeqNum s, ViewNum v, const char* sig, uint16_t sigLen, const concordUtils::SpanContext& span_context)
      : seqNumber{s}, view{v}, combinedSig(sig, sig + sigLen), span_context_{span_context} {}
};

struct CombinedCommitSigFailedInternalMsg {
  const SeqNum seqNumber;
  const ViewNum view;
  const std::set<uint16_t> replicasWithBadSigs;

  CombinedCommitSigFailedInternalMsg(SeqNum s, ViewNum v, const std::set<uint16_t>& repsWithBadSigs)
      : seqNumber{s}, view{v}, replicasWithBadSigs{repsWithBadSigs} {}
};

struct VerifyCombinedCommitSigResultInternalMsg {
  const SeqNum seqNumber;
  const ViewNum view;
  const bool isValid;

  VerifyCombinedCommitSigResultInternalMsg(SeqNum s, ViewNum v, bool result) : seqNumber{s}, view{v}, isValid{result} {}
};

}  // namespace bftEngine::impl
