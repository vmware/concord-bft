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

struct FastPathCombinedCommitSigSucceededInternalMsg {
  const SeqNum seqNumber;
  const ViewNum view;
  const CommitPath commitPath;
  const std::vector<char> combinedSig;
  concordUtils::SpanContext span_context_;

  FastPathCombinedCommitSigSucceededInternalMsg(SeqNum s,
                                                ViewNum v,
                                                CommitPath cPath,
                                                const char* sig,
                                                uint16_t sigLen,
                                                const concordUtils::SpanContext& span_context)
      : seqNumber{s}, view{v}, commitPath{cPath}, combinedSig(sig, sig + sigLen), span_context_{span_context} {}
};

struct FastPathCombinedCommitSigFailedInternalMsg {
  const SeqNum seqNumber;
  const ViewNum view;
  const CommitPath commitPath;
  const std::set<uint16_t> replicasWithBadSigs;

  FastPathCombinedCommitSigFailedInternalMsg(SeqNum s,
                                             ViewNum v,
                                             CommitPath cPath,
                                             const std::set<uint16_t>& repsWithBadSigs)
      : seqNumber{s}, view{v}, commitPath(cPath), replicasWithBadSigs{repsWithBadSigs} {}
};

struct FastPathVerifyCombinedCommitSigResultInternalMsg {
  const SeqNum seqNumber;
  const ViewNum view;
  const CommitPath commitPath;
  const bool isValid;

  FastPathVerifyCombinedCommitSigResultInternalMsg(SeqNum s, ViewNum v, CommitPath cPath, bool result)
      : seqNumber{s}, view{v}, commitPath(cPath), isValid{result} {}
};

}  // namespace bftEngine::impl
