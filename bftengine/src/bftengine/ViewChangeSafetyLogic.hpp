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

#include "messages/ViewChangeMsg.hpp"
#include <vector>
#include "threshsign/IThresholdVerifier.h"

using std::vector;

class IThresholdVerifier;

namespace bftEngine {
namespace impl {

class ViewChangeSafetyLogic {
 public:
  ViewChangeSafetyLogic(const uint16_t n, const uint16_t f, const uint16_t c, const Digest& digestOfNull);

  struct Restriction {
    bool isNull;
    Digest digest;
  };

  SeqNum calcLBStableForView(ViewChangeMsg** const viewChangeMsgsOfPendingView) const;

  void computeRestrictions(ViewChangeMsg** const inViewChangeMsgsOfCurrentView,
                           const SeqNum inLBStableForView,
                           SeqNum& outMinRestrictedSeqNum,
                           SeqNum& outMaxRestrictedSeqNum,
                           Restriction* outSafetyRestrictionsArray) const;
  // Notes about outSafetyRestrictionsArray:
  // - It should have kWorkWindowSize elements.
  // - If at the end of this method outMaxRestrictedSeqNum==0, then outSafetyRestrictionsArray is 'empty'
  // - Otherwise, its first (outMaxRestrictedSeqNum-outMinRestrictedSeqNum+1) elements are valid : they represents the
  // restrictions between outMinRestrictedSeqNum and outMaxRestrictedSeqNum

 protected:
  bool computeRestrictionsForSeqNum(SeqNum s,
                                    vector<ViewChangeMsg::ElementsIterator*>& VCIterators,
                                    const SeqNum upperBound,
                                    Digest& outRestrictedDigest) const;

  const uint16_t N;  // number of replicas
  const uint16_t F;
  const uint16_t C;

  std::shared_ptr<IThresholdVerifier> preparedCertVerifier;

  const Digest nullDigest;
};

}  // namespace impl
}  // namespace bftEngine
