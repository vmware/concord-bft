// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#ifdef ERROR  // TODO(GG): should be fixed by encapsulating relic (or windows) definitions in cpp files
#undef ERROR
#endif

#include "threshsign/bls/relic/BlsAccumulatorBase.h"
#include "threshsign/bls/relic/Library.h"
#include "threshsign/bls/relic/FastMultExp.h"

#include "BlsAlmostMultisigCoefficients.h"
#include "LagrangeInterpolation.h"

#include <vector>

#include "Logger.hpp"

using std::endl;

namespace BLS {
namespace Relic {

std::pair<ShareID, G1T> BlsSigshareParser::operator()(const char* sigShare, int len) {
  // Get the signer's index from the signature share
  BNT idNum(reinterpret_cast<const unsigned char*>(sigShare), sizeof(ShareID));
  ShareID id = static_cast<ShareID>(idNum.toDigit());

  // Get the sigshare
  G1T sigShareNum(reinterpret_cast<const unsigned char*>(sigShare + sizeof(ShareID)),
                  len - static_cast<int>(sizeof(ShareID)));

  return std::make_pair(id, sigShareNum);
}

BlsAccumulatorBase::BlsAccumulatorBase(const std::vector<BlsPublicKey>& verifKeys,
                                       NumSharesType reqSigners,
                                       NumSharesType totalSigners,
                                       bool withShareVerification)
    : ThresholdAccumulatorBase(verifKeys, reqSigners, totalSigners), shareVerificationEnabled(withShareVerification) {
  assertEqual(vks.size(), static_cast<std::vector<BlsPublicKey>::size_type>(totalSigners + 1));

  g2_get_gen(gen2);  // NOTE: requires BLS::Relic::Library::Get() call above to be made
}

void BlsAccumulatorBase::onExpectedDigestSet() {
  assertNotNull(expectedDigest);
  assertStrictlyPositive(expectedDigestLen);

  g1_map(hash, static_cast<const uint8_t*>(expectedDigest.get()), expectedDigestLen);
}

bool BlsAccumulatorBase::verifyShare(ShareID id, const G1T& sigShare) {
  assertTrue(hasExpectedDigest());
  assertInclusiveRange(1, id, totalSigners);

  GTT e1, e2;

  // Pair hash with PK
  // FIXME: RELIC: This should not require non-const y
  G2T& vk = const_cast<G2T&>(vks[static_cast<size_t>(id)].getPoint());
  LOG_TRACE(BLS_LOG, sigShare << " on hash " << hash << " against VK of signer " << id << " " << vk);
  pc_map(e1, hash, vk);

  // Pair signature with group's generator
  // FIXME: RELIC: This should not require non-const sigShare
  pc_map(e2, const_cast<G1T&>(sigShare), gen2);

  // Make sure the two pairings are equal
  if (gt_cmp(e1, e2) != CMP_EQ) {
    return false;
  } else {
    return true;
  }
}

} /* namespace Relic */
} /* namespace BLS */
