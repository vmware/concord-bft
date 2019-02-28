// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#ifdef ERROR  // TODO(GG): should be fixed by encapsulating relic (or windows)
              // definitions in cpp files
#undef ERROR
#endif

#include "threshsign/Configuration.h"

#include "threshsign/bls/relic/BlsThresholdVerifier.h"
#include "threshsign/bls/relic/BlsThresholdAccumulator.h"
#include "threshsign/bls/relic/BlsPublicKey.h"
#include "threshsign/bls/relic/BlsPublicParameters.h"

#include "BlsAlmostMultisigAccumulator.h"

#include <algorithm>
#include <iterator>

#include "Log.h"
#include "XAssert.h"

using std::endl;

namespace BLS {
namespace Relic {

BlsThresholdVerifier::BlsThresholdVerifier(
    const BlsPublicParameters& params,
    const G2T& pk,
    NumSharesType reqSigners,
    NumSharesType numSigners,
    const std::vector<BlsPublicKey>& verifKeys)
    : params(params),
      pk(pk),
      vks(verifKeys.begin(), verifKeys.end()),
      gen2(params.getGen2()),
      reqSigners(reqSigners),
      numSigners(numSigners) {
  assertEqual(
      verifKeys.size(),
      static_cast<std::vector<BlsPublicKey>::size_type>(numSigners + 1));
  // verifKeys[0] was copied as well, but it's set to a dummy PK so it does not
  // matter
  assertEqual(
      vks.size(),
      static_cast<std::vector<BlsPublicKey>::size_type>(numSigners + 1));

#ifdef TRACE
  logtrace << "VKs (array has size " << vks.size() << ")" << endl;
  std::copy(vks.begin(),
            vks.end(),
            std::ostream_iterator<BlsPublicKey>(std::cout, "\n"));
#endif
}

BlsThresholdVerifier::~BlsThresholdVerifier() {}

const IShareVerificationKey& BlsThresholdVerifier::getShareVerificationKey(
    ShareID signer) const {
  return vks.at(static_cast<size_t>(signer));
}

IThresholdAccumulator* BlsThresholdVerifier::newAccumulator(
    bool withShareVerification) const {
  if (reqSigners == numSigners - 1) {
    return new BlsAlmostMultisigAccumulator(vks, numSigners);
  } else {
    return new BlsThresholdAccumulator(
        vks, reqSigners, numSigners, withShareVerification);
  }
}

bool BlsThresholdVerifier::verify(const char* msg,
                                  int msgLen,
                                  const char* sigBuf,
                                  int sigLen) const {
  G1T h, sig;
  // Convert hash to elliptic curve point
  g1_map(h, reinterpret_cast<const unsigned char*>(msg), msgLen);
  // Convert signature to elliptic curve point
  sig.fromBytes(reinterpret_cast<const unsigned char*>(sigBuf), sigLen);

  return verify(h, sig, pk.y);
}

bool BlsThresholdVerifier::verify(const G1T& msgHash,
                                  const G1T& sigShare,
                                  const G2T& pk) const {
  // FIXME: RELIC: Dealing with library peculiarities here by using a const cast
  // Pair hash with PK
  GTT e1, e2;
  pc_map(e1, const_cast<G1T&>(msgHash), const_cast<G2T&>(pk));

  // Pair signature with group's generator
  pc_map(e2, const_cast<G1T&>(sigShare), const_cast<G2T&>(gen2));

  // Make sure the two pairings are equal
  if (gt_cmp(e1, e2) != CMP_EQ) {
    return false;
  } else {
    return true;
  }
}

} /* namespace Relic */
} /* namespace BLS */
