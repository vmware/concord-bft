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

#include "threshsign/Configuration.h"

#include "threshsign/bls/relic/BlsThresholdAccumulator.h"
#include "threshsign/bls/relic/Library.h"
#include "threshsign/bls/relic/FastMultExp.h"

#include "BlsAlmostMultisigCoefficients.h"
#include "LagrangeInterpolation.h"

#include <vector>

#include "Logger.hpp"

using std::endl;

namespace BLS {
namespace Relic {

BlsThresholdAccumulator::BlsThresholdAccumulator(const std::vector<BlsPublicKey>& vks,
                                                 NumSharesType reqSigners,
                                                 NumSharesType totalSigners,
                                                 bool withShareVerification)
    : BlsAccumulatorBase(vks, reqSigners, totalSigners, withShareVerification) {
  coeffs.resize(static_cast<size_t>(totalSigners + 1));
  assertEqual(threshSig, G1T::Identity());
}

void BlsThresholdAccumulator::computeLagrangeCoeff() {
  lagrangeCoeffAccumReduced(validSharesBits, coeffs, BLS::Relic::Library::Get().getG2Order());
}

void BlsThresholdAccumulator::exponentiateLagrangeCoeff() {
  // Raise shares[i] to the power of coeffs[i]
  // for(ShareID id = validSharesBits.first(); validSharesBits.isEnd(id) == false; id = validSharesBits.next(id)) {
  //    size_t i = static_cast<size_t>(id);
  //    g1_mul(sharesPow[i], validShares[i], coeffs[i]);
  //}

  int maxBits = Library::Get().getG2OrderNumBits();
  threshSig = fastMultExp<G1T>(validSharesBits, validShares, coeffs, maxBits);
}

} /* namespace Relic */
} /* namespace BLS */
