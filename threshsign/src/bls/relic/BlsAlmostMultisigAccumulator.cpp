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

// TODO: ALIN: Why are we including this here?
#ifdef ERROR  // TODO(GG): should be fixed by encapsulating relic (or windows) definitions in cpp files
#undef ERROR
#endif

#include "threshsign/Configuration.h"

#include "BlsAlmostMultisigAccumulator.h"
#include "BlsAlmostMultisigCoefficients.h"

#include "Logger.hpp"

using std::endl;

namespace BLS {
namespace Relic {

BlsAlmostMultisigAccumulator::BlsAlmostMultisigAccumulator(const std::vector<BlsPublicKey>& vks,
                                                           NumSharesType numSigners)
    : BlsThresholdAccumulator(vks, numSigners - 1, numSigners, false),
      almostMultisigCoeffs(BlsAlmostMultisigCoefficients::Get().computeMapAndGet(numSigners)) {
  LOG_DEBUG(BLS_LOG,
            "Using 'almost multisig' optimization for reqSigners = " << reqSigners << " out of " << numSigners);
}

void BlsAlmostMultisigAccumulator::computeLagrangeCoeff() {
  ShareID missingSigner = validSharesBits.findFirstGap();
  assertInclusiveRange(1, missingSigner, totalSigners);

  LOG_TRACE(BLS_LOG, " * Almost multisig, missing signer " << missingSigner);

  for (ShareID xVal = validSharesBits.first(); validSharesBits.isEnd(xVal) == false;
       xVal = validSharesBits.next(xVal)) {
    coeffs[static_cast<size_t>(xVal)] = almostMultisigCoeffs.getCoeff(xVal, missingSigner);
  }
}

} /* namespace Relic */
} /* namespace BLS */
