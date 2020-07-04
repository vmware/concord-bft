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

#include "BlsAlmostMultisigCoefficients.h"
#include "LagrangeInterpolation.h"

#include "threshsign/bls/relic/Library.h"
#include "threshsign/VectorOfShares.h"

#include <vector>

#include "Logger.hpp"

namespace BLS {
namespace Relic {

BlsAlmostMultisigCoefficients BlsAlmostMultisigCoefficients::_instance;

void BlsCoefficientsMap::calculateCoefficientsForAll(const VectorOfShares& vec, ShareID missing) {
  std::vector<BNT> tmp(static_cast<size_t>(n + 1));
  lagrangeCoeffAccumReduced(vec, tmp, fieldOrder);

  // ...and store them in our "matrix"
  for (ShareID i = vec.first(); vec.isEnd(i) == false; i = vec.next(i)) {
    size_t key = getIndex(i, missing);
    coeffs[key] = tmp[static_cast<size_t>(i)];
  }
}

} /* namespace Relic */
} /* namespace BLS */
