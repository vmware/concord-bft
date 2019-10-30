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

#include "threshsign/Configuration.h"

#include "BlsPolynomial.h"
#include <algorithm>

#include "threshsign/bls/relic/Relic.h"

namespace BLS {
namespace Relic {

BlsPolynomial::BlsPolynomial(const BNT& secret, int degree, const BNT& fieldSize)
    : SecretSharingPolynomial<BNT>(secret, degree, fieldSize) {}

BlsPolynomial::~BlsPolynomial() {}

BNT BlsPolynomial::randomCoeff(const BNT& coeffLimit) const {
  BNT coeff;

  BNT upperBound(std::min(coeffLimit, modBase));
  bn_rand_mod(coeff, upperBound);

  return coeff;
}

} /* namespace Relic */
} /* namespace BLS */
