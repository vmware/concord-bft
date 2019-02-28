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

#include "threshsign/Configuration.h"

#include "threshsign/bls/relic/BlsPublicParameters.h"
#include "threshsign/bls/relic/BlsThresholdKeygen.h"

#include "BlsPolynomial.h"

#include "Log.h"
#include "XAssert.h"

using std::endl;

namespace BLS {
namespace Relic {

BlsThresholdKeygenBase::BlsThresholdKeygenBase(NumSharesType numSigners)
    : IThresholdKeygen(numSigners) {}

BlsThresholdKeygen::BlsThresholdKeygen(const BlsPublicParameters& params,
                                       NumSharesType reqSigners,
                                       NumSharesType numSigners)
    : BlsThresholdKeygenBase(numSigners) {
  assertLessThanOrEqual(reqSigners, numSigners);

  if (cp_bls_gen(sk, pk) != STS_OK) {
    throw std::runtime_error("RELIC failed generating BLS keypair");
  }

  BlsPolynomial poly(sk, reqSigners - 1, params.getGroupOrder());
  poly.generate();

  for (ShareID i = 1; i <= numSigners; i++) {
    size_t idx = static_cast<size_t>(i);

    skShares[idx] = poly.get(i);

    g2_mul_gen(pkShares[idx], skShares[idx]);
  }

  logalloc << "Created: " << this << endl;
}

BlsThresholdKeygen::~BlsThresholdKeygen() {
  logalloc << "Destroyed: " << this << endl;
}

} /* namespace Relic */
} /* namespace BLS */
