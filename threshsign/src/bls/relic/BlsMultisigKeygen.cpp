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

#include "threshsign/bls/relic/BlsMultisigKeygen.h"
#include "threshsign/bls/relic/BlsPublicParameters.h"
#include "threshsign/bls/relic/Library.h"

#include "Log.h"

using std::endl;

namespace BLS {
namespace Relic {

BlsMultisigKeygen::BlsMultisigKeygen(const BlsPublicParameters& params,
                                     NumSharesType n)
    : BlsThresholdKeygenBase(n) {
  const BNT& fieldOrder = params.getGroupOrder();

  for (ShareID i = 1; i <= n; i++) {
    size_t idx = static_cast<size_t>(i);  // thanks, C++!
    BNT& skShare = skShares[idx];
    G2T& pkShare = pkShares[idx];

    if (cp_bls_gen(skShare, pkShare) != STS_OK) {
      throw std::runtime_error("RELIC's BLS key generation routine failed");
    }

    sk = (sk + skShare).SlowModulo(fieldOrder);
  }

  g2_mul_gen(pk, sk);

  logalloc << "Created: " << this << endl;
}

BlsMultisigKeygen::~BlsMultisigKeygen() {
  logalloc << "Destroyed: " << this << endl;
}

} /* namespace Relic */
} /* namespace BLS */
