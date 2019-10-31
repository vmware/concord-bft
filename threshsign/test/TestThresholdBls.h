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

#include <vector>
#include <memory>

#include "ThresholdViabilityTest.h"

#include "threshsign/bls/relic/BlsThresholdScheme.h"

#include "Utils.h"
#include "XAssert.h"

#pragma once

using std::endl;

namespace BLS {
namespace Relic {

class ThresholdBlsTest : public ThresholdViabilityTest<G1T,
                                                       BlsPublicParameters,
                                                       BlsAccumulatorBase,
                                                       BlsThresholdSigner,
                                                       BlsThresholdVerifier> {
 protected:
  bool useMultisig;

 public:
  ThresholdBlsTest(const BlsPublicParameters& params, int n, int k, bool useMultisig)
      : ThresholdViabilityTest(params, n, k), useMultisig(useMultisig) {
    verifiesShares = true;
  }

 public:
  std::unique_ptr<IThresholdFactory> makeThresholdFactory() const {
    return std::unique_ptr<IThresholdFactory>(new BlsThresholdFactory(params, useMultisig));
  }

  G1T hashMessage(const unsigned char* msg, int msgSize) const {
    G1T h;
    g1_map(h, msg, msgSize);
    return h;
  }
};

} /* namespace Relic */
} /* namespace BLS */
