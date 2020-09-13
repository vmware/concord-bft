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

#pragma once

#include "threshsign/bls/relic/BlsThresholdAccumulator.h"

namespace BLS {
namespace Relic {

class BlsCoefficientsMap;

class BlsAlmostMultisigAccumulator : public BlsThresholdAccumulator {
 public:
  BlsAlmostMultisigAccumulator(const std::vector<BlsPublicKey>& vks, NumSharesType numSigners);

 protected:
  // The Lagrange coefficients for sigshare i, indexed by the missing signer j (manual indexing is used: i * num_cols +
  // j)
  const BlsCoefficientsMap& almostMultisigCoeffs;

 public:
  virtual void computeLagrangeCoeff();
};

} /* namespace Relic */
} /* namespace BLS */
