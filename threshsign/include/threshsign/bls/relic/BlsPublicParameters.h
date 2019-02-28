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

#pragma once

#include "threshsign/IPublicParameters.h"

#include "BlsNumTypes.h"

namespace BLS {
namespace Relic {

class BlsPublicParameters : public IPublicParameters {
 protected:
  G1T gen1;
  G2T gen2;
  int curveType;

 public:
  BlsPublicParameters(int securityLevel, const int curveType);
  BlsPublicParameters(const BlsPublicParameters& params);

  virtual ~BlsPublicParameters();

 public:
  /**
   * Needed by IThresholdSigner/Verifier.
   */
  int getSignatureSize() const;

  int getCurveType() const { return curveType; }

  const G1T& getGen1() const { return gen1; }
  const G2T& getGen2() const { return gen2; }

  const BNT& getGroupOrder() const;
};

}  // namespace Relic
}  // namespace BLS
