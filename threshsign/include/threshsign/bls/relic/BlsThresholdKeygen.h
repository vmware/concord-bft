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

#include <vector>

#include "BlsNumTypes.h"

#include "threshsign/IThresholdKeygen.h"
#include "threshsign/ThresholdSignaturesTypes.h"

namespace BLS {
namespace Relic {

class BlsPublicParameters;

class BlsThresholdKeygenBase : public IThresholdKeygen<BNT, G2T> {
 public:
  BlsThresholdKeygenBase(NumSharesType numSigners);
};

class BlsThresholdKeygen : public BlsThresholdKeygenBase {
 public:
  BlsThresholdKeygen(const BlsPublicParameters& params, NumSharesType reqSigners, NumSharesType numSigners);
  virtual ~BlsThresholdKeygen();
};

} /* namespace Relic */
} /* namespace BLS */
