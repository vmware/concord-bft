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

#include "threshsign/IThresholdAccumulator.h"
#include "threshsign/ThresholdAccumulatorBase.h"
#include "threshsign/ThresholdSignaturesTypes.h"

#include "BlsNumTypes.h"
#include "BlsAccumulatorBase.h"
#include "BlsPublicKey.h"

#include <vector>

namespace BLS {
namespace Relic {

class BlsMultisigAccumulator : public BlsAccumulatorBase {
 protected:
 public:
  BlsMultisigAccumulator(const std::vector<BlsPublicKey>& vks,
                         NumSharesType reqSigners,
                         NumSharesType totalSigners,
                         bool withShareVerification);
  virtual ~BlsMultisigAccumulator() {}

  // IThresholdAccumulator overloads.
 public:
  virtual size_t getFullSignedData(char* outThreshSig, int threshSigLen);

  virtual bool hasShareVerificationEnabled() const { return shareVerificationEnabled; }

  // Used internally or for testing
 public:
  virtual void aggregateShares();

  virtual size_t sigToBytes(unsigned char* buf, int len) const;
};

} /* namespace Relic */
} /* namespace BLS */
