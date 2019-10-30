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

#include <map>
#include <vector>

#include "AlmostMultisigCoefficients.h"

#include "threshsign/ThresholdSignaturesTypes.h"
#include "threshsign/bls/relic/BlsNumTypes.h"
#include "threshsign/bls/relic/Library.h"

#include "XAssert.h"

namespace BLS {
namespace Relic {

class BlsCoefficientsMap : public AlmostMultisigCoefficientsMap<BNT> {
 private:
  BNT fieldOrder;

 public:
  /**
   * When storing this object in a std::map, we need its default constructor defined.
   */
  BlsCoefficientsMap() : AlmostMultisigCoefficientsMap<BNT>() { fieldOrder = Library::Get().getG2Order(); }
  BlsCoefficientsMap(NumSharesType n) : AlmostMultisigCoefficientsMap<BNT>(n) {
    fieldOrder = Library::Get().getG2Order();
  }

 public:
  virtual void calculateCoefficientsForAll(const VectorOfShares& vec, ShareID missing);
};

class BlsAlmostMultisigCoefficients : public AlmostMultisigCoefficients<BlsCoefficientsMap> {
 private:
  static BlsAlmostMultisigCoefficients _instance;

 public:
  static BlsAlmostMultisigCoefficients& Get() { return _instance; }

 private:
  BlsAlmostMultisigCoefficients() {}
};

} /* namespace Relic */
} /* namespace BLS */
