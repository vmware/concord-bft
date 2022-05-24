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
#include "threshsign/VectorOfShares.h"

#include "FastMultExp.h"
#include "BlsNumTypes.h"
#include "BlsAccumulatorBase.h"
#include "BlsPublicKey.h"

#include <vector>

namespace BLS {
namespace Relic {

class BlsThresholdAccumulator : public BlsAccumulatorBase {
 protected:
  /**
   * Lagrange coefficients \ell_i^{S(0)} for the current set S of signers in
   * ThresholdAccumulatorBase::validSharesBits
   */
  std::vector<BNT> coeffs;

 public:
  BlsThresholdAccumulator(const std::vector<BlsPublicKey>& vks,
                          NumSharesType reqSigners,
                          NumSharesType totalSigners,
                          bool withShareVerification);
  virtual ~BlsThresholdAccumulator() {}

  // IThresholdAccumulator overloads.
 public:
  virtual size_t getFullSignedData(char* outThreshSig, int threshSigLen) {
    computeLagrangeCoeff();
    exponentiateLagrangeCoeff();
    aggregateShares();

    return sigToBytes(reinterpret_cast<unsigned char*>(outThreshSig), threshSigLen);
  }

  virtual bool hasShareVerificationEnabled() const { return shareVerificationEnabled; }

  // Used internally, by benchmarks and by subclasses
 public:
  /**
   * NOTE: Must be virtual because BlsAlmostMultisigAccumulator overrides this to fetch precomputed Lagrange coeffs.
   */
  virtual void computeLagrangeCoeff();

  void exponentiateLagrangeCoeff();

  /**
   * NOTE: We are now using fast multi exponentiation which computes \sigma = \prod_i \sigma_i ^ \ell_i directly and
   * faster than computing \sigma_i ^ \ell_i individually. (Here \sigma denotes the threshold signature, \ell_i is the
   * ith Lagrange coeff and \sigma_i is the ith signature share.) As a result, this function does nothing.
   */
  virtual void aggregateShares() {
    // for(ShareID id = validSharesBits.first(); validSharesBits.isEnd(id) == false; id = validSharesBits.next(id)) {
    //    size_t i = static_cast<size_t>(id);
    //    g1_add(threshSig, threshSig, sharesPow[i]);
    //}
  }
};

} /* namespace Relic */
} /* namespace BLS */
