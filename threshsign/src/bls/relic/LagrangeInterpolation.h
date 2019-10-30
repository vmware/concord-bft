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

#include <set>
#include <vector>

#include "threshsign/bls/relic/BlsNumTypes.h"
#include "threshsign/bls/relic/Library.h"
#include "threshsign/ThresholdSignaturesTypes.h"
#include "threshsign/VectorOfShares.h"

/**
 * These methods take the set of signers and compute the vector of Lagrange
 * coefficients (i.e., l_i^S(0) for each signer i).
 *
 * See some benchmarks numbers in doc/\*-bls-relic-lagr-bench*.txt.
 */
namespace BLS {
namespace Relic {

class BlsPublicParameters;

// NOTE: ALIN: Not faster than SlowModulo on Linux!
//#define WITH_FAST_MODULO

class ModulusBNT {
 protected:
#ifdef WITH_FAST_MODULO
  // precomputed data for fast(er) modular reduction
  BNT u;
#endif
  BNT modulus;

 public:
  ModulusBNT(const BNT& mod) : modulus(mod) {
#ifdef WITH_FAST_MODULO
    u = BNT::FastModuloPreBarrett(modulus);
#endif
  }

 public:
  const BNT& getModulus() const { return modulus; }

  void Reduce(BNT& num) const {
#ifdef WITH_FAST_MODULO
    num.FastModuloBarrett(modulus, u);
#else
    num.SlowModulo(modulus);
#endif
  }
};

class AccumulatedBNT {
 protected:
  BNT v;
  BNT vAcc;
  ModulusBNT modulus;

 public:
  AccumulatedBNT(const BNT& mod) : v(BNT::One()), vAcc(BNT::One()), modulus(mod) {}

 public:
  inline void reset() {
    v = BNT::One();
    vAcc = BNT::One();
  }

  void reduceIf();
  void reduceAlways();

  void Times(const BNT& n) {
    if (n < 0)
      throw std::runtime_error(
          "Must only multiply by positive numbers (most performant way of dealing with RELIC issues).");
    vAcc.Times(n);
    reduceIf();
  }

  template <class Numeric>
  inline void Times(Numeric n) {
    if (n < 0)
      throw std::runtime_error(
          "Must only multiply by positive numbers (most performant way of dealing with RELIC issues).");
    vAcc.Times(static_cast<dig_t>(n));
    reduceIf();
  }

  const BNT& toBNT();
};

class LagrangeIncrementalCoeffs {
 protected:
  /**
   * We save some modular reductions by multiplying things in denomsAccum until >= fieldOrder
   * and only then multiplying denomsAccum in denoms and doing modular reduction.
   * Same for numerator: see numer and numerAccum.
   */

  const ModulusBNT fieldOrder;
  std::vector<AccumulatedBNT> denoms;
  std::vector<bool> denomSigns;
  AccumulatedBNT numerFull;
  int numerSign;     // finalizeCoefficient needs shared access to this
  BNT numerReduced;  // finalizeCoefficient needs shared access to this

  VectorOfShares signers;
  const PrecomputedInverses& pi;

 public:
  LagrangeIncrementalCoeffs(NumSharesType numSigners, const BlsPublicParameters& params);

 protected:
  void finalizeCoefficient(ShareID id, BNT& coeff);

 public:
  /**
   * Updates the Lagrange coefficients incrementally as signers are added (the full numerator
   * and individual denominators).
   */
  bool addSigner(ShareID newSigner);

  void removeSigner(ShareID badSigner);

  void finalize(std::vector<BNT>& coeffsOut);

  const VectorOfShares& getSigners() const { return signers; }
};

/**
 * Computes the numerator and denominator without modular reduction.
 * Reduces them at the end before dividing numerator by denominator.
 *
 * WARNING: Doesn't work for more than 512 signers due to limitations in RELIC's
 * bignum implementation.
 */
void lagrangeCoeffNaive(const VectorOfShares& signers, std::vector<BNT>& lagrangeCoeffs, const BNT& fieldOrder);

/**
 * Computes the numerator and denominator with modular reduction after every multiplication.
 *
 * WARNING: Still slow because reduces too often.
 */
void lagrangeCoeffNaiveReduced(const VectorOfShares& signers, std::vector<BNT>& coeffs, const BNT& fieldOrder);

/**
 * Computes the numerator and denominator with modular reduction as necessary.
 * Keeps an accumulator for the numerator and denominator such that
 * |accumulator| < fieldOrder or else the accumulator is reduced modulo the fieldOrder.
 *
 * Also, computes numerator faster by avoiding repeated computations (as opposed to naive).
 *
 * NOTE: Fastest implementation because reduces very rarely.
 * TODO: could reduce even more rarely when |accumulator| < c * fieldOrder, for some c.
 */
void lagrangeCoeffAccumReduced(const VectorOfShares& signers, std::vector<BNT>& lagrangeCoeffs, const BNT& fieldOrder);

}  // namespace Relic
}  // namespace BLS
