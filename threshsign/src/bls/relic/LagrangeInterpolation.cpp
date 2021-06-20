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

#include "threshsign/Configuration.h"

#include "LagrangeInterpolation.h"

#include "threshsign/VectorOfShares.h"
#include "threshsign/bls/relic/BlsNumTypes.h"
#include "threshsign/bls/relic/BlsPublicParameters.h"

#include "NotImplementedException.h"
#include "Logger.hpp"
#include "XAssert.h"
#include "Timer.h"

#include <algorithm>

using std::endl;

namespace BLS {
namespace Relic {

inline void AccumulatedBNT::reduceIf() {
  if (vAcc >= modulus.getModulus()) {
    reduceAlways();
  }
}

inline void AccumulatedBNT::reduceAlways() {
  v.Times(vAcc);
  modulus.Reduce(v);
  vAcc = BNT::One();
}

const BNT& AccumulatedBNT::toBNT() {
  if (vAcc > BNT::One()) {
    reduceAlways();
  }

  return v;
}

LagrangeIncrementalCoeffs::LagrangeIncrementalCoeffs(NumSharesType numSigners, const BlsPublicParameters& params)
    : fieldOrder(params.getGroupOrder()),
      denoms(static_cast<size_t>(numSigners + 1), AccumulatedBNT(fieldOrder.getModulus())),
      denomSigns(static_cast<size_t>(numSigners + 1), true),  // all positive
      numerFull(fieldOrder.getModulus()),
      numerSign(0),
      pi(Library::Get().getPrecomputedInverses()) {
  assertEqual(denoms.size(), static_cast<std::vector<BLS::Relic::AccumulatedBNT>::size_type>(numSigners + 1));
  assertEqual(denoms.size(), denomSigns.size());
}

/**
 * Functions to compute all n Lagrange coefficients incrementally as signers show up.
 */
bool LagrangeIncrementalCoeffs::addSigner(ShareID newSigner) {
  assertStrictlyPositive(newSigner);

  if (signers.contains(newSigner)) {
    LOG_WARN(BLS_LOG, "Tried to add signer #" << newSigner << " more than once");
    return false;
  }

  // Update numerator
  numerFull.Times(static_cast<dig_t>(newSigner));

  // TODO: ALIN: time intensive after a certain |signers| size, might want to split up in separate threads

  // Update denominators for all past signers and add a new denominator for the new signer
  for (ShareID i = signers.first(); signers.isEnd(i) == false; i = signers.next(i)) {
    // update denom(l_i^S) with (i - j) where j = new signer
    // and
    // create new denom(l_j^s) = \prod_{i \ne j} (j - i), where j = newSigner
    size_t ii = static_cast<size_t>(i);
    size_t jj = static_cast<size_t>(newSigner);
    auto diff = i - newSigner;  // i - j
    if (diff > 0) {
      denoms[ii].Times(diff);  // l_i^S *= (i - j)
      denoms[jj].Times(diff);  // l_j^S *= (j - i) via marked sign below
      denomSigns[jj] = !denomSigns[jj];
    } else {
      denoms[ii].Times(-diff);  // l_i^S *= (i - j) via marked sign below
      denomSigns[ii] = !denomSigns[ii];
      denoms[jj].Times(-diff);  // l_j^S *= (j - i)
    }
  }

  // Update signers
  signers.add(newSigner);

  return true;
}

/**
 * WARNING: This will be slower as it involves division which requires inverting mod p.
 */
void LagrangeIncrementalCoeffs::removeSigner(ShareID badSigner) {
  assertTrue(signers.contains(badSigner));
  assertStrictlyPositive(badSigner);

  // Update the set of signers
  signers.remove(badSigner);

  // Tweak the numerator (multiply by the inverse of badSigner)
  // NOTE: PERF: This will lead to a modular reduction every time as the inverse is large.
  numerFull.Times(pi.get(badSigner));

  // Tweak the other Lagrange coefficients
  for (ShareID i = signers.first(); signers.isEnd(i) == false; i = signers.next(i)) {
    // remove bad signer j from denom(l_i^S) by dividing it with (i - j), where i != j
    size_t ii = static_cast<size_t>(i);
    // NOTE: PERF: This will lead to a modular reduction every time as the inverse is large.
    auto diff = i - badSigner;  // i - j
    if (diff > 0) {
      denoms[ii].Times(pi.get(diff));  // l_i^S /= (i - j)
    } else {
      denoms[ii].Times(pi.get(-diff));  // l_i^S /= (i - j) via marked sign below
      denomSigns[ii] = !denomSigns[ii];
    }
  }

  // Reset the denom for the removed signer
  size_t jj = static_cast<size_t>(badSigner);
  denoms[jj].reset();
  denomSigns[jj] = true;
}

void LagrangeIncrementalCoeffs::finalizeCoefficient(ShareID id, BNT& coeff) {
  size_t idx = static_cast<size_t>(id);

  // Initialize numerator to numerAcc / i
  BNT numer(numerReduced);

  // pi.get(id) returns the inverse of id (mod p)
  numer.Times(pi.get(id));
  fieldOrder.Reduce(numer);

  // Get numerator (applies final reduction if needed)
  BNT denom = denoms[idx].toBNT();

  assertNotEqual(denom, BNT::Zero());
  assertStrictlyLessThan(denom, fieldOrder.getModulus());

  numer.Times(denom.invertModPrime(fieldOrder.getModulus()));
  fieldOrder.Reduce(numer);

  // So far we ignored the signed in the numerator and denominator.
  int finalSign = numerSign * (denomSigns[idx] ? 1 : -1);

  assertProperty(finalSign, finalSign == -1 || finalSign == 1) assertStrictlyLessThan(numer, fieldOrder.getModulus());

  // Compute Lagrange coefficient as numerator * (denominator)^(-1) mod fieldOrder
  if (finalSign == -1) {
    // We have -coeff, so turn it into a valid field element via (fieldOrder - coeff)
    coeff = fieldOrder.getModulus();
    coeff.Subtract(numer);
  } else {
    coeff = numer;
  }

  assertStrictlyLessThan(coeff, fieldOrder.getModulus());
}

/**
 * NOTE: If removeSigner() is called then the denoms for all signers change
 * so finalize has to be called again to invert the new denominator.
 *
 * TODO: LATER: Here we are wasting time: to remove signer j from signer i's coefficient,
 * we can multiply the coefficient directly by (i-j) mod p and by (-j)^{-1} mod p,
 * rather than dividing in removeSigner and then inverting the denominator in finalize.
 * We should change the class so that coeffsOut is a member that we update directly
 * in removeSigner() and addSigner(). Thus, finalize() only needs to be called once and we can restrict
 * removeSigner() to only be called after finalize() perhaps (though we have to
 * update the tests then?). addSigner(j) will multiply each coeff by -j (except for coeff j)
 * and divide each coeff i != j by (i-j). Could be a little slower now, unless we use another layer of
 * AccumulatedBNTs.
 */
void LagrangeIncrementalCoeffs::finalize(std::vector<BNT>& coeffsOut) {
  numerSign = (signers.count() % 2 == 0 ? -1 : 1);

  // Get numerator (applies final reduction if needed)
  numerReduced = numerFull.toBNT();

  for (ShareID i = signers.first(); signers.isEnd(i) == false; i = signers.next(i)) {
    finalizeCoefficient(i, coeffsOut[static_cast<size_t>(i)]);
  }
}

/**
 * Functions to compute all n Lagrange coefficients in one go.
 */

void lagrangeCoeffAccumReduced(const VectorOfShares& signers, std::vector<BNT>& coeffs, const BNT& fieldOrder) {
  AccumulatedBNT numerAcc(fieldOrder);
  BNT numer;
  ModulusBNT modulus(fieldOrder);

  int numerSign = (signers.count() % 2 == 0 ? -1 : 1);

  /**
   * The numerator for each l_i^S(0) = ( \prod_{j \in S} {0-j} ) / (0 - i)
   * So we compute \prod_{j \in S} {0-j} which we divide by (0-i)
   * for each i later on.
   */
  for (ShareID j = signers.first(); signers.isEnd(j) == false; j = signers.next(j)) {
    // For the numerator, instead of doing:
    //   \prod_{j \ne i} {0-j},
    // do:
    //   \prod_{j \ne i} {j}
    // ...and then multiply by (-1)^{signers.size() - 1}
    numerAcc.Times(j);
  }

  // Leftovers
  BNT numerFinal = numerAcc.toBNT();

  assertStrictlyLessThan(numerFinal, fieldOrder);

  // Let S denote the signers, then compute the Lagrange coefficients:
  // l_i^S(0) = \prod_{j\in S; j \ne i} {(0-j)/(i-j)} (mod ord(G))
  for (ShareID i = signers.first(); signers.isEnd(i) == false; i = signers.next(i)) {
    LOG_TRACE(BLS_LOG, " i = " << i);

    // Initialize numerator to numerFull / i and denominator to 1
    numer = numerFinal;
    // Need to check if it's divisible by i, because numerFull might have been reduced
    if (numer % i == BNT::Zero()) {
      numer.DivideBy(static_cast<dig_t>(i));
    } else {
      numer.Times(BNT::invertModPrime(static_cast<dig_t>(i), fieldOrder));
      modulus.Reduce(numer);
    }

    LOG_TRACE(BLS_LOG, "num[" << i << "] = " << numer);

    AccumulatedBNT denomAccum(fieldOrder);

    // Keep track of the denominator's sign (already have numerator sign from above)
    bool denomSign = true;  // positive
    int finalSign;

    for (ShareID j = signers.first(); signers.isEnd(j) == false; j = signers.next(j)) {
      if (i == j) {
        continue;
      }

      LOG_TRACE(BLS_LOG, " j = " << j);
      LOG_TRACE(BLS_LOG, " i - j  = " << i - j);

      // For the denominator, multiply by |i-j| first and then adjust the sign!
      if (i > j) {
        denomAccum.Times(i - j);
      } else {
        denomAccum.Times(j - i);  // i.e., -(i-j) and we multiply by -1 below
        denomSign = !denomSign;
      }
    }

    BNT denom = denomAccum.toBNT();

    LOG_TRACE(BLS_LOG, "denom[" << i << "] = " << denom);

    numer.Times(denom.invertModPrime(fieldOrder));
    modulus.Reduce(numer);

    // So far we ignored the signed in the numerator and denominator.
    finalSign = numerSign * (denomSign ? 1 : -1);
    assertProperty(finalSign, finalSign == -1 || finalSign == 1) assertStrictlyLessThan(numer, fieldOrder);

    // Compute Lagrange coefficient as numerator * (denominator)^(-1) mod fieldOrder
    size_t idx = static_cast<size_t>(i);
    if (finalSign == -1) {
      // We have -coeff, so turn it into a valid field element via (fieldOrder - coeff)
      coeffs[idx] = fieldOrder;
      coeffs[idx].Subtract(numer);
    } else {
      coeffs[idx] = numer;
    }

    assertStrictlyLessThan(coeffs[idx], fieldOrder);
    LOG_TRACE(BLS_LOG, "lagr[ i = " << idx << " ] = " << coeffs[idx]);
  }
}

void lagrangeCoeffNaiveReduced(const VectorOfShares& signers, std::vector<BNT>& coeffs, const BNT& fieldOrder) {
  BNT numer;
  BNT denom;
  ModulusBNT modulus(fieldOrder);

  // Let S denote the signers, then compute the Lagrange coefficients:
  // l_i^S(0) = \prod_{j\in S; j \ne i} {(0-j)/(i-j)} (mod ord(G))
  for (ShareID i = signers.first(); signers.isEnd(i) == false; i = signers.next(i)) {
    LOG_TRACE(BLS_LOG, " i = " << i);

    // Initialize numerator and denominator to 1
    numer = 1;
    denom = 1;
    // Keep track of the numerator and denominator's sign
    int numerSign = (signers.count() % 2 == 0 ? -1 : 1);
    bool denomSign = true;
    int finalSign;

    for (ShareID j = signers.first(); signers.isEnd(j) == false; j = signers.next(j)) {
      if (i == j) {
        continue;
      }

      LOG_TRACE(BLS_LOG, " j = " << j);
      LOG_TRACE(BLS_LOG, " i - j  = " << i - j);

      // For the numerator, instead of doing:
      //   \prod_{j \ne i} {0-j},
      // do:
      //   \prod_{j \ne i} {j}
      // ...and then multiply by (-1)^{signers.size() - 1}
      numer.Times(static_cast<dig_t>(j));
      modulus.Reduce(numer);

      // For the denominator, multiply by |i-j| first and then adjust the sign!
      if (i > j) {
        denom.Times(static_cast<dig_t>(i - j));  // i.e., (i-j)
        modulus.Reduce(denom);
      } else {
        denom.Times(static_cast<dig_t>(j - i));  // i.e., -(i-j) and we multiply by -1 below
        modulus.Reduce(denom);
        denomSign = !denomSign;
      }
    }

    // Avoid inverting if the numerator is divisible by the denominator!
    // NOTE: Does not seem to improve performance much.
    if (numer % denom == BNT::Zero()) {
      numer.DivideBy(denom);
    } else {
      numer.Times(denom.invertModPrime(fieldOrder));
      modulus.Reduce(numer);
    }

    // So far we ignored the signed in the numerator and denominator.
    finalSign = numerSign * (denomSign ? 1 : -1);
    assertProperty(finalSign, finalSign == -1 || finalSign == 1) assertStrictlyLessThan(numer, fieldOrder);

    // Compute Lagrange coefficient as numerator * (denominator)^(-1) mod fieldOrder
    size_t idx = static_cast<size_t>(i);
    if (finalSign == -1) {
      // We have -coeff, so turn it into a valid field element via (fieldOrder - coeff)
      coeffs[idx] = fieldOrder;
      coeffs[idx].Subtract(numer);
    } else {
      coeffs[idx] = numer;
    }

    assertStrictlyLessThan(coeffs[idx], fieldOrder);

    LOG_TRACE(BLS_LOG, "lagr[ i = " << idx << " ] = " << coeffs[idx]);
  }
}

void lagrangeCoeffNaive(const VectorOfShares& signers, std::vector<BNT>& coeffs, const BNT& fieldOrder) {
  BNT lagrNum, lagrDenom;

  // Let S denote the signers, then compute the Lagrange coefficients:
  // l_i^S(0) = \prod_{j\in S; j \ne i} {(0-j)/(i-j)} (mod ord(G))
#ifndef NDEBUG
  int maxNumeratorBits = 0, maxDenomBits = 0, avgNumeratorBits = 0, avgDenomBits = 0;
#endif

  for (ShareID i = signers.first(); signers.isEnd(i) == false; i = signers.next(i)) {
    // LOG_TRACE(BLS_LOG, "Computing l_" << i << "(0) relative to other signers...");

    // Initialize numerator and denominator to 1
    lagrNum = 1;
    lagrDenom = 1;
    for (ShareID j = signers.first(); signers.isEnd(j) == false; j = signers.next(j)) {
      // LOG_TRACE(BLS_LOG, "l_" << i << "(0), incorporating signer " << j);

      if (i == j) {
        // LOG_TRACE(BLS_LOG, "Skipping over j = i = " << j);
        continue;
      }

      // For the numerator, instead of doing:
      //   \prod_{j \ne i} {0-j},
      // do:
      //   \prod_{j \ne i} {j}
      // ...and then multiply by (-1)^{signers.size() - 1}
      lagrNum.Times(static_cast<dig_t>(j));

      // For the denominator, multiply by |i-j| first and then adjust the sign!
      // WARNING: bn_mul_dig only takes an unsigned type as an arg!
      if (i > j) {
        lagrDenom.Times(static_cast<dig_t>(i - j));
      } else {
        lagrDenom.Times(static_cast<dig_t>(j - i));  // i.e., -(i-j) and we multiply by -1 below
        lagrDenom.Negate();
      }
    }

    // Multiply numerator by (-1)^{signers.size() - 1} (i.e. negate it if number of signers is even)
    if (signers.count() % 2 == 0) {
      lagrNum.Negate();
      // LOG_TRACE(BLS_LOG, "|S| - 1 is odd, multiplying numerator by (-1): " << lagrNum);
    }

#ifndef NDEBUG
    // Let's see how big these numbers get
    maxNumeratorBits = std::max(maxNumeratorBits, bn_bits(lagrNum));
    maxDenomBits = std::max(maxDenomBits, bn_bits(lagrDenom));

    avgNumeratorBits += bn_bits(lagrNum);
    avgDenomBits += bn_bits(lagrDenom);
    // LOG_TRACE(BLS_LOG, "Denominator bits for l_" << i << "(0): " << bn_bits(lagrDenom));
    // LOG_TRACE(BLS_LOG, "Numerator bits for l_" << i << "(0): " << bn_bits(lagrNum));
#endif
    // Reduce numerator, also takes care of negative numberator.
    lagrNum.SlowModulo(fieldOrder);

    // WARNING: This does NOT work with negative denom when |denom| > fieldOrder so we bring it
    // back to a positive number via a modular reduction.
    lagrDenom.SlowModulo(fieldOrder);
    BNT lagrDenomInv = lagrDenom.invertModPrime(fieldOrder);

    // Compute Lagrange coefficient as numerator * (denominator)^(-1) mod fieldOrder
    size_t idx = static_cast<size_t>(i);
    bn_mul(coeffs[idx], lagrNum, lagrDenomInv);
    coeffs[idx].SlowModulo(fieldOrder);

    assertStrictlyLessThan(coeffs[idx], fieldOrder);
  }

#ifndef NDEBUG
  // The numerator/denom get pretty big! For n = 128, k = 128 (max and avg are around to 700 bits)
  LOG_TRACE(BLS_LOG, "Average numerator bits: " << avgNumeratorBits / static_cast<int>(signers.count()));
  LOG_TRACE(BLS_LOG, "Average denominator bits: " << avgDenomBits / static_cast<int>(signers.count()));
  LOG_TRACE(BLS_LOG, "Max numerator bits: " << maxNumeratorBits);
  LOG_TRACE(BLS_LOG, "Max denominator bits: " << maxDenomBits);
#endif
}
}  // namespace Relic
}  // namespace BLS
