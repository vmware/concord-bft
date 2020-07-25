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
#include <mutex>

#include "XAssert.h"
#include "Logger.hpp"

#include "threshsign/VectorOfShares.h"
#include "threshsign/ThresholdSignaturesTypes.h"

/**
 * NOTE: There should be a way to speed this up even further (only useful for speeding up benchmarks).
 */

template <class T>
class AlmostMultisigCoefficientsMap {
 protected:
  NumSharesType n;
  std::vector<T> coeffs;

 public:
  /**
   * When storing this object in a std::map, we need its default constructor defined.
   */
  AlmostMultisigCoefficientsMap() : n(0) {}
  AlmostMultisigCoefficientsMap(NumSharesType n) : n(n), coeffs(static_cast<size_t>((n + 1) * (n + 1))) {}
  virtual ~AlmostMultisigCoefficientsMap() {}

 public:
  void precompute() {
    LOG_DEBUG(THRESHSIGN_LOG, "Precomputing Lagrange coefficients for n = " << n);

    VectorOfShares full;

    // Create a vector full of all signers
    for (ShareID i = 1; i <= n; i++) {
      full.add(i);
    }

    // Then, for each participant j remove j and compute the coefficients for each participant i w.r.t. to the set of
    // signers without j
    for (ShareID j = 1; j <= n; j++) {
      VectorOfShares vec = full;
      vec.remove(j);

      calculateCoefficientsForAll(vec, j);
    }

    LOG_DEBUG(THRESHSIGN_LOG, "Done precomputing Lagrange coefficients for " << n - 1 << " out of " << n);
  }

  virtual void calculateCoefficientsForAll(const VectorOfShares& vec, ShareID missing) = 0;

  /**
   * NOTE: i and missing are always >= 1
   *
   * Recall that matrix[i][j] = vector[i * row_size + j], where row_size = num_cols and
   * i and j are 0-based indices.
   */
  size_t getIndex(ShareID i, ShareID missing) const {
    assertInclusiveRange(1, i, n);
    assertInclusiveRange(1, missing, n);
    return static_cast<size_t>((i - 1) * n + (missing - 1));
  }

  /**
   * Returns the coefficient L_i^S(0) for player i w.r.t. to the set S which excludes player 'missing'
   */
  const T& getCoeff(ShareID i, ShareID missing) const {
    // LOG_TRACE(THRESHSIGN_LOG, "Getting precomputed L_" << i << "^S-{" << missing << "} (n = " << n << ")...");
    return coeffs[getIndex(i, missing)];
  }
};

template <class CoeffMap>
class AlmostMultisigCoefficients {
 protected:
  /**
   * coeffsMap[n] are the Lagrange coefficients for an n-1 out of n threshold scheme
   * coeffsMap[n].get(i, j) is the Lagrange coefficient L_i(0) for player i's share in the n-1 out of n threshold scheme
   * when player j is the missing one
   */
  std::map<NumSharesType, CoeffMap> coeffsMap;
  std::mutex m;

 public:
  AlmostMultisigCoefficients() {}

  ~AlmostMultisigCoefficients() {}

 public:
  void precompute(NumSharesType n) {
    // TODO: ALIN: Can speed this up using the incremental Lagrange code!
    if (coeffsMap.count(n) == 0) {
      coeffsMap[n] = CoeffMap(n);
      coeffsMap[n].precompute();
    }
  }

  const CoeffMap& computeMapAndGet(NumSharesType n) {
    std::lock_guard<std::mutex> lock(m);

    if (coeffsMap.count(n) == 0) precompute(n);
    return coeffsMap.at(n);
  }
};
