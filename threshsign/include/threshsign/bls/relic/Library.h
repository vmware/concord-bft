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

#include "Relic.h"
#include "BlsNumTypes.h"

#include "threshsign/ThresholdSignaturesTypes.h"

#include <map>
#include <string>
#include <vector>

namespace BLS {
namespace Relic {

/**
 * Precomputes i^{-1} mod p for all 1 <= i <= MAX_NUM_OF_SHARES.
 *
 * Useful for speeding up Lagrange interpolation.
 */
class Library;

class PrecomputedInverses {
 protected:
  std::vector<BNT> invs;     // inverses of i
  std::vector<BNT> negInvs;  // inverses of (-i)
  BNT fieldOrder;
  bool precomputed;

  friend class Library;

 protected:
  PrecomputedInverses()
      : invs(static_cast<size_t>(MAX_NUM_OF_SHARES + 1)),
        negInvs(static_cast<size_t>(MAX_NUM_OF_SHARES + 1)),
        precomputed(false) {}

  void setFieldOrder(const BNT& order) { fieldOrder = order; }

  void precompute();

 public:
  const BNT& get(int i) const {
    if (i > 0)
      return invs[static_cast<size_t>(i)];
    else if (i < 0)
      return negInvs[static_cast<size_t>(-i)];
    else
      throw std::runtime_error("Only non-zero numbers have multiplicative inverses.");
  }
};

class LibraryInitializer {
 public:
  LibraryInitializer();
  ~LibraryInitializer();
};

class Library {
 public:
  static Library* GetPtr() {
    // C++11 guarantees this is thread-safe (but all threads will share this variable!)
    static Library* lib = new Library();
    return lib;
  }

  static const Library& Get() { return *Library::GetPtr(); }

  ~Library();

  static std::string getCurveName(int curveType);
  static int getCurveByName(const char* curveName);

 public:
  int getCurrentCurve() const { return ep_param_get(); }

  std::string getCurrentCurveName() const { return getCurveName(ep_param_get()); }

  int getSecurityLevel() const { return pc_param_level(); }

  int getG1PointSize() const { return numBytesG1; }

  int getG2PointSize() const { return numBytesG2; }

  const BNT& getG2Order() const { return g2size; }

  /**
   * Get the number of bits needed to represent the order of an element in G2.
   * If the group has order n, then it has elements g^0 through g^{n-1}. Thus,
   * the highest element we need to represent has order n-1 (not n!).
   */
  int getG2OrderNumBits() const { return g2bits; }

  const PrecomputedInverses& getPrecomputedInverses() const {
    pi.precompute();
    return pi;
  }

 private:
  Library();

 private:
  LibraryInitializer li;
  G1T dummyG1;
  G2T dummyG2;
  BNT g2size;
  int g2bits;
  int numBytesG1, numBytesG2;
  std::map<int, std::string> curveIdToName;
  std::map<std::string, int> curveNameToId;
  mutable PrecomputedInverses pi;
};

} /* namespace Relic */
} /* namespace BLS */
