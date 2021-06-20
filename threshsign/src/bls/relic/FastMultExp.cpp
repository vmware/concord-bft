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

#include "threshsign/VectorOfShares.h"
#include "threshsign/bls/relic/Library.h"

#include <vector>

#include "XAssert.h"
#include "Logger.hpp"

using std::endl;

namespace BLS {
namespace Relic {

template <class GT>
GT fastMultExp(const VectorOfShares& s, const std::vector<GT>& a, const std::vector<BNT>& e, int maxBits) {
  return fastMultExp(s, s.first(), s.count(), a, e, maxBits);
}

template <class GT>
GT fastMultExp(const VectorOfShares& s,
               ShareID first,
               int count,
               const std::vector<GT>& a,
               const std::vector<BNT>& e,
               int maxBits) {
  GT r;
  assertEqual(r, GT::Identity());

  for (int j = maxBits - 1; j >= 0; j--) {
    r.Double();

    ShareID i = first;
    for (int c = 0; c < count; c++) {
      assertFalse(s.isEnd(i));

      size_t idx = static_cast<size_t>(i);
      assertLessThanOrEqual(e[idx].getBits(), maxBits);

      if (e[idx].getBit(j)) r.Add(a[idx]);

      // Next share
      i = s.next(i);
    }
  }

  return r;
}

template <class GT>
GT fastMultExpTwo(const VectorOfShares& s, const std::vector<GT>& a, const std::vector<BNT>& e) {
  GT r;
  assertEqual(r, GT::Identity());

  for (ShareID i = s.first(); s.isEnd(i) == false; i = s.next(s.next(i))) {
    ShareID j = s.next(i);
    size_t ii = static_cast<size_t>(i);

    if (s.isEnd(j) == false) {
      size_t jj = static_cast<size_t>(j);
      // r += a1^e1 + a2^e2
      r.Add(GT::TimesTwice(a[ii], e[ii], a[jj], e[jj]));
    } else {
      // r += a^e
      r.Add(GT::Times(a[ii], e[ii]));
    }
  }

  return r;
}

/**
 * Template exports
 */
template G1T fastMultExp<G1T>(const VectorOfShares& s,
                              const std::vector<G1T>& a,
                              const std::vector<BNT>& e,
                              int maxBits);
template G1T fastMultExp<G1T>(const VectorOfShares& s,
                              ShareID first,
                              int count,
                              const std::vector<G1T>& a,
                              const std::vector<BNT>& e,
                              int maxBits);
template G2T fastMultExp<G2T>(const VectorOfShares& s,
                              const std::vector<G2T>& a,
                              const std::vector<BNT>& e,
                              int maxBits);
template G2T fastMultExp<G2T>(const VectorOfShares& s,
                              ShareID first,
                              int count,
                              const std::vector<G2T>& a,
                              const std::vector<BNT>& e,
                              int maxBits);

template G1T fastMultExpTwo<G1T>(const VectorOfShares& s, const std::vector<G1T>& a, const std::vector<BNT>& e);
template G2T fastMultExpTwo<G2T>(const VectorOfShares& s, const std::vector<G2T>& a, const std::vector<BNT>& e);

}  // namespace Relic
}  // namespace BLS
