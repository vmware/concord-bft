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

#include "threshsign/ThresholdSignaturesTypes.h"
#include "threshsign/VectorOfShares.h"

#include "BlsNumTypes.h"

#include <vector>

namespace BLS {
namespace Relic {

/**
 * Computes r = \prod_{i \in s} { a[i]^e[i] } faster than naive method.
 * Thanks to "Fast Batch Verification for Modular Exponentiation and Digital Signatures"
 */
template <class GT>
GT fastMultExp(const VectorOfShares& s, const std::vector<GT>& a, const std::vector<BNT>& e, int maxBits);
template <class GT>
GT fastMultExp(const VectorOfShares& s,
               ShareID first,
               int count,
               const std::vector<GT>& a,
               const std::vector<BNT>& e,
               int maxBits);

/**
 * Computes r = \prod_{i \in s} { a[i]^e[i] } faster than naive method by recursing on
 * a fast way to compute a1^e1 * a2^e2.
 *
 * NOTE: Slower than fastMultExp above.
 */
template <class GT>
GT fastMultExpTwo(const VectorOfShares& s, const std::vector<GT>& a, const std::vector<BNT>& e);

}  // namespace Relic
}  // namespace BLS
