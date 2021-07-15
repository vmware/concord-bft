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

#include <map>
#include <set>
#include <vector>
#include <string>
#include <cassert>
#include <memory>
#include <algorithm>
#include <stdexcept>

#include "Logger.hpp"
#include "Utils.h"
#include "Timer.h"
#include "XAssert.h"
#include "app/RelicMain.h"

#include "threshsign/VectorOfShares.h"
#include "threshsign/bls/relic/Library.h"
#include "threshsign/bls/relic/BlsPublicParameters.h"
#include "threshsign/bls/relic/PublicParametersFactory.h"

#include "bls/relic/LagrangeInterpolation.h"

using namespace std;
using namespace BLS::Relic;

/**
 * Converts a sequence of +/- i's into a subset of signers.
 */
void seqToSubset(VectorOfShares& signers, const std::vector<int>& seq) {
  for (int i : seq) {
    testAssertNotZero(i);
    testAssertLessThanOrEqual(i, MAX_NUM_OF_SHARES);

    if (i > 0)
      signers.add(i);
    else {
      testAssertTrue(signers.contains(-i));
      signers.remove(-i);
    }
  }
}

int RelicAppMain(const Library& lib, const std::vector<std::string>& args) {
  (void)lib;
  (void)args;

  std::vector<int> seqs[] = {
      {1, 2, 3},
      {1, -1, 1},
      {1, -1, 1, 2},
      {1, 2, 3, 4, -4},
      {1, 2, 4, 5, 7, -2},
      {1, 2, 4, 5, 7, -2, -5},
      {1, 2, 4, 5, 7, -2, -5, 2, -4, 4, 9},
      {2, 1, 3, -1, -2, 4, 5, 2, 1, 9, -5, 5, 6, -9, 9, 7, -5, 5, -7, 8, 7, -8, 8},
  };

  BlsPublicParameters params = PublicParametersFactory::getWhatever();

  VectorOfShares signers;
  for (const std::vector<int>& seq : seqs) {
    signers.clear();
    seqToSubset(signers, seq);
    LOG_INFO(THRESHSIGN_LOG, " * Testing signers: " << signers);
    int numSigners = *std::max_element(seq.begin(), seq.end());
    // LOG_DEBUG(THRESHSIGN_LOG, "Max signer ID: " << numSigners);

    std::vector<BNT> incrCoeffs(static_cast<size_t>(numSigners + 1), BNT(0));
    LagrangeIncrementalCoeffs lagr(numSigners, params);

    // Add and remove signers as indicated by the sequence of operations
    for (int s : seq) {
      if (s < 0) {
        LOG_DEBUG(THRESHSIGN_LOG, "Remove signer " << -s << ": ");
        lagr.removeSigner(-s);
      } else {
        LOG_DEBUG(THRESHSIGN_LOG, "Add signer " << s << ": ");
        lagr.addSigner(s);
      }
      LOG_DEBUG(THRESHSIGN_LOG, lagr.getSigners());
    }

    lagr.finalize(incrCoeffs);

    // Should have the same signers in our Lagrange code as the ones we're computing Lagrange for next
    testAssertEqual(signers, lagr.getSigners());

    // Now construct Lagrange coefficients normally
    std::vector<BNT> naiveCoeffs(static_cast<size_t>(numSigners + 1), BNT(0));
    testAssertEqual(incrCoeffs.size(), naiveCoeffs.size());
    lagrangeCoeffNaiveReduced(signers, naiveCoeffs, lib.getG2Order());
    testAssertEqual(incrCoeffs.size(), naiveCoeffs.size());

    // Make sure the coefficients match
    auto result = std::mismatch(incrCoeffs.begin(), incrCoeffs.end(), naiveCoeffs.begin());
    if (result.first != incrCoeffs.end()) {
      size_t pos = static_cast<size_t>(result.first - incrCoeffs.begin());
      LOG_ERROR(THRESHSIGN_LOG, "Mismatch for signer " << pos);
      LOG_ERROR(THRESHSIGN_LOG, "  incrCoeffs[" << pos << "] = " << *result.first);
      LOG_ERROR(THRESHSIGN_LOG, "  naiveCoeffs[" << pos << "] = " << *result.second);
      throw std::runtime_error("Bad coeffs");
    }

    std::vector<BNT> lagrCoeffs(static_cast<size_t>(numSigners + 1), BNT(0));
    testAssertEqual(incrCoeffs.size(), lagrCoeffs.size());
    lagrangeCoeffAccumReduced(signers, lagrCoeffs, lib.getG2Order());
    testAssertEqual(incrCoeffs.size(), lagrCoeffs.size());

    // Make sure the coefficients match
    result = std::mismatch(incrCoeffs.begin(), incrCoeffs.end(), lagrCoeffs.begin());
    if (result.first != incrCoeffs.end()) {
      size_t pos = static_cast<size_t>(result.first - incrCoeffs.begin());
      LOG_ERROR(THRESHSIGN_LOG, "Mismatch for signer " << pos);
      LOG_ERROR(THRESHSIGN_LOG, "  incrCoeffs[" << pos << "] = " << *result.first);
      LOG_ERROR(THRESHSIGN_LOG, "  lagrCoeffs[" << pos << "] = " << *result.second);
      throw std::runtime_error("Bad coeffs");
    }
  }
  return 0;
}
