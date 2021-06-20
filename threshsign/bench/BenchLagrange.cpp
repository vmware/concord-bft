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

#include <iostream>
#include <fstream>
#include <vector>

#include "bls/relic/LagrangeInterpolation.h"
#include "threshsign/bls/relic/BlsPublicParameters.h"
#include "threshsign/bls/relic/PublicParametersFactory.h"

#include "Utils.h"
#include "Timer.h"
#include "Logger.hpp"
#include "app/RelicMain.h"

#include "lib/Benchmark.h"

extern "C" {
#include <relic/relic.h>
#include <relic/relic_err.h>
}

using namespace BLS::Relic;
using std::endl;

void printTime(const AveragingTimer& t, int numCoeffs, int numIters) {
  std::cout << t.getName() << " (" << numIters << " iters avg): " << t.averageLapTime() / numCoeffs
            << " microsecs per coefficient"
            << " / " << t.averageLapTime() << " microsecs in total" << endl;
}

int RelicAppMain(const Library& lib, const std::vector<std::string>& args) {
  (void)args;

  unsigned int seed = static_cast<unsigned int>(time(NULL));
  LOG_INFO(THRESHSIGN_LOG, "Randomness seed passed to srand(): " << seed);
  // NOTE: srand is not and should not be used for any cryptographic randomness.
  srand(seed);

#ifdef NDEBUG
  const int numIters = 10;
#else
  const int numIters = 1;
#endif
  int maxNumShares = 2048;
  if (maxNumShares > MAX_NUM_OF_SHARES) {
    LOG_ERROR(THRESHSIGN_LOG,
              "MAX_NUM_OF_SHARES = " << MAX_NUM_OF_SHARES << " is smaller need max of " << maxNumShares);
    throw std::runtime_error("Recompile with higher MAX_NUM_OF_SHARES");
  }

  // Precomputes inverses of i for all signers i
  lib.getPrecomputedInverses();

  std::vector<BNT> redCoeffs, rnCoeffs, naiveCoeffs, incrCoeffs;

  BlsPublicParameters params = PublicParametersFactory::getWhatever();

  for (int n = 128; n <= maxNumShares; n *= 2) {
    // int k = (n*2)/3 + (rand() % 2);	// sometimes odd, sometimes even.
    int k = n - 2 + (rand() % 2);  // sometimes odd, sometimes even.

    LOG_INFO(THRESHSIGN_LOG,
             "Testing k = " << k << " out of n = " << n << " (field order is " << params.getGroupOrder().getBits()
                            << " bits)");

    // for(int c = 0; c <= 100; c++) {
    VectorOfShares subset;
    VectorOfShares::randomSubset(subset, n, k);

    // TODO: stddev, min, max

    // RELIC does not have sufficient bignum precision; cannot compute coefficients
    // without reducing for n > 512
    if (n < 512) {
      naiveCoeffs.resize(static_cast<size_t>(n + 1), BNT(0));
      AveragingTimer t1("Naive implementation");
      for (int i = 0; i < numIters; i++) {
        t1.startLap();
        lagrangeCoeffNaive(subset, naiveCoeffs, params.getGroupOrder());
        t1.endLap();
      }
      printTime(t1, subset.count(), numIters);
    }

    redCoeffs.resize(static_cast<size_t>(n + 1), BNT(0));
    AveragingTimer t2("Accum+reduced implm.");
    for (int i = 0; i < numIters; i++) {
      t2.startLap();
      lagrangeCoeffAccumReduced(subset, redCoeffs, params.getGroupOrder());
      t2.endLap();
    }
    printTime(t2, subset.count(), numIters);

    incrCoeffs.resize(static_cast<size_t>(n + 1), BNT(0));
    AveragingTimer t3("Incr+accum implemen.");
    AveragingTimer tf("Incr+accum finalize ");
    for (int i = 0; i < numIters; i++) {
      t3.startLap();
      LagrangeIncrementalCoeffs lagr(n, params);
      for (ShareID s = subset.first(); subset.isEnd(s) == false; s = subset.next(s)) {
        lagr.addSigner(s);
      }

      tf.startLap();
      lagr.finalize(incrCoeffs);
      tf.endLap();

      t3.endLap();
    }
    printTime(t3, subset.count(), numIters);
    std::cout << "      \\-> Time used to finalize " << k << " coeffs: " << tf.averageLapTime() << " microsecs" << endl;

    // Starts being slow after 512, no point in waiting...
    if (n < 512) {
      rnCoeffs.resize(static_cast<size_t>(n + 1), BNT(0));
      AveragingTimer t("Naive+reduced implm.");
      for (int i = 0; i < numIters; i++) {
        t.startLap();
        lagrangeCoeffNaiveReduced(subset, rnCoeffs, params.getGroupOrder());
        t.endLap();
      }
      printTime(t, subset.count(), numIters);
    }

    std::cout << endl;

    // Make sure naive and reduced implementations agree!
    for (ShareID i = subset.first(); subset.isEnd(i) == false; i = subset.next(i)) {
      size_t idx = static_cast<size_t>(i);
      if (n < 512) {
        if (redCoeffs[idx] != rnCoeffs[idx]) {
          LOG_ERROR(THRESHSIGN_LOG, "ReducedAccumCoeff[" << idx << "] != ReducedNaiveCoeff [" << idx << "]");
          LOG_ERROR(THRESHSIGN_LOG, redCoeffs[idx] << " != " << rnCoeffs[idx]);
          throw std::runtime_error("Bad coeff");
        }

        if (redCoeffs[idx] != incrCoeffs[idx]) {
          LOG_ERROR(THRESHSIGN_LOG, "ReducedAccumCoeff[" << idx << "] != IncrAccumCoeff [" << idx << "]");
          LOG_ERROR(THRESHSIGN_LOG, redCoeffs[idx] << " != " << incrCoeffs[idx]);
          throw std::runtime_error("Bad coeff");
        }

        if (redCoeffs[idx] != naiveCoeffs[idx]) {
          LOG_ERROR(THRESHSIGN_LOG, "ReducedAccumCoeff[" << idx << "] != NaiveCoeffs [" << idx << "]");
          LOG_ERROR(THRESHSIGN_LOG, rnCoeffs[idx] << " != " << naiveCoeffs[idx]);
          throw std::runtime_error("Bad coeff");
        }
      }
    }

    //} 100 iterations
  }

#ifdef INCREMENTAL_BENCH
  logperf << "Separate numbers per-coefficient for incremental Lagrange: " << endl;
  int n = 256;
  incrCoeffs.resize(static_cast<size_t>(n + 1), BNT(0));
  LagrangeIncrementalCoeffs lagr(n, params);

  for (ShareID i = 1; i < n; i++) {
    std::stringstream str;
    str << "Time to add signer " << i << ": ";
    ScopedTimer t(std::cout, str.str());
    lagr.addSigner(i);
  }
#endif

  return 0;
}
