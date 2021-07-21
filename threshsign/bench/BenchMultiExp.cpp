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
#include <stdexcept>

#include "Logger.hpp"
#include "Utils.h"
#include "Timer.h"
#include "XAssert.h"

using namespace std;

#include "threshsign/bls/relic/Library.h"
#include "threshsign/bls/relic/FastMultExp.h"
#include "threshsign/VectorOfShares.h"
#include "app/RelicMain.h"

using namespace BLS::Relic;

template <class GT>
void benchFastMultExp(int numIters, int numSigners, int reqSigners);

int RelicAppMain(const Library& lib, const std::vector<std::string>& args) {
  (void)lib;
  (void)args;

  unsigned int seed = static_cast<unsigned int>(time(NULL));
  LOG_INFO(THRESHSIGN_LOG, "Randomness seed passed to srand(): " << seed);
  // NOTE: srand is not and should not be used for any cryptographic randomness.
  srand(seed);

  LOG_INFO(THRESHSIGN_LOG, "Benchmarking fast exponentiated multiplication in G1...");
  benchFastMultExp<G1T>(100, 1500, 1000);

  LOG_INFO(THRESHSIGN_LOG, "");

  LOG_INFO(THRESHSIGN_LOG, "Benchmarking fast exponentiated multiplication in G2...");
  benchFastMultExp<G2T>(100, 1500, 1000);

  return 0;
}

template <class GT>
void benchFastMultExp(int numIters, int numSigners, int reqSigners) {
  GT r1, r2, r3;
  int n = numSigners + (rand() % 2);
  int k = reqSigners + (rand() % 2);
  assertLessThanOrEqual(reqSigners, numSigners);
  int maxBits = Library::Get().getG2OrderNumBits();
  // int maxBits = 256;
  LOG_INFO(THRESHSIGN_LOG,
           "iters = " << numIters << ", reqSigners = " << reqSigners << ", numSigners = " << numSigners
                      << ", max bits = " << maxBits);

  VectorOfShares s;
  VectorOfShares::randomSubset(s, n, k);

  std::vector<GT> a;
  std::vector<BNT> e;
  a.resize(static_cast<size_t>(n) + 1);
  e.resize(static_cast<size_t>(n) + 1);

  for (ShareID i = s.first(); s.isEnd(i) == false; i = s.next(i)) {
    a[static_cast<size_t>(i)].Random();
    e[static_cast<size_t>(i)].RandomMod(Library::Get().getG2Order());
  }

  assertEqual(r1, GT::Identity());
  assertEqual(r2, GT::Identity());
  assertEqual(r3, GT::Identity());

  // Slow way
  AveragingTimer t1("Naive way:      ");
  for (int i = 0; i < numIters; i++) {
    t1.startLap();
    r2 = GT::Identity();
    for (ShareID i = s.first(); s.isEnd(i) == false; i = s.next(i)) {
      GT& base = a[static_cast<size_t>(i)];
      BNT& exp = e[static_cast<size_t>(i)];

      GT pow = GT::Times(base, exp);
      r2.Add(pow);
    }
    t1.endLap();
  }

  // Fast way
  AveragingTimer t2("fastMultExp:    ");
  for (int i = 0; i < numIters; i++) {
    t2.startLap();
    r1 = fastMultExp<GT>(s, a, e, maxBits);
    t2.endLap();
  }

  // Fast way
  AveragingTimer t3("fastMultExpTwo: ");
  for (int i = 0; i < numIters; i++) {
    t3.startLap();
    r3 = fastMultExpTwo<GT>(s, a, e);
    t3.endLap();
  }

  LOG_INFO(THRESHSIGN_LOG, "Ran for " << numIters << " iterations");
  LOG_INFO(THRESHSIGN_LOG, t1);
  LOG_INFO(THRESHSIGN_LOG, t2);
  LOG_INFO(THRESHSIGN_LOG, t3);

  // Same way?
  if (r1 != r2 || r1 != r3) {
    throw std::runtime_error("Incorrect results returned by one of the implementations.");
  }
}
