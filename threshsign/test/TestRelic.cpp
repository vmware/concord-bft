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
#include "threshsign/bls/relic/BlsThresholdAccumulator.h"  // fastMultExp
#include "threshsign/bls/relic/PublicParametersFactory.h"  // BlsPublicParameters for group order
#include "threshsign/VectorOfShares.h"
#include "app/RelicMain.h"

using namespace BLS::Relic;

template <class GT>
void testAddMult();

void testBNTbits();

void testFastModulo(const BNT& fieldOrder);

template <class GT>
void testMultiExp();

int RelicAppMain(const Library& lib, const std::vector<std::string>& args) {
  (void)lib;
  (void)args;

  BNT fieldOrder = PublicParametersFactory::getWhatever().getGroupOrder();

  int numIters = 100;
  LOG_DEBUG(THRESHSIGN_LOG, "Testing fast modular reduction...");
  for (int i = 0; i < numIters; i++) {
    testFastModulo(fieldOrder);
  }

  LOG_DEBUG(THRESHSIGN_LOG, "Testing accumulated multiplication and addition in G1...");
  for (int i = 0; i < numIters; i++) {
    testAddMult<G1T>();
  }

  LOG_DEBUG(THRESHSIGN_LOG, "Testing accumulated multiplication and addition in G2...");
  for (int i = 0; i < numIters; i++) {
    testAddMult<G2T>();
  }

  LOG_DEBUG(THRESHSIGN_LOG, "Testing RELIC's and BNT's getBit() ...");
  for (int i = 0; i < numIters; i++) {
    testBNTbits();
  }

  // NOTE: Now we are pretending G1 and G2 are groups using multiplicative notation
  LOG_DEBUG(THRESHSIGN_LOG, "Testing fast exponentiated multiplication in G1...");
  for (int i = 0; i < numIters; i++) {
    testMultiExp<G1T>();
  }

  LOG_DEBUG(THRESHSIGN_LOG, "Testing fast exponentiated multiplication in G2...");
  for (int i = 0; i < numIters; i++) {
    testMultiExp<G2T>();
  }

  return 0;
}

void testBNTbits() {
  BNT n(rand() % 544232);
  // BNT n(4);
  int a = 0;
  int bits = n.getBits();
  testAssertLessThanOrEqual(static_cast<size_t>(bits), (sizeof a) * 8);

  // for(int i = bits - 1; i >= 0; i--) {
  for (int i = 0; i < bits; i++) {
    bool bit = n.getBit(i);
    LOG_TRACE(THRESHSIGN_LOG, "Bit " << i << " of " << n << " = " << static_cast<int>(bit));

    if (bit) {
      a += (1 << i);
    }
  }

  LOG_TRACE(THRESHSIGN_LOG, n << " =?= " << a);

  testAssertEqual(static_cast<int>(n.toDigit()), a);
}

template <class GT>
void testAddMult() {
  GT x, y;
  BNT e1, e2;
  BNT order;
  // Recall: G1, G2 and GT all have the same order
  g1_get_ord(order);
  x.Random();
  y.Random();
  e1.RandomMod(order);
  e2.RandomMod(order);

  GT r1, r2, r3;

  r1 = GT::Add(x, y);
  r2 = x;
  r2.Add(y);
  testAssertEqual(r1, r2);

  r1 = GT::Times(x, e1);
  r2 = x;
  r2.Times(e1);
  testAssertEqual(r1, r2);

  r1 = GT::Double(x);
  r2 = x;
  r2.Double();
  r3 = GT::Add(x, x);
  testAssertEqual(r1, r2);
  testAssertEqual(r1, r3);

  r1 = GT::TimesTwice(x, e1, y, e2);
  r2 = GT::Add(GT::Times(x, e1), GT::Times(y, e2));
  testAssertEqual(r1, r2);
}

template <class GT>
void testMultiExp() {
  GT r1, r2, r3;
  int n = 10 + (rand() % 2);
  int k = 5;
  int maxBits = Library::Get().getG2OrderNumBits();
  // int maxBits = 256;
  // LOG_DEBUG(THRESHSIGN_LOG, "Max bits: " << maxBits);

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

  testAssertEqual(r1, GT::Identity());
  testAssertEqual(r2, GT::Identity());
  testAssertEqual(r3, GT::Identity());

  // Fast way
  r1 = fastMultExp<GT>(s, a, e, maxBits);

  // Slow way
  for (ShareID i = s.first(); s.isEnd(i) == false; i = s.next(i)) {
    GT& base = a[static_cast<size_t>(i)];
    BNT& exp = e[static_cast<size_t>(i)];

    GT pow = GT::Times(base, exp);
    r2.Add(pow);
  }

  // Fast way
  r3 = fastMultExpTwo<GT>(s, a, e);

  // Same way?
  testAssertEqual(r1, r2);
  testAssertEqual(r1, r3);
}

void testFastModulo(const BNT& fieldOrder) {
  BNT u = BNT::FastModuloPre(fieldOrder);
  BNT a;
  // Pick random a > fieldOrder so we can reduce it
  a.RandomMod(fieldOrder);
  a.Times(a + BNT(3) + fieldOrder);

  BNT b = a;
  BNT pmers = a, monty = a, barrt = a;

  a.SlowModulo(fieldOrder);
  b.FastModulo(fieldOrder, u);
  testAssertEqual(a, b);

  u = BNT::FastModuloPrePmers(fieldOrder);
  pmers.FastModuloPmers(fieldOrder, u);
  testAssertEqual(a, pmers);

  u = BNT::FastModuloPreMonty(fieldOrder);
  monty.FastModuloMonty(fieldOrder, u);
  testAssertEqual(a, monty);

  u = BNT::FastModuloPreBarrett(fieldOrder);
  barrt.FastModuloBarrett(fieldOrder, u);
  testAssertEqual(a, barrt);
}
