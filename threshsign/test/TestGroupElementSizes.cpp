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
#include <inttypes.h>

#include "Logger.hpp"
#include "Utils.h"
#include "Timer.h"
#include "XAssert.h"

using namespace std;

#include "threshsign/bls/relic/Library.h"
#include "app/RelicMain.h"

using namespace BLS::Relic;

int RelicAppMain(const Library& lib, const std::vector<std::string>& args) {
  (void)args;
  (void)lib;

  /**
   * In the BLS::Relic::Library class we rely on the fact that g1_size_bin and g2_size_bin will always
   * return the same size for all points on the EC. Here we make sure that's true.
   */
  int n = 10000;
  for (int i = 0; i < n; i++) {
    if (i % 1000 == 0) {
      LOG_INFO(THRESHSIGN_LOG, i + 1 << " out of " << n);
    }

    G1T g1a, g1b;
    G2T g2a, g2b;
    int lena, lenb;

    g1_rand(g1a);
    g1_rand(g1b);
    testAssertNotEqual(g1a, g1b);

    lena = g1_size_bin(g1a, 1);
    lenb = g1_size_bin(g1b, 1);
    testAssertEqual(lena, lenb);

    g2_rand(g2a);
    g2_rand(g2b);
    testAssertNotEqual(g2a, g2b);

    lena = g2_size_bin(g2a, 1);
    lenb = g2_size_bin(g2b, 1);
    testAssertEqual(lena, lenb);
  }

  return 0;
}
